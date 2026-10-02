import asyncio
import multiprocessing
import os
import threading

import pytest

from loguru import logger


def test_receipt_identifies_synchronous_enqueued_and_failed_sinks():
    started = threading.Event()
    release = threading.Event()
    processed = []

    def good_sink(message):
        processed.append(("good", message.record["extra"]["request_id"]))

    def delayed_sink(message):
        started.set()
        assert release.wait(1)
        processed.append(("delayed", message.record["extra"]["request_id"]))

    def failing_sink(message):
        raise RuntimeError("unavailable: %s" % message.record["extra"]["request_id"])

    good_id = logger.add(good_sink, format="{message}", catch=False)
    delayed_id = logger.add(delayed_sink, format="{message}", enqueue=True, catch=False)
    failing_id = logger.add(failing_sink, format="{message}", catch=False)
    filtered_id = logger.add(failing_sink, format="{message}", filter=lambda _: False)

    try:
        receipt = (
            logger.bind(request_id="request-1").opt(receipt=True).info("Audit record")
        )

        assert started.wait(1)
        with pytest.raises(TimeoutError):
            receipt.wait(0.01)

        release.set()
        result = receipt.wait(1)

        assert [item.handler_id for item in result] == [
            good_id,
            delayed_id,
            failing_id,
            filtered_id,
        ]
        assert [item.handler_id for item in result.succeeded] == [good_id, delayed_id]
        assert [item.handler_id for item in result.filtered] == [filtered_id]
        assert result[failing_id].status == "failed"
        assert str(result[failing_id].error) == "unavailable: request-1"
        assert processed == [("good", "request-1"), ("delayed", "request-1")]
    finally:
        release.set()
        logger.remove()


def test_receipt_waits_for_coroutine_sink_completion():
    started = asyncio.Event()
    release = asyncio.Event()
    completed = []

    async def delayed_coroutine_sink(message):
        started.set()
        await release.wait()
        completed.append(str(message).strip())

    async def failing_coroutine_sink(message):
        raise RuntimeError("coroutine failed")

    delayed_id = logger.add(delayed_coroutine_sink, format="{message}", catch=False)
    failing_id = logger.add(failing_coroutine_sink, format="{message}", catch=False)

    async def main():
        receipt = logger.opt(receipt=True).info("Async audit")
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(receipt, 0.01)

        release.set()
        result = await receipt
        assert result[delayed_id].status == "succeeded"
        assert result[failing_id].status == "failed"
        assert str(result[failing_id].error) == "coroutine failed"

    asyncio.run(main())

    assert completed == ["Async audit"]
    logger.remove()


def test_timeout_does_not_remove_accepted_record():
    accepted = threading.Event()
    release = threading.Event()

    def sink(message):
        accepted.set()
        release.wait(1)

    logger.add(sink, format="{message}", enqueue=True, catch=False)

    try:
        receipt = logger.opt(receipt=True).info("Still accepted")
        with pytest.raises(TimeoutError):
            receipt.wait(0.01)
        assert accepted.wait(1)
        release.set()
        result = receipt.wait(1)
        assert result.results[0].status == "succeeded"
    finally:
        release.set()
        logger.remove()


def test_multiple_enqueued_sinks_are_reported_separately():
    first = threading.Event()
    second = threading.Event()

    def first_sink(message):
        first.wait(1)

    def second_sink(message):
        second.wait(1)

    first_id = logger.add(first_sink, format="{message}", enqueue=True, catch=False)
    second_id = logger.add(second_sink, format="{message}", enqueue=True, catch=False)
    receipt = logger.opt(receipt=True).info("Two queues")

    with pytest.raises(TimeoutError):
        receipt.wait(0.01)

    first.set()
    with pytest.raises(TimeoutError):
        receipt.wait(0.01)

    second.set()
    result = receipt.wait(1)
    assert [item.handler_id for item in result.succeeded] == [first_id, second_id]
    logger.remove()


def test_async_cancellation_does_not_remove_accepted_record():
    started = asyncio.Event()

    async def sink(message):
        started.set()
        await asyncio.sleep(0.02)

    async def runner():
        logger.add(sink, format="{message}", catch=False)
        receipt = logger.opt(receipt=True).info("Still accepted")
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(receipt, 0.001)
        await started.wait()
        result = await receipt
        assert result.results[0].status == "succeeded"

    asyncio.run(runner())
    logger.remove()


def test_receipt_converges_when_handler_removed_after_acceptance():
    started = threading.Event()
    release = threading.Event()
    completed = threading.Event()
    removing = threading.Event()

    def sink(message):
        started.set()
        removing.wait(1)
        release.set()
        completed.set()

    handler_id = logger.add(sink, format="{message}", catch=False)
    receipt = logger.opt(receipt=True).info("Concurrent removal")
    assert started.wait(1)
    remover = threading.Thread(target=lambda: logger.remove(handler_id))
    remover.start()
    removing.set()
    result = receipt.wait(1)
    remover.join(1)
    assert result[handler_id].status == "succeeded"
    assert completed.wait(1)


def test_normal_logging_call_returns_none():
    logger.add(lambda message: None, format="{message}")
    try:
        assert logger.info("No receipt") is None
    finally:
        logger.remove()


_parent_receipt = None
_parent_output_queue = None


def child_sink(message):
    _parent_output_queue.put(str(message))


def child_receipt_worker():
    logger.complete()


@pytest.mark.skipif(os.name == "nt", reason="fork is not available on Windows")
def test_enqueued_receipt_from_forked_child():
    global _parent_receipt
    global _parent_output_queue

    context = multiprocessing.get_context("fork")
    _parent_output_queue = context.Queue()
    handler_id = logger.add(
        child_sink,
        context=context,
        format="{message}",
        enqueue=True,
        catch=False,
    )
    _parent_receipt = logger.opt(receipt=True).info("Child receipt")
    process = context.Process(target=child_receipt_worker)
    process.start()
    result = _parent_receipt.wait(2)
    assert result[handler_id].status == "succeeded"
    assert _parent_output_queue.get(timeout=2) == "Child receipt\n"
    process.join(2)
    assert process.exitcode == 0
    logger.remove(handler_id)

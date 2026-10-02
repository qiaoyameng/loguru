import asyncio
import threading
import time

import pytest

from loguru import logger
from loguru._receipt import LogReceipt, SinkReceipt


def test_receipt_aggregates_selected_sinks():
    processed = []

    def success_sink(message):
        processed.append(("success", str(message)))

    def delayed_sink(message):
        time.sleep(0.1)
        processed.append(("delayed", str(message)))

    failure = ValueError("sink failure")

    def failing_sink(message):
        raise failure

    success_id = logger.add(success_sink, format="{message}")
    delayed_id = logger.add(delayed_sink, format="{message}")
    failing_id = logger.add(failing_sink, format="{message}")

    receipt = logger.log_with_receipt("INFO", "Message {}", 1)
    receipt.wait(timeout=10)

    assert receipt.done
    assert not receipt.ok

    results = {result.handler_id: result for result in receipt.results}
    assert set(results) == {success_id, delayed_id, failing_id}

    assert results[success_id].sink_name == "success_sink"
    assert results[success_id].status == SinkReceipt.SUCCESS
    assert results[success_id].exception is None

    assert results[delayed_id].sink_name == "delayed_sink"
    assert results[delayed_id].status == SinkReceipt.SUCCESS

    assert results[failing_id].sink_name == "failing_sink"
    assert results[failing_id].status == SinkReceipt.FAILED
    assert results[failing_id].exception is failure

    assert {r.handler_id for r in receipt.succeeded} == {success_id, delayed_id}
    assert {r.handler_id for r in receipt.failed} == {failing_id}
    assert processed == [("success", "Message 1\n"), ("delayed", "Message 1\n")]


def test_filtered_sinks_reported_separately(writer):
    level_filtered_id = logger.add(writer, level="WARNING")
    function_filtered_id = logger.add(writer, filter=lambda record: False)
    accepted_id = logger.add(writer, format="{message}")

    receipt = logger.log_with_receipt("INFO", "Message")
    receipt.wait(timeout=10)

    assert receipt.done
    assert receipt.ok

    results = {result.handler_id: result for result in receipt.results}
    assert results[level_filtered_id].status == SinkReceipt.FILTERED
    assert results[function_filtered_id].status == SinkReceipt.FILTERED
    assert results[accepted_id].status == SinkReceipt.SUCCESS

    assert {r.handler_id for r in receipt.filtered} == {level_filtered_id, function_filtered_id}
    assert writer.read() == "Message\n"


def test_enqueue_receipt_reflects_processing_not_enqueueing():
    started = threading.Event()
    release = threading.Event()
    processed = []

    def sink(message):
        started.set()
        release.wait(timeout=10)
        processed.append(str(message))

    handler_id = logger.add(sink, format="{message}", enqueue=True)

    receipt = logger.log_with_receipt("INFO", "Message")
    assert started.wait(timeout=10)

    (result,) = [r for r in receipt.results if r.handler_id == handler_id]
    assert result.status == SinkReceipt.ACCEPTED
    assert not receipt.done
    assert processed == []

    release.set()
    receipt.wait(timeout=10)

    (result,) = [r for r in receipt.results if r.handler_id == handler_id]
    assert result.status == SinkReceipt.SUCCESS
    assert processed == ["Message\n"]


def test_enqueue_sink_failure_reports_original_exception():
    failure = RuntimeError("enqueued failure")

    def sink(message):
        raise failure

    handler_id = logger.add(sink, format="{message}", enqueue=True)

    receipt = logger.log_with_receipt("INFO", "Message")
    receipt.wait(timeout=10)

    (result,) = [r for r in receipt.results if r.handler_id == handler_id]
    assert result.status == SinkReceipt.FAILED
    assert result.exception is failure


def test_coroutine_sink_receipt_success():
    processed = []

    async def sink(message):
        await asyncio.sleep(0.01)
        processed.append(str(message))

    handler_id = logger.add(sink, format="{message}")

    async def main():
        receipt = logger.log_with_receipt("INFO", "Message")
        assert not receipt.done
        await receipt
        return receipt

    receipt = asyncio.run(main())

    (result,) = [r for r in receipt.results if r.handler_id == handler_id]
    assert result.status == SinkReceipt.SUCCESS
    assert result.exception is None
    assert processed == ["Message\n"]


def test_coroutine_sink_receipt_failure():
    failure = RuntimeError("coroutine failure")

    async def sink(message):
        await asyncio.sleep(0.01)
        raise failure

    handler_id = logger.add(sink, format="{message}")

    async def main():
        receipt = logger.log_with_receipt("INFO", "Message")
        await receipt
        return receipt

    receipt = asyncio.run(main())

    (result,) = [r for r in receipt.results if r.handler_id == handler_id]
    assert result.status == SinkReceipt.FAILED
    assert result.exception is failure


def test_wait_timeout_does_not_discard_record():
    release = threading.Event()
    processed = []

    def sink(message):
        release.wait(timeout=10)
        processed.append(str(message))

    logger.add(sink, format="{message}", enqueue=True)

    receipt = logger.log_with_receipt("INFO", "Message")

    with pytest.raises(TimeoutError):
        receipt.wait(timeout=0.1)

    assert not receipt.done

    release.set()
    receipt.wait(timeout=10)

    assert receipt.done
    assert receipt.ok
    assert processed == ["Message\n"]


def test_concurrent_remove_converges():
    started = threading.Event()
    release = threading.Event()
    processed = []

    def sink(message):
        started.set()
        release.wait(timeout=10)
        processed.append(str(message))

    handler_id = logger.add(sink, format="{message}", enqueue=True)

    receipt = logger.log_with_receipt("INFO", "Message")
    assert started.wait(timeout=10)

    removed = threading.Event()

    def remove():
        logger.remove(handler_id)
        removed.set()

    thread = threading.Thread(target=remove)
    thread.start()
    time.sleep(0.1)
    assert not removed.is_set()

    release.set()
    thread.join(timeout=10)
    receipt.wait(timeout=10)

    assert removed.is_set()
    assert receipt.done
    assert receipt.ok
    assert processed == ["Message\n"]


def test_receipt_without_handlers():
    receipt = logger.log_with_receipt("INFO", "Message")
    assert receipt.done
    assert receipt.ok
    assert receipt.results == ()
    receipt.wait(timeout=0)


def test_receipt_below_min_level(writer):
    logger.add(writer, level="WARNING")

    receipt = logger.log_with_receipt("INFO", "Message")

    assert receipt.done
    assert receipt.ok
    assert [r.status for r in receipt.results] == [SinkReceipt.FILTERED]
    assert writer.read() == ""


def test_receipt_with_disabled_module(writer):
    logger.add(writer)
    logger.disable(__name__)

    receipt = logger.log_with_receipt("INFO", "Message")

    assert receipt.done
    assert receipt.ok
    assert [r.status for r in receipt.results] == [SinkReceipt.FILTERED]
    assert writer.read() == ""


def test_receipt_dropped_when_handler_removed_before_dispatch(writer):
    handler_id = logger.add(writer)

    def patcher(record):
        logger.remove(handler_id)

    receipt = logger.patch(patcher).log_with_receipt("INFO", "Message")

    assert receipt.done
    assert receipt.ok
    assert [r.status for r in receipt.results] == [SinkReceipt.DROPPED]
    assert [r.handler_id for r in receipt.dropped] == [handler_id]
    assert writer.read() == ""


def test_receipt_with_arguments_and_options(writer):
    logger.add(writer, format="{message}")

    receipt = logger.bind(key="value").log_with_receipt("INFO", "{} + {}", 1, 2)
    receipt.wait(timeout=10)

    assert receipt.done
    assert receipt.ok
    assert writer.read() == "1 + 2\n"


def test_standard_log_methods_keep_returning_none(writer):
    logger.add(writer, format="{message}")

    assert logger.debug("Message") is None
    assert logger.info("Message") is None
    assert logger.log("INFO", "Message") is None

    assert isinstance(logger.log_with_receipt("INFO", "Message"), LogReceipt)

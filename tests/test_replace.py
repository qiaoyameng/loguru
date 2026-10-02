import asyncio
import itertools
import threading
import time

import pytest

from loguru import logger


class TrackedStream:
    def __init__(self):
        self.messages = []
        self.stopped = False

    def write(self, message):
        self.messages.append(message)

    def flush(self):
        pass

    def stop(self):
        self.stopped = True


def sequences(stream):
    return set(int(message.split("-")[1]) for message in stream.messages)


def test_replace_switches_handlers():
    old = TrackedStream()
    new_1 = TrackedStream()
    new_2 = TrackedStream()

    old_id = logger.add(old, format="{message}")
    logger.info("before")

    ids = logger.replace(
        handlers=[
            dict(sink=new_1, format="{message}"),
            dict(sink=new_2, format="[{message}]"),
        ]
    )

    logger.info("after")

    assert ids == [old_id + 1, old_id + 2]
    assert list(logger._core.handlers) == ids
    assert old.messages == ["before\n"]
    assert new_1.messages == ["after\n"]
    assert new_2.messages == ["[after]\n"]
    assert old.stopped


def test_replace_without_handlers_keeps_them(writer):
    handler_id = logger.add(writer, format="{message}")
    ids = logger.replace(extra={"a": 1})
    assert ids == []
    assert list(logger._core.handlers) == [handler_id]
    logger.info("test")
    assert writer.read() == "test\n"


def test_replace_with_empty_handler_list(writer):
    logger.add(writer, format="{message}")
    ids = logger.replace(handlers=[])
    assert ids == []
    assert logger._core.handlers == {}
    logger.info("nope")
    assert writer.read() == ""


def test_replace_applies_levels_extra_and_patcher():
    new = TrackedStream()
    logger.add(TrackedStream(), format="{message}")

    logger.replace(
        handlers=[dict(sink=new, format="{level.name}:{extra[tag]}:{message}")],
        levels=[dict(name="NOTICE", no=25)],
        extra={"tag": "original"},
        patcher=lambda record: record["extra"].update(tag="patched"),
    )

    logger.log("NOTICE", "hello")
    assert new.messages == ["NOTICE:patched:hello\n"]


def test_replace_activation(writer):
    logger.replace(handlers=[dict(sink=writer, format="{message}")], activation=[("", False)])
    logger.info("nope")
    assert writer.read() == ""
    logger.enable("")
    logger.info("yes")
    assert writer.read() == "yes\n"


def test_replace_failure_keeps_previous_configuration(tmp_path):
    old = TrackedStream()
    prepared = TrackedStream()
    handler_id = logger.add(old, format="{message}")
    logger.replace(levels=[dict(name="NOTICE", no=25)], extra={"tag": "old"})

    with pytest.raises(OSError):
        logger.replace(
            handlers=[
                dict(sink=prepared, format="{message}"),
                dict(sink=tmp_path, format="{message}"),
            ],
            levels=[dict(name="SHOULD_NOT_EXIST", no=30)],
            extra={"tag": "new"},
        )

    # The previous handlers, levels and extra are left untouched.
    assert list(logger._core.handlers) == [handler_id]
    assert not old.stopped
    assert logger.level("NOTICE").no == 25
    with pytest.raises(ValueError, match="does not exist"):
        logger.level("SHOULD_NOT_EXIST")
    assert logger._core.extra == {"tag": "old"}

    # The handler prepared during the failed attempt was cleaned up.
    assert prepared.stopped
    assert prepared.messages == []

    # The previous configuration still works.
    logger.info("still works")
    assert old.messages == ["still works\n"]


def test_replace_failure_with_invalid_handler_params(writer):
    handler_id = logger.add(writer, format="{message}")
    with pytest.raises(ValueError, match="does not exist"):
        logger.replace(handlers=[dict(sink=TrackedStream(), level="UNKNOWN_LEVEL")])
    assert list(logger._core.handlers) == [handler_id]
    logger.info("still works")
    assert writer.read() == "still works\n"


def test_replace_failure_with_invalid_level_params(writer):
    handler_id = logger.add(writer, format="{message}")
    with pytest.raises(TypeError):
        logger.replace(levels=[dict(name="BROKEN", no="not-an-int")])
    assert list(logger._core.handlers) == [handler_id]
    with pytest.raises(ValueError, match="does not exist"):
        logger.level("BROKEN")


def test_replace_flushes_enqueued_records():
    received = []
    logger.add(lambda message: received.append(message), format="{message}", enqueue=True)

    for i in range(100):
        logger.info("seq-{}", i)

    logger.replace(handlers=[dict(sink=TrackedStream(), format="{message}")])

    assert len(received) == 100
    assert sequences(type("S", (), {"messages": received})) == set(range(100))


def test_replace_completes_async_sink_tasks():
    received = []
    loop_ready = threading.Event()
    loop = asyncio.new_event_loop()

    def run_loop():
        asyncio.set_event_loop(loop)
        loop_ready.set()
        # Keep the loop waking up periodically so that it processes callbacks
        # scheduled from other threads even when the self-pipe wakeup of the
        # event loop is unavailable (e.g. restricted sandbox).
        def keep_awake():
            loop.call_later(0.01, keep_awake)

        loop.call_soon(keep_awake)
        loop.run_forever()

    thread = threading.Thread(target=run_loop)
    thread.start()
    loop_ready.wait()

    try:
        async def sink(message):
            await asyncio.sleep(0.05)
            received.append(str(message))

        logger.add(sink, format="{message}", loop=loop)

        for i in range(5):
            logger.info("seq-{}", i)

        logger.replace(handlers=[dict(sink=TrackedStream(), format="{message}")])

        # The tasks scheduled by the old coroutine sink were awaited before it was
        # stopped, so no accepted record was lost.
        assert sorted(received) == ["seq-%d\n" % i for i in range(5)]
    finally:
        loop.call_soon_threadsafe(loop.stop)
        thread.join()
        loop.close()


def test_replace_reports_close_failure_without_rollback():
    class UnclosableStream(TrackedStream):
        def stop(self):
            raise RuntimeError("cannot close")

    old = UnclosableStream()
    new = TrackedStream()
    logger.add(old, format="{message}")

    with pytest.warns(UserWarning, match="Failed to close handler"):
        ids = logger.replace(handlers=[dict(sink=new, format="{message}")])

    # The new configuration is active despite the old sink failing to close.
    assert list(logger._core.handlers) == ids
    logger.info("after")
    assert new.messages == ["after\n"]


def test_replace_waits_for_inflight_records():
    old = TrackedStream()
    new = TrackedStream()
    entered = threading.Event()
    release = threading.Event()

    def blocking_patcher(record):
        # Block the first record while it is being emitted, so that "replace()"
        # is called while a record of the old generation is still inflight.
        if not entered.is_set():
            entered.set()
            release.wait(10)

    logger.add(old, format="{message}")
    logger.configure(patcher=blocking_patcher)

    emitter = threading.Thread(target=lambda: logger.info("inflight"))
    emitter.start()
    assert entered.wait(10)

    replaced = threading.Event()

    def do_replace():
        logger.replace(handlers=[dict(sink=new, format="{message}")])
        replaced.set()

    replacer = threading.Thread(target=do_replace)
    replacer.start()
    time.sleep(0.2)

    # The replacement is committed but waits for the inflight record before
    # stopping the old handler.
    assert not replaced.is_set()

    # New records are dispatched to the new configuration without waiting.
    logger.info("after")
    assert new.messages == ["after\n"]

    release.set()
    replacer.join(10)
    emitter.join(10)

    # The inflight record was fully dispatched to the old generation, it was
    # neither lost nor duplicated to the new one.
    assert replaced.is_set()
    assert old.messages == ["inflight\n"]
    assert new.messages == ["after\n"]


def test_replace_atomic_boundary_under_concurrent_logging(tmp_path):
    old_sinks = [TrackedStream(), TrackedStream()]
    new_sinks = [TrackedStream(), TrackedStream()]

    logger.add(old_sinks[0], format="{message}")
    logger.add(old_sinks[1], format="{message}", enqueue=True)

    counter = itertools.count()
    stop = threading.Event()

    def emit_loop():
        while not stop.is_set():
            logger.info("seq-{}", next(counter))

    threads = [threading.Thread(target=emit_loop) for _ in range(4)]
    for thread in threads:
        thread.start()

    time.sleep(0.05)

    # First attempt: the second new sink cannot be created, so the whole
    # replacement is aborted and the previous configuration keeps running.
    with pytest.raises(OSError):
        logger.replace(
            handlers=[
                dict(sink=new_sinks[0], format="{message}"),
                dict(sink=tmp_path, format="{message}"),
            ]
        )

    # The handler prepared for the first new sink was cleaned up.
    assert new_sinks[0].stopped
    assert new_sinks[0].messages == []

    time.sleep(0.05)

    # Second attempt: the replacement succeeds.
    logger.replace(
        handlers=[
            dict(sink=new_sinks[0], format="{message}"),
            dict(sink=new_sinks[1], format="{message}"),
        ]
    )

    time.sleep(0.05)

    stop.set()
    for thread in threads:
        thread.join()

    total = next(counter)
    old_sequences = [sequences(sink) for sink in old_sinks]
    new_sequences = [sequences(sink) for sink in new_sinks]

    # The boundary was actually crossed in both generations.
    assert old_sequences[0]
    assert new_sequences[0]

    # Each record was dispatched to exactly one generation: both sinks of the
    # same generation saw the same records, and no record was dispatched to
    # both generations (no duplicate dispatch).
    assert old_sequences[0] == old_sequences[1]
    assert new_sequences[0] == new_sequences[1]
    assert old_sequences[0].isdisjoint(new_sequences[0])

    # No record was lost at the failure and switch boundaries (no gap).
    assert old_sequences[0] | new_sequences[0] == set(range(total))

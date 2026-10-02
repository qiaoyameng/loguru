import asyncio
import threading

import pytest

from loguru import logger


class Sink:
    def __init__(self, name):
        self.name = name
        self.messages = []
        self.stopped = False
        self.stop_error = None

    def write(self, message):
        self.messages.append(str(message).strip())

    def stop(self):
        self.stopped = True
        if self.stop_error is not None:
            error = self.stop_error
            self.stop_error = None
            raise error


def parse(messages):
    records = []
    for message in messages:
        sink, number = message.split(":")
        records.append((sink, int(number)))
    return records


def test_failed_reconfigure_rolls_back_new_handlers_and_keeps_old_configuration(tmp_path):
    old_sink = Sink("old")
    first_new_sink = Sink("new-1")

    logger.reconfigure(
        handlers=[{"sink": old_sink, "format": "{message}"}],
        levels=[{"name": "ROLLBACK_LEVEL", "no": 31}],
        extra={"generation": "old"},
    )
    old_ids = list(logger._core.handlers)
    failing_path = tmp_path / "new-2"
    failing_path.mkdir()

    with pytest.raises(IsADirectoryError):
        logger.reconfigure(
            handlers=[
                {"sink": first_new_sink, "format": "{message}"},
                {"sink": failing_path, "format": "{message}"},
            ],
            levels=[{"name": "SHOULD_NOT_EXIST", "no": 32}],
            extra={"generation": "new"},
        )

    assert list(logger._core.handlers) == old_ids
    assert logger._core.handlers[old_ids[0]]._sink._stream is old_sink
    assert "SHOULD_NOT_EXIST" not in logger._core.levels
    assert "ROLLBACK_LEVEL" in logger._core.levels
    assert logger._core.extra == {"generation": "old"}
    assert first_new_sink.stopped
    assert not old_sink.stopped

    logger.info("after-rollback")
    assert old_sink.messages == ["after-rollback"]
    assert first_new_sink.messages == []

    failing_path.rmdir()
    logger.reconfigure(
        handlers=[{"sink": failing_path, "format": "{message}"}],
        extra={"generation": "new"},
    )
    logger.info("after-success")
    assert failing_path.read_text() == "after-success\n"
    logger.remove()


def test_reconfigure_has_generation_boundary_for_concurrent_records(tmp_path):
    old_sink = Sink("old")
    new_sink = Sink("new")
    prepared = threading.Event()
    release = threading.Event()

    class BlockingSink:
        def write(self, message):
            prepared.set()
            release.wait(5)

        def stop(self):
            pass

    logger.reconfigure(
        handlers=[{"sink": BlockingSink(), "format": "{message}", "catch": False}],
        extra={"sink": "blocking"},
    )

    blocked = threading.Thread(
        target=logger.info, args=("blocked",), kwargs={"catch": False}, daemon=True
    )
    blocked.start()
    assert prepared.wait(5)
    failing_path = tmp_path / "new"
    failing_path.mkdir()

    with pytest.raises(IsADirectoryError):
        logger.reconfigure(
            handlers=[
                {"sink": failing_path, "format": "{message}"},
            ],
            extra={"sink": "new-failing"},
        )
    failing_path.rmdir()

    successful = threading.Thread(
        target=logger.reconfigure,
        kwargs={
            "handlers": [{"sink": failing_path, "format": "{message}"}],
            "extra": {"sink": "new"},
        },
        daemon=True,
    )
    successful.start()
    release.set()
    blocked.join(5)
    successful.join(5)

    assert not blocked.is_alive()
    assert not successful.is_alive()
    logger.reconfigure(
        handlers=[{"sink": old_sink, "format": "{extra[sink]}:{message}"}],
        extra={"sink": "old"},
    )
    logger.info("after-old")
    logger.reconfigure(
        handlers=[{"sink": new_sink, "format": "{extra[sink]}:{message}"}],
        extra={"sink": "new"},
    )
    logger.info("after-new")

    assert old_sink.messages == ["old:after-old"]
    assert new_sink.messages == ["new:after-new"]


def test_reconfigure_drains_old_enqueued_sink_before_close(tmp_path):
    old_file = tmp_path / "old.log"
    new_file = tmp_path / "new.log"

    ids = logger.reconfigure(
        handlers=[{"sink": old_file, "format": "{message}", "enqueue": True}]
    )

    for number in range(100):
        logger.info("old:{}", number)

    retirement = logger.reconfigure(
        handlers=[{"sink": new_file, "format": "{message}", "enqueue": True}]
    )
    for number in range(100):
        logger.info("new:{}", number)

    assert len(ids) == 1
    old_records = parse(line.strip() for line in old_file.read_text().splitlines())
    assert old_records == [("old", number) for number in range(100)]
    logger.remove()
    new_records = parse(line.strip() for line in new_file.read_text().splitlines())
    assert new_records == [("new", number) for number in range(100)]
    assert len(retirement) == 1
    assert retirement[0] == ids[0] + 1


def test_reconfigure_reports_close_error_after_commit():
    old_sink = Sink("old")
    old_sink.stop_error = OSError("close failed")
    new_sink = Sink("new")

    logger.reconfigure(handlers=[{"sink": old_sink, "format": "{message}"}])

    with pytest.raises(OSError, match="close failed"):
        logger.reconfigure(handlers=[{"sink": new_sink, "format": "{message}"}])

    logger.info("committed")
    assert old_sink.messages == []
    assert new_sink.messages == ["committed"]


def test_reconfigure_awaits_accepted_async_sink_records():
    sink = Sink("async-old")

    async def old_sink_write(message):
        await asyncio.sleep(0.01)
        sink.write(message)

    async def workflow():
        logger.reconfigure(handlers=[{"sink": old_sink_write, "format": "{message}"}])
        logger.info("accepted")
        retirement = logger.reconfigure(
            handlers=[{"sink": lambda message: None, "format": "{message}"}]
        )
        await retirement

    asyncio.run(workflow())
    assert sink.messages == ["accepted"]


def test_reconfigure_continuous_numbered_logs_have_no_gap_or_duplicate(tmp_path):
    old_sink = Sink("old")
    new_sink = Sink("new")
    blocked_path = tmp_path / "blocked"
    blocked_path.mkdir()
    writer_started = threading.Event()
    allow_success = threading.Event()
    total = 200

    def emit_numbers():
        writer_started.set()
        allow_success.wait(5)
        for number in range(total):
            logger.info("{}", number)

    writer = threading.Thread(target=emit_numbers, daemon=True)
    logger.reconfigure(
        handlers=[{"sink": old_sink, "format": "{message}"}],
        extra={"generation": "old"},
    )
    writer.start()
    assert writer_started.wait(5)

    with pytest.raises(IsADirectoryError):
        logger.reconfigure(
            handlers=[{"sink": blocked_path, "format": "{message}"}],
            extra={"generation": "failed"},
        )

    def switch_to_new_generation():
        allow_success.set()
        logger.reconfigure(
            handlers=[{"sink": new_sink, "format": "{message}"}],
            extra={"generation": "new"},
        )

    switch = threading.Thread(target=switch_to_new_generation, daemon=True)
    switch.start()
    writer.join(5)
    switch.join(5)
    assert not writer.is_alive()
    assert not switch.is_alive()

    old_numbers = list(map(int, old_sink.messages))
    new_numbers = list(map(int, new_sink.messages))
    assert old_numbers + new_numbers == list(range(total))
    assert sorted(old_numbers + new_numbers) == list(range(total))
    assert len(set(old_numbers) & set(new_numbers)) == 0


def test_reconfigure_reports_async_sink_close_error_after_commit():
    written = []

    async def sink(message):
        written.append(str(message).strip())

    sink.stop = lambda: (_ for _ in ()).throw(OSError("async close failed"))

    async def workflow():
        logger.reconfigure(handlers=[{"sink": sink, "format": "{message}"}])
        logger.info("accepted")
        retirement = logger.reconfigure(handlers=[])
        with pytest.raises(OSError, match="async close failed"):
            await retirement

    asyncio.run(workflow())
    assert written == ["accepted"]
    assert not logger._core.handlers

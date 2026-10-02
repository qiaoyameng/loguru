import time
import threading
from datetime import datetime, timedelta, timezone

from loguru import RepeatSuppression, logger


class FakeClock:
    def __init__(self):
        self.current = datetime(2020, 1, 1, tzinfo=timezone.utc)

    def now(self):
        return self.current

    def advance(self, seconds):
        self.current += timedelta(seconds=seconds)


def exception(message, exception_type):
    try:
        raise exception_type(message)
    except exception_type:
        logger.exception(message)


def test_repeated_records_summarized_and_flushed_on_remove():
    messages = []
    unsuppressed = []
    clock = FakeClock()
    suppression = RepeatSuppression(timedelta(seconds=10), clock=clock.now)

    handler_id = logger.add(
        messages.append,
        format=lambda record: "{level.name}: {message}\n",
        catch=False,
        suppression=suppression,
    )
    other_id = logger.add(
        unsuppressed.append,
        format=lambda record: "{level.name}: {message}\n",
        catch=False,
    )

    exception("first failure", ValueError)
    clock.advance(1)
    exception("second failure", KeyError)

    clock.advance(1)
    exception("first failure", ValueError)
    exception("second failure", KeyError)

    assert [message.rstrip() for message in messages] == [
        "ERROR: first failure",
        "ERROR: second failure",
    ]

    clock.advance(8)
    exception("first failure", ValueError)

    assert len(messages) == 4
    assert messages[2].record["extra"]["suppression_count"] == 1
    assert "1 repeated log message suppressed between" in messages[2]
    assert "2020-01-01T00:00:02" in messages[2]
    assert messages[3].rstrip() == "ERROR: first failure"

    logger.remove(handler_id)

    assert len(messages) == 5
    assert messages[3].rstrip() == "ERROR: first failure"
    assert messages[4].record["extra"]["suppression_summary"] is True
    assert messages[4].record["extra"]["suppression_count"] == 1
    assert "second failure" in messages[4]

    logger.remove(other_id)

    assert len(unsuppressed) == 5
    assert all(not message.record["extra"].get("suppression_summary") for message in unsuppressed)


def test_level_change_and_different_exception_values_use_separate_groups():
    groups = set()

    def fingerprint(record):
        exception = None if record["exception"] is None else record["exception"].value.args
        key = (record["level"].name, record["message"], exception)
        groups.add(key)
        return key

    messages = []
    clock = FakeClock()
    suppression = RepeatSuppression(
        timedelta(seconds=10),
        fingerprint=fingerprint,
        clock=clock.now,
    )
    handler_id = logger.add(
        messages.append,
        format=lambda record: "{message}\n",
        catch=False,
        suppression=suppression,
    )

    logger.error("same message")
    logger.critical("same message")
    exception("failure", ValueError)
    exception("different", ValueError)

    assert [message.rstrip() for message in messages] == [
        "same message",
        "same message",
        "failure",
        "different",
    ]
    assert len(groups) == 4

    logger.remove(handler_id)


def test_same_exception_with_different_traceback_is_grouped():
    messages = []
    clock = FakeClock()
    suppression = RepeatSuppression(timedelta(seconds=10), clock=clock.now)
    handler_id = logger.add(
        messages.append,
        format="{message}",
        catch=False,
        suppression=suppression,
    )

    exception("traceback failure", RuntimeError)
    exception("traceback failure", RuntimeError)
    logger.remove(handler_id)

    assert len(messages) == 2
    assert messages[1].record["extra"]["suppression_count"] == 1


def test_same_policy_configuration_creates_independent_sink_state():
    first = []
    second = []
    policy = RepeatSuppression(
        timedelta(seconds=10),
        fingerprint=lambda record: record["message"],
    )

    first_id = logger.add(first.append, format="{message}", catch=False, suppression=policy)
    second_id = logger.add(second.append, format="{message}", catch=False, suppression=policy)

    logger.info("shared policy")
    logger.info("shared policy")

    assert len(first) == 1
    assert len(second) == 1

    logger.remove(first_id)
    logger.remove(second_id)

    assert len(first) == 2
    assert len(second) == 2


def test_fingerprint_receives_read_only_record():
    messages = []
    seen = []
    mutation_errors = []

    def fingerprint(record):
        seen.append(record)
        try:
            record["message"] = "mutated"
        except TypeError as error:
            mutation_errors.append(error)
        try:
            record["extra"]["key"] = "mutated"
        except TypeError as error:
            mutation_errors.append(error)
        return record["message"]

    handler_id = logger.add(
        messages.append,
        format="{message}",
        catch=False,
        suppression=RepeatSuppression(
            timedelta(seconds=10),
            fingerprint=fingerprint,
        ),
    )

    logger.info("Read only")
    logger.remove(handler_id)

    assert messages == ["Read only\n"]
    assert seen
    assert len(mutation_errors) == 2


def test_eviction_flushes_unreported_count():
    messages = []
    clock = FakeClock()
    suppression = RepeatSuppression(
        timedelta(seconds=10),
        max_groups=1,
        fingerprint=lambda record: record["message"],
        clock=clock.now,
    )
    handler_id = logger.add(
        messages.append,
        format="{message}",
        catch=False,
        suppression=suppression,
    )

    logger.info("first")
    clock.advance(1)
    logger.info("first")
    clock.advance(1)
    logger.info("second")

    assert len(messages) == 3
    assert messages[1].record["extra"]["suppression_count"] == 1
    assert "first" in messages[1]
    assert messages[2].rstrip() == "second"

    logger.remove(handler_id)


def test_enqueue_groups_on_sender_before_consumer():
    consumed = []
    first_written = threading.Event()
    release_sink = threading.Event()

    def sink(message):
        if first_written.is_set() is False:
            first_written.set()
            release_sink.wait(1)
        consumed.append(message)

    clock = FakeClock()
    suppression = RepeatSuppression(
        timedelta(seconds=10),
        fingerprint=lambda record: record["message"],
        clock=clock.now,
    )
    handler_id = logger.add(
        sink,
        format="{message}",
        enqueue=True,
        catch=False,
        suppression=suppression,
    )

    logger.error("queued failure")
    first_written.wait(1)
    clock.advance(1)
    logger.error("queued failure")
    clock.advance(11)
    logger.error("queued failure")
    release_sink.set()
    logger.complete()

    assert consumed[0].rstrip() == "queued failure"
    assert "1 repeated log message suppressed between 2020-01-01T00:00:01" in consumed[1]
    assert consumed[2].rstrip() == "queued failure"

    logger.remove(handler_id)
    assert len(consumed) == 3
    assert "1 repeated log message suppressed" in consumed[1]

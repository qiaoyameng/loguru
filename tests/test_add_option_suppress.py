import re
from datetime import timedelta

import pytest

from loguru import SuppressionPolicy, logger


class FakeClock:
    def __init__(self, start=1000.0):
        self.now = start

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


def make_policy(clock, **kwargs):
    kwargs.setdefault("window", 10)
    kwargs.setdefault("clock", clock)
    return SuppressionPolicy(**kwargs)


def test_interleaved_groups_summary_and_close_report(writer):
    clock = FakeClock()
    raw = []
    logger.add(writer, format="{message}", suppress=make_policy(clock))
    logger.add(lambda message: raw.append(str(message)), format="{message}")

    logger.error("boom A")
    logger.error("boom B")
    logger.error("boom A")
    logger.error("boom B")
    logger.error("boom A")

    # First occurrence of each group is emitted, duplicates are counted.
    assert writer.read() == "boom A\nboom B\n"

    clock.advance(11)
    logger.error("boom A")

    out = writer.read()
    lines = out.splitlines()
    assert lines[0] == "boom A"
    assert lines[1] == "boom B"
    # Window expired: one summary per group (least recently used first),
    # then the new first occurrence.
    assert re.match(
        r"Suppressed 1 duplicate log\(s\) from .+ to .+ \[level=ERROR, message='boom B'\]",
        lines[2],
    )
    assert re.match(
        r"Suppressed 2 duplicate log\(s\) from .+ to .+ \[level=ERROR, message='boom A'\]",
        lines[3],
    )
    assert lines[4] == "boom A"

    logger.error("boom A")
    logger.error("boom A")

    # Closing the sink reports the count still pending in the open window.
    logger.remove()
    out = writer.read()
    lines = out.splitlines()
    assert re.match(
        r"Suppressed 2 duplicate log\(s\) from .+ to .+ \[level=ERROR, message='boom A'\]",
        lines[-1],
    )

    # The sink without suppression policy received every raw record.
    assert raw == [
        "boom A\n",
        "boom B\n",
        "boom A\n",
        "boom B\n",
        "boom A\n",
        "boom A\n",
        "boom A\n",
        "boom A\n",
    ]


def test_summary_contains_time_range(writer, freeze_time):
    clock = FakeClock()
    with freeze_time("2026-10-02 10:00:00"):
        logger.add(writer, format="{message}", suppress=make_policy(clock))
        logger.error("stuck")
        logger.error("stuck")
        logger.remove()
    out = writer.read()
    assert "Suppressed 1 duplicate log(s) from 2026-10-02T10:00:00" in out
    assert " to 2026-10-02T10:00:00" in out


def test_level_change_starts_new_group(writer):
    clock = FakeClock()
    logger.add(writer, format="{message}", suppress=make_policy(clock))

    logger.error("same text")
    logger.warning("same text")
    logger.warning("same text")
    logger.error("same text")

    assert writer.read() == "same text\nsame text\n"

    logger.remove()
    out = writer.read()
    assert "Suppressed 1 duplicate log(s)" in out
    assert "level=WARNING" in out
    assert "level=ERROR" in out


def test_different_exception_types_not_grouped(writer):
    clock = FakeClock()
    logger.add(writer, format="{message}", suppress=make_policy(clock))

    for exc_type in (ValueError, KeyError, ValueError):
        try:
            raise exc_type("nope")
        except exc_type:
            logger.exception("failed")

    # ValueError and KeyError are distinct groups, the second ValueError is suppressed.
    assert writer.read().count("failed\n") == 2

    logger.remove()
    assert "Suppressed 1 duplicate log(s)" in writer.read()


def test_same_exception_type_grouped_despite_different_values(writer):
    clock = FakeClock()
    logger.add(writer, format="{message}", suppress=make_policy(clock))

    for value in ("a", "b", "c"):
        try:
            raise ValueError(value)
        except ValueError:
            logger.exception("failed")

    assert writer.read().count("failed\n") == 1

    logger.remove()
    assert "Suppressed 2 duplicate log(s)" in writer.read()


def test_custom_fingerprint(writer):
    clock = FakeClock()
    policy = make_policy(clock, fingerprint=lambda record: record["extra"].get("tenant"))
    logger.add(writer, format="{message}", suppress=policy)

    logger.bind(tenant="acme").error("first problem")
    logger.bind(tenant="acme").error("second problem")
    logger.bind(tenant="globex").error("second problem")

    assert writer.read() == "first problem\nsecond problem\n"

    logger.remove()
    assert "Suppressed 1 duplicate log(s)" in writer.read()


def test_max_groups_eviction_reports_pending_count(writer):
    clock = FakeClock()
    logger.add(writer, format="{message}", suppress=make_policy(clock, max_groups=2))

    logger.error("a")
    logger.error("b")
    logger.error("b")
    logger.error("c")  # Evicts "a" (no pending count, no summary).
    logger.error("a")  # Evicts "b", its pending count is reported immediately.

    out = writer.read()
    lines = out.splitlines()
    assert lines[0] == "a"
    assert lines[1] == "b"
    assert lines[2] == "c"
    assert re.match(
        r"Suppressed 1 duplicate log\(s\) from .+ to .+ \[level=ERROR, message='b'\]",
        lines[3],
    )
    assert lines[4] == "a"


def test_window_as_duration_string_and_timedelta(writer):
    clock = FakeClock()
    logger.add(writer, format="{message}", suppress={"window": "10s", "clock": clock})
    logger.error("x")
    logger.error("x")
    clock.advance(11)
    logger.error("x")
    assert writer.read().count("x\n") == 2
    logger.remove()

    logger.add(writer, format="{message}", suppress=timedelta(seconds=5))
    logger.error("y")
    logger.error("y")
    logger.remove()
    assert "Suppressed 1 duplicate log(s)" in writer.read()


def test_window_number_shorthand(writer):
    logger.add(writer, format="{message}", suppress=60)
    logger.error("only")
    logger.error("only")
    logger.remove()
    out = writer.read()
    assert out.startswith("only\n")
    assert "Suppressed 1 duplicate log(s)" in out


def test_suppress_with_enqueue(writer):
    clock = FakeClock()
    logger.add(writer, format="{message}", enqueue=True, suppress=make_policy(clock))

    logger.error("dup")
    logger.error("dup")
    logger.error("dup")
    logger.remove()

    out = writer.read()
    assert out.count("dup\n") == 1
    assert "Suppressed 2 duplicate log(s)" in out


def test_summary_not_recursively_suppressed(writer):
    clock = FakeClock()
    logger.add(writer, format="{message}", suppress=make_policy(clock))

    logger.error("loop")
    logger.error("loop")
    clock.advance(11)
    # Triggers the summary of "loop", then starts a new window.
    logger.error("loop")
    logger.error("loop")
    clock.advance(11)
    logger.error("loop")

    out = writer.read()
    assert out.count("Suppressed 1 duplicate log(s)") == 2
    assert out.count("loop\n") == 3


def test_no_suppression_by_default(writer):
    logger.add(writer, format="{message}")
    logger.error("plain")
    logger.error("plain")
    assert writer.read() == "plain\nplain\n"


def test_suppressed_records_not_retained():
    clock = FakeClock()
    policy = make_policy(clock)
    logger.add(lambda message: None, format="{message}", suppress=policy)
    handler = next(iter(logger._core.handlers.values()))
    for i in range(1000):
        logger.error("flood %d", 0)
    (group,) = handler._suppressor._groups.values()
    assert group.suppressed == 999
    logger.remove()


@pytest.mark.parametrize("suppress", [object(), [1, 2], (x for x in range(3))])
def test_invalid_suppress_type(writer, suppress):
    with pytest.raises(TypeError, match="Invalid suppress"):
        logger.add(writer, suppress=suppress)


@pytest.mark.parametrize("window", [0, -1, -0.5, timedelta(0)])
def test_invalid_window_value(window):
    with pytest.raises(ValueError, match="Invalid suppress window"):
        SuppressionPolicy(window=window)


@pytest.mark.parametrize("window", [None, object(), True, "not a duration"])
def test_invalid_window_type(window):
    with pytest.raises((TypeError, ValueError), match="Invalid suppress window"):
        SuppressionPolicy(window=window)


@pytest.mark.parametrize("max_groups", [0, -1])
def test_invalid_max_groups(max_groups):
    with pytest.raises(ValueError, match="Invalid suppress max_groups"):
        SuppressionPolicy(window=1, max_groups=max_groups)


@pytest.mark.parametrize("option", ["fingerprint", "clock", "summary_format"])
def test_invalid_callable_options(option):
    with pytest.raises(TypeError, match="Invalid suppress %s" % option):
        SuppressionPolicy(window=1, **{option: 42})

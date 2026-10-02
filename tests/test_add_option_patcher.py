import json

import pytest

from loguru import logger


def test_patcher_modifies_extra(writer):
    logger.add(writer, format="{extra[a]} {message}", patcher=lambda r: r["extra"].update(a=1))
    logger.debug("A")
    assert writer.read() == "1 A\n"


def test_patcher_modifies_message(writer):
    def patcher(record):
        record["message"] = record["message"].upper()

    logger.add(writer, format="{message}", patcher=patcher)
    logger.debug("abc")
    assert writer.read() == "ABC\n"


def test_patcher_modifies_nested_extra(writer):
    def patcher(record):
        record["extra"]["data"]["x"] = "patched"

    logger.add(writer, format="{extra[data][x]} {message}", patcher=patcher)
    logger.bind(data={"x": "original"}).debug("A")
    assert writer.read() == "patched A\n"


def test_patcher_isolated_between_handlers():
    def patcher(record):
        record["extra"]["data"]["x"] = "patched"
        record["message"] = "modified"

    patched = []
    other = []

    def patched_sink(message):
        patched.append(message)

    def other_sink(message):
        other.append(message)

    logger.add(patched_sink, format="{extra[data][x]} {message}", patcher=patcher)
    logger.add(other_sink, format="{extra[data][x]} {message}")

    logger.bind(data={"x": "original"}).debug("A")

    assert "".join(patched) == "patched modified\n"
    assert "".join(other) == "original A\n"


def test_patcher_does_not_mutate_bound_object(writer):
    data = {"nested": {"x": "original"}}

    def patcher(record):
        record["extra"]["data"]["nested"]["x"] = "patched"

    logger.add(writer, format="{message}", patcher=patcher)
    logger.bind(data=data).debug("A")

    assert writer.read() == "A\n"
    assert data == {"nested": {"x": "original"}}


def test_patcher_does_not_mutate_contextualized_object(writer):
    data = {"nested": {"x": "original"}}

    def patcher(record):
        record["extra"]["data"]["nested"]["x"] = "patched"

    logger.add(writer, format="{message}", patcher=patcher)
    with logger.contextualize(data=data):
        logger.debug("A")

    assert writer.read() == "A\n"
    assert data == {"nested": {"x": "original"}}


def test_failing_patcher_does_not_affect_other_handler(writer, capsys):
    data = {"nested": {"x": "original"}}
    other = []

    def other_sink(message):
        other.append(message)

    def bad_patcher(record):
        record["extra"]["data"]["nested"]["x"] = "patched"
        record["message"] = "modified"
        raise RuntimeError("patcher failed")

    logger.add(writer, format="{extra[data][nested][x]} {message}", patcher=bad_patcher)
    logger.add(other_sink, format="{extra[data][nested][x]} {message}")

    logger.bind(data=data).debug("A")

    out, err = capsys.readouterr()
    assert writer.read() == ""
    assert "".join(other) == "original A\n"
    assert data == {"nested": {"x": "original"}}
    assert out == ""
    assert "--- Logging error in Loguru Handler #0 ---" in err
    assert "RuntimeError: patcher failed" in err


def test_failing_patcher_not_caught(writer):
    def bad_patcher(record):
        raise ValueError("patcher failed")

    logger.add(writer, format="{message}", patcher=bad_patcher, catch=False)

    with pytest.raises(ValueError, match="patcher failed"):
        logger.debug("A")

    assert writer.read() == ""


def test_patcher_applied_before_serialize(writer):
    def patcher(record):
        record["extra"]["a"] = 42
        record["message"] = "patched"

    logger.add(writer, format="{message}", serialize=True, patcher=patcher)
    logger.debug("A")

    serialized = json.loads(writer.read())
    assert serialized["text"] == "patched\n"
    assert serialized["record"]["message"] == "patched"
    assert serialized["record"]["extra"] == {"a": 42}


def test_patcher_applied_before_dynamic_format(writer):
    seen = []

    def dynamic_format(record):
        seen.append(record["extra"].get("a"))
        return "{extra[a]} {message}\n"

    logger.add(writer, format=dynamic_format, patcher=lambda r: r["extra"].update(a=7))
    logger.debug("A")

    assert writer.read() == "7 A\n"
    assert seen == [7]


def test_patcher_applied_before_enqueue(writer):
    records = []

    def sink(message):
        records.append(message.record["extra"]["a"])

    logger.add(sink, format="{message}", enqueue=True, patcher=lambda r: r["extra"].update(a=5))
    logger.debug("A")
    logger.complete()

    assert records == [5]


def test_filter_applied_before_patcher(writer):
    seen = []

    def filter_(record):
        seen.append("extra" in record and record["extra"].get("a"))
        return True

    logger.add(writer, format="{message}", filter=filter_, patcher=lambda r: r["extra"].update(a=1))
    logger.debug("A")

    assert writer.read() == "A\n"
    assert seen == [None]


def test_patcher_none_by_default(writer):
    logger.add(writer, format="{message}")
    logger.debug("A")
    assert writer.read() == "A\n"


def test_invalid_patcher(writer):
    with pytest.raises(TypeError, match="Invalid patcher, it should be a function"):
        logger.add(writer, patcher=42)


def test_patcher_shared_record_values_are_not_copied(writer):
    shared = []

    def sink(message):
        pass

    def patcher(record):
        shared.append((record["exception"], record["file"], record["thread"]))

    logger.add(sink, format="{message}", patcher=patcher)
    logger.debug("A")

    (exception, file, thread), = shared
    assert exception is None
    assert file.name.endswith(".py")
    assert isinstance(thread.name, str)

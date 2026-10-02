import json

import pytest

from loguru import logger


class NotCopyable:
    def __deepcopy__(self, memo):
        raise AssertionError("This object must not be copied")


def read(messages):
    return [str(message) for message in messages], [message.record for message in messages]


def test_patcher_isolation_and_caught_failure(capsys):
    source = {"value": "source", "items": [{"name": "nested"}]}
    context_source = {"value": "context source"}
    first_messages = []
    second_messages = []

    def failing_patcher(record):
        record["message"] = "changed"
        record["extra"]["bind_payload"]["value"] = "changed"
        record["extra"]["bind_payload"]["items"][0]["name"] = "changed nested"
        record["extra"]["context_payload"]["value"] = "changed context"
        raise RuntimeError("patcher failed")

    logger.add(
        first_messages.append,
        format="{message}",
        patcher=failing_patcher,
        catch=True,
    )
    logger.add(second_messages.append, format="{message}")

    with logger.contextualize(context_payload=context_source):
        logger.bind(bind_payload=source).info("original")

    first_output, first_records = read(first_messages)
    second_output, second_records = read(second_messages)

    assert first_output == []
    assert first_records == []
    assert second_output == ["original\n"]
    assert second_records[0]["extra"]["bind_payload"] is source
    assert second_records[0]["extra"]["context_payload"] is context_source
    assert source == {"value": "source", "items": [{"name": "nested"}]}
    assert context_source == {"value": "context source"}
    error = capsys.readouterr().err
    assert "'message': 'changed'" in error
    assert "RuntimeError: patcher failed" in error


def test_successful_patcher_does_not_affect_other_handler_or_source():
    bind_source = {"value": "source", "items": [{"name": "nested"}]}
    contextualized_source = {"value": "context source"}
    first_messages = []
    second_messages = []

    def patcher(record):
        record["message"] = "changed"
        record["extra"]["bind_payload"]["value"] = "changed"
        record["extra"]["bind_payload"]["items"][0]["name"] = "changed nested"
        record["extra"]["context_payload"]["value"] = "changed context"

    logger.add(
        first_messages.append,
        format=lambda record: "{message} {extra[bind_payload][value]}\n",
        patcher=patcher,
        catch=False,
    )
    logger.add(second_messages.append, format="{message}", catch=False)

    with logger.contextualize(context_payload=contextualized_source):
        logger.bind(bind_payload=bind_source).info("original")

    first_output, first_records = read(first_messages)
    second_output, second_records = read(second_messages)

    assert first_output == ["changed changed\n"]
    assert first_records[0]["message"] == "changed"
    assert first_records[0]["extra"]["bind_payload"] == {
        "value": "changed",
        "items": [{"name": "changed nested"}],
    }
    assert first_records[0]["extra"]["context_payload"] == {"value": "changed context"}
    assert first_records[0]["extra"]["bind_payload"] is not bind_source
    assert second_output == ["original\n"]
    assert second_records[0]["extra"]["bind_payload"] is bind_source
    assert second_records[0]["extra"]["context_payload"] is contextualized_source
    assert bind_source == {"value": "source", "items": [{"name": "nested"}]}
    assert contextualized_source == {"value": "context source"}


def test_patcher_does_not_copy_contextualized_container_object():
    source = NotCopyable()
    messages = []

    def patcher(record):
        assert record["extra"]["object"] is source

    with logger.contextualize(object=source):
        logger.add(messages.append, format="{message}", patcher=patcher, catch=False)
        logger.info("original")

    assert messages[0].record["extra"]["object"] is source


def test_patcher_keeps_runtime_record_objects_shared():
    source = NotCopyable()
    first_messages = []
    second_messages = []

    logger.add(first_messages.append, format="{message}", catch=False)
    logger.add(
        second_messages.append,
        format="{message}",
        patcher=lambda record: None,
        catch=False,
    )

    try:
        raise ValueError("failure")
    except ValueError:
        with logger.contextualize(object=source):
            logger.opt(exception=True).error("original")

    _, first_records = read(first_messages)
    _, second_records = read(second_messages)

    assert first_records[0]["exception"] is second_records[0]["exception"]
    assert first_records[0]["file"] is second_records[0]["file"]
    assert first_records[0]["thread"] is second_records[0]["thread"]
    assert second_records[0]["extra"]["object"] is source


def test_patcher_copies_mutable_bytearray_extra():
    source = bytearray(b"source")
    messages = []

    def patcher(record):
        record["extra"]["data"][:] = b"changed"

    logger.add(messages.append, format="{message}", patcher=patcher, catch=False)
    logger.bind(data=source).info("original")

    assert messages[0].record["extra"]["data"] == bytearray(b"changed")
    assert source == bytearray(b"source")


def test_patcher_runs_before_serialization():
    source = {"value": "source"}
    messages = []

    def patcher(record):
        record["message"] = "changed"
        record["extra"]["payload"]["value"] = "changed"

    logger.add(messages.append, format="{message}", serialize=True, patcher=patcher, catch=False)
    logger.bind(payload=source).info("original")

    data = json.loads(str(messages[0]))

    assert data["text"] == "changed\n"
    assert data["record"]["message"] == "changed"
    assert data["record"]["extra"]["payload"] == {"value": "changed"}
    assert messages[0].record["extra"]["payload"] == {"value": "changed"}
    assert source == {"value": "source"}


def test_patcher_runs_before_enqueue():
    source = {"value": "source"}
    messages = []

    def patcher(record):
        record["message"] = "changed"
        record["extra"]["payload"]["value"] = "changed"

    logger.add(messages.append, format="{message}", enqueue=True, patcher=patcher, catch=False)
    logger.bind(payload=source).info("original")
    logger.complete()

    assert list(map(str, messages)) == ["changed\n"]
    assert messages[0].record["extra"]["payload"] == {"value": "changed"}
    assert source == {"value": "source"}


def test_patcher_failure_without_catch_propagates_after_isolation():
    source = {"value": "source"}
    messages = []

    logger.add(messages.append, format="{message}", catch=False)

    def failing_patcher(record):
        record["extra"]["payload"]["value"] = "changed"
        raise RuntimeError("patcher failed")

    logger.add(
        lambda message: None,
        patcher=failing_patcher,
        catch=False,
    )

    with pytest.raises(RuntimeError, match="patcher failed"):
        logger.bind(payload=source).info("second")

    assert list(map(str, messages)) == ["second\n"]
    assert messages[0].record["extra"]["payload"] is source
    assert source == {"value": "source"}


def test_invalid_patcher():
    with pytest.raises(TypeError, match="Invalid patcher"):
        logger.add(lambda message: None, patcher=False)

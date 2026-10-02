import gzip
import pathlib
import tempfile

import pytest

from loguru import ByteBudget, logger


@pytest.fixture
def log_dir():
    # Pytest 'tmp_path' creates directories in /tmp, but /tmp does not support xattr,
    # causing some tests would fail.
    with tempfile.TemporaryDirectory(dir=".") as path:
        yield pathlib.Path(path)
        logger.remove()  # Deleting file not possible if still in use by Loguru.


def read_files(log_dir, name="file.log"):
    current = log_dir / name
    rotated = sorted(f for f in log_dir.iterdir() if f.name != name)
    files = rotated + ([current] if current.exists() else [])
    return [(f.name, f.read_bytes()) for f in files]


def check_records_intact(files, messages, encoding="utf-8"):
    # The concatenation of all files must be exactly the concatenation of the
    # encoded records, and every file must start and end on a record boundary,
    # which proves that no record was ever split across two files.
    records = [(message + "\n").encode(encoding) for message in messages]
    stream = b"".join(records)
    assert b"".join(data for _, data in files) == stream

    record_end = 0
    offset = 0
    for _, data in files:
        offset += len(data)
        while record_end < offset:
            record_end += len(records.pop(0))
        assert record_end == offset


@pytest.mark.parametrize(
    ("size_limit", "expected"),
    [
        (4, [[b"aaa\n"], [b"bbb\n"], [b"ccc\n"]]),
        (5, [[b"aaa\n"], [b"bbb\n"], [b"ccc\n"]]),
        (8, [[b"aaa\n", b"bbb\n"], [b"ccc\n"]]),
        (9, [[b"aaa\n", b"bbb\n"], [b"ccc\n"]]),
        (12, [[b"aaa\n", b"bbb\n", b"ccc\n"]]),
    ],
)
def test_ascii_exact_byte_boundaries(log_dir, size_limit, expected):
    logger.add(log_dir / "file.log", rotation=ByteBudget(size_limit), format="{message}")
    for message in ["aaa", "bbb", "ccc"]:
        logger.debug(message)
    logger.remove()

    files = read_files(log_dir)
    assert len(files) == len(expected)
    for (_, data), expected_records in zip(files, expected):
        assert data == b"".join(expected_records)
        assert len(data) <= max(size_limit, len(expected_records[0]))


@pytest.mark.parametrize(
    ("size_limit", "expected"),
    [
        (6, [["中文\n"], ["日志\n"], ["测试\n"]]),
        (7, [["中文\n"], ["日志\n"], ["测试\n"]]),
        (13, [["中文\n"], ["日志\n"], ["测试\n"]]),
        (14, [["中文\n", "日志\n"], ["测试\n"]]),
        (21, [["中文\n", "日志\n", "测试\n"]]),
    ],
)
def test_chinese_counted_as_bytes_not_characters(log_dir, size_limit, expected):
    # Each record is 7 bytes once encoded in UTF-8 (2 x 3 bytes + "\n"), while
    # it is only 3 characters long, so a character-based limit would rotate differently.
    logger.add(log_dir / "file.log", rotation=ByteBudget(size_limit), format="{message}")
    for message in ["中文", "日志", "测试"]:
        logger.debug(message)
    logger.remove()

    files = read_files(log_dir)
    assert len(files) == len(expected)
    for (_, data), expected_records in zip(files, expected):
        assert data == "".join(expected_records).encode("utf-8")


@pytest.mark.parametrize(
    ("size_limit", "expected"),
    [
        (7, [["ab\n"], ["cd\n"], ["ef\n"]]),
        (8, [["ab\n"], ["cd\n"], ["ef\n"]]),
        (13, [["ab\n"], ["cd\n"], ["ef\n"]]),
        (14, [["ab\n", "cd\n"], ["ef\n"]]),
        (19, [["ab\n", "cd\n"], ["ef\n"]]),
        (20, [["ab\n", "cd\n", "ef\n"]]),
    ],
)
def test_utf16_bom_counted_once_per_file(log_dir, size_limit, expected):
    # With "utf-16", each file starts with a 2-bytes BOM: the first record of a
    # file costs 8 bytes ("ab\n" -> BOM + 6 bytes) and the next ones 6 bytes.
    bom = "".encode("utf-16")
    logger.add(
        log_dir / "file.log",
        rotation=ByteBudget(size_limit),
        format="{message}",
        encoding="utf-16",
    )
    for message in ["ab", "cd", "ef"]:
        logger.debug(message)
    logger.remove()

    files = read_files(log_dir)
    assert len(files) == len(expected)
    for (_, data), expected_records in zip(files, expected):
        assert data == "".join(expected_records).encode("utf-16")
        assert data.startswith(bom)
        assert len(data) <= max(size_limit, len(expected_records[0].encode("utf-16")))


def test_message_with_newlines_not_split(log_dir):
    logger.add(log_dir / "file.log", rotation=ByteBudget(8), format="{message}")
    for message in ["a\nb", "c\nd", "e\nf"]:
        logger.debug(message)
    logger.remove()

    files = read_files(log_dir)
    assert [data for _, data in files] == [b"a\nb\nc\nd\n", b"e\nf\n"]
    check_records_intact(files, ["a\nb", "c\nd", "e\nf"])


def test_single_record_larger_than_budget(log_dir):
    logger.add(log_dir / "file.log", rotation=ByteBudget(5), format="{message}")
    for message in ["x" * 10, "y" * 10, "z"]:
        logger.debug(message)
    logger.remove()

    files = read_files(log_dir)
    # Each oversized record is written whole to its own file: no split, no empty
    # file and exactly one rotation per record (no infinite rotation).
    assert [data for _, data in files] == [b"x" * 10 + b"\n", b"y" * 10 + b"\n", b"z\n"]
    assert all(len(data) > 0 for _, data in files)


def test_oversize_record_on_preexisting_file(log_dir):
    (log_dir / "file.log").write_bytes(b"already here\n")
    logger.add(log_dir / "file.log", rotation=ByteBudget(5), format="{message}")
    logger.debug("x" * 10)
    logger.remove()

    files = read_files(log_dir)
    assert [data for _, data in files] == [b"already here\n", b"x" * 10 + b"\n"]


def test_append_mode_counts_existing_bytes(log_dir):
    (log_dir / "file.log").write_bytes(b"12345\n")
    logger.add(log_dir / "file.log", rotation=ByteBudget(10), format="{message}")
    logger.debug("ab")
    logger.debug("cd")
    logger.remove()

    files = read_files(log_dir)
    assert [data for _, data in files] == [b"12345\nab\n", b"cd\n"]


def test_append_mode_file_already_over_budget(log_dir):
    (log_dir / "file.log").write_bytes(b"1" * 20)
    logger.add(log_dir / "file.log", rotation=ByteBudget(10), format="{message}")
    logger.debug("ab")
    logger.remove()

    files = read_files(log_dir)
    assert [data for _, data in files] == [b"1" * 20, b"ab\n"]


def test_append_mode_utf16_existing_file(log_dir):
    # When appending to a non-empty file, the file object does not emit a new BOM,
    # so the appended record only costs its 6 encoded bytes. With a budget of 15, a
    # BOM wrongly accounted for would rotate the file one record too early.
    (log_dir / "file.log").write_bytes("ab\n".encode("utf-16"))
    logger.add(
        log_dir / "file.log", rotation=ByteBudget(15), format="{message}", encoding="utf-16"
    )
    logger.debug("cd")  # Costs 6 bytes (no BOM while appending), reaching 14 bytes.
    logger.debug("ef")
    logger.remove()

    bom = "".encode("utf-16")
    files = read_files(log_dir)
    assert [data for _, data in files] == [
        "ab\n".encode("utf-16") + "cd\n".encode("utf-16")[len(bom):],
        "ef\n".encode("utf-16"),
    ]


def test_compression_of_rotated_files(log_dir):
    logger.add(
        log_dir / "file.log",
        rotation=ByteBudget(10),
        compression="gz",
        format="{message}",
    )
    for message in ["aaaa", "bbbb", "cccc"]:
        logger.debug(message)
    logger.remove()

    (rotated,) = log_dir.glob("*.gz")
    assert rotated.name.startswith("file.") and rotated.name.endswith(".log.gz")
    with gzip.open(rotated, "rb") as f:
        assert f.read() == b"aaaa\nbbbb\n"
    # The file still being written is left untouched at sink stop.
    assert (log_dir / "file.log").read_bytes() == b"cccc\n"


def test_encoding_error_is_reported_and_accounting_stays_exact(log_dir, capsys):
    logger.add(
        log_dir / "file.log",
        rotation=ByteBudget(8),
        format="{message}",
        encoding="ascii",
    )
    logger.debug("ok")
    logger.debug("中文")  # Not encodable with "ascii" and strict errors.
    logger.debug("fine")
    logger.debug("x")
    logger.remove()

    assert "UnicodeEncodeError" in capsys.readouterr().err
    files = read_files(log_dir)
    # The failed record left no trace and did not corrupt the byte accounting:
    # "ok\n" + "fine\n" is exactly 8 bytes, so the rotation happens before "x\n".
    assert [data for _, data in files] == [b"ok\nfine\n", b"x\n"]


def test_encoding_error_with_replace_errors(log_dir):
    logger.add(
        log_dir / "file.log",
        rotation=ByteBudget(6),
        format="{message}",
        encoding="ascii",
        errors="replace",
    )
    logger.debug("ab")
    logger.debug("中文")
    logger.debug("cd")
    logger.remove()

    files = read_files(log_dir)
    assert [data for _, data in files] == [b"ab\n??\n", b"cd\n"]


def test_records_never_split_across_files(log_dir):
    messages = ["abc", "中文", "a\nb", "", "x" * 20, "末"]
    logger.add(log_dir / "file.log", rotation=ByteBudget(15), format="{message}")
    for message in messages:
        logger.debug(message)
    logger.remove()

    files = read_files(log_dir)
    assert len(files) > 1
    check_records_intact(files, messages)
    records = [(message + "\n").encode("utf-8") for message in messages]
    for _, data in files:
        if len(data) > 15:
            # Only a file containing a single oversized record may exceed the budget.
            assert data in records


def test_delay_mode(log_dir):
    logger.add(log_dir / "file.log", rotation=ByteBudget(5), format="{message}", delay=True)
    assert not (log_dir / "file.log").exists()
    logger.debug("aaa")
    logger.debug("bbb")
    logger.remove()

    files = read_files(log_dir)
    assert [data for _, data in files] == [b"aaa\n", b"bbb\n"]


def test_budget_accepts_size_string(log_dir):
    logger.add(log_dir / "file.log", rotation=ByteBudget("10 B"), format="{message}")
    for message in ["aaaa", "bbbb", "cccc"]:
        logger.debug(message)
    logger.remove()

    files = read_files(log_dir)
    assert [data for _, data in files] == [b"aaaa\nbbbb\n", b"cccc\n"]


@pytest.mark.parametrize(
    ("size_limit", "expected"),
    [("10 B", 10), ("1 KB", 1000), ("1 KiB", 1024), ("0.5 MB", 500000), (42, 42), (4.5, 4.5)],
)
def test_budget_size_parsing(size_limit, expected):
    assert ByteBudget(size_limit).size_limit == expected


@pytest.mark.parametrize("size_limit", ["nope", "10", "10 XB"])
def test_budget_invalid_string(size_limit):
    with pytest.raises(ValueError, match="Cannot parse byte budget"):
        ByteBudget(size_limit)


@pytest.mark.parametrize("size_limit", [0, -1, -0.5])
def test_budget_not_positive(size_limit):
    with pytest.raises(ValueError, match="strictly positive"):
        ByteBudget(size_limit)


def test_budget_invalid_type():
    with pytest.raises(TypeError, match="Cannot infer byte budget"):
        ByteBudget(object())


def test_budget_repr():
    assert repr(ByteBudget(10)) == "ByteBudget(10)"

import codecs
import copy
import datetime
import decimal
import glob
import numbers
import os
import shutil
import string
import sys
from functools import partial
from stat import ST_DEV, ST_INO

from . import _string_parsers as string_parsers
from ._ctime_functions import get_ctime, set_ctime
from ._datetime import aware_now


def generate_rename_path(root, ext, creation_time):
    creation_datetime = datetime.datetime.fromtimestamp(creation_time)
    date = FileDateFormatter(creation_datetime)

    renamed_path = "{}.{}{}".format(root, date, ext)
    counter = 1

    while os.path.exists(renamed_path):
        counter += 1
        renamed_path = "{}.{}.{}{}".format(root, date, counter, ext)

    return renamed_path


class FileDateFormatter:
    def __init__(self, datetime=None):
        self.datetime = datetime or aware_now()

    def __format__(self, spec):
        if not spec:
            spec = "%Y-%m-%d_%H-%M-%S_%f"
        return self.datetime.__format__(spec)


class Compression:
    @staticmethod
    def add_compress(path_in, path_out, opener, **kwargs):
        with opener(path_out, **kwargs) as f_comp:
            f_comp.add(path_in, os.path.basename(path_in))

    @staticmethod
    def write_compress(path_in, path_out, opener, **kwargs):
        with opener(path_out, **kwargs) as f_comp:
            f_comp.write(path_in, os.path.basename(path_in))

    @staticmethod
    def copy_compress(path_in, path_out, opener, **kwargs):
        with open(path_in, "rb") as f_in:
            with opener(path_out, **kwargs) as f_out:
                shutil.copyfileobj(f_in, f_out)

    @staticmethod
    def compression(path_in, ext, compress_function):
        path_out = "{}{}".format(path_in, ext)

        if os.path.exists(path_out):
            creation_time = get_ctime(path_out)
            root, ext_before = os.path.splitext(path_in)
            renamed_path = generate_rename_path(root, ext_before + ext, creation_time)
            os.rename(path_out, renamed_path)
        compress_function(path_in, path_out)
        os.remove(path_in)


class Retention:
    @staticmethod
    def retention_count(logs, number):
        def key_log(log):
            return (-os.stat(log).st_mtime, log)

        for log in sorted(logs, key=key_log)[number:]:
            os.remove(log)

    @staticmethod
    def retention_age(logs, seconds):
        t = datetime.datetime.now().timestamp()
        for log in logs:
            if os.stat(log).st_mtime <= t - seconds:
                os.remove(log)


class ByteSizeRotation:
    def __init__(self, size_limit, encoding, errors=None, newline=None):
        self.size_limit = size_limit
        self.encoding = encoding
        self.errors = errors
        self.newline = newline

        self._codec_info = None
        self._file = None
        self._encoder = None
        self._candidate_encoder = None
        self._file_has_bom = False

    def __call__(self, message, file):
        if self._codec_info is None:
            self._codec_info = codecs.lookup(self.encoding)

        if self._file is not file:
            self._bind_file(file)

        self._candidate_encoder = copy.copy(self._encoder)
        bom = self._candidate_encoder.encode("")
        if self._file_has_bom:
            bom = b""
        data = self._candidate_encoder.encode(self._transform_newlines(message))
        candidate_size = len(bom) + len(data)

        file.seek(0, 2)
        current_size = file.tell()

        if current_size == 0 or current_size + candidate_size <= self.size_limit:
            return False

        return True

    def commit(self):
        if self._candidate_encoder is None:
            return

        payload_encoding = self._payload_encoding()
        if payload_encoding is not None:
            encoder = codecs.lookup(payload_encoding).incrementalencoder(self.errors)
            encoder.encode(self._candidate_encoder.buffer)
            self._encoder = encoder
            self._file_has_bom = True
        else:
            self._encoder = self._candidate_encoder

        self._candidate_encoder = None

    def _payload_encoding(self):
        codec_name = self._codec_info.name
        if codec_name == "utf-16":
            return "utf-16-le" if sys.byteorder == "little" else "utf-16-be"
        if codec_name == "utf-32":
            return "utf-32-le" if sys.byteorder == "little" else "utf-32-be"
        if codec_name == "utf-8-sig":
            return "utf-8"
        return None

    def reset(self):
        if self._codec_info is None:
            self._codec_info = codecs.lookup(self.encoding)

        encoder = self._codec_info.incrementalencoder(self.errors)
        self._file = None
        self._encoder = encoder
        self._candidate_encoder = None
        self._file_has_bom = False

    def _bind_file(self, file):
        file.seek(0, 2)
        current_size = file.tell()

        if self._codec_info is None:
            self._codec_info = codecs.lookup(self.encoding)

        encoder = self._codec_info.incrementalencoder(self.errors)

        if current_size:
            encoder.encode("")
            self._file_has_bom = True
        else:
            self._file_has_bom = False

        self._file = file
        self._encoder = encoder

    def _transform_newlines(self, message):
        if self.newline == "\r":
            return message.replace("\n", "\r")
        if self.newline == "\r\n":
            return message.replace("\r\n", "\n").replace("\n", "\r\n")
        if self.newline is None:
            return message.replace("\r\n", "\n").replace("\n", os.linesep)
        return message


class Rotation:
    @staticmethod
    def forward_day(t):
        return t + datetime.timedelta(days=1)

    @staticmethod
    def forward_weekday(t, weekday):
        days = (weekday - t.weekday()) % 7 or 7
        return t + datetime.timedelta(days=days)

    @staticmethod
    def forward_interval(t, interval):
        return t + interval

    class RotationGroup:
        def __init__(self, rotations) -> None:
            self._rotations = rotations

        def __call__(self, message, file) -> bool:
            return any(rotation(message, file) for rotation in self._rotations)

        def commit(self) -> None:
            for rotation in self._rotations:
                commit = getattr(rotation, "commit", None)
                if commit is not None:
                    commit()

        def reset(self) -> None:
            for rotation in self._rotations:
                reset = getattr(rotation, "reset", None)
                if reset is not None:
                    reset()

    class RotationTime:
        def __init__(self, step_forward, time_target=None, day_target=None):
            self._step_forward = step_forward
            self._time_target = time_target
            self._day_target = day_target
            self._limit = None

        def __call__(self, message, file):
            record_time = message.record["time"]

            if self._limit is None:
                filepath = os.path.realpath(file.name)
                creation_time = get_ctime(filepath)
                set_ctime(filepath, creation_time)
                start_time = datetime.datetime.fromtimestamp(
                    creation_time, tz=datetime.timezone.utc
                )

                if self._time_target is None:
                    limit = start_time.astimezone(record_time.tzinfo).replace(tzinfo=None)
                    limit = self._step_forward(limit)
                else:
                    tzinfo = (
                        record_time.tzinfo
                        if self._time_target.tzinfo is None
                        else self._time_target.tzinfo
                    )
                    limit = start_time.astimezone(tzinfo).replace(
                        hour=self._time_target.hour,
                        minute=self._time_target.minute,
                        second=self._time_target.second,
                        microsecond=self._time_target.microsecond,
                    )

                    if self._day_target is not None and limit.weekday() != self._day_target:
                        limit = Rotation.forward_weekday(limit, self._day_target)

                    if limit <= start_time:
                        limit = self._step_forward(limit)

                    if self._time_target.tzinfo is None:
                        limit = limit.replace(tzinfo=None)

                self._limit = limit

            if self._limit.tzinfo is None:
                record_time = record_time.replace(tzinfo=None)

            if record_time >= self._limit:
                while self._limit <= record_time:
                    self._limit = self._step_forward(self._limit)
                return True
            return False


class FileSink:
    def __init__(
        self,
        path,
        *,
        rotation=None,
        retention=None,
        compression=None,
        delay=False,
        watch=False,
        mode="a",
        buffering=1,
        encoding="utf8",
        **kwargs
    ):
        self.encoding = encoding

        self._kwargs = {**kwargs, "mode": mode, "buffering": buffering, "encoding": self.encoding}
        self._path = str(path)

        self._glob_patterns = self._make_glob_patterns(self._path)
        self._rotation_function = self._make_rotation_function(
            rotation, self.encoding, self._kwargs.get("errors"), self._kwargs.get("newline")
        )
        self._retention_function = self._make_retention_function(retention)
        self._compression_function = self._make_compression_function(compression)

        self._file = None
        self._file_path = None

        self._watch = watch
        self._file_dev = -1
        self._file_ino = -1

        if not delay:
            path = self._create_path()
            self._create_dirs(path)
            self._create_file(path)

    def write(self, message):
        if self._file is None:
            path = self._create_path()
            self._create_dirs(path)
            self._create_file(path)

        if self._watch:
            self._reopen_if_needed()

        should_rotate = (
            self._rotation_function is not None
            and self._rotation_function(message, self._file)
        )

        if should_rotate:
            self._terminate_file(is_rotating=True)
            reset = getattr(self._rotation_function, "reset", None)
            if reset is not None:
                reset()

        self._file.write(message)

        if self._rotation_function is not None:
            commit = getattr(self._rotation_function, "commit", None)
            if commit is not None:
                commit()

    def stop(self):
        if self._watch:
            self._reopen_if_needed()

        self._terminate_file(is_rotating=False)

    def tasks_to_complete(self):
        return []

    def _create_path(self):
        path = self._path.format_map({"time": FileDateFormatter()})
        return os.path.abspath(path)

    def _create_dirs(self, path):
        dirname = os.path.dirname(path)
        os.makedirs(dirname, exist_ok=True)

    def _create_file(self, path):
        self._file = open(path, **self._kwargs)
        self._file_path = path

        if self._watch:
            fileno = self._file.fileno()
            result = os.fstat(fileno)
            self._file_dev = result[ST_DEV]
            self._file_ino = result[ST_INO]

    def _close_file(self):
        self._file.flush()
        self._file.close()

        self._file = None
        self._file_path = None
        self._file_dev = -1
        self._file_ino = -1

    def _reopen_if_needed(self):
        # Implemented based on standard library:
        # https://github.com/python/cpython/blob/cb589d1b/Lib/logging/handlers.py#L486
        if not self._file:
            return

        filepath = self._file_path

        try:
            result = os.stat(filepath)
        except FileNotFoundError:
            result = None

        if not result or result[ST_DEV] != self._file_dev or result[ST_INO] != self._file_ino:
            self._close_file()
            self._create_dirs(filepath)
            self._create_file(filepath)

    def _terminate_file(self, *, is_rotating=False):
        old_path = self._file_path

        if self._file is not None:
            self._close_file()

        if is_rotating:
            new_path = self._create_path()
            self._create_dirs(new_path)

            if new_path == old_path:
                creation_time = get_ctime(old_path)
                root, ext = os.path.splitext(old_path)
                renamed_path = generate_rename_path(root, ext, creation_time)
                os.rename(old_path, renamed_path)
                old_path = renamed_path

        if is_rotating or self._rotation_function is None:
            if self._compression_function is not None and old_path is not None:
                self._compression_function(old_path)

            if self._retention_function is not None:
                logs = {
                    file
                    for pattern in self._glob_patterns
                    for file in glob.glob(pattern)
                    if os.path.isfile(file)
                }
                self._retention_function(list(logs))

        if is_rotating:
            self._create_file(new_path)
            set_ctime(new_path, datetime.datetime.now().timestamp())

    @staticmethod
    def _make_glob_patterns(path):
        formatter = string.Formatter()
        tokens = formatter.parse(path)
        escaped = "".join(glob.escape(text) + "*" * (name is not None) for text, name, *_ in tokens)

        root, ext = os.path.splitext(escaped)

        if not ext:
            return [escaped, escaped + ".*"]

        return [escaped, escaped + ".*", root + ".*" + ext, root + ".*" + ext + ".*"]

    @staticmethod
    def _make_rotation_function(rotation, encoding=None, errors=None, newline=None):
        if rotation is None:
            return None
        if isinstance(rotation, (list, tuple, set)):
            if len(rotation) == 0:
                raise ValueError("Must provide at least one rotation condition")
            return Rotation.RotationGroup(
                [
                    FileSink._make_rotation_function(rot, encoding, errors, newline)
                    for rot in rotation
                ]
            )
        if isinstance(rotation, str):
            size = string_parsers.parse_size(rotation)
            if size is not None:
                return FileSink._make_rotation_function(size, encoding, errors, newline)
            interval = string_parsers.parse_duration(rotation)
            if interval is not None:
                return FileSink._make_rotation_function(interval)
            frequency = string_parsers.parse_frequency(rotation)
            if frequency is not None:
                return Rotation.RotationTime(frequency)
            daytime = string_parsers.parse_daytime(rotation)
            if daytime is not None:
                day, time = daytime
                if day is None:
                    return FileSink._make_rotation_function(time)
                if time is None:
                    time = datetime.time(0, 0, 0)
                step_forward = partial(Rotation.forward_weekday, weekday=day)
                return Rotation.RotationTime(step_forward, time, day)
            raise ValueError("Cannot parse rotation from: '%s'" % rotation)
        if isinstance(rotation, (numbers.Real, decimal.Decimal)):
            return ByteSizeRotation(rotation, encoding, errors, newline)
        if isinstance(rotation, datetime.time):
            return Rotation.RotationTime(Rotation.forward_day, rotation)
        if isinstance(rotation, datetime.timedelta):
            step_forward = partial(Rotation.forward_interval, interval=rotation)
            return Rotation.RotationTime(step_forward)
        if callable(rotation):
            return rotation
        raise TypeError("Cannot infer rotation for objects of type: '%s'" % type(rotation).__name__)

    @staticmethod
    def _make_retention_function(retention):
        if retention is None:
            return None
        if isinstance(retention, str):
            interval = string_parsers.parse_duration(retention)
            if interval is None:
                raise ValueError("Cannot parse retention from: '%s'" % retention)
            return FileSink._make_retention_function(interval)
        if isinstance(retention, int):
            return partial(Retention.retention_count, number=retention)
        if isinstance(retention, datetime.timedelta):
            return partial(Retention.retention_age, seconds=retention.total_seconds())
        if callable(retention):
            return retention
        raise TypeError(
            "Cannot infer retention for objects of type: '%s'" % type(retention).__name__
        )

    @staticmethod
    def _make_compression_function(compression):
        if compression is None:
            return None
        if isinstance(compression, str):
            ext = compression.strip().lstrip(".")

            if ext == "gz":
                import gzip

                compress = partial(Compression.copy_compress, opener=gzip.open, mode="wb")
            elif ext == "bz2":
                import bz2

                compress = partial(Compression.copy_compress, opener=bz2.open, mode="wb")

            elif ext == "xz":
                import lzma

                compress = partial(
                    Compression.copy_compress, opener=lzma.open, mode="wb", format=lzma.FORMAT_XZ
                )

            elif ext == "lzma":
                import lzma

                compress = partial(
                    Compression.copy_compress, opener=lzma.open, mode="wb", format=lzma.FORMAT_ALONE
                )
            elif ext == "tar":
                import tarfile

                compress = partial(Compression.add_compress, opener=tarfile.open, mode="w:")
            elif ext == "tar.gz":
                import gzip
                import tarfile

                compress = partial(Compression.add_compress, opener=tarfile.open, mode="w:gz")
            elif ext == "tar.bz2":
                import bz2
                import tarfile

                compress = partial(Compression.add_compress, opener=tarfile.open, mode="w:bz2")

            elif ext == "tar.xz":
                import lzma
                import tarfile

                compress = partial(Compression.add_compress, opener=tarfile.open, mode="w:xz")
            elif ext == "zip":
                import zipfile

                compress = partial(
                    Compression.write_compress,
                    opener=zipfile.ZipFile,
                    mode="w",
                    compression=zipfile.ZIP_DEFLATED,
                )
            else:
                raise ValueError("Invalid compression format: '%s'" % ext)

            return partial(Compression.compression, ext="." + ext, compress_function=compress)
        if callable(compression):
            return compression
        raise TypeError(
            "Cannot infer compression for objects of type: '%s'" % type(compression).__name__
        )

from collections import OrderedDict
from datetime import timedelta
from types import MappingProxyType


def aware_now():
    from ._datetime import aware_now as _aware_now

    return _aware_now()


def default_fingerprint(record):
    exception = record["exception"]

    if exception is None:
        exception_fingerprint = None
    else:
        exception_fingerprint = (
            None if exception.type is None else exception.type.__name__,
            repr(exception.value),
        )

    extra = tuple(
        sorted((str(key), repr(value)) for key, value in record["extra"].items())
    )

    return (
        record["level"].name,
        record["message"],
        record["name"],
        record["file"].path,
        record["line"],
        record["function"],
        record["module"],
        exception_fingerprint,
        extra,
    )


class _RepeatGroup:
    __slots__ = (
        "fingerprint",
        "first_record",
        "window_start",
        "first_suppressed_time",
        "last_suppressed_time",
        "count",
    )

    def __init__(self, fingerprint, record, window_start):
        self.fingerprint = fingerprint
        self.first_record = record
        self.window_start = window_start
        self.first_suppressed_time = None
        self.last_suppressed_time = None
        self.count = 1


class RepeatSuppression:
    """Group equivalent records and emit one summary after a suppression window.

    The first record in a group is always sent to the sink. Records with the same fingerprint are
    counted while the window remains open. When the window expires, or when the sink is removed, a
    synthetic record is emitted with the number of suppressed records and their time range.

    Parameters
    ----------
    window: timedelta
        Duration of a fixed grouping window.
    max_groups: int
        Maximum number of groups tracked at the same time.
    fingerprint: callable
        Function receiving a read-only record and returning a hashable fingerprint. The default
        fingerprint includes the level, message, source, exception type/value and ``extra`` data.
        Tracebacks are intentionally excluded, so different tracebacks for the same exception are
        grouped. Different severity levels or different exception values produce separate groups.
    clock: callable
        Zero-argument function returning the current timezone-aware datetime. It defaults to
        Loguru's own clock, which makes the strategy compatible with time-freezing test helpers.
    """

    def __init__(
        self, window, max_groups=128, fingerprint=default_fingerprint, clock=aware_now
    ):
        if not isinstance(window, timedelta):
            raise TypeError(
                "Invalid suppression window, it should be a timedelta, not: '%s'"
                % type(window).__name__
            )
        if window <= timedelta(0):
            raise ValueError("Invalid suppression window, it should be greater than zero")
        if not isinstance(max_groups, int) or isinstance(max_groups, bool):
            raise TypeError(
                "Invalid suppression max_groups, it should be an integer, not: '%s'"
                % type(max_groups).__name__
            )
        if max_groups < 1:
            raise ValueError("Invalid suppression max_groups, it should be greater than zero")
        if not callable(fingerprint):
            raise TypeError(
                "Invalid suppression fingerprint, it should be a callable, not: '%s'"
                % type(fingerprint).__name__
            )
        if not callable(clock):
            raise TypeError(
                "Invalid suppression clock, it should be a callable, not: '%s'"
                % type(clock).__name__
            )

        self._window = window
        self._max_groups = max_groups
        self._fingerprint = fingerprint
        self._clock = clock
        self._groups = OrderedDict()

    def bind(self):
        """Return an independent state using this strategy's configuration."""
        return self.__class__(
            self._window,
            self._max_groups,
            self._fingerprint,
            self._clock,
        )

    def process(self, record):
        """Process one record and return records that should now be written.

        Returns
        -------
        tuple of (bool, list)
            The boolean tells whether the input record itself should be written. The list contains
            zero or more synthetic summary records for windows that have expired.
        """
        now = self._clock()
        summaries = self._due_summaries(now)

        read_only_extra = MappingProxyType(record["extra"])
        read_only_record = MappingProxyType(
            dict(record, extra=read_only_extra)
        )
        fingerprint = self._fingerprint(read_only_record)
        hash(fingerprint)

        group = self._groups.get(fingerprint)

        if group is not None:
            group.count += 1
            if group.first_suppressed_time is None:
                group.first_suppressed_time = now
            group.last_suppressed_time = now
            self._groups.move_to_end(fingerprint)
            return False, summaries

        group = _RepeatGroup(fingerprint, record, now)
        self._groups[fingerprint] = group

        while len(self._groups) > self._max_groups:
            _evicted_fingerprint, evicted_group = self._groups.popitem(last=False)
            summary = self._summary_record(evicted_group, now)
            if summary is not None:
                summaries.append(summary)

        return True, summaries

    def flush(self):
        """Return summaries for all groups which currently have suppressed records."""
        now = self._clock()
        return self._due_summaries(now, flush_all=True)

    def close(self):
        """Flush all remaining counts and prevent further grouping."""
        summaries = self.flush()
        return summaries

    def _due_summaries(self, now, flush_all=False):
        summaries = []
        due = []

        for fingerprint, group in self._groups.items():
            if flush_all or now - group.window_start >= self._window:
                due.append((fingerprint, group))

        for fingerprint, group in due:
            del self._groups[fingerprint]
            summary = self._summary_record(group, now)
            if summary is not None:
                summaries.append(summary)

        return summaries

    def _summary_record(self, group, now):
        if group.count == 1:
            return None

        count = group.count - 1
        plural = "" if count == 1 else "s"
        start = group.first_suppressed_time
        end = group.last_suppressed_time

        message = (
            "%d repeated log message%s suppressed between %s and %s. Original message: %s"
            % (count, plural, start.isoformat(), end.isoformat(), group.first_record["message"])
        )

        exception = group.first_record["exception"]
        if exception is not None and exception.type is not None:
            message += " (%s: %s)" % (exception.type.__name__, exception.value)

        record = group.first_record.copy()
        extra = dict(record["extra"])
        extra.update(
            {
                "suppression_count": count,
                "suppression_start_time": start,
                "suppression_end_time": end,
                "suppression_fingerprint": repr(group.fingerprint),
                "suppression_summary": True,
            }
        )
        record.update(
            {
                "message": message,
                "template": message,
                "exception": None,
                "extra": extra,
                "time": now,
                "elapsed": group.first_record["elapsed"],
            }
        )
        return record

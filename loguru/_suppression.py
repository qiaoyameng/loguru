"""Per-sink duplicate suppression ("log throttling") for the `Loguru` library.

A suppression policy is attached to an individual sink (see the ``suppress``
parameter of ``Logger.add()``). The first occurrence of a log record is emitted
normally. Subsequent records sharing the same fingerprint are counted for the
duration of a time window instead of being written. When the window ends, a
single summary message reporting the number of suppressed duplicates and the
covered time range is emitted to the sink.
"""

import time
from collections import OrderedDict
from datetime import timedelta

from ._handler import Message
from ._string_parsers import parse_duration


def default_fingerprint(record):
    """Compute the default grouping key of a log record.

    The function is read-only: it must not mutate the received record.

    Two records belong to the same group if and only if they share the same
    severity level, the same (formatted) message and the same exception type.
    Consequently, a change of level starts a new group, and two records
    carrying different exception types are not grouped together, while two
    exceptions of the same type (even with different values) are.
    """
    exception = record["exception"]
    exception_type = None if exception is None else exception.type.__name__
    return (record["level"].no, record["message"], exception_type)


def default_summary_format(info):
    """Build the default text of a suppression summary message.

    The ``info`` dict contains the ``suppressed`` count, the ``first_time`` and
    ``last_time`` of the grouped records, the ``level`` name, the ``message``
    of the first grouped record and the ``fingerprint`` of the group.
    """
    return "Suppressed %d duplicate log(s) from %s to %s [level=%s, message=%r]" % (
        info["suppressed"],
        info["first_time"].isoformat(),
        info["last_time"].isoformat(),
        info["level"],
        info["message"],
    )


class SuppressionPolicy:
    """Configuration of the duplicate suppression strategy of a sink.

    Parameters
    ----------
    window : |float|, |int|, |str| or |timedelta|
        The duration of the grouping window. Records sharing a fingerprint are
        counted (instead of emitted) until the window opened by the first
        record of the group expires. A summary is then emitted lazily, when the
        next record is processed, or at the latest when the sink is stopped.
    max_groups : |int|, optional
        The maximum number of simultaneously tracked fingerprints. When the
        limit is reached, the least recently used group is evicted and its
        pending suppressed count is immediately reported as a summary.
    fingerprint : |callable|, optional
        A function receiving the log record and returning a hashable grouping
        key. It must be read-only with regard to the record. Defaults to
        :func:`default_fingerprint`.
    clock : |callable|, optional
        A zero-argument function returning the current time in seconds, used
        solely to measure windows. Defaults to ``time.monotonic``. Injecting a
        fake clock makes window expiration deterministic in tests.
    summary_format : |callable|, optional
        A function receiving a dict of summary information and returning the
        summary text. Defaults to :func:`default_summary_format`.
    """

    def __init__(
        self,
        window,
        *,
        max_groups=1024,
        fingerprint=None,
        clock=None,
        summary_format=None,
    ):
        if isinstance(window, str):
            parsed = parse_duration(window)
            if parsed is None:
                raise ValueError("Invalid suppress window duration: '%s'" % window)
            window = parsed
        if isinstance(window, timedelta):
            window = window.total_seconds()
        if not isinstance(window, (int, float)) or isinstance(window, bool):
            raise TypeError(
                "Invalid suppress window, it should be a number of seconds, a duration "
                "string or a timedelta, not: '%s'" % type(window).__name__
            )
        if window <= 0:
            raise ValueError(
                "Invalid suppress window, it should be strictly positive, not: %r" % window
            )
        if not isinstance(max_groups, int) or isinstance(max_groups, bool):
            raise TypeError(
                "Invalid suppress max_groups, it should be an integer, not: '%s'"
                % type(max_groups).__name__
            )
        if max_groups < 1:
            raise ValueError(
                "Invalid suppress max_groups, it should be a positive integer, not: %d"
                % max_groups
            )
        if fingerprint is None:
            fingerprint = default_fingerprint
        elif not callable(fingerprint):
            raise TypeError(
                "Invalid suppress fingerprint, it should be a callable, not: '%s'"
                % type(fingerprint).__name__
            )
        if clock is None:
            clock = time.monotonic
        elif not callable(clock):
            raise TypeError(
                "Invalid suppress clock, it should be a callable, not: '%s'"
                % type(clock).__name__
            )
        if summary_format is None:
            summary_format = default_summary_format
        elif not callable(summary_format):
            raise TypeError(
                "Invalid suppress summary_format, it should be a callable, not: '%s'"
                % type(summary_format).__name__
            )

        self.window = float(window)
        self.max_groups = max_groups
        self.fingerprint = fingerprint
        self.clock = clock
        self.summary_format = summary_format


class _Group:
    __slots__ = (
        "fingerprint",
        "record",
        "first_seen",
        "last_seen",
        "first_time",
        "last_time",
        "suppressed",
    )

    def __init__(self, fingerprint, record, now):
        self.fingerprint = fingerprint
        # Only the first record of the group is retained (for summary context),
        # the suppressed records are counted but not stored.
        self.record = record
        self.first_seen = now
        self.last_seen = now
        self.first_time = record["time"]
        self.last_time = record["time"]
        self.suppressed = 0


class DuplicateSuppressor:
    """Runtime state machine applying a :class:`SuppressionPolicy`.

    The suppressor is owned by a single handler and is always accessed while
    holding the handler lock. When the sink is enqueued, grouping happens on
    the sender side, before messages are put into the queue, so that
    duplicates never cross the queue and summaries are simply forwarded to the
    consumer thread like any other message.
    """

    def __init__(self, policy):
        self._window = policy.window
        self._max_groups = policy.max_groups
        self._fingerprint = policy.fingerprint
        self._clock = policy.clock
        self._summary_format = policy.summary_format
        self._groups = OrderedDict()

    def offer(self, message):
        """Process a formatted message and return the messages to write now.

        The returned list contains the summaries of expired or evicted groups
        followed by the offered message itself, unless the message is a
        duplicate within an open window, in which case it is counted and
        swallowed. Summary messages are produced here and written directly to
        the sink: they never re-enter this suppressor, which prevents any
        recursive suppression of summaries.
        """
        now = self._clock()
        out = self._expire(now)

        record = message.record
        fingerprint = self._fingerprint(record)
        group = self._groups.get(fingerprint)

        if group is not None:
            group.suppressed += 1
            group.last_seen = now
            group.last_time = record["time"]
            self._groups.move_to_end(fingerprint)
            return out

        if len(self._groups) >= self._max_groups:
            _, evicted = self._groups.popitem(last=False)
            summary = self._build_summary(evicted)
            if summary is not None:
                out.append(summary)

        self._groups[fingerprint] = _Group(fingerprint, record, now)
        out.append(message)
        return out

    def flush(self):
        """Report the pending counts of all groups and reset the suppressor.

        Called when the sink is stopped so that duplicates suppressed in a
        still-open window are not silently lost.
        """
        out = []
        for group in self._groups.values():
            summary = self._build_summary(group)
            if summary is not None:
                out.append(summary)
        self._groups.clear()
        return out

    def _expire(self, now):
        out = []
        for fingerprint, group in list(self._groups.items()):
            if now - group.first_seen >= self._window:
                del self._groups[fingerprint]
                summary = self._build_summary(group)
                if summary is not None:
                    out.append(summary)
        return out

    def _build_summary(self, group):
        if group.suppressed == 0:
            return None
        info = {
            "suppressed": group.suppressed,
            "first_seen": group.first_seen,
            "last_seen": group.last_seen,
            "first_time": group.first_time,
            "last_time": group.last_time,
            "level": group.record["level"].name,
            "message": group.record["message"],
            "fingerprint": group.fingerprint,
        }
        text = self._summary_format(info)
        if not text.endswith("\n"):
            text += "\n"
        summary = Message(text)
        summary.record = group.record
        return summary

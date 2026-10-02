import asyncio
import threading


class SinkReceipt:
    """The outcome of one logged record for one specific sink.

    Attributes
    ----------
    handler_id : int
        The id of the handler, as returned by ``Logger.add()``, in charge of the sink.
    sink_name : str
        The name of the sink as computed when the handler was added.
    status : str
        One of ``"pending"``, ``"accepted"``, ``"filtered"``, ``"dropped"``, ``"success"``
        or ``"failed"``. The ``"accepted"`` status means the sink took ownership of the
        record (for example it was enqueued or scheduled) but did not finish processing it
        yet. It must not be interpreted as a successful processing.
    exception : Exception or None
        The original exception raised by the sink if the status is ``"failed"``, ``None``
        otherwise.
    """

    PENDING = "pending"
    ACCEPTED = "accepted"
    FILTERED = "filtered"
    DROPPED = "dropped"
    SUCCESS = "success"
    FAILED = "failed"

    __slots__ = ("handler_id", "sink_name", "status", "exception")

    def __init__(self, handler_id, sink_name, status=PENDING, exception=None):
        self.handler_id = handler_id
        self.sink_name = sink_name
        self.status = status
        self.exception = exception

    def __repr__(self):
        return "SinkReceipt(handler_id=%r, sink_name=%r, status=%r, exception=%r)" % (
            self.handler_id,
            self.sink_name,
            self.status,
            self.exception,
        )


class LogReceipt:
    """An aggregation of the per-sink outcomes of one logged record.

    A receipt is created for each call to ``Logger.log_with_receipt()`` and tracks the
    handlers that were selected for this very call. Each selected handler is represented by
    a ``SinkReceipt`` which converges to a final status (``"filtered"``, ``"dropped"``,
    ``"success"`` or ``"failed"``) once the fate of the record is known for this sink.

    A successful receipt means the sink finished processing the record (for example the
    ``write()`` method of the sink returned, or the coroutine of an asynchronous sink
    completed). It does **not** guarantee that the logged data has been persisted to any
    external storage, database or remote endpoint.
    """

    _FINAL_STATUSES = frozenset(
        (SinkReceipt.FILTERED, SinkReceipt.DROPPED, SinkReceipt.SUCCESS, SinkReceipt.FAILED)
    )

    def __init__(self):
        self._condition = threading.Condition()
        self._entries = {}

    def __repr__(self):
        return "LogReceipt(done=%r, results=%r)" % (self.done, self.results)

    def _attach(self, handlers):
        with self._condition:
            for handler_id, handler in handlers.items():
                self._entries[handler_id] = SinkReceipt(handler_id, handler._name)

    def _accept(self, handler_id):
        with self._condition:
            entry = self._entries.get(handler_id)
            if entry is not None and entry.status == SinkReceipt.PENDING:
                entry.status = SinkReceipt.ACCEPTED

    def _resolve(self, handler_id, status, exception=None):
        with self._condition:
            entry = self._entries.get(handler_id)
            if entry is None or entry.status in self._FINAL_STATUSES:
                return
            entry.status = status
            entry.exception = exception
            self._condition.notify_all()

    def _callback(self, handler_id):
        def callback(exception=None):
            if exception is None:
                self._resolve(handler_id, SinkReceipt.SUCCESS)
            else:
                self._resolve(handler_id, SinkReceipt.FAILED, exception)

        return callback

    def _filter_all(self):
        with self._condition:
            for entry in self._entries.values():
                if entry.status == SinkReceipt.PENDING:
                    entry.status = SinkReceipt.FILTERED
            self._condition.notify_all()

    def _seal(self):
        with self._condition:
            for entry in self._entries.values():
                if entry.status == SinkReceipt.PENDING:
                    entry.status = SinkReceipt.DROPPED
            self._condition.notify_all()

    def _is_done(self):
        return all(entry.status in self._FINAL_STATUSES for entry in self._entries.values())

    @property
    def done(self):
        """Whether all selected sinks reported a final status for the record."""
        with self._condition:
            return self._is_done()

    @property
    def ok(self):
        """Whether all selected sinks finished without any of them failing."""
        with self._condition:
            return self._is_done() and not any(
                entry.status == SinkReceipt.FAILED for entry in self._entries.values()
            )

    @property
    def results(self):
        """A snapshot of the ``SinkReceipt`` of each handler selected for the record."""
        with self._condition:
            return tuple(
                SinkReceipt(entry.handler_id, entry.sink_name, entry.status, entry.exception)
                for entry in self._entries.values()
            )

    @property
    def succeeded(self):
        """The ``SinkReceipt`` of the sinks which successfully processed the record."""
        return self._select(SinkReceipt.SUCCESS)

    @property
    def failed(self):
        """The ``SinkReceipt`` of the sinks which failed to process the record."""
        return self._select(SinkReceipt.FAILED)

    @property
    def filtered(self):
        """The ``SinkReceipt`` of the sinks which filtered out the record."""
        return self._select(SinkReceipt.FILTERED)

    @property
    def dropped(self):
        """The ``SinkReceipt`` of the sinks which did not accept the record."""
        return self._select(SinkReceipt.DROPPED)

    def _select(self, status):
        with self._condition:
            return tuple(
                SinkReceipt(entry.handler_id, entry.sink_name, entry.status, entry.exception)
                for entry in self._entries.values()
                if entry.status == status
            )

    def wait(self, timeout=None):
        """Block until all selected sinks reported a final status for the record.

        Parameters
        ----------
        timeout : float or None
            The maximum number of seconds to wait. If ``None``, wait indefinitely.

        Returns
        -------
        LogReceipt
            The receipt itself, once resolved.

        Raises
        ------
        TimeoutError
            If the timeout elapsed before all sinks reported a final status. Timing out
            does not cancel the logging call: the record remains accepted by the sinks and
            will still be processed, the receipt can be waited on again later.
        """
        with self._condition:
            if not self._condition.wait_for(self._is_done, timeout):
                raise TimeoutError(
                    "Timed out waiting for the log receipt, the record is still being "
                    "processed by the sinks and has not been discarded"
                )
        return self

    def __await__(self):
        return self._wait_async().__await__()

    async def _wait_async(self):
        while not self.done:
            await asyncio.sleep(0.001)
        return self

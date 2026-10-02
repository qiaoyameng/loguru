import itertools
import multiprocessing
import threading
from collections import namedtuple


HandlerReceipt = namedtuple("HandlerReceipt", ("handler_id", "status", "error"))

_receipt_token_counter = itertools.count(1)


class ReceiptResult:
    """The aggregated outcome of a logged record.

    A successful entry means the configured sink returned without raising. It does not guarantee
    that an external storage or remote service has durably committed the record.
    """

    def __init__(self, results):
        self._results = tuple(results)

    @property
    def results(self):
        return self._results

    @property
    def succeeded(self):
        return tuple(result for result in self._results if result.status == "succeeded")

    @property
    def selected(self):
        return tuple(result for result in self._results if result.status != "filtered")

    @property
    def failed(self):
        return tuple(result for result in self._results if result.status == "failed")

    @property
    def filtered(self):
        return tuple(result for result in self._results if result.status == "filtered")

    @property
    def skipped(self):
        return tuple(result for result in self._results if result.status == "skipped")

    @property
    def ok(self):
        return not any(result.status == "failed" for result in self._results)

    def __iter__(self):
        return iter(self._results)

    def __len__(self):
        return len(self._results)

    def __getitem__(self, handler_id):
        for result in self._results:
            if result.handler_id == handler_id:
                return result
        raise KeyError(handler_id)

    def __repr__(self):
        return "ReceiptResult(%r)" % (self._results,)


class Receipt:
    """An awaitable completion record for one logging call.

    The receipt is completed after every handler selected by that call has reached a terminal
    state. Waiting for or cancelling the receipt does not cancel accepted sink work.
    """

    def __init__(self, handler_ids):
        self._token = next(_receipt_token_counter)
        self._handler_ids = tuple(handler_ids)
        self._results = {}
        self._result = None
        self._completed = threading.Event()
        self._lock = threading.Lock()
        self._futures = set()
        self._process_queues = set()

    @property
    def result(self):
        return self._result

    def create_acknowledgment(self, handler_id, local=True):
        callback = self._record_result if local else None
        return Acknowledgment(self._token, handler_id, None, callback)

    def create_process_acknowledgment(self, handler_id, result_queue):
        with self._lock:
            if result_queue not in self._process_queues:
                self._process_queues.add(result_queue)
                thread = threading.Thread(
                    target=self._collect_process_results,
                    args=(result_queue,),
                    daemon=True,
                )
                thread.start()
        return Acknowledgment(self._token, handler_id, result_queue)

    def _collect_process_results(self, result_queue):
        while True:
            payload = result_queue.get()
            receipt_token, handler_id, status, error = payload
            if receipt_token == self._token:
                self._record_result(handler_id, status, error)

    def wait(self, timeout=None):
        """Wait until every selected handler reached a terminal state."""
        if self._result is None:
            self._completed.wait(timeout)
        if self._result is None:
            raise TimeoutError("Timed out while waiting for logging receipt")
        return self._result

    def _record_result(self, handler_id, status, error=None):
        with self._lock:
            if handler_id not in self._handler_ids or handler_id in self._results:
                return
            self._results[handler_id] = HandlerReceipt(handler_id, status, error)
            completed = len(self._results) == len(self._handler_ids)
            if completed:
                results = [self._results[handler_id] for handler_id in self._handler_ids]
                self._result = ReceiptResult(results)
                futures = list(self._futures)
            else:
                futures = []

        if completed:
            self._completed.set()
            for loop, future in futures:
                if not future.done():
                    try:
                        loop.call_soon_threadsafe(_set_future_result, future, None)
                    except RuntimeError:
                        pass

    def __await__(self):
        if self._result is None:
            from ._asyncio_loop import get_running_loop

            loop = get_running_loop()
            future = loop.create_future()
            with self._lock:
                self._futures.add((loop, future))
            future.add_done_callback(lambda _: self._discard_future(loop, future))
            yield from future
        return self._result

    def _discard_future(self, loop, future):
        with self._lock:
            self._futures.discard((loop, future))

    def __repr__(self):
        return "Receipt(handlers=%r)" % (self._handler_ids,)


def _set_future_result(future, result):
    if not future.done():
        future.set_result(result)



class Acknowledgment:
    """A terminal completion signal returned by a handler."""

    __slots__ = (
        "_receipt_id",
        "_handler_id",
        "_result_queue",
        "_callback",
        "_local_token",
        "_completed",
    )

    _counter = itertools.count(1)

    def __init__(
        self,
        receipt_id,
        handler_id,
        result_queue,
        callback=None,
        local_token=None,
    ):
        self._receipt_id = receipt_id
        self._handler_id = handler_id
        self._result_queue = result_queue
        self._callback = callback
        self._local_token = local_token
        self._completed = False

    @property
    def local_token(self):
        if self._local_token is None:
            self._local_token = next(self._counter)
        return self._local_token

    def succeed(self):
        self._report("succeeded")

    def filtered(self):
        self._report("filtered")

    def skipped(self):
        self._report("skipped")

    def fail(self, error):
        self._report("failed", error)

    def _report(self, status, error=None):
        if self._completed:
            return
        self._completed = True
        if self._callback is not None:
            self._callback(self._handler_id, status, error)
            return
        payload = (self._receipt_id, self._handler_id, status, error)
        try:
            import pickle

            pickle.dumps(payload)
        except Exception:
            error = RuntimeError("%s: %s" % (type(error).__name__, error))
            payload = (self._receipt_id, self._handler_id, "failed", error)
        self._result_queue.put(payload)

    def __getstate__(self):
        return (
            self._receipt_id,
            self._handler_id,
            self._result_queue,
            self.local_token,
            self._completed,
        )

    def __setstate__(self, state):
        receipt_id, handler_id, result_queue, local_token, completed = state
        self._receipt_id = receipt_id
        self._handler_id = handler_id
        self._result_queue = result_queue
        self._callback = None
        self._local_token = local_token
        self._completed = completed

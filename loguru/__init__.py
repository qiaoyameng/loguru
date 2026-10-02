"""
The Loguru library provides a pre-instanced logger to facilitate dealing with logging in Python.

Just ``from loguru import logger``.
"""

import atexit as _atexit
import sys as _sys

from . import _defaults
from ._logger import Core as _Core
from ._logger import Logger as _Logger
from ._receipt import HandlerReceipt as _HandlerReceipt
from ._receipt import Receipt as _Receipt
from ._receipt import ReceiptResult as _ReceiptResult

__version__ = "0.7.3"

__all__ = ["logger", "HandlerReceipt", "Receipt", "ReceiptResult"]

HandlerReceipt = _HandlerReceipt
Receipt = _Receipt
ReceiptResult = _ReceiptResult

logger = _Logger(
    core=_Core(),
    exception=None,
    depth=0,
    record=False,
    lazy=False,
    colors=False,
    raw=False,
    capture=True,
    patchers=[],
    extra={},
)

if _defaults.LOGURU_AUTOINIT and _sys.stderr:
    logger.add(_sys.stderr)

_atexit.register(logger.remove)

import os
from collections.abc import Iterable
from typing import Final, Iterator, Literal, final, overload

from typing_extensions import Buffer, Generic, Self, TypeVar

_T = TypeVar("_T")

# Value returned by get/key/value/item etc.  bytes when buffers=False
# (the default), memoryview when buffers=True.
_VT = TypeVar("_VT", bound=bytes | memoryview)
_VT_co = TypeVar(
    "_VT_co",
    bound=bytes | memoryview,
    default=bytes | memoryview,
    covariant=True,
)

__version__: str

# Module-level functions

def version() -> tuple[int, int, int]: ...
def enable_drop_gil() -> None: ...

# Exceptions

class Error(Exception):
    def __init__(self, what: str, code: int = ...) -> None: ...

class KeyExistsError(Error): ...
class NotFoundError(Error): ...
class PageNotFoundError(Error): ...
class CorruptedError(Error): ...
class PanicError(Error): ...
class VersionMismatchError(Error): ...
class InvalidError(Error): ...
class MapFullError(Error): ...
class DbsFullError(Error): ...
class ReadersFullError(Error): ...
class TlsFullError(Error): ...
class TxnFullError(Error): ...
class CursorFullError(Error): ...
class PageFullError(Error): ...
class MapResizedError(Error): ...
class IncompatibleError(Error): ...
class BadDbiError(Error): ...
class BadRslotError(Error): ...
class BadTxnError(Error): ...
class BadValsizeError(Error): ...
class ReadonlyError(Error): ...
class InvalidParameterError(Error): ...
class LockError(Error): ...
class MemoryError(Error): ...
class DiskError(Error): ...

# Core classes

@final
class Environment:
    readonly: bool

    def __new__(
        cls,
        path: os.PathLike[bytes],
        map_size: int = 10485760,
        subdir: bool = True,
        readonly: bool = False,
        metasync: bool = True,
        sync: bool = True,
        map_async: bool = False,
        mode: int = 0o755,
        create: bool = True,
        readahead: bool = True,
        writemap: bool = False,
        meminit: bool = True,
        max_readers: int = 126,
        max_dbs: int = 0,
        max_spare_txns: int = 1,
        lock: bool = True,
    ) -> Self: ...
    def __enter__(self) -> Self: ...
    def __exit__(self, *args: object) -> None: ...
    def close(self) -> None: ...
    def path(self) -> str: ...
    def copy(
        self, path: str, compact: bool = ..., txn: Transaction | None = ...
    ) -> None: ...
    def copyfd(
        self, fd: int, compact: bool = ..., txn: Transaction | None = ...
    ) -> None: ...
    def sync(self, force: bool = ...) -> None: ...
    def stat(self) -> dict[str, int]: ...
    def info(self) -> dict[str, int]: ...
    def flags(self) -> dict[str, bool]: ...
    def max_key_size(self) -> int: ...
    def max_readers(self) -> int: ...
    def readers(self) -> str: ...
    def reader_check(self) -> int: ...
    def set_mapsize(self, map_size: int) -> None: ...
    def open_db(
        self,
        key: bytes | None = ...,
        txn: Transaction | None = ...,
        reverse_key: bool = ...,
        dupsort: bool = ...,
        create: bool = ...,
        integerkey: bool = ...,
        integerdup: bool = ...,
        dupfixed: bool = ...,
    ) -> _Database: ...

    #
    @overload
    def dbs(self, txn: None = None) -> list[bytes]: ...
    @overload
    def dbs(self, txn: Transaction[_VT]) -> list[_VT]: ...

    #
    @overload
    def begin(
        self,
        db: _Database | None = None,
        parent: Transaction | None = None,
        write: bool = False,
        buffers: Literal[False] = False,
    ) -> Transaction[bytes]: ...
    @overload
    def begin(
        self,
        db: _Database | None,
        parent: Transaction | None,
        write: bool,
        buffers: Literal[True],
    ) -> Transaction[memoryview]: ...
    @overload
    def begin(
        self,
        db: _Database | None = None,
        parent: Transaction | None = None,
        write: bool = False,
        *,
        buffers: Literal[True],
    ) -> Transaction[memoryview]: ...

open = Environment

@final
class _Database:
    def flags(self) -> dict[str, bool]: ...

@final
class Transaction(Generic[_VT_co]):
    env: Final[Environment]

    @overload
    def __new__(
        cls,
        env: Environment,
        db: _Database | None = None,
        parent: Transaction | None = None,
        write: bool = False,
        buffers: Literal[False] = False,
    ) -> Transaction[bytes]: ...
    @overload
    def __new__(
        cls,
        env: Environment,
        db: _Database | None,
        parent: Transaction | None,
        write: bool,
        buffers: Literal[True],
    ) -> Transaction[memoryview]: ...
    @overload
    def __new__(
        cls,
        env: Environment,
        db: _Database | None = None,
        parent: Transaction | None = None,
        write: bool = False,
        *,
        buffers: Literal[True],
    ) -> Transaction[memoryview]: ...

    #
    def __enter__(self) -> Self: ...
    def __exit__(self, *args: object) -> None: ...
    def id(self) -> int: ...
    def stat(self, db: _Database | None = ...) -> dict[str, int]: ...
    def drop(self, db: _Database, delete: bool = ...) -> None: ...
    def commit(self) -> None: ...
    def abort(self) -> None: ...

    #
    @overload
    def get(
        self, key: Buffer, default: None = None, db: _Database | None = None
    ) -> _VT_co | None: ...
    @overload
    def get(
        self, key: Buffer, default: _T, db: _Database | None = None
    ) -> _VT_co | _T: ...

    #
    def put(
        self,
        key: Buffer,
        value: Buffer,
        dupdata: bool = ...,
        overwrite: bool = ...,
        append: bool = ...,
        db: _Database | None = ...,
    ) -> bool: ...
    def replace(
        self, key: Buffer, value: Buffer, db: _Database | None = None
    ) -> _VT_co | None: ...
    def pop(self, key: Buffer, db: _Database | None = None) -> _VT_co | None: ...
    def delete(
        self, key: Buffer, value: Buffer = ..., db: _Database | None = ...
    ) -> bool: ...
    def cursor(self, db: _Database | None = ...) -> Cursor: ...

@final
class Cursor(Generic[_VT_co]):
    db: Final[_Database]
    txn: Transaction[_VT_co]

    def __new__(cls, db: _Database, txn: Transaction[_VT_co]) -> Self: ...
    def __enter__(self) -> Self: ...
    def __exit__(self, *args: object) -> None: ...
    def __iter__(self) -> Iterator[tuple[_VT_co, _VT_co]]: ...
    def close(self) -> None: ...
    def key(self) -> _VT_co: ...
    def value(self) -> _VT_co: ...
    def item(self) -> tuple[_VT_co, _VT_co]: ...

    # Positioning
    def first(self) -> bool: ...
    def first_dup(self) -> bool: ...
    def last(self) -> bool: ...
    def last_dup(self) -> bool: ...
    def next(self) -> bool: ...
    def next_dup(self) -> bool: ...
    def next_nodup(self) -> bool: ...
    def prev(self) -> bool: ...
    def prev_dup(self) -> bool: ...
    def prev_nodup(self) -> bool: ...
    def set_key(self, key: Buffer, /) -> bool: ...
    def set_key_dup(self, key: Buffer, value: Buffer) -> bool: ...
    def set_range(self, key: Buffer, /) -> bool: ...
    def set_range_dup(self, key: Buffer, value: Buffer) -> bool: ...

    # Data operations
    @overload
    def get(self, key: Buffer, default: None = None) -> _VT_co | None: ...
    @overload
    def get(self, key: Buffer, default: _T) -> _VT_co | _T: ...

    #
    @overload  # keyfixed=False (default), values=True (default)
    def getmulti(
        self,
        keys: Iterable[Buffer],
        dupdata: bool = False,
        dupfixed_bytes: int | None = None,
        keyfixed: Literal[False] = False,
        values: Literal[True] = True,
    ) -> list[tuple[_VT_co, _VT_co]]: ...
    @overload  # keyfixed=False (default), values=False
    def getmulti(
        self,
        keys: Iterable[Buffer],
        dupdata: bool = False,
        dupfixed_bytes: int | None = None,
        keyfixed: Literal[False] = False,
        *,
        values: Literal[False],
    ) -> list[_VT_co]: ...
    @overload  # keyfixed=True  (positional)
    def getmulti(
        self,
        keys: Iterable[Buffer],
        dupdata: bool,
        dupfixed_bytes: int,
        keyfixed: Literal[True],
        values: bool = True,
    ) -> memoryview: ...
    @overload  # keyfixed=True  (keyword)
    def getmulti(
        self,
        keys: Iterable[Buffer],
        dupdata: bool = False,
        *,
        dupfixed_bytes: int,
        keyfixed: Literal[True],
        values: bool = True,
    ) -> memoryview: ...

    #
    def put(
        self,
        key: Buffer,
        val: Buffer,
        dupdata: bool = ...,
        overwrite: bool = ...,
        append: bool = ...,
    ) -> bool: ...
    def putmulti(
        self,
        items: Iterable[tuple[Buffer, Buffer]],
        dupdata: bool = ...,
        overwrite: bool = ...,
        append: bool = ...,
    ) -> tuple[int, int]: ...
    def delete(self, dupdata: bool = ...) -> bool: ...
    def replace(self, key: Buffer, val: Buffer) -> _VT_co | None: ...
    def pop(self, key: Buffer) -> _VT_co | None: ...
    def count(self) -> int: ...

    # Iteration

    #
    @overload
    def iternext(
        self, key: Literal[True] = True, value: Literal[True] = True
    ) -> Iterator[tuple[_VT_co]]: ...
    @overload
    def iternext(
        self, key: Literal[True] = True, *, value: Literal[False]
    ) -> Iterator[_VT_co]: ...
    @overload
    def iternext(self, key: Literal[False], value: bool = True) -> Iterator[_VT_co]: ...

    # keep in sync with `iternext`
    @overload
    def iternext_dup(
        self, key: Literal[True] = True, value: Literal[True] = True
    ) -> Iterator[tuple[_VT_co, _VT_co]]: ...
    @overload
    def iternext_dup(
        self, key: Literal[True] = True, *, value: Literal[False]
    ) -> Iterator[_VT_co]: ...
    @overload
    def iternext_dup(
        self, key: Literal[False], value: bool = True
    ) -> Iterator[_VT_co]: ...

    # keep in sync with `iternext`
    @overload
    def iternext_nodup(
        self, key: Literal[True] = True, value: Literal[True] = True
    ) -> Iterator[tuple[_VT_co, _VT_co]]: ...
    @overload
    def iternext_nodup(
        self, key: Literal[True] = True, *, value: Literal[False]
    ) -> Iterator[_VT_co]: ...
    @overload
    def iternext_nodup(
        self, key: Literal[False], value: bool = True
    ) -> Iterator[_VT_co]: ...

    # keep in sync with `iternext`
    @overload
    def iterprev(
        self, key: Literal[True] = True, value: Literal[True] = True
    ) -> Iterator[tuple[_VT_co, _VT_co]]: ...
    @overload
    def iterprev(
        self, key: Literal[True] = True, *, value: Literal[False]
    ) -> Iterator[_VT_co]: ...
    @overload
    def iterprev(self, key: Literal[False], value: bool = True) -> Iterator[_VT_co]: ...

    # keep in sync with `iternext`
    @overload
    def iterprev_dup(
        self, key: Literal[True] = True, value: Literal[True] = True
    ) -> Iterator[tuple[_VT_co, _VT_co]]: ...
    @overload
    def iterprev_dup(
        self, key: Literal[True] = True, *, value: Literal[False]
    ) -> Iterator[_VT_co]: ...
    @overload
    def iterprev_dup(
        self, key: Literal[False], value: bool = True
    ) -> Iterator[_VT_co]: ...

    # keep in sync with `iternext`
    @overload
    def iterprev_nodup(
        self, key: Literal[True] = True, value: Literal[True] = True
    ) -> Iterator[tuple[_VT_co, _VT_co]]: ...
    @overload
    def iterprev_nodup(
        self, key: Literal[True] = True, *, value: Literal[False]
    ) -> Iterator[_VT_co]: ...
    @overload
    def iterprev_nodup(
        self, key: Literal[False], value: bool = True
    ) -> Iterator[_VT_co]: ...

    #
    def _iter_from(
        self, k: Buffer, reverse: bool
    ) -> Iterator[tuple[_VT_co, _VT_co]]: ...

__all__ = [
    "Cursor",
    "Environment",
    "Transaction",
    "_Database",
    "enable_drop_gil",
    "version",
    "BadDbiError",
    "BadRslotError",
    "BadTxnError",
    "BadValsizeError",
    "CorruptedError",
    "CursorFullError",
    "DbsFullError",
    "DiskError",
    "Error",
    "IncompatibleError",
    "InvalidError",
    "InvalidParameterError",
    "KeyExistsError",
    "LockError",
    "MapFullError",
    "MapResizedError",
    "MemoryError",
    "NotFoundError",
    "PageFullError",
    "PageNotFoundError",
    "PanicError",
    "ReadersFullError",
    "ReadonlyError",
    "TlsFullError",
    "TxnFullError",
    "VersionMismatchError",
]

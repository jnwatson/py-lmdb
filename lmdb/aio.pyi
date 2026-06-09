import asyncio
from collections.abc import Awaitable, Generator, Iterable
from concurrent.futures import Executor
from types import TracebackType
from typing import Any, Final, Literal, overload

from _typeshed import Unused
from typing_extensions import Buffer, Generic, Self, TypeVar

from . import (
    Cursor,
    Environment,
    Transaction,
    _Database,
    _EnvFlagsDict,
    _InfoDict,
    _StatDict,
)

_DefaultT = TypeVar("_DefaultT", default=None)
_T_co = TypeVar("_T_co", covariant=True)
_VT = TypeVar("_VT", bound=bytes | memoryview)
_VT_co = TypeVar(
    "_VT_co",
    bound=bytes | memoryview,
    default=bytes | memoryview,
    covariant=True,
)

###

def wrap(env: Environment, executor: Executor | None = None) -> AsyncEnvironment: ...

# undocumented
class _AsyncContextWrapper(Generic[_T_co]):
    __slots__ = "_coro", "_result"

    _coro: Awaitable[_T_co]
    _result: _T_co | None

    def __init__(self, coro: Awaitable[_T_co]) -> None: ...
    def __await__(self) -> Generator[Any, Any, _T_co]: ...
    async def __aenter__(self) -> _T_co: ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
        /,
    ) -> None: ...

class AsyncEnvironment:
    __slots__ = "_env", "_executor"

    _env: Final[Environment]
    _executor: Final[Executor | None]

    def __init__(self, env: Environment, executor: Executor | None = None) -> None: ...
    async def __aenter__(self) -> Self: ...
    async def __aexit__(self, *_exc: Unused) -> None: ...

    #
    @overload
    def begin(
        self,
        db: _Database | None = None,
        parent: Transaction | None = None,
        write: bool = False,
        buffers: Literal[False] = False,
    ) -> _AsyncContextWrapper[AsyncTransaction[bytes]]: ...
    @overload
    def begin(
        self,
        db: _Database | None,
        parent: Transaction | None,
        write: bool,
        buffers: Literal[True],
    ) -> _AsyncContextWrapper[AsyncTransaction[memoryview]]: ...
    @overload
    def begin(
        self,
        db: _Database | None = None,
        parent: Transaction | None = None,
        write: bool = False,
        *,
        buffers: Literal[True],
    ) -> _AsyncContextWrapper[AsyncTransaction[memoryview]]: ...

    # proxied sync methods

    def path(self) -> str: ...
    def max_key_size(self) -> int: ...
    def max_readers(self) -> int: ...
    def flags(self) -> _EnvFlagsDict: ...

    # proxied async methods

    async def stat(self) -> _StatDict: ...
    async def info(self) -> _InfoDict: ...
    async def close(self) -> None: ...
    async def copy(
        self, path: str, compact: bool = False, txn: Transaction | None = None
    ) -> None: ...
    async def copyfd(
        self, fd: int, compact: bool = False, txn: Transaction | None = None
    ) -> None: ...
    async def sync(self, force: bool = False) -> None: ...
    async def readers(self) -> str: ...
    async def reader_check(self) -> int: ...
    async def set_mapsize(self, map_size: int) -> None: ...
    async def open_db(
        self,
        key: bytes | None = None,
        txn: Transaction | None = None,
        reverse_key: bool = False,
        dupsort: bool = False,
        create: bool = True,
        integerkey: bool = False,
        integerdup: bool = False,
        dupfixed: bool = False,
    ) -> _Database: ...
    #
    @overload
    async def dbs(self, txn: None = None) -> list[bytes]: ...
    @overload
    async def dbs(self, txn: Transaction[_VT]) -> list[_VT]: ...

class AsyncTransaction(Generic[_VT_co]):
    __slots__ = "_txn", "_executor", "_lock"

    _txn: Transaction[_VT_co]
    _executor: Final[Executor | None]
    _lock: Final[asyncio.Lock]

    def __init__(
        self, txn: Transaction[_VT_co], executor: Executor | None = None
    ) -> None: ...
    async def __aenter__(self) -> Self: ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
        /,
    ) -> None: ...
    def cursor(
        self, db: _Database | None = None
    ) -> _AsyncContextWrapper[AsyncCursor[_VT_co]]: ...

    # proxied sync methods

    def id(self) -> int: ...

    # proxied async methods

    async def stat(self, db: _Database | None = None) -> _StatDict: ...
    async def drop(self, db: _Database, delete: bool = True) -> None: ...
    async def commit(self) -> None: ...
    async def abort(self) -> None: ...

    #
    async def get(
        self, key: Buffer, default: _DefaultT | None = None, db: _Database | None = None
    ) -> _VT_co | _DefaultT: ...

    #
    async def put(
        self,
        key: Buffer,
        value: Buffer,
        dupdata: bool = True,
        overwrite: bool = True,
        append: bool = False,
        db: _Database | None = None,
    ) -> bool: ...
    async def replace(
        self, key: Buffer, value: Buffer, db: _Database | None = None
    ) -> _VT_co | None: ...
    async def pop(self, key: Buffer, db: _Database | None = None) -> _VT_co | None: ...
    async def delete(
        self, key: Buffer, value: Buffer = b"", db: _Database | None = None
    ) -> bool: ...

class AsyncCursor(Generic[_VT_co]):
    __slots__ = "_cursor", "_executor", "_lock"

    _cursor: Cursor[_VT_co]
    _executor: Final[Executor | None]
    _lock: Final[asyncio.Lock]

    def __init__(
        self,
        cursor: Cursor[_VT_co],
        executor: Executor | None = None,
        lock: asyncio.Lock | None = None,
    ) -> None: ...
    async def __aenter__(self) -> Self: ...
    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
        /,
    ) -> None: ...

    #
    @overload
    async def iternext(
        self, *, keys: Literal[True] = True, values: Literal[True] = True
    ) -> list[tuple[_VT_co, _VT_co]]: ...
    @overload
    async def iternext(
        self, *, keys: Literal[True] = True, values: Literal[False]
    ) -> list[_VT_co]: ...
    @overload
    async def iternext(
        self, *, keys: Literal[False], values: Literal[True] = True
    ) -> list[_VT_co]: ...

    # keep in sync with `iternext`
    @overload
    async def iternext_dup(
        self, *, keys: Literal[True], values: Literal[True] = True
    ) -> list[tuple[_VT_co, _VT_co]]: ...
    @overload
    async def iternext_dup(
        self, *, keys: Literal[True], values: Literal[False]
    ) -> list[_VT_co]: ...
    @overload
    async def iternext_dup(
        self, *, keys: Literal[False] = False, values: Literal[True] = True
    ) -> list[_VT_co]: ...

    # keep in sync with `iternext`
    @overload
    async def iternext_nodup(
        self, *, keys: Literal[True] = True, values: Literal[True]
    ) -> list[tuple[_VT_co, _VT_co]]: ...
    @overload
    async def iternext_nodup(
        self, *, keys: Literal[True] = True, values: Literal[False] = False
    ) -> list[_VT_co]: ...
    @overload
    async def iternext_nodup(
        self, *, keys: Literal[False], values: Literal[True]
    ) -> list[_VT_co]: ...

    # keep in sync with `iternext`
    @overload
    async def iterprev(
        self, *, keys: Literal[True] = True, values: Literal[True] = True
    ) -> list[tuple[_VT_co, _VT_co]]: ...
    @overload
    async def iterprev(
        self, *, keys: Literal[True] = True, values: Literal[False]
    ) -> list[_VT_co]: ...
    @overload
    async def iterprev(
        self, *, keys: Literal[False], values: Literal[True] = True
    ) -> list[_VT_co]: ...

    # keep in sync with `iternext`
    @overload
    async def iterprev_dup(
        self, *, keys: Literal[True], values: Literal[True] = True
    ) -> list[tuple[_VT_co, _VT_co]]: ...
    @overload
    async def iterprev_dup(
        self, *, keys: Literal[True], values: Literal[False]
    ) -> list[_VT_co]: ...
    @overload
    async def iterprev_dup(
        self, *, keys: Literal[False] = False, values: Literal[True] = True
    ) -> list[_VT_co]: ...

    # keep in sync with `iternext`
    @overload
    async def iterprev_nodup(
        self, *, keys: Literal[True] = True, values: Literal[True]
    ) -> list[tuple[_VT_co, _VT_co]]: ...
    @overload
    async def iterprev_nodup(
        self, *, keys: Literal[True] = True, values: Literal[False] = False
    ) -> list[_VT_co]: ...
    @overload
    async def iterprev_nodup(
        self, *, keys: Literal[False], values: Literal[True]
    ) -> list[_VT_co]: ...

    # proxied sync methods

    def key(self) -> _VT_co: ...
    def value(self) -> _VT_co: ...
    def item(self) -> tuple[_VT_co, _VT_co]: ...

    # proxied async methods

    async def close(self) -> None: ...
    async def first(self) -> bool: ...
    async def first_dup(self) -> bool: ...
    async def last(self) -> bool: ...
    async def last_dup(self) -> bool: ...
    async def prev(self) -> bool: ...
    async def prev_dup(self) -> bool: ...
    async def prev_nodup(self) -> bool: ...
    async def next(self) -> bool: ...
    async def next_dup(self) -> bool: ...
    async def next_nodup(self) -> bool: ...
    async def set_key(self, key: Buffer, /) -> bool: ...
    async def set_key_dup(self, key: Buffer, value: Buffer) -> bool: ...
    async def set_range(self, key: Buffer, /) -> bool: ...
    async def set_range_dup(self, key: Buffer, value: Buffer) -> bool: ...
    async def delete(self, dupdata: bool = False) -> bool: ...
    async def count(self) -> int: ...
    async def put(
        self,
        key: Buffer,
        val: Buffer,
        dupdata: bool = True,
        overwrite: bool = True,
        append: bool = False,
    ) -> bool: ...
    async def putmulti(
        self,
        items: Iterable[tuple[Buffer, Buffer]],
        dupdata: bool = True,
        overwrite: bool = True,
        append: bool = False,
    ) -> tuple[int, int]: ...
    async def replace(self, key: Buffer, val: Buffer) -> _VT_co | None: ...
    async def pop(self, key: Buffer) -> _VT_co | None: ...

    #
    async def get(
        self, key: Buffer, default: _DefaultT | None = None
    ) -> _VT_co | _DefaultT: ...

    #
    @overload  # keyfixed=False (default), values=True (default)
    async def getmulti(
        self,
        keys: Iterable[Buffer],
        dupdata: bool = False,
        dupfixed_bytes: int | None = None,
        keyfixed: Literal[False] = False,
        values: Literal[True] = True,
    ) -> list[tuple[_VT_co, _VT_co]]: ...
    @overload  # keyfixed=False (default), values=False
    async def getmulti(
        self,
        keys: Iterable[Buffer],
        dupdata: bool = False,
        dupfixed_bytes: int | None = None,
        keyfixed: Literal[False] = False,
        *,
        values: Literal[False],
    ) -> list[_VT_co]: ...
    @overload  # keyfixed=True  (positional)
    async def getmulti(
        self,
        keys: Iterable[Buffer],
        dupdata: bool,
        dupfixed_bytes: int,
        keyfixed: Literal[True],
        values: bool = True,
    ) -> memoryview: ...
    @overload  # keyfixed=True  (keyword)
    async def getmulti(
        self,
        keys: Iterable[Buffer],
        dupdata: bool = False,
        *,
        dupfixed_bytes: int,
        keyfixed: Literal[True],
        values: bool = True,
    ) -> memoryview: ...

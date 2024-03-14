import asyncio
import inspect
import json
import logging
import shelve
import threading
import typeguard

from dbm import gnu as gdbm

from datetime import datetime
from pathlib import Path

from typing import (
    Any,
    Callable,
    Dict,
    List,
    Generic,
    Optional,
    overload,
    Union,
    ParamSpec,
    Protocol,
    TypeVar,
)

lg = logging.getLogger('novi')

P = ParamSpec('P')
T = TypeVar('T')
R = TypeVar('R')


def try_await(obj):
    if not inspect.isawaitable(obj):
        return obj

    try:
        loop = asyncio.get_running_loop()
        return loop.run_until_complete(obj)
    except RuntimeError:
        return asyncio.run(obj)


class NoviError(Exception):
    error_core: int
    kind: str
    message: str

    def __init__(self, error_code: int, kind: str, message: str):
        super().__init__(message)

        self.error_code = error_code
        self.kind = kind
        self.message = message


class TagValue:
    value: Optional[str]
    updated: datetime


Tags = Dict[str, Optional[str]]
RpcArgs = Dict[str, Any]

plugin_db = None
plugin_db_lock = threading.Lock()


class BoxedFuture(Protocol, Generic[T]):
    def block(self) -> T: ...

    async def coroutine(self) -> T: ...


class BaseClient:
    def __init__(self, impl):
        self.impl = impl

    def _subscribe(
        self,
        obj_wrapper,
        filter: str,
        callback,
        checkpoint: Optional[datetime],
        kinds: List[str],
        with_history: bool,
        exclude_unrelated: bool,
        once: bool,
        once_key: Optional[str],
    ) -> BoxedFuture[str]:
        if once and once_key is None:
            once_key = callback.__name__

        once_lock = threading.Lock()

        def wrapper(obj, kind):
            if kind in kinds:
                obj = obj_wrapper(obj)
                resp = callback(obj, kind)
                resp = try_await(resp)

                if once:
                    once_lock.acquire()
                    plugin_db[f'novi.ckpt:{once_key}'] = obj.updated
                    once_lock.release()

        if once:
            if checkpoint:
                raise ValueError(
                    'forward_only_key and checkpoint cannot be used together'
                )

            if not with_history:
                lg.warn(
                    'forward-only subscribing with with_history set to False is meaningless'
                )

            else:
                old_filter = self.db.get(f'novi.filter:{once_key}', None)
                if old_filter and old_filter == filter:
                    checkpoint = self.db[f'novi.ckpt:{once_key}']
                    self.db[f'novi.filter:{once_key}'] = filter

        return self.impl.subscribe(
            filter, wrapper, checkpoint, with_history, exclude_unrelated
        )

    def unsubscribe(self, id: str):
        self.impl.unsubscribe(id).block()

    def register_rpc(
        self, name: str, callback: Callable[[str, RpcArgs], Any]
    ) -> BoxedFuture[str]:
        def wrapper(name: str, args: RpcArgs):
            res = callback(name, args)
            if hasattr(res, 'model_dump_json'):
                return res.model_dump_json()
            else:
                return json.dumps(res)

        return self.impl.register_rpc(name, wrapper).block()

    def unregister_rpc(self, name: str):
        self.impl.unregister_rpc(name).block()

    @property
    def root_path(self) -> Path:
        return Path(self.impl.root_path())

    @property
    def object_storage_path(self) -> Path:
        return self.root_path / 'storage'

    @property
    def data_path(self) -> Path:
        path = Path('data')
        path.mkdir(exist_ok=True)
        return path

    def open_db(self, name: str) -> shelve.Shelf:
        db = gdbm.open(str(self.data_path / name), 'c')
        return shelve.Shelf(db)

    @property
    def db(self) -> shelve.Shelf:
        global plugin_db, plugin_db_lock

        if plugin_db:
            return plugin_db

        # Why we need a lock here:
        # open_db contains native methods that can allow
        # GIL to be released, which can cause re-entrance
        plugin_db_lock.acquire()
        try:
            if plugin_db is None:
                plugin_db = self.open_db('plugin')
        finally:
            plugin_db_lock.release()

        return plugin_db

    def rpc(
        self,
        name: str,
        *,
        explicit: bool = False,
        type_check: bool = True,
        sync: bool = True,
    ):
        def decorator(cb):
            if type_check:
                cb = typeguard.typechecked(cb)

            if sync:
                # TODO a better way to do this
                cb_before_sync = cb
                lock = threading.Lock()

                def wrapper(*args, **kwargs):
                    lock.acquire()
                    try:
                        result = cb_before_sync(*args, **kwargs)
                    finally:
                        lock.release()

                    return result

                cb = wrapper

            res_cb = cb

            if not explicit:
                cb_before_explicit = cb
                cb = lambda _, args: cb_before_explicit(**args)

            cb_before_await = cb
            cb = lambda name, args: try_await(cb_before_await(name, args))

            self.register_rpc(name, cb)

            return res_cb

        return decorator

    def subs(self, filter: str, **kwargs):
        def decorator(cb):
            self.subscribe(filter, cb, **kwargs)
            return cb

        return decorator


class Client(BaseClient):
    def __init__(self, impl):
        super().__init__(impl)

    def to_async(self) -> 'AsyncClient':
        return AsyncClient(self.impl)

    def add_object(self, tags: Tags) -> 'Object':
        return Object(self.impl.add_object(tags).block(), self)

    def get_object(self, id: str) -> 'Object':
        return Object(self.impl.get_object(id).block(), self)

    def set_object_tag(
        self,
        id: str,
        tag: str,
        value: Optional[str] = None,
        *,
        force_update: bool = False,
    ) -> 'Object':
        return Object(
            self.impl.set_object_tags(id, {tag: value}, force_update).block(), self
        )

    def set_object_tags(
        self, id: str, tags: Tags, *, force_update: bool = False
    ) -> 'Object':
        return Object(self.impl.set_object_tags(id, tags, force_update).block(), self)

    def delete_object_tag(self, id: str, tag: str) -> 'Object':
        return Object(self.impl.delete_object_tag(id, tag).block(), self)

    def delete_object(self, id: str):
        self.impl.delete_object(id).block()

    def query(
        self,
        filter: str = '',
        *,
        checkpoint: Optional[datetime] = None,
        updated_after: Optional[datetime] = None,
        updated_before: Optional[datetime] = None,
        created_after: Optional[datetime] = None,
        created_before: Optional[datetime] = None,
        order: str = '-updated',
        limit: Optional[int] = 20,
    ) -> List['Object']:
        return [
            Object(obj, self)
            for obj in self.impl.query(
                filter,
                checkpoint,
                updated_after,
                updated_before,
                created_after,
                created_before,
                order,
                limit,
            ).block()
        ]

    def query_one(self, filter: str) -> 'Object':
        res = self.query(filter, limit=1)
        return res[0] if res else None

    def subscribe(
        self,
        filter: str,
        callback: Callable[['Object', str], None],
        *,
        checkpoint: Optional[datetime] = None,
        kinds: List[str] = ['created', 'updated'],
        with_history: bool = True,
        exclude_unrelated: bool = True,
        once: bool = True,
        once_key: Optional[str] = None,
    ) -> str:
        return self._subscribe(
            lambda obj: Object(obj, self),
            filter,
            callback,
            checkpoint,
            kinds,
            with_history,
            exclude_unrelated,
            once,
            once_key,
        ).block()

    def call(self, name: str, timeout: Optional[float] = None, **kwargs) -> Any:
        return self.impl.call(name, kwargs, timeout).block()

    def call_explicit(
        self, name: str, args: RpcArgs, *, timeout: Optional[float] = None
    ) -> Any:
        return self.impl.call(name, args, timeout).block()


class AsyncClient(BaseClient):
    def __init__(self, impl):
        super().__init__(impl)

    def to_sync(self) -> Client:
        return Client(self.impl)

    async def add_object(self, tags: Tags) -> 'AsyncObject':
        return AsyncObject(await self.impl.add_object(tags).coroutine(), self)

    async def get_object(self, id: str) -> 'AsyncObject':
        return AsyncObject(await self.impl.get_object(id).coroutine(), self)

    async def set_object_tag(
        self,
        id: str,
        tag: str,
        value: Optional[str] = None,
        *,
        force_update: bool = False,
    ) -> 'AsyncObject':
        return AsyncObject(
            await self.impl.set_object_tags(id, {tag: value}, force_update).coroutine(),
            self,
        )

    async def set_object_tags(
        self, id: str, tags: Tags, *, force_update: bool = False
    ) -> 'AsyncObject':
        return AsyncObject(
            await self.impl.set_object_tags(id, tags, force_update).coroutine(), self
        )

    async def delete_object_tag(self, id: str, tag: str) -> 'AsyncObject':
        return AsyncObject(await self.impl.delete_object_tag(id, tag).coroutine(), self)

    async def delete_object(self, id: str):
        await self.impl.delete_object(id).coroutine()

    async def query(
        self,
        filter: str = '',
        *,
        checkpoint: Optional[datetime] = None,
        updated_after: Optional[datetime] = None,
        updated_before: Optional[datetime] = None,
        created_after: Optional[datetime] = None,
        created_before: Optional[datetime] = None,
        order: str = '-updated',
        limit: Optional[int] = 20,
    ) -> List['AsyncObject']:
        return [
            AsyncObject(obj, self)
            for obj in await self.impl.query(
                filter,
                checkpoint,
                updated_after,
                updated_before,
                created_after,
                created_before,
                order,
                limit,
            ).coroutine()
        ]

    async def query_one(self, filter: str) -> 'AsyncObject':
        res = await self.query(filter, limit=1)
        return res[0] if res else None

    def subscribe(
        self,
        filter: str,
        callback: Callable[['Object', str], None],
        *,
        checkpoint: Optional[datetime] = None,
        kinds: List[str] = ['created', 'updated'],
        with_history: bool = True,
        exclude_unrelated: bool = True,
        once: bool = True,
        once_key: Optional[str] = None,
    ) -> str:
        return self._subscribe(
            lambda obj: AsyncObject(obj, self),
            filter,
            callback,
            checkpoint,
            kinds,
            with_history,
            exclude_unrelated,
            once,
            once_key,
        ).block()

    async def call(self, name: str, timeout: Optional[float] = None, **kwargs) -> Any:
        return await self.impl.call(name, kwargs, timeout).coroutine()

    async def call_explicit(
        self, name: str, args: RpcArgs, *, timeout: Optional[float] = None
    ) -> Any:
        return await self.impl.call(name, args, timeout).coroutine()


class ObjectBase:
    def __init__(self, impl, client):
        self.impl = impl
        self.client = client

    @property
    def id(self) -> str:
        return self.impl.id()

    @property
    def creator(self) -> Optional[str]:
        return self.impl.creator()

    @property
    def created(self) -> datetime:
        return self.impl.created()

    @property
    def updated(self) -> datetime:
        return self.impl.updated()

    @property
    def tags(self) -> Dict[str, TagValue]:
        return self.impl.tags()

    def to_dict(self) -> Dict[str, Any]:
        return self.impl.to_dict()

    def to_json(self) -> str:
        return self.impl.to_json()

    def keys(self):
        return self.tags.keys()

    def get(self, tag: str) -> TagValue:
        return self.impl.get(tag)

    def get_opt(self, tag: str) -> Optional[TagValue]:
        try:
            return self.get(tag)
        except:
            return None

    def has(self, tag: str) -> bool:
        return self.impl.has(tag)

    def __getitem__(self, tag: str) -> Optional[str]:
        return self.get(tag).value

    def __contains__(self, tag: str) -> bool:
        return self.has(tag)

    @property
    def path(self) -> Path:
        return self.client.object_storage_path / self.id

    @property
    def thumbnail_path(self) -> Path:
        return self.path.with_suffix('.thumb.jpg')

    def __str__(self) -> str:
        return f'Object(id={self.id})'

    def __repr__(self) -> str:
        return f'Object(id={self.id}, tags={repr(self.tags)})'


class Object(ObjectBase):
    @overload
    def set(
        self, tag: str, value: Optional[str] = None, *, force_update: bool = False
    ) -> 'Object': ...

    @overload
    def set(self, tags: Tags, force_update: bool = False) -> 'Object': ...

    def set(self, tag, value=None, *, force_update=False):
        if isinstance(tag, str):
            tag = {tag: value}

        self.impl = self.client.set_object_tags(
            self.id, tag, force_update=force_update
        ).impl

    # TODO i dont think overloading two functions with different usage is a good idea
    @overload
    def delete(self, tag: str) -> 'Object': ...
    @overload
    def delete(self): ...

    def delete(self, tag=None):
        if tag:
            self.impl = self.client.delete_object_tag(self.id, tag).impl
        else:
            self.client.delete_object(self.id)


class AsyncObject(ObjectBase):
    @overload
    async def set(
        self, tag: str, value: Optional[str] = None, *, force_update: bool = False
    ) -> 'AsyncObject': ...

    @overload
    async def set(self, tags: Tags, force_update: bool = False) -> 'AsyncObject': ...

    async def set(self, tag, value=None, *, force_update=False):
        if isinstance(tag, str):
            tag = {tag: value}

        self.impl = (
            await self.client.set_object_tags(self.id, tag, force_update=force_update)
        ).impl

    # TODO i dont think overloading two functions with different usage is a good idea
    @overload
    async def delete(self, tag: str) -> 'AsyncObject': ...
    @overload
    async def delete(self): ...

    async def delete(self, tag=None):
        if tag:
            self.impl = (await self.client.delete_object_tag(self.id, tag)).impl
        else:
            await self.client.delete_object(self.id)


def move(src: Union[Path, str], dst: Union[Path, str], /):
    if isinstance(src, str):
        src = Path(src)

    try:
        src.replace(dst)
    except:
        import shutil

        shutil.move(str(src), str(dst))
        src.unlink()


core = _core
guest_client = Client(_guest_client)
client = Client(_internal_client)
aclient = client.to_async()


def login(name: str, password: str) -> Client:
    return Client(core.login(name, password).block())


async def alogin(name: str, password: str) -> AsyncClient:
    return Client(await core.login(name, password).coroutine())


__all__ = [
    'AsyncClient',
    'Client',
    'AsyncObject',
    'Object',
    'TagValue',
    'Tags',
    'RpcArgs',
    'NoviError',
    'login',
    'alogin',
    'guest_client',
    'client',
    'aclient',
    'move',
]

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
    Optional,
    overload,
    Union,
    ParamSpec,
    TypeVar,
)

lg = logging.getLogger('novi')

P = ParamSpec('P')
T = TypeVar('T')


class TagValue:
    value: Optional[str]
    updated: datetime


Tags = Dict[str, Optional[str]]
RpcArgs = Dict[str, any]


class Client:
    def __init__(self, impl):
        self.impl = impl
        self.plugin_db = None
        self.plugin_db_lock = threading.Lock()

    def add_object(self, tags: Tags) -> 'Object':
        return Object(self.impl.add_object(tags), self)

    def get_object(self, id: str) -> Optional['Object']:
        obj = self.impl.get_object(id)
        return Object(obj, self) if obj else None

    def set_object_tag(
        self,
        id: str,
        tag: str,
        value: Optional[str] = None,
        *,
        force_update: bool = False,
    ) -> 'Object':
        return self.set_object_tags(id, {tag: value}, force_update=force_update)

    def set_object_tags(
        self, id: str, tags: Tags, *, force_update: bool = False
    ) -> 'Object':
        return Object(self.impl.set_object_tags(id, tags, force_update), self)

    def delete_object_tag(self, id: str, tag: str) -> 'Object':
        return Object(self.impl.delete_object_tag(id, tag), self)

    def delete_object(self, id: str) -> None:
        self.impl.delete_object(id)

    def query(
        self,
        filter: str,
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
            )
        ]

    def query_one(self, filter: str) -> Optional['Object']:
        objects = self.query(filter, limit=1)
        return objects[0] if objects else None

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
        if once and once_key is None:
            once_key = callback.__name__

        once_lock = threading.Lock()

        def wrapper(obj, kind):
            if kind in kinds:
                obj = Object(obj, self)
                callback(obj, kind)

                if once:
                    once_lock.acquire()
                    self.plugin_db[f'novi.ckpt:{once_key}'] = obj.updated
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

    def unsubscribe(self, id: str) -> None:
        self.impl.unsubscribe(id)

    def call(self, name: str, timeout: Optional[float] = None, **kwargs) -> Any:
        return self.impl.call(name, kwargs, timeout)

    def call_explicit(
        self, name: str, args: RpcArgs, *, timeout: Optional[float] = None
    ) -> Any:
        return self.impl.call(name, args, timeout)

    def register_rpc(self, name: str, callback: Callable[[str, RpcArgs], any]) -> str:
        def wrapper(name: str, args: RpcArgs):
            res = callback(name, args)
            if hasattr(res, 'model_dump_json'):
                return res.model_dump_json()
            else:
                return json.dumps(res)

        return self.impl.register_rpc(name, wrapper)

    def unregister_rpc(self, name: str) -> None:
        self.impl.unregister_rpc(name)

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
        if self.plugin_db:
            return self.plugin_db

        # Why we need a lock here:
        # open_db contains native methods that can allow
        # GIL to be released, which can cause re-entrance
        self.plugin_db_lock.acquire()
        try:
            if self.plugin_db is None:
                self.plugin_db = self.open_db('plugin')
        finally:
            self.plugin_db_lock.release()

        return self.plugin_db

    def subs(self, filter: str, **kwargs):
        return lambda cb: self.subscribe(filter, cb, **kwargs)

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

            self.register_rpc(name, cb)

            return res_cb

        return decorator


client = Client(_impl)


class Object:
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

    def keys(self):
        return self.tags.keys()

    def get(self, tag: str) -> TagValue:
        return self.impl.get(tag)

    def get_opt(self, tag: str) -> Optional[TagValue]:
        try:
            return self.get(tag)
        except:
            return None

    @overload
    def set(
        self, tag: str, value: Optional[str] = None, *, force_update: bool = False
    ) -> 'Object': ...
    @overload
    def set(self, tags: Tags, force_update: bool = False) -> 'Object': ...

    def set(self, tag, value=None, *, force_update=False):
        if isinstance(tag, str):
            tag = {tag: value}

        self.impl = self.client.set_object_tags(self.id, tag, force_update).impl

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

    def has(self, tag: str) -> bool:
        return self.impl.has(tag)

    def __getitem__(self, tag: str) -> Optional[str]:
        return self.get(tag).value

    def __setitem__(self, tag: str, value: Optional[str]) -> None:
        self.set(tag, value)

    def __delitem__(self, tag: str) -> None:
        self.delete(tag)

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


def move(src: Union[Path, str], dst: Union[Path, str], /):
    if isinstance(src, str):
        src = Path(src)

    try:
        src.replace(dst)
    except:
        import shutil

        shutil.move(str(src), str(dst))
        src.unlink()


__all__ = ['Client', 'Object', 'TagValue', 'move']

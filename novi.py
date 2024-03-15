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


# TODO check this
def try_await(obj):
    if not inspect.isawaitable(obj):
        return obj

    try:
        loop = asyncio.get_running_loop()
        return loop.run_until_complete(obj)
    except RuntimeError:
        return asyncio.run(obj)


def copy_docstring(from_func):
    def decorator(func):
        func.__doc__ = from_func.__doc__
        return func

    return decorator


class NoviError(Exception):
    """Error type in Novi.

    The error code always matches kind.
    """

    error_code: int
    kind: str
    message: str

    def __init__(self, error_code: int, kind: str, message: str):
        super().__init__(message)

        self.error_code = error_code
        self.kind = kind
        self.message = message


class TagValue:
    """A tag of object consists of nullable string value and the last updated time.

    Retrievable from `Object.get` method or `Object.tags[key]`.
    Notice that `Object[key]` returns the string value instead of `TagValue` object."""

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
        kinds: List[str],
        checkpoint: Optional[datetime],
        with_history: bool,
        exclude_unrelated: bool,
        once: bool,
        once_key: Optional[str],
    ) -> BoxedFuture[str]:
        """Subscribes to object changes.

        Callback will be called when an object matches the filter.
        The callback should have a signature of `(obj: Object, kind: str)`,
        where `kind` is one of `created`, `updated`, `deleted`.

        Args:
            filter: A filter string to match objects. See more in docs.
            callback: A callback function to be called.
            kinds:
                A list of kinds to be matched.
                Default is `['created', 'updated']`.
            checkpoint:
                If specified, only objects updated after the checkpoint will
                be matched.
                Should be used with `with_history` set to True, otherwise
                it's meaningless.
                When `once` is set to True (which is the default behavior),
                the checkpoint is automatically saved and restored when the
                subscription is created.
            with_history:
                When set to True, all objects that match the filter will be
                sent to the callback once when the subscription is created.
                Otherwise, only objects that updated afterwards will be sent.
                Default is True.
            exclude_unrelated:
                When set to True, only object updates that are revant to the
                filter will be sent. By 'relevant', it means the updated tags
                intersect with the filter's tags.
                For example, when the filter is `t1 t2` and `exclude_unrelated`
                is True,
                    - An update from `{t1, t2}` to `{t1, t2, t3}` WON'T be sent.
                    - An update from `{t1, t2}` to `{t1}` will be sent.
                Default is True.
            once:
                When set to True, the checkpoint will be automatically saved
                and restored, ensuring that the callback will only be called
                once for each object update.
                Use `once_key` to specify a custom key for the checkpoint.
                Default is True.
            once_key:
                See `once`.
                Defaults to the name of the callback function.

        Returns:
            A subscription ID that can be used to unsubscribe.

        See also:
            `unsubscribe`
        """

        if checkpoint and not with_history:
            lg.warn('once mode with with_history set to False is meaningless')

        if once and once_key is None:
            once_key = callback.__name__
            if once_key == '<lambda>':
                raise ValueError('once_key must be specified when using lambda')

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
                raise ValueError('once mode and checkpoint cannot be used together')

            else:
                old_filter = self.db.get(f'novi.filter:{once_key}', None)
                if old_filter and old_filter == filter:
                    checkpoint = self.db[f'novi.ckpt:{once_key}']
                    self.db[f'novi.filter:{once_key}'] = filter

        return self.impl.subscribe(
            filter, wrapper, checkpoint, with_history, exclude_unrelated
        )

    def unsubscribe(self, id: str):
        """Unsubscribes from object changes."""
        self.impl.unsubscribe(id).block()

    def register_rpc(self, name: str, callback: Callable[[str, RpcArgs], Any]):
        """Registers a new RPC endpoint.

        The name should be unique. The callback receives a JSON object and
        returns JSON-serializable object.

        Args:
            name: The name of the RPC endpoint.
            callback:
                The callback function to be called when the RPC is called.
                Should have a signature of `(name: str, args: RpcArgs) -> Any`.
        """

        def wrapper(name: str, args: RpcArgs):
            res = callback(name, args)
            if hasattr(res, 'model_dump_json'):
                return res.model_dump_json()
            else:
                return json.dumps(res)

        return self.impl.register_rpc(name, wrapper).block()

    def unregister_rpc(self, name: str):
        """Unregisters an RPC endpoint."""
        self.impl.unregister_rpc(name).block()

    @property
    def root_path(self) -> Path:
        """The root path of the Novi instance."""
        return Path(self.impl.root_path())

    @property
    def object_storage_path(self) -> Path:
        """The path where object files are stored.

        **SUBJECT TO CHANGE**

        Do not read under this path directly. Use `Object.open` instead."""
        return self.root_path / 'storage'

    @property
    def data_path(self) -> Path:
        """The path where the plugin's data is stored."""
        path = Path('data')
        path.mkdir(exist_ok=True)
        return path

    def open_db(self, name: str) -> shelve.Shelf:
        """Opens a shelve database under the data path."""
        db = gdbm.open(str(self.data_path / name), 'c')
        return shelve.Shelf(db)

    @property
    def db(self) -> shelve.Shelf:
        """The default database for the plugin."""
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
        """Decorator for registering an RPC endpoint.

        Args:
            explicit:
                When set to True, the callback will receive the name and
                arguments separately. Otherwise, arguments will be passed
                in kwargs.
                Default is False.
            type_check:
                When set to True, the arguments will be type-checked.
                Default is True.
            sync:
                When set to True, the callback will be wrapped with a lock,
                ensuring that only one call can be processed at a time.
                Default is True.

        See also:
            `register_rpc`
        """

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
        """Decorator for subscribing to object changes.

        See also:
            `subscribe`"""

        def decorator(cb):
            self.subscribe(filter, cb, **kwargs)
            return cb

        return decorator


class Client(BaseClient):
    """The client containing the main API for Novi.

    See also:
        `AsyncClient`"""

    def __init__(self, impl):
        super().__init__(impl)

    def to_async(self) -> 'AsyncClient':
        """Converts the client to an async client."""
        return AsyncClient(self.impl)

    def add_object(self, tags: Tags) -> 'Object':
        """Adds a new object to the database.

        Args:
            tags: A dictionary of tags (Dict[str, Optional[str]]).

        Returns:
            The newly created object."""

        return Object(self.impl.add_object(tags).block(), self)

    def get_object(self, id: str) -> 'Object':
        """Gets an object by ID.

        Args:
            id: The ID of the object (UUID).

        Returns:
            The object with the given ID.

        Raises:
            `NoviError` (ObjectNotFound) if the object does not exist."""
        return Object(self.impl.get_object(id).block(), self)

    def set_object_tag(
        self,
        id: str,
        tag: str,
        value: Optional[str] = None,
        *,
        force_update: bool = False,
    ) -> 'Object':
        """Sets a tag of an object.

        Args:
            id: The ID of the object (UUID).
            tag: The tag name.
            value: The tag value. Default is None.
            force_update:
                When set to True, the tag's updated time will be updated even
                if the value is not changed.
                Default is False.

        Returns:
            The updated object.

        See also:
            `set_object_tags`
        """

        return Object(
            self.impl.set_object_tags(id, {tag: value}, force_update).block(), self
        )

    def set_object_tags(
        self, id: str, tags: Tags, *, force_update: bool = False
    ) -> 'Object':
        """Sets multiple tags of an object. (patching)

        Args:
            id: The ID of the object (UUID).
            tags: A dictionary of tags (Dict[str, Optional[str]]).
            force_update:
                When set to True, the tag's updated time will be updated even
                if the value is not changed.
                Default is False.

        Returns:
            The updated object.

        See also:
            `set_object_tag`
        """

        return Object(self.impl.set_object_tags(id, tags, force_update).block(), self)

    def delete_object_tag(self, id: str, tag: str) -> 'Object':
        """Deletes a tag of an object.

        Args:
            id: The ID of the object (UUID).
            tag: The tag name.

        Returns:
            The updated object."""
        return Object(self.impl.delete_object_tag(id, tag).block(), self)

    def delete_object(self, id: str):
        """Deletes an object.

        Args:
            id: The ID of the object (UUID)."""
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
        """Queries objects.

        Args:
            filter: A filter string to match objects. See more in docs.
            checkpoint: Used as anchor for update queries. See more in docs.
            updated_after, updated_before, created_after, created_before:
                Specify the time range for the query. All inclusive.
            order:
                The order of the results. Default is `-updated`.
            limit:
                The maximum number of results to return. Set to None to
                return all results.
                Default is 20.

        Returns:
            A list of objects that match the query."""

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
        """Queries one object.

        Args:
            filter: The filter string.

        Returns:
            The first object that matches the query.
            If there's no match, returns None."""

        res = self.query(filter, limit=1)
        return res[0] if res else None

    @copy_docstring(BaseClient._subscribe)
    def subscribe(
        self,
        filter: str,
        callback: Callable[['Object', str], None],
        *,
        kinds: List[str] = ['created', 'updated'],
        checkpoint: Optional[datetime] = None,
        with_history: bool = True,
        exclude_unrelated: bool = True,
        once: bool = True,
        once_key: Optional[str] = None,
    ) -> str:
        return self._subscribe(
            lambda obj: Object(obj, self),
            filter,
            callback,
            kinds,
            checkpoint,
            with_history,
            exclude_unrelated,
            once,
            once_key,
        ).block()

    def call(self, name: str, timeout: Optional[float] = None, **kwargs) -> Any:
        """Calls an RPC endpoint.

        Args:
            name: The name of the RPC endpoint.
            timeout: The maximum time (in seconds) to wait. Default is None.
            kwargs: The arguments to be passed to the RPC endpoint.

        Returns:
            The result of RPC.

        See also:
            `call_explicit`"""

        return self.impl.call(name, kwargs, timeout).block()

    def call_explicit(
        self, name: str, args: RpcArgs, *, timeout: Optional[float] = None
    ) -> Any:
        """Calls an RPC endpoint with explicit arguments.

        Args:
            name: The name of the RPC endpoint.
            args: The arguments to be passed to the RPC endpoint.
            timeout: The maximum time (in seconds) to wait. Default is None.

        Returns:
            The result of RPC.

        See also:
            `call`"""

        return self.impl.call(name, args, timeout).block()


class AsyncClient(BaseClient):
    """The async version of `Client`.

    See also:
        `Client`"""

    def __init__(self, impl):
        super().__init__(impl)

    def to_sync(self) -> Client:
        """Converts the client to a sync client."""
        return Client(self.impl)

    @copy_docstring(Client.add_object)
    async def add_object(self, tags: Tags) -> 'AsyncObject':
        return AsyncObject(await self.impl.add_object(tags).coroutine(), self)

    @copy_docstring(Client.get_object)
    async def get_object(self, id: str) -> 'AsyncObject':
        return AsyncObject(await self.impl.get_object(id).coroutine(), self)

    @copy_docstring(Client.set_object_tag)
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

    @copy_docstring(Client.set_object_tags)
    async def set_object_tags(
        self, id: str, tags: Tags, *, force_update: bool = False
    ) -> 'AsyncObject':
        return AsyncObject(
            await self.impl.set_object_tags(id, tags, force_update).coroutine(), self
        )

    @copy_docstring(Client.delete_object_tag)
    async def delete_object_tag(self, id: str, tag: str) -> 'AsyncObject':
        return AsyncObject(await self.impl.delete_object_tag(id, tag).coroutine(), self)

    @copy_docstring(Client.delete_object)
    async def delete_object(self, id: str):
        await self.impl.delete_object(id).coroutine()

    @copy_docstring(Client.query)
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

    @copy_docstring(Client.query_one)
    async def query_one(self, filter: str) -> 'AsyncObject':
        res = await self.query(filter, limit=1)
        return res[0] if res else None

    @copy_docstring(Client.subscribe)
    def subscribe(
        self,
        filter: str,
        callback: Callable[['Object', str], None],
        *,
        kinds: List[str] = ['created', 'updated'],
        checkpoint: Optional[datetime] = None,
        with_history: bool = True,
        exclude_unrelated: bool = True,
        once: bool = True,
        once_key: Optional[str] = None,
    ) -> str:
        return self._subscribe(
            lambda obj: AsyncObject(obj, self),
            filter,
            callback,
            kinds,
            checkpoint,
            with_history,
            exclude_unrelated,
            once,
            once_key,
        ).block()

    @copy_docstring(Client.call)
    async def call(self, name: str, timeout: Optional[float] = None, **kwargs) -> Any:
        return await self.impl.call(name, kwargs, timeout).coroutine()

    @copy_docstring(Client.call_explicit)
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
        """The ID of the object (UUID)."""
        return self.impl.id()

    @property
    def creator(self) -> Optional[str]:
        """The creator of the object."""
        return self.impl.creator()

    @property
    def created(self) -> datetime:
        """The time when the object was created."""
        return self.impl.created()

    @property
    def updated(self) -> datetime:
        """The time when the object was last updated."""
        return self.impl.updated()

    @property
    def tags(self) -> Dict[str, TagValue]:
        """The tags of the object (Dict[str, TagValue])."""
        return self.impl.tags()

    def to_dict(self) -> Dict[str, Any]:
        """Serializes the object to a dictionary."""
        return self.impl.to_dict()

    def to_json(self) -> str:
        """Serializes the object to a JSON string.
        Equivalent to `json.dumps(obj.to_dict())`, but might be faster since
        the work is done in native code."""

        return self.impl.to_json()

    def keys(self):
        """Returns the keys of the object's tags."""
        return self.tags.keys()

    def get(self, tag: str) -> TagValue:
        """Gets a tag (`TagValue`) of the object.

        Raises:
            `NoviError` (TagNotFound) if the tag does not exist.

        See also:
            `get_opt`"""

        return self.impl.get(tag)

    def get_opt(self, tag: str) -> Optional[TagValue]:
        """Gets a tag (`TagValue`) of the object, or returns None if not found.

        See also:
            `get`"""

        try:
            return self.get(tag)
        except NoviError as e:
            if e.kind == 'TagNotFound':
                return None

            raise e

    def has(self, tag: str) -> bool:
        """Checks if the object has a tag."""
        return self.impl.has(tag)

    def __getitem__(self, tag: str) -> Optional[str]:
        return self.get(tag).value

    def __contains__(self, tag: str) -> bool:
        return self.has(tag)

    @property
    def path(self) -> Path:
        """The path of the object file (Path).

        **SUBJECT TO CHANGE**"""
        return self.client.object_storage_path / self.id

    @property
    def thumbnail_path(self) -> Path:
        """The path of the object's thumbnail file (Path).

        **SUBJECT TO CHANGE**"""

        return self.path.with_suffix('.thumb.jpg')

    def __str__(self) -> str:
        return f'Object(id={self.id})'

    def __repr__(self) -> str:
        return f'Object(id={self.id}, tags={repr(self.tags)})'


class Object(ObjectBase):
    """The object in Novi.

    See also:
        `AsyncObject`"""

    @overload
    def set(
        self, tag: str, value: Optional[str] = None, *, force_update: bool = False
    ): ...

    @overload
    def set(self, tags: Tags, force_update: bool = False): ...

    def set(self, tag, value=None, *, force_update=False):
        """Sets a tag of the object.

        Examples:
        >>> obj.set('tag1', 'value1')
        >>> obj.set('tag2', force_update=False)
        >>> obj.set({'tag1': 'value1', 'tag2': 'value2'})
        """
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
        """Deletes ausing `typeguard` tag of the object, or the object itself
        if no tag is specified.

        Examples:
        >>> obj.delete('tag1')
        >>> obj.delete() # delete the object
        """
        if tag:
            self.impl = self.client.delete_object_tag(self.id, tag).impl
        else:
            self.client.delete_object(self.id)


class AsyncObject(ObjectBase):
    """The async version of `Object`.

    See also:
        `Object`"""

    @overload
    async def set(
        self, tag: str, value: Optional[str] = None, *, force_update: bool = False
    ): ...

    @overload
    async def set(self, tags: Tags, force_update: bool = False): ...

    @copy_docstring(Object.set)
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

    @copy_docstring(Object.delete)
    async def delete(self, tag=None):
        if tag:
            self.impl = (await self.client.delete_object_tag(self.id, tag)).impl
        else:
            await self.client.delete_object(self.id)


def move(src: Union[Path, str], dst: Union[Path, str], /):
    """Moves a file or directory to another location efficiently."""

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
client = Client(_client)
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

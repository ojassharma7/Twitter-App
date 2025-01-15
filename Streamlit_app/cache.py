import time

from collections import OrderedDict
from collections.abc import MutableMapping

import redis
import json
import pandas as pd
import random

# from util import (
#     _TTLLink,
#     _TTLLinkedList,
# )

__all__ = ("Base", "LRUCache", "TTLCache", "RedisCache")

# from collections import OrderedDict


class _TTLLink:
    """Link in TTL Doubly Linked List.

    Attributes:
        key (hashable): Cache Item Key.
        expiry (int): Item Expiry Time.
        next (_TTLLink): Next Link in the DLL.
        prev (_TTLLink): Prev Link in the DLL. None
        if no previous link exists.
    """

    def __init__(self, key=None, expiry=None, nxt=None, prev=None):
        self.key = key
        self.expiry = expiry
        self.next = nxt
        self.prev = prev


class _TTLLinkedList:
    """Doubly Linked List of TTL Links.

    Attributes:
        head (_TTLLink): Head of the Linked List.
    """

    def __init__(self, head=None) -> None:
        self.__head = head
        # 'TTLCache' only inserts at the end of the
        # list. Reference to the tail of the list
        # for O(1) insertions.
        self.__tail = head

    @property
    def head(self):
        """Returns the head of the linked list."""
        return self.__head

    def insert(self, link):
        """Insert a new link at the end of the linked list.

        Args:
            link (_TTLLink): `_TTLLink` to insert.
        """
        if self.__head:
            link.prev = self.__tail
            link.prev.next = self.__tail = link
        else:
            self.__head = self.__tail = link

    def remove(self, link):
        """Remove a link from the linked list.

        Args:
            link (_TTLLink): `_TTLLink` to remove.
        """
        if self.__head == link:
            self.__head = self.__head.next
        elif self.__tail == link:
            self.__tail = self.__tail.prev
        else:
            link.prev.next = link.next
            link.next.prev = link.prev


class RedisCache:
    def __init__(self, host="localhost", port=6379, db=0):
        """Initialize Redis connection"""
        self.redis = redis.StrictRedis(host=host, port=port, db=db)

    def exists(self, key):
        """Check if the key exists in Redis"""
        return self.redis.exists(key)

    def set(self, key, obj, expire=None):
        """Store the object in Redis, optionally setting an expiry time"""
        if isinstance(obj, pd.DataFrame):
            self.redis.set(key, obj.to_json())
        elif isinstance(obj, (dict, list)):
            obj = json.dumps(obj)
        else:
            self.redis.set(key, obj, ex=expire, nx=True)

    def get(self, key):
        """Retrieve the object from Redis, if available"""
        # obj = self.redis.get(key)
        # if obj:
        #     obj = json.loads(obj)
        #     return obj
        return self.redis.get(key)

    def delete(self, key):
        """Delete the object from Redis"""
        self.redis.delete(key)


class Base(MutableMapping):
    """Cache base-class.

    Evicts the oldest item from the cache
    when the cache reaches maximum capacity.

    Attributes:
        capacity (int): Maximum capacity of the cache.
        callback (callable, optional): Callable defining
        behaviour when an item is evicted from the cache.
        Defaults to None.
    """

    __singleton = object()

    def __init__(self, capacity, callback=None):
        self.__cache = {}

        self.__size = 0  # Number of Items in the Cache
        self.__capacity = capacity
        self._callback = callback

    @property
    def capacity(self):
        return self.__capacity

    def __setitem__(self, _key, _value):
        if _key not in self.__cache:
            while self.__size >= self.__capacity:
                self._evict()
            self.__cache[_key] = _value
            self.__size += 1
        else:
            self.__cache[_key] = _value

    def __getitem__(self, _key):
        try:
            return self.__cache[_key]
        except KeyError:
            raise KeyError(_key) from None

    def get(self, _key, _default=None):
        try:
            return self[_key]
        except KeyError:
            return _default

    def __delitem__(self, _key):
        del self.__cache[_key]
        self.__size -= 1

    def pop(self, _key, default=__singleton):
        try:
            _value = self[_key]
            del self[_key]
        except KeyError:
            if default is self.__singleton:
                raise
            return default
        else:
            return _value

    def popitem(self):
        """Pop the most recent item from the cache."""
        try:
            itm = self.__cache.popitem()
        except KeyError:
            raise KeyError("cache is empty") from None
        else:
            self.__size -= 1
            return itm

    def _evict(self):
        """Evicts an item from the cache determined
        by the relevant algorithm.

        Raises:
            KeyError: Cache is empty.
        """
        try:
            key = next(iter(self))
        except StopIteration:
            raise KeyError("cache is empty") from None

        value = self[key]
        del self.__cache[key]
        self.__size -= 1

        if self._callback:
            self._callback(key, value)

        return key, value

    def __contains__(self, _key):
        return _key in self.__cache

    def __iter__(self):
        return iter(self.__cache)

    def __len__(self):
        return len(self.__cache)

    def __repr__(self):
        return "{}{}".format(self.__class__.__name__, self.__cache)

    def keys(self):
        return self.__cache.keys()

    def values(self):
        return self.__cache.values()

    def items(self):
        return self.__cache.items()

    def __eq__(self, obj):
        if isinstance(obj, Base):
            if self.__dict__ == obj.__dict__:
                return True
        return False


class LRUCache(Base):
    """Least Recently Used Cache.

    Attributes:
        capacity (int): Maximum capacity of the cache.
        callback (callable, optional): Callable defining
        behaviour when an item is evicted from the cache.
        Defaults to None.
    """

    def __init__(self, capacity, callback=None):
        Base.__init__(self, capacity, callback)
        self._lru = OrderedDict()
        self._id = random.randint(0, 1000)

    def __getitem__(self, _key):
        """Retrieves item from the cache.

        If the item exists, retrieve it from the cache
        and move it to the back of the OrderedDict.

        Args:
            _key (hashable): Key.
        """
        try:
            _value = Base.__getitem__(self, _key)
        except KeyError:
            raise KeyError from None
        else:
            self._lru.move_to_end(_key, last=False)
            return _value

    def __setitem__(self, _key, _value):
        """Add item to the cache..

        If the item exists in the cache, update it's LRU ordering.
        If the item does not exist in the cache, add the item and
        then update it's LRU ordering.

        Args:
            _key (hashable): Item Key.
            _value (object): Item Value.
        """
        Base.__setitem__(self, _key, _value)
        self._lru[_key] = _value

        # Update LRU Ordering
        self._lru.move_to_end(_key, last=False)

    def __delitem__(self, _key):
        Base.__delitem__(self, _key)
        del self._lru[_key]

    def popitem(self):
        """Force eviction of least-recently used item."""
        try:
            _key, _value = self._lru.popitem()
        except KeyError:
            raise KeyError("cannot pop from empty cache") from None
        else:
            Base.__delitem__(self, _key)
            return (_key, _value)

    def _evict(self):
        """Evict the least-recently used item.

        Called when items are implicitly evicted
        from the cache.

        Evicts the least-recently used item from
        the cache and updates the LRU ordering.
        If a callback function is specified, the callback
        function is invoked.
        """
        try:
            _key, _value = self.popitem()
        except KeyError:
            raise KeyError("cannot evict from empty cache") from None
        else:
            if self._callback:
                self._callback(_key, _value)

    def exists(self, _key):
        return _key in self._lru

    def print_all(self):
        # for key in self._links:
        #     print(key, self._links[key])
        return self._lru.items()


class TTLCache(LRUCache):
    """TTL cache with global object fixed expiry times.

    Monotonic time is used to track key expiry times.

    Attributes:
        capacity (int): Maximum capacity of the cache.
        ttl (int): Cache items time-to-live.
        callback (callable, optional): Callable defining
        behaviour when an item is evicted from the cache.
        Defaults to None.
        time (callable): Callable time function used by the
        cache.
    """

    def __init__(self, capacity, ttl, callback=None, _time=time.monotonic):
        LRUCache.__init__(self, capacity, callback)

        self._time = _time
        self.__ttl = ttl

        # Dict Mapping Keys to `_TTLLinks`
        # this is primarily used for O(1)
        # lookup and deletions of `_TTLLinks`
        self._links = {}

        # Linked List of '_TTLLinks'
        # The linked list is 'sorted' in time-ascending
        # order. The key with the nearest expiry time is
        # at the front of the list.
        self._list = _TTLLinkedList()

    def expire(_time):
        """Removes expired keys from the cache.

        Decorator for class methods. Iterates over the linked
        list and removes expired keys from the cache when
        the cache is accessed.
        """

        def wrap(func):
            def wrapped_f(self, *args):
                curr = self._list.head

                while curr:
                    if curr.expiry <= _time():
                        LRUCache.__delitem__(self, curr.key)
                        self._list.remove(curr)
                        del self._links[curr.key]
                        curr = curr.next
                    else:
                        return func(self, *args)

                return func(self, *args)

            return wrapped_f

        return wrap

    @expire(_time=time.monotonic)
    def __setitem__(self, _key, _value):
        LRUCache.__setitem__(self, _key, _value)
        try:
            link = self._links[_key]
        except KeyError:
            expiry = self._time() + self.__ttl
            self._links[_key] = link = _TTLLink(_key, expiry, None, None)
        else:
            self._list.remove(link)
            expiry = self._time() + self.__ttl
            link.expiry = expiry

        self._list.insert(link)

    @expire(_time=time.monotonic)
    def __getitem__(self, _key):
        try:
            _value = LRUCache.__getitem__(self, _key)
        except KeyError:
            raise KeyError(f"{_key}") from None
        else:
            return _value

    @expire(_time=time.monotonic)
    def get(self, _key, _default=None):
        try:
            return self[_key]
        except KeyError:
            return _default

    @expire(_time=time.monotonic)
    def __delitem__(self, _key):
        try:
            LRUCache.__delitem__(self, _key)
        except KeyError:
            raise KeyError(f"{_key}") from None
        else:
            link = self._links[_key]
            self._list.remove(link)
            del self._links[_key]

    @expire(_time=time.monotonic)
    def __contains__(self, _object: object):
        return Base.__contains__(self, _object)

    @expire(_time=time.monotonic)
    def __iter__(self):
        return Base.__iter__(self)

    @expire(_time=time.monotonic)
    def __len__(self):
        return Base.__len__(self)

    def exists(self, _key):
        return _key in self._links

    def print_all(self):
        # for key in self._links:
        #     print(key, self._links[key])
        return self._links.items()

    def _evict(self):
        """Handle evictions when Cache exceeds capacity.
        Not time-related.

        Invokes callback function whenever an item is evicted.

        """
        # Fetch and Evict LRU Item from LRUCache
        _key, _value = LRUCache.popitem(self)

        # Remove References to Link
        link = self._links[_key]
        self._list.remove(link)
        del self._links[_key]

        if self._callback:
            self._callback(_key, _value)

    @expire(_time=time.monotonic)
    def __str__(self):
        return Base.__repr__(self)

    def popitem(self):
        """Evict the LRU item."""
        # Fetch and Evict LRU Item from LRUCache
        _key, _value = LRUCache.popitem(self)

        # Remove References to Link
        link = self._links[_key]
        self._list.remove(link)
        del self._links[_key]

    # Enable 'expire' decorator to be accessed
    # outside of the scope of the class, while
    # still being inside the class namespace.
    expire = staticmethod(expire)

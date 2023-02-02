# python imports
from functools import partial
import hashlib
import inspect
from inspect import signature
import logging
import os
import pathlib
import pickle
from typing import Any, Callable, Union
from types import FunctionType

# third party imports
import binfootprint

hex_alphabet = "0123456789abcdef"

log = logging.getLogger(__name__)
log.setLevel("DEBUG")


class CacheFileBased:
    def __init__(
        self,
        fnc: FunctionType,
        path: Union[str, pathlib.Path] = ".cache",
        include_module_name: bool = True,
    ):
        """
        Extend the function `fnc` by caching and adds the extra kwarg `_cache_flag` which
        modifies the caching behavior as follows:

            `_cache_flag = 'no_cache'`: Simple call of `fnc` with no caching.
            `_cache_flag = 'update'`: Call `fnc` and update the cache with recent return value.
            `_cache_flag = 'has_key'`: Return `True` if the call has already been cached, otherwise `False`.
            `_cache_flag = 'cache_only'`: Raises a `KeyError` if the result has not been cached yet.

        The behavior is in accordance with the ShelveCache class of the binfootprint module.

        The difference lies in the storage of te cache. Here each item is stored in a different file.
        The filename is constructed from the hex representation of the hash value of the arguments
        of the function call, i.e., the key.
        In order to cope with very large numbers of items, the hash value is split into three parts
        s1, s2 and f_name which make up the path to the file: `path / module.fnc_name / s1 / s2 / f_name`.
        The first two parts s1 and s2 represent 14 bits (16384 different values) each.
        In that way up to 2.6e8 items can be accessed efficiently (according to a simple benchmark based
        on an ext4 file system, see doc/file_access_time.md for details)

        If `include_module_name` is set to False `fnc_name` only is used instead of `module.fnc_name`.
        This can be useful during developing stage. However, it obviously requires that function names
        need to be distinctive.

        Args:
            fnc:
                The function to be cached.
            path (default '.cache):
                The location where the cache data is stored.
            include_module_name (default True):
                If True the database is named `module.fnc_name`, otherwise `fnc_name`.
        """
        self.path = pathlib.Path(path).absolute()
        self.fnc = fnc

        # note that a decorator receives the __func__ of a bounded method to
        # inspect.isbounded cannot be used to identify a bounded method
        if fnc.__name__ != fnc.__qualname__:
            raise TypeError(
                f"The function to cache must not be a bounded method, e.g. a class method, but is '{fnc.__qualname__}'"
            )
        self.fnc_sig = signature(fnc)
        if include_module_name:
            self.cache_dir = self.path / (self.fnc.__module__ + "." + self.fnc.__name__)
        else:
            self.cache_dir = self.path / self.fnc.__name__
        self.cache_dir.mkdir(parents=True, exist_ok=True)

    def param_hash_bytes(self, *args: Any, **kwargs: Any) -> bytes:
        """
        Calculate the hash value for the parameters `args` and `kwargs` with respect to the
        function `fnc`. The full mapping (kwargs dictionary) between the name of the arguments and their
        values, including default values, is used to calculate the hash.

        Args:
            args: positional arguments intended to call `fnc`
            kwargs: keyword arguments intended to call `fnc`
        """
        ba = self.fnc_sig.bind(*args, **kwargs)
        ba.apply_defaults()
        fnc_args = ba.arguments
        fnc_args_key_bytes = hashlib.sha256(binfootprint.dump(fnc_args)).digest()
        return fnc_args_key_bytes

    @staticmethod
    def four_bit_int_to_hex(i: int) -> str:
        """
        Convert the integer `i` to single hex digit (i.e., int representation with base 16).

        Args:
            i: integer to be converted (0 <= i <= 15 must hold)
        Returns:
            the hex digit as single character string
        """
        if (i < 0) or (i >= 16):
            raise ValueError("0 <= i < 16 needs to hold!")
        return hex_alphabet[i]

    @staticmethod
    def hash_bytes_to_3_hex(hash_bytes: bytes) -> tuple[str, str, str]:
        """
        Split a byte sequence into 3 parts of hex strings.
        The first and the second part are 4 digit hex strings which encode 14 bit each
        (16384 different values). The third part encodes the rest.
        That number 16384 was chosen from the file system benchmark (see doc/file_access_time.md).
        Up to that number of files in a single directory, the time to open a file a read a single
        character remains nearly constant.

        Notes:
            the 8 bits b of the first byte are associated with the parts 1,2 and 3 as follows
            bbbbbbbb = 11223333

            the 8 bits of the second byte are associated with the parts 1 and 2 as follows
            bbbbbbbb = 11112222

        Args:
            hash_bytes:
                A byte sequence representing a hash value.
        Returns:
            A tuple with three strings consisting of hex digits only.
            The first two string have length of 4 characters each.
        """
        b = hash_bytes[0]
        b1 = (b & 0b11000000) >> 6
        b2 = (b & 0b00110000) >> 4
        b3 = b & 0b00001111

        c = hash_bytes[1]
        c1 = (c & 0b11110000) >> 4
        c2 = c & 0b00001111

        s1 = (
            CacheFileBased.four_bit_int_to_hex(b1)
            + CacheFileBased.four_bit_int_to_hex(c1)  # 2 bit
            + hash_bytes[2:3].hex()  # 4 bit  # 8 bit
        )
        s2 = (
            CacheFileBased.four_bit_int_to_hex(b2)
            + CacheFileBased.four_bit_int_to_hex(c2)  # 2 bit
            + hash_bytes[3:4].hex()  # 4 bit  # 8 bit
        )
        s3 = CacheFileBased.four_bit_int_to_hex(b3) + hash_bytes[4:].hex()  # 8 bit

        return s1, s2, s3

    def get_f_name(self, *args: Any, **kwargs: Any) -> pathlib.Path:
        """
        Construct the path to the file which contains the cached result for the call fnc(*args, **kwargs).

        Args:
            args: positional arguments intended to call `fnc`
            kwargs: keyword arguments intended to call `fnc`
        """
        fnc_args_key_bytes = self.param_hash_bytes(*args, **kwargs)
        s1, s2, s3 = self.hash_bytes_to_3_hex(fnc_args_key_bytes)
        return self.cache_dir / s1 / s2 / s3

    @staticmethod
    def item_exists(f_name: pathlib.Path) -> bool:
        """
        Check existence of the file with path `f_name`.

        Args:
            f_name: file name to check for existence
        Returns:
            True if the path `f_name` exists, otherwise False.
        """
        try:
            return f_name.exists()
        except FileNotFoundError:
            return False

    @staticmethod
    def _write_item(f_name: pathlib.Path, item: Any) -> None:
        """
        writes item to disk at location f_name

        Removes partially written file on InterruptError.

        Args:
            f_name: Path object, where to dump the item
            item: the python object to be dumped
        """
        f_name.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(f_name, "wb") as f:
                pickle.dump(item, f)
        except Exception:
            os.remove(f_name)
            raise

    def __call__(
        self, *args: Any, _cache_flag: Union[str, None] = None, **kwargs: Any
    ) -> Any:
        """
        the actual wrapper function that implements the caching for call `fnc(*args, **kwargs)`

        Args:
            _cache_flag: may be None or one of the following strings
                'no_cache': Simple call of `fnc` without caching.
                'update': Call `fnc` and update the cache with recent return value.
                'has_key': Return `True` if the call has already been cached, otherwise `False`.
                'cache_only': Return result from cache. Raises a `KeyError` if the result has not been cached yet.

        Returns:
            The result of `fnc(*args, **kwargs)`. If `_cache_flag == 'has_key'` return a boolean.
        """
        if _cache_flag == "no_cache":
            return self.fnc(*args, **kwargs)
        else:
            f_name = self.get_f_name(*args, **kwargs)
            item_exists = self.item_exists(f_name)

            if _cache_flag == "has_key":
                return item_exists
            elif _cache_flag == "cache_only":
                if not item_exists:
                    raise KeyError(
                        "Item not found in cache! (File '{}' does not exist.)".format(
                            f_name
                        )
                    )
                with open(f_name, "rb") as f:
                    return pickle.load(f)
            elif (not item_exists) or (_cache_flag == "update"):
                r = self.fnc(*args, **kwargs)
                self._write_item(f_name=f_name, item=r)
                return r
            else:
                with open(f_name, "rb") as f:
                    return pickle.load(f)

    def set_result(
        self,
        *args: Any,
        _cache_result: Any,
        _cache_overwrite: bool = False,
        **kwargs: Any,
    ) -> None:
        """
        Write the results given in `_cache_result` which belongs to `*args` and `**kwargs` to the cache.
        Raise a ValueError if a result for such  `*args` and `**kwargs` exists already.
        Setting `_cache_overwrite` True overwrites an existing result without raises an exception.

        Args:
            _cache_result: the python object to be cached as result
            _cache_overwrite (default False): if True, silently overwrite an existing result in the cache
        """
        f_name = self.get_f_name(*args, **kwargs)
        item_exists = self.item_exists(f_name)
        if item_exists and not _cache_overwrite:
            raise ValueError(
                "Result has already been cached! "
                + "Set '_cache_overwrite' to True to force an update."
            )
        f_name.parent.mkdir(parents=True, exist_ok=True)
        self._write_item(f_name=f_name, item=_cache_result)


class CacheFileBasedDec:
    """
    Provides a decorator to cache the return values of a function to disk.
    """

    def __init__(
        self,
        path: Union[str, pathlib.Path] = ".cache",
        include_module_name: bool = True,
    ):
        """
        Allows to adjust `path` and `include_module_name` to be passed to the CacheFileBased constructor.

        Args:
            path (default '.cache'):
                The path under which cache data is stored.
                The path is created if necessary. The actual files are in a subdirectory with name
                retrieved from the name of the function and the name of the module defining that function.
                It is, thus, safe to use the CacheFileBasedDec with the same path parameter on
                different functions.
            include_module_name (default True):
                If True the database is named `module.fnc_name`, otherwise `fnc_name`.
        """
        self.path = path
        self.include_module_name = include_module_name

    def __call__(self, fnc: FunctionType) -> CacheFileBased:
        """
        The returned CacheFileBased instance extends the function `fnc` by caching.

        Args:
            fnc: the function to be cached

        Returns:
            an instance of CacheFileBased
        """
        return CacheFileBased(fnc, self.path, self.include_module_name)


def pickle_serializer(obj: Any) -> bytes:
    """
    serialize an object to binary data via Python's pickle

    Used to calculate its hash value, so ideally the byte sequence should be unique.
    Note that this is not guaranteed for pickle (e.g. when pickling dictionaries).
    """
    return pickle.dumps(obj)


def binfootprint_serializer(obj: Any) -> bytes:
    """
    serialize an object to binary data using binfootprint module

    By using the [binfootprint](https://github.com/richard-hartmann/binfootprint) module
    it is guaranteed that the resulting byte sequence is unique.
    The drawback compares to pickle is, that not all types can be dumped using binfootprint
    (see the docs for details).
    """
    return binfootprint.dump(obj)


class CacheInitSubclassError(ValueError):
    pass


class DenyFurtherSubclassing:
    def __init__(self, cls):
        self.cls = cls

    def __call__(self, **kwargs):
        raise CacheInitSubclassError(
            f"You cannot subclass a class which inherits from 'CacheInit'! "
            + f"The class '{self.cls.__qualname__}' has the method resolve order (MRO) {self.cls.__mro__}"
        )


def _cached_init(
    obj,
    *args,
    _CacheInit_serializer=binfootprint_serializer,
    _CacheInit_path=".CacheInit",
    _CacheInit_include_module_name=True,
    **kwargs,
):
    if not obj.loaded_from_cache:
        obj.init_of_subclass(*args, **kwargs)
        key = _gen_hash_key(
            sig=obj.sig_of_subclass, args=args, kwargs=kwargs, serializer=obj.serializer
        )
        full_path = obj.path_for_cache / key
        with open(full_path, "wb") as f:
            pickle.dump(obj, f)


def _gen_hash_key(
    sig: inspect.Signature,
    args: tuple,
    kwargs: dict,
    serializer: Callable[[Any], bytes],
) -> str:
    """
    Bind `args` and `kwargs` to signature `sig`.
    Convert the resulting dict to a tuple of (key, value) sorted by the keys of the dictionary.
    Return the SHA256 hex string of the binary data of that tuple.
    """
    log.debug(f"exec gen_hash_key, args={args}, kwargs={kwargs}")
    log.debug(f"signature = {sig}")
    ba = sig.bind(*args, **kwargs)
    ba.apply_defaults()
    all_kwargs = ba.arguments
    log.debug(f"ba={ba}")
    all_kwargs_sorted_tuple = tuple(
        (arg_i, all_kwargs[arg_i]) for arg_i in sorted(all_kwargs)
    )
    log.debug(f"exec gen_hash_key, all_kwargs_sorted_tuple={all_kwargs_sorted_tuple}")
    return hashlib.sha256(serializer(all_kwargs_sorted_tuple)).hexdigest()


def _get_path_for_cache(
    cls_name: str,
    mod_name: str,
    path: str,
    include_module_name: bool,
) -> pathlib.Path:
    """
    construct the directory for dumping the object which is identified by its hash value

    If `use_mod_name` is `True` the returned path is `path / mod_name / cls_name`,
    otherwise it is `path / cls_name`.
    """
    if include_module_name:
        p = pathlib.Path(path).absolute() / mod_name / cls_name
    else:
        p = pathlib.Path(path).absolute() / cls_name
    return p


def _remove_params_from_signature(
    sig: inspect.Signature, params: Union[str, tuple[str, ...]]
) -> inspect.Signature:
    """
    returns a copy of `sig` (signature of a callable) but without the parameters `params`

    Parameters:
        sig: a signature of a callable
        params: the parameters to exclude as tuple of strings
    Returns:
        the new signature
    """
    params_of_sig = sig._parameters

    list_of_params = []
    if isinstance(params, str):
        params = (params,)

    for p in params_of_sig:
        if p not in params:
            list_of_params.append(params_of_sig[p])

    return inspect.Signature(list_of_params)


class CacheInit:
    path_for_cache: pathlib.Path = ""
    sig_of_subclass: inspect.Signature = None
    serializer: Callable[[Any], bytes] = None

    special_kwargs = [
        "_CacheInit_serializer",
        "_CacheInit_path",
        "_CacheInit_include_module_name",
    ]

    def __new__(
        cls,
        *args: tuple,
        _CacheInit_serializer=binfootprint_serializer,
        _CacheInit_path=".CacheInit",
        _CacheInit_include_module_name=True,
        **kwargs: dict,
    ) -> object:
        """
        implements caching of object initialization

        Bind `args` and `kwargs` to the `__init__`-function of the subclass (see `__init_subclass__`).
        Generate binary data from the keyword-argument-tuples sorted by keywords.
        Use the resulting hash value as key to for caching an instance.

        In addition to the `args` and `kwargs` for the initialization of `cls` the following parameters
        can be passed as **keywords arguments only**

        Parameters:
            _CacheInit_serializer: a function which serializes an object to binary data
                (default is binfootprint.dump).
            _CacheInit_path: the path where to put the cache data (default is '.CacheInit')
            _CacheInit_include_module_name: if `True` (default) include the name of module where the class
                `cls` is defined into the path where the instance is cached.
        """
        log.debug(f"exec CacheInit.__new__(cls={cls}, args={args}, kwargs={kwargs}")
        # when pickle.load create the object, it calls __new__(cls) without
        # further arguments. In that case we create a bare instance of cls
        # and load fills it with live by calling something like __setstate__
        # on the loaded data
        if (len(args) == 0) and (len(kwargs) == 0):
            log.debug(
                "found empty parameters -> create instance using super().__new__(...)"
            )
            try:
                new_instance = super().__new__(cls, *args, **kwargs)
            except TypeError:
                new_instance = super().__new__(cls)
            new_instance.loaded_from_cache = False
            log.debug("set loaded_from_cache to False")
            return new_instance

        log.debug(f"__new__ has keyword args {kwargs}")
        cls.serializer = staticmethod(_CacheInit_serializer)

        cls.path_for_cache = _get_path_for_cache(
            cls_name=cls.__qualname__,
            mod_name=cls.__module__,
            path=_CacheInit_path,
            include_module_name=_CacheInit_include_module_name,
        )
        log.debug(f"path for cache is {cls.path_for_cache} (create if necessary)")
        cls.path_for_cache.mkdir(parents=True, exist_ok=True)

        key = _gen_hash_key(
            sig=cls.sig_of_subclass, args=args, kwargs=kwargs, serializer=cls.serializer
        )
        full_path = cls.path_for_cache / key
        log.debug(
            f"full_path for caching based on init parameters *args and **kwargs is {full_path}"
        )

        if full_path.exists():
            log.debug("path exists! -> load cache data")
            with open(full_path, "rb") as f:
                # load instance from cache
                new_instance = pickle.load(f)
                # mark that it comes from the cache, which prevents __init__ to be called
                new_instance.loaded_from_cache = True
                log.debug(
                    "instance created via pickle.load, set loaded_from_cache to True"
                )
                return new_instance

        # in case no cached object was found, create the bare object of type cls
        # __init__ will be called to populate the object, the init-wrapper `cached_init`
        # will also take care of caching the instance once it was successfully instantiated.
        log.debug("path does NOT exists! create new instance calling super().__new__")
        try:
            new_instance = super().__new__(cls, *args, **kwargs)
        except TypeError:
            new_instance = super().__new__(cls)
        new_instance.loaded_from_cache = False
        log.debug("set loaded_from_cache to False")
        return new_instance

    def __init_subclass__(cls, **kwargs):
        """
        Upon subclassing, first, save the original `__init__`-function as class member `_init_of_subclass`.
        Then replace the `__init__`-function by `CacheInit.cached_init` which is aware of the
        class member `_from_cache` being set when creating a new instance with `__new__`.
        The original `__init__` is only called when `loaded_from_cache` is `False`.

        Remember that this gets called when CacheInit is being subclassed, i.e. when the line
        NewClass(CacheInit) is parsed.
        """
        log.debug(
            f"hijack class '{cls.__qualname__}', since it derives from 'CacheInit'"
        )
        if inspect.isbuiltin(super().__init_subclass__):
            log.debug(
                "super().__init_subclass__ appears to be built-in, we conclude it is object.__init_subclass__ "
                + "and call 'super().__init_subclass__()'"
            )
            super().__init_subclass__()
        else:
            log.debug(
                f"call {super().__init_subclass__.__qualname__}(cls={cls}, **kwargs={kwargs}) ..."
            )
            super().__init_subclass__(**kwargs)
            log.debug("done!")

        # store the original init function of the subclass cls
        cls.init_of_subclass = cls.__init__
        log.debug(
            f"saved {cls.__init__.__qualname__} to {cls.__qualname__}.init_of_subclass"
        )
        cls.sig_of_subclass = _remove_params_from_signature(
            sig=signature(cls.__init__),
            params="self",
        )
        log.debug(f"saved signature of cls: {cls.sig_of_subclass}")
        # overwrite the init of the subclass cls by cached_init
        # which calls _init_of_subclass only if loaded_from_cache is False
        log.debug(
            f"overwrite {cls.__init__.__qualname__} with CacheInit.cached_init(obj, *args, **kwargs)"
        )
        cls.__init__ = _cached_init
        log.debug(
            f"overwrite {cls.__init_subclass__.__qualname__} with CacheInit.deny_further_subclassing"
        )
        cls.__init_subclass__ = DenyFurtherSubclassing(cls)

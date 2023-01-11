# python imports
import hashlib
from inspect import signature
import os
import pathlib
import pickle
from typing import Any, Callable, Union

# third party imports
from binfootprint import dump

hex_alphabet = "0123456789abcdef"


class CacheFileBased:
    def __init__(
        self,
        fnc: Callable[..., Any],
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
        fnc_args_key_bytes = hashlib.sha256(dump(fnc_args)).digest()
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
        except InterruptedError:
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
        **kwargs: Any
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

    def __call__(self, fnc: Callable[..., Any]) -> CacheFileBased:
        """
        The returned CacheFileBased instance extends the function `fnc` by caching.

        Args:
            fnc: the function to be cached

        Returns:
            an instance of CacheFileBased
        """
        return CacheFileBased(fnc, self.path, self.include_module_name)

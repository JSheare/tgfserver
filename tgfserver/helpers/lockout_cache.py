"""A module containing a class that implements a cache for keeping track of authorization lockouts."""
from cachetools import TTLCache

from tgfserver.helpers.helper_funcs import get_obj_size


class LockoutCache:
    """A class that keeps a memory-limited cache of attempts for keys and locks them out if they go over a maximum.

    Parameters
    ----------
    mem_lim : int
        The total memory to be used by cache entries.
    period_sec : int
        The window for attempts in seconds, and the time in seconds that a lockout lasts for.
    max_attempts : int
        The maximum number of attempts that a key can make before being locked out.

    Attributes
    ----------
    _cache : cachetools.TTLCache
        The cachetools TTLCache used to store the number of attempts per key for the specified window.

    """

    def __init__(self, mem_lim: int, period_sec: int, max_attempts: int) -> None:
        self._max_attempts = max_attempts
        self._cache = TTLCache(mem_lim, period_sec, getsizeof=get_obj_size)

    def is_locked_out(self, key: str) -> bool:
        """A function that returns a bool indicating whether the given key has been locked out.

        Parameters
        ----------
        key : str
            The key to check lockout status for.

        Returns
        -------
        bool
            True if the key has been locked out, False otherwise.

        """

        if key in self._cache:
            return self._cache[key] >= self._max_attempts

        return False

    def increment_attempts(self, key: str) -> None:
        """A function that increments the attempt count for the given key.

        Parameters
        ----------
        key : str
            The key that attempts will be incremented for.

        """

        if key in self._cache:
            self._cache[key] += 1
            if self._cache[key] >= self._max_attempts:
                # Resets the key's TTL
                self._cache[key] = self._max_attempts

        else:
            self._cache[key] = 1

    def reset_attempts(self, key: str) -> None:
        """A function that resets the attempt count for the given key.

        Parameters
        ----------
        key : str
            The key that attempts will be reset for.

        """

        if key in self._cache:
            self._cache[key] = 0

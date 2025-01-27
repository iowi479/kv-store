import sys
import threading


def addr_from_pid(pid) -> tuple[str, int]:
    ip, port = pid.split(":")
    return (ip, int(port))


def check_input(to_close):
    """checks input for program commands continuously"""

    print(
        "[CONFIG] Help: \n\t'q' to [q]uit\n\t'c' to [c]rash\n\t'v' to toggle [v]erbosity\n\n"
    )
    while True:
        text = input()
        check_single_input(text, to_close)


def check_single_input(text, to_close):
    """checks a single input for program commands"""

    if text == "q":
        print("[CONFIG] [q]uitting")
        if to_close and callable(getattr(to_close, "close")):
            to_close.close()
        else:
            print("[CONFIG] no close method found")

        sys.exit(0)

    elif text == "c":
        print("[CONFIG] [c]rashing")
        sys.exit(1)

    elif text.startswith("v"):
        if len(text) == 2 and text[1].isdigit() and 0 <= int(text[1]) <= 3:
            level = int(text[1])
            global LOGGING_LEVEL
            LOGGING_LEVEL = level
            print("[CONFIG] set verbosity to:", level)
        else:
            print("[CONFIG] invalid verbosity level")


class ThreadSafeKVCache:
    def __init__(self, dict=None):
        if dict is None:
            self._kv_cache = {}
        else:
            self._kv_cache = dict
        self._lock = threading.Lock()

    def set(self, key, value):
        """Store in kv cache - locked to one thread"""
        with self._lock:
            self._kv_cache[key] = value

    def get(self, key, default=None):
        """retrieve from kv cache - no lock"""
        return self._kv_cache.get(key, default)

    def remove(self, key):
        """remove from kv cache - locked to one thread"""
        with self._lock:
            if key in self._kv_cache:
                del self._kv_cache[key]

    def get_dict(self):
        """get inner dict"""
        return self._kv_cache

    def __str__(self):
        """return kv cache as string"""
        return str(self._kv_cache)

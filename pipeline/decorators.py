
def memoize(f):
    """Memoize a function that takes one argument."""
    class wrapper(dict):
        def __missing__(self, key):
            val = self[key] = f(key)
            return val
    return wrapper().__getitem__

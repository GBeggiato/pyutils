"""
quick and dirty value checking.
useful when using for example strings that must match certain patterns or
numbers that must live in a given range

# NOTE
you loose access to the classmethods define on the father AND child

# Example
>>> from enforce import enforce
>>> 
>>> 
>>> @enforce(lambda s: "." in s)
>>> class HasPoint(str):
>>>     pass
>>> 
>>>    # you can define extra methods you need and rely on the provided assumption to be true 
>>>    @property
>>>    def first(self):
>>>        return self.split(".")[0] # no need to check now
>>> 
>>> 
>>> def _is_positive_int(i) -> bool:
>>>     return isinstance(i, int) and i > 0
>>> 
>>> 
>>> @enforce(_is_positive_int) # accepts also more complex callables
>>> class Positive(int):
>>>     # NOTE: like regular 'int' you can also pass a float in the constructor
>>>     pass
>>> 
>>> 
>>> hp = HasPoint("a.b")
>>> parts = hp.split(".")
>>> print(parts) # ['a', 'b']
>>> 
>>> age = Positive(-1) # ValueError: '-1' is not a valid value for class 'Positive'
"""

from typing import Any, Callable


def enforce(is_valid: Callable[[Any], bool]):
    "forces the instances of a class to obey to the provided predicate"
    def inner(klass: type):
        def wrapper(*args, **kwargs):
            v = klass(*args, **kwargs)
            if is_valid(v):
                return v
            raise ValueError(f"'{v}' is not a valid value for class '{klass.__name__}'")
        return wrapper
    return inner


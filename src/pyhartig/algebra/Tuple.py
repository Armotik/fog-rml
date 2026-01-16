from typing import Dict, Union, Iterator, Mapping


class _Epsilon:
    """
    Means 'Processing error' or 'Undefined value'
    """

    def __repr__(self) -> str:
        """
        String representation of the Epsilon object
        :return: String "ε"
        """
        return "ε"

    def __eq__(self, other) -> bool:
        """
        Equality check for Epsilon objects
        :param other: Object to compare with
        :return: True if other is an instance of _Epsilon, False otherwise
        """
        return isinstance(other, _Epsilon)

    def __hash__(self) -> int:
        """
        Make Epsilon hashable for use in sets and as dict keys.
        :return: Hash value for the Epsilon object
        """
        return hash("_Epsilon_singleton")


EPSILON = _Epsilon()  # Singleton instance of Epsilon

# Definition of AlgebraicValue type
# Base Python types + EPSILON (None is excluded to avoid confusion with EPSILON)
AlgebraicValue = Union[str, int, float, bool, _Epsilon]


class MappingTuple(Mapping):
    """
    Represents an immutable data row (t) in a Mapping Relation.
    Partial function t: A -> T U {ε}

    This class implements the Mapping protocol (read-only dict-like interface)
    to ensure immutability as required by relational algebra semantics.
    The paper defines t' as a new tuple resulting from extending t, not a mutation of t.

    Being immutable also makes MappingTuple hashable, allowing tuples to be
    placed in sets for duplicate elimination (Set vs Bag semantics).
    """

    __slots__ = ('_data', '_hash') # To prevent dynamic attribute creation

    def __init__(self, data: Dict[str, AlgebraicValue] = None, **kwargs):
        """
        Initialize the MappingTuple with optional data.
        :param data: Dictionary of attribute-value pairs
        :param kwargs: Additional attribute-value pairs
        :raises TypeError: If any key is not a string
        :raises ValueError: If any value is None (use EPSILON instead)
        """
        if data is None:
            data = {}

        merged = {**data, **kwargs}

        # Validate keys and values
        for key, value in merged.items():
            if not isinstance(key, str):
                raise TypeError(f"The attribute (key) of a MappingTuple must be a string, received: {type(key)}")
            if value is None:
                raise ValueError(f"None is not allowed in MappingTuple. Use EPSILON for undefined values.")

        # Store data and initialize hash cache
        object.__setattr__(self, '_data', merged)
        object.__setattr__(self, '_hash', None)

    def __getitem__(self, key: str) -> AlgebraicValue:
        """
        Get the value for a given key.
        :param key: The attribute name
        :return: The value associated with the attribute
        :raises KeyError: If the key does not exist
        """
        return self._data[key]

    def __iter__(self) -> Iterator[str]:
        """
        Return an iterator over the attribute names.
        :return: An iterator of attribute names
        """
        return iter(self._data)

    def __len__(self) -> int:
        """
        Return the number of attributes in the tuple.
        :return: The number of attributes
        """
        return len(self._data)

    def __contains__(self, key: object) -> bool:
        """
        Check if the tuple contains a given key.
        :param key: The attribute name to check
        :return: True if the attribute exists, False otherwise
        """
        return key in self._data

    def __hash__(self) -> int:
        """
        Compute the hash of the MappingTuple for use in sets and as dict keys.
        Caches the hash value after the first computation for efficiency.
        :return: The hash value of the MappingTuple
        """
        if self._hash is None:
            object.__setattr__(self, '_hash', hash(frozenset(self._data.items())))
        return self._hash

    def __eq__(self, other: object) -> bool:
        """
        Equality check between two MappingTuples or a MappingTuple and a dict.
        :param other: The other object to compare with
        :return: True if equal, False otherwise
        """
        if isinstance(other, MappingTuple):
            return self._data == other._data
        if isinstance(other, dict):
            return self._data == other
        return False

    def __repr__(self) -> str:
        """
        String representation of the MappingTuple (for debugging)
        :return: String representation of the underlying dictionary
        """
        items_str = ", ".join(f"{k}={repr(v)}" for k, v in self._data.items())
        return f"Tuple({items_str})"

    def merge(self, other: 'MappingTuple') -> 'MappingTuple':
        """
        Operation t U t
        Merges two compatible tuples, returning a new immutable tuple.
        :param other: The other MappingTuple to merge with
        :return: A new MappingTuple resulting from the merge
        :raises ValueError: If tuples have conflicting values for the same key
        """
        # Check compatibility
        for key in self._data:
            if key in other and self._data[key] != other[key]:
                raise ValueError(
                    f"Tuples are not compatible for merging: conflict on attribute '{key}' : {self._data[key]} != {other[key]}")

        # Merge tuples (creates a new immutable tuple)
        new_data = {**self._data, **other._data}
        return MappingTuple(new_data)

    def extend(self, key: str, value: AlgebraicValue) -> 'MappingTuple':
        """
        Create a new tuple with an additional attribute.
        This is the algebraic extend operation: t' = t ∪ {(a, v)}
        :param key: The new attribute name
        :param value: The value for the new attribute
        :return: A new MappingTuple with the additional attribute
        """
        new_data = {**self._data, key: value}
        return MappingTuple(new_data)

    def project(self, attributes: set) -> 'MappingTuple':
        """
        Create a new tuple with only the specified attributes.
        This is the algebraic projection: t[P]
        :param attributes: Set of attribute names to keep
        :return: A new MappingTuple with only the specified attributes
        """
        new_data = {k: v for k, v in self._data.items() if k in attributes}
        return MappingTuple(new_data)

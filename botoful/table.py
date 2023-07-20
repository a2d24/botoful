from __future__ import annotations

import copy
from functools import wraps
from typing import List, Set, Tuple, Optional, Dict

from botoful.reserved import RESERVED_KEYWORDS
from botoful.serializers import serialize, deserialize


def fluent(func):
    # Decorator that assists in a fluent api.
    # It clones the current 'self', calls the wrapped method on the clone and returns the clone
    @wraps(func)
    def fluent_wrapper(self, *args, **kwargs) -> Item:
        new_self = copy.deepcopy(self)
        return func(new_self, *args, **kwargs)

    return fluent_wrapper


class Table:

    def __init__(self, name, client=None):
        self.name = name
        self.client = client

    def __copy__(self):
        return type(self)(name=self.name, client=self.client)

    def __deepcopy__(self, memo):
        # A boto3 client should not be deepcopied (the instance should be maintained across copies)
        copy = type(self)(name=self.name, client=self.client)
        memo[id(copy)] = copy
        return copy

    def item(self, **kwargs) -> Item:
        return Item(table=self).key(**kwargs)


class Item:

    def __init__(self, table):
        self.table: Table = table
        self._consistent_read = False
        self._attributes_to_fetch: Set[str] = set()
        self._named_variables: Set[str] = set()
        self._key_conditions: List[Tuple] = []

    @fluent
    def consistent(self, consistent_read: bool = True) -> Item:
        self._consistent_read = consistent_read
        return self

    @fluent
    def attributes(self, keys: List[str]) -> Item:
        self._attributes_to_fetch.update(keys)
        return self

    @fluent
    def key(self, **kwargs) -> Item:
        if len(kwargs) > 2:
            raise ValueError("The key method can take a maximum of two keyword arguments")

        for key, value in kwargs.items():
            attr = self._name_variable(key)

            self._key_conditions.append((attr, value))

        return self

    def get(self, client=None, consistent: Optional[bool]=None) -> Optional[Dict]:
        client = client if client is not None else self.table.client

        if client is None:
            raise RuntimeError("You need to provide a boto3 dynamodb client")

        if consistent is not None:
            self._consistent_read = consistent

        response = client.get_item(**self.build())

        if 'Item' not in response:
            return None

        return deserialize(response['Item'])

    def build(self):

        if len(self._key_conditions) == 0:
            raise RuntimeError("No key conditions specified for query. A query requires at least one key condition")

        result = {}
        expression_attribute_names = {}

        if self.table:
            result['TableName'] = self.table.name

        result['Key'] = {k: serialize(v) for k, v in self._key_conditions}

        if self._named_variables:
            expression_attribute_names.update({f"#{var}": var for var in self._named_variables})

        # Build ProjectionExpression

        if self._attributes_to_fetch:
            result['ProjectionExpression'] = ', '.join(
                [f"#{attr}" if attr.upper() in RESERVED_KEYWORDS else attr for attr in self._attributes_to_fetch]
            )

            reserved_keywords_attributes = list(
                filter(lambda item: item.upper() in RESERVED_KEYWORDS, self._attributes_to_fetch))
            if reserved_keywords_attributes:
                expression_attribute_names.update({f"#{attr}": attr for attr in reserved_keywords_attributes})

        if self._consistent_read:
            result['ConsistentRead'] = self._consistent_read

        if expression_attribute_names:
            result['ExpressionAttributeNames'] = expression_attribute_names

        return result

    def _name_variable(self, variable):
        if variable.upper() not in RESERVED_KEYWORDS:
            return variable

        self._named_variables.add(variable)

        return f"#{variable}"

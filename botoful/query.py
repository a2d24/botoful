from __future__ import annotations

import copy
import numbers
from functools import wraps, reduce
from typing import List, Set

from .reserved import RESERVED_KEYWORDS
from .serializers import deserialize, serialize


def fluent(func):
    # Decorator that assists in a fluent api.
    # It clones the current 'self', calls the wrapped method on the clone and returns the clone
    @wraps(func)
    def fluent_wrapper(self, *args, **kwargs):
        new_self = copy.deepcopy(self)
        return func(new_self, *args, **kwargs)

    return fluent_wrapper


class QueryResult:

    def __init__(self, items=None, next_token=None, model=None):
        if items is None:
            items = []

        self.items = [model(**item) for item in items] if model else items
        self.count = len(items)
        self.next_token = next_token

        self._model = model


class Condition:

    def __init__(self, key, operator, value):
        self.key = key
        self.operator = operator
        self.value = value

    def as_key_condition_expression(self):

        if self.operator == '=':
            return f"{self.key} = :{self.raw_key}"
        elif self.operator == 'begins_with':
            return f"begins_with ({self.key}, :{self.raw_key})"
        elif self.operator == 'gte':
            return f"{self.key} >= :{self.raw_key}"
        elif self.operator == 'lte':
            return f"{self.key} <= :{self.raw_key}"
        elif self.operator == 'gt':
            return f"{self.key} > :{self.raw_key}"
        elif self.operator == 'lt':
            return f"{self.key} < :{self.raw_key}"
        elif self.operator == 'between':
            return f"{self.key} BETWEEN :{self.raw_key}_lower AND :{self.raw_key}_upper"

        raise NotImplementedError(f"Operator {self.operator} is currently not supported")

    def as_expression_attribute_values(self, params):

        if self.operator == 'between':
            lower = self.value[0].format(**params) if isinstance(self.value[0], str) else self.value[0]
            upper = self.value[1].format(**params) if isinstance(self.value[0], str) else self.value[1]

            return {
                f":{self.raw_key}_lower": serialize(lower),
                f":{self.raw_key}_upper": serialize(upper),
            }

        _key = f":{self.raw_key}"
        if isinstance(self.value, str):
            return {_key: serialize(self.value.format(**params))}

        if isinstance(self.value, numbers.Number):
            return {_key: serialize(self.value)}

    @property
    def raw_key(self):
        return f"{self.key[1:]}" if self.key.startswith('#') else self.key


class Query:

    def __init__(self, table=None):

        self.table = table

        self._max_items = None
        self._index = None
        self._key_conditions: List[Condition] = []

        self._named_variables: Set[str] = set()
        self._attributes_to_fetch: Set[str] = set()
        self._page_size = None

    @fluent
    def page_size(self, page_size) -> Query:
        self._page_size = page_size
        return self

    def limit(self, limit) -> Query:
        return self.page_size(page_size=limit)

    @fluent
    def index(self, index_name: str) -> Query:
        self._index = index_name
        return self

    @fluent
    def key(self, **kwargs) -> Query:

        for key, condition in kwargs.items():
            if len(self._key_conditions) >= 2:
                raise ValueError("The key method can take a maximum of two keyword arguments")

            tokens = key.split('__') if '__' in key else (key, '=')
            key = self._name_variable(tokens[0])
            operator = tokens[1]
            self._key_conditions.append(Condition(key=key, operator=operator, value=condition))

        return self

    @fluent
    def attributes(self, keys: List[str]):
        self._attributes_to_fetch.update(keys)
        return self

    def _name_variable(self, variable):
        if variable.upper() not in RESERVED_KEYWORDS:
            return variable

        self._named_variables.add(variable)

        return f"#{variable}"

    def build(self, params, starting_token=None):
        result = {}
        if self.table:
            result['TableName'] = self.table

        if self._page_size:
            result['PaginationConfig'] = dict(
                MaxItems=self._page_size,
                PageSize=self._page_size,
                StartingToken=starting_token
            )

        if self._index:
            result['IndexName'] = self._index

        if self._key_conditions:
            result['KeyConditionExpression'] = " AND ".join(
                (c.as_key_condition_expression() for c in self._key_conditions)
            )

            expression_attribute_values = [c.as_expression_attribute_values(params=params) for c in
                                           self._key_conditions]
            result['ExpressionAttributeValues'] = reduce(lambda a, b: {**a, **b}, expression_attribute_values)
        else:
            raise RuntimeError("No key conditions specified for query. A query requires at least one key condition")

        if self._named_variables:
            result['ExpressionAttributeNames'] = {f"#{var}": var for var in self._named_variables}

        # Build ProjectionExpression

        if self._attributes_to_fetch:

            result['ProjectionExpression'] = ','.join(
                [f"#{attr}" if attr.upper() in RESERVED_KEYWORDS else attr for attr in self._attributes_to_fetch]
            )

            reserved_keywords_attributes = list(
                filter(lambda item: item.upper() in RESERVED_KEYWORDS, self._attributes_to_fetch))
            if reserved_keywords_attributes:
                result['ExpressionAttributeNames'] = {f"#{attr}": attr for attr in reserved_keywords_attributes}

        return result

    def preview(self, params=None, starting_token=None):
        if params is None:
            params = {}

        import json
        print(json.dumps(self.build(params=params, starting_token=starting_token), indent=2))

    def execute(self, client, starting_token=None, model=None, params=None) -> QueryResult:

        if params is None:
            params = {}

        if not self.table:
            raise RuntimeError("Queries cannot be executed without a table name specified")

        paginator = client.get_paginator('query')
        query = self.build(params=params, starting_token=starting_token)

        response = paginator.paginate(**query).build_full_result()

        items = [deserialize(item) for item in response.get('Items')]
        next_token = response.get('NextToken')

        return QueryResult(items=items, next_token=next_token, model=model)

    def execute_paginated(self, starting_token=None, *args, **kwargs) -> QueryResult:
        while True:
            result = self.execute(*args, **kwargs, starting_token=starting_token)

            yield result
            starting_token = result.next_token

            if starting_token is None:
                break

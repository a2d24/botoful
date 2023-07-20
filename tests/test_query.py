from decimal import Decimal

import pytest

import botoful
import botoful.serializers as serializers
from botoful import ValueOf
from conftest import TABLE_NAME

TEST_ITEM_1 = {
    'PK': 'TestItem1',
    'SK': 'TestItem1SK',
    'number': 1,
    'boolean': True,
    'string': 'hello'
}

TEST_ITEMS = [
    {
        "PK": f"FluentAPITest",
        "SK": f"FluentAPITest{i:02}SK",
        "number": Decimal(str(i)),
        "string": f"{i:02}"
    } for i in range(20)
]

base_query = botoful.Query(table=TABLE_NAME).key(PK='FluentAPITest')


def test_client(client):
    pass


def test_missing_item_returns_empty_result(client):
    result = botoful.Query(table=TABLE_NAME).key(PK='test', SK='test').execute(client)
    assert result.count == 0
    assert result.items == []
    assert result.next_token is None


def test_fetch_item(client):
    client.put_item(
        TableName=TABLE_NAME,
        Item=serializers.serialize(TEST_ITEM_1)['M']
    )

    result = botoful.Query(table=TABLE_NAME).key(PK='TestItem1', SK='TestItem1SK').execute(client)

    assert result.count == 1
    assert result.next_token is None
    assert result.items == [TEST_ITEM_1]


def test_fetch_attributes(client):
    client.put_item(
        TableName=TABLE_NAME,
        Item=serializers.serialize(TEST_ITEM_1)['M']
    )

    result = (
        botoful.Query(table=TABLE_NAME)
        .key(PK='TestItem1', SK='TestItem1SK')
        .attributes(['number'])
        .execute(client)
    )

    assert result.count == 1
    assert result.next_token is None
    assert result.items == [{'number': 1}]


def test_pagination_and_query_reuse(client):
    for item in TEST_ITEMS:
        client.put_item(
            TableName=TABLE_NAME,
            Item=serializers.serialize(item)['M']
        )

    paginated_query = base_query.page_size(10)

    results_base_query = base_query.execute(client=client)
    results_paginated_query = paginated_query.execute(client=client)

    assert results_base_query.count == 20
    assert results_base_query.items == TEST_ITEMS
    assert results_base_query.next_token is None

    assert results_paginated_query.count == 10
    assert results_paginated_query.items == TEST_ITEMS[0:10]
    assert results_paginated_query.next_token is not None

    results_paginated_query = paginated_query.execute(client=client, starting_token=results_paginated_query.next_token)

    assert results_paginated_query.count == 10
    assert results_paginated_query.items == TEST_ITEMS[10:20]
    assert results_paginated_query.next_token is None


def test_filters(client):
    filtered_query_result_1 = base_query.filter(ValueOf('number').between(5, 10)).execute(client)

    assert filtered_query_result_1.count == 6
    assert filtered_query_result_1.items == TEST_ITEMS[5:11]
    assert filtered_query_result_1.next_token is None

    filtered_query_result_2 = base_query.filter(
        ValueOf('number').between(5, 15) & ValueOf('string').begins_with('1')).execute(client)

    assert filtered_query_result_2.count == 6
    assert filtered_query_result_2.items == TEST_ITEMS[10:16]
    assert filtered_query_result_2.next_token is None


def test_backwards_query(client):
    results = base_query.backwards().execute(client)

    assert results.count == 20
    assert results.next_token is None
    assert results.items == list(reversed(TEST_ITEMS))


def test_begins_with(client):
    results = base_query.key(SK__begins_with='FluentAPITest0').execute(client)

    assert results.count == 10
    assert results.next_token is None
    assert results.items == TEST_ITEMS[0:10]


def test_comparision_queries(client):
    # TODO: I am not exactly sure why gte, lte, gt and lt do not appear to behave correctly
    results_gt = base_query.key(SK__gt='FluentAPITest05').execute(client)

    assert results_gt.count == 15
    assert results_gt.next_token is None
    assert results_gt.items == TEST_ITEMS[5:20]

    resultsa_gte = base_query.key(SK__gte='FluentAPITest05').execute(client)

    assert resultsa_gte.count == 15
    assert resultsa_gte.next_token is None
    assert resultsa_gte.items == TEST_ITEMS[5:20]

    results_lt = base_query.key(SK__lt='FluentAPITest05').execute(client)

    assert results_lt.count == 5
    assert results_lt.next_token is None
    assert results_lt.items == TEST_ITEMS[0:5]

    resultsa_lte = base_query.key(SK__lte='FluentAPITest05').execute(client)

    assert resultsa_lte.count == 5
    assert resultsa_lte.next_token is None
    assert resultsa_lte.items == TEST_ITEMS[0:5]


def test_invalid_number_of_keys():
    with pytest.raises(ValueError):
        botoful.Query(table=TABLE_NAME).key(PK=1, SK=1, GSI1PK=1)

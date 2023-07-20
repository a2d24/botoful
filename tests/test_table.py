import botoful
import botoful.serializers as serializers
from conftest import TABLE_NAME

TEST_ITEM_1 = {
    'PK': 'TableTestItem1',
    'SK': 'TestItem1SK',
    'number': 1,
    'boolean': True,
    'string': 'hello'
}


def test_fetch_item_does_not_exist(client):
    table = botoful.Table(name=TABLE_NAME, client=client)
    item = table.item(PK='does-not-exist', SK='does-not-exist').get()
    assert item is None


def test_fetch_item(client):
    client.put_item(
        TableName=TABLE_NAME,
        Item=serializers.serialize(TEST_ITEM_1)['M']
    )

    table = botoful.Table(name=TABLE_NAME, client=client)
    item = table.item(PK='TableTestItem1', SK='TestItem1SK').get()
    assert item == TEST_ITEM_1

    item = table.item(PK='TableTestItem1', SK='TestItem1SK').attributes(['boolean', 'string']).get()
    assert item == {'boolean': True, 'string': 'hello'}

import boto3
import pytest
from moto import mock_dynamodb

from tests.db_settings import GSI, DBSettings, create_table


@pytest.fixture(scope='session')
def client():
    with mock_dynamodb():
        client = boto3.client('dynamodb', region_name='us-west-2')

        clinical_table_settings = DBSettings(
            name='TestTable',
            stream='NEW_AND_OLD_IMAGES',
            GSIs=[
                GSI(name='GSI1', PK='GSI1PK', SK='GSI1SK'),
            ]
        )

        create_table(client, clinical_table_settings)

        response = client.list_tables()

        assert set(response['TableNames']) == {'TestTable'}

        yield client

import itertools
from dataclasses import dataclass
from typing import List, Union, Optional


@dataclass
class DBIndex:
    PK: str
    SK: Optional[str] = None
    type: str = 'S'


@dataclass
class GSI:
    name: str
    PK: str
    SK: Union[str, None] = None
    projection_type: str = "ALL"
    non_key_attributes: Optional[List[str]] = None


@dataclass
class DBSettings:
    name: str = None
    index: DBIndex = DBIndex(PK='PK', SK='SK')
    GSIs: Optional[List[GSI]] = None
    stream: str = None
    removal_policy: str = "RETAIN"


def create_table(client, settings):
    attributes = itertools.chain((settings.index.PK, settings.index.SK),
                                 *[(gsi.PK, gsi.SK) for gsi in settings.GSIs])

    client.create_table(
        TableName=settings.name,
        AttributeDefinitions=[{'AttributeName': name, 'AttributeType': 'S'} for name in attributes],
        KeySchema=[
            {'AttributeName': settings.index.PK, 'KeyType': 'HASH'},
            {'AttributeName': settings.index.SK, 'KeyType': 'RANGE'}
        ],
        BillingMode='PAY_PER_REQUEST',
        GlobalSecondaryIndexes=[
            dict(
                IndexName=gsi.name,
                KeySchema=[dict(AttributeName=gsi.PK, KeyType='HASH'), dict(AttributeName=gsi.SK, KeyType='RANGE')],
                Projection=dict(ProjectionType=gsi.projection_type)
            )
            for gsi in settings.GSIs
        ]
    )

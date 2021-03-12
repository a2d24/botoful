from collections import namedtuple

from boto3.dynamodb.conditions import Attr, ConditionExpressionBuilder, BuiltConditionExpression, ConditionBase

from botoful.serializers import serialize

Filter = namedtuple('Filter', ['expression', 'name_placeholders', 'value_placeholders'])
ValueOf = Attr # alias

builder = ConditionExpressionBuilder()


def build_filter(expression: ConditionBase):
    builder.reset()
    built_condition: BuiltConditionExpression = builder.build_expression(expression)

    return Filter(
        expression=built_condition.condition_expression,
        name_placeholders=built_condition.attribute_name_placeholders,
        value_placeholders=serialize(built_condition.attribute_value_placeholders)['M']
    )

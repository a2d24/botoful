from boto3.dynamodb.types import TypeDeserializer, TypeSerializer


deserializer = TypeDeserializer()
serializer = TypeSerializer()

def deserialize(document):
    return deserializer.deserialize({'M': document})

def serialize(value):
    return serializer.serialize(value)
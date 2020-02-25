from boto3 import resource


class DynamoDBHelper:
    DB = None

    def __init__(self, db_type, db_region):
        self.DB = resource(db_type, region_name=db_region)

    def getDocument(self, table_name, key, val):
        table = self.DB.Table(table_name)
        item = table.get_item(Key={key: val})

        if not item:
            return None

        if 'Item' not in item:
            return None

        return item['Item']

    def createDocument(self, table_name, doc):
        table = self.DB.Table(table_name)
        response = table.put_item(Item=doc)

        return response

    def updateDocument(self, table_name, key_dict, update_expression, expression_attribute_values):
        table = self.DB.Table(table_name)
        response = table.update_item(Key=key_dict, UpdateExpression=update_expression, ExpressionAttributeValues=expression_attribute_values, ReturnValues="UPDATED_NEW")

        return response
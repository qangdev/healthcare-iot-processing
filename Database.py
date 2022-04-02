from functools import update_wrapper
import boto3

class DynamoDatabase:

    def __init__(self):
        self.client = boto3.client(
            'dynamodb',
            region_name='us-east-2', # 'local',
            aws_access_key_id='AKIAYAXNR4SSPWXTD4WP',
            aws_secret_access_key='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'
        )

    def get_raw_data_inrange(self, start, end):
        data = self.client.query(
            TableName="bsm_data",
            KeyConditionExpression='datatype = :dtt AND #timestamp BETWEEN :start AND :end',
            ExpressionAttributeNames={ "#timestamp": "timestamp" },
            ExpressionAttributeValues={
                ':dtt': {'S': 'HeartRate'},
                ':start': {'S': str(start)},
                ':end': {'S': str(end)},
            }
        )
        return data

    def add_agg_data(self, item):
        self.client.put_item(
            TableName="bsm_agg_data",
            Item=item
        )

    def get_lowerbound_inrange(self, deviceid, datatype, start, end, rule):
        lower_data = self.client.query(
            TableName="bsm_agg_data", 
            KeyConditionExpression='datatype = :datatype AND #timestamp BETWEEN :start AND :end',
            FilterExpression="#avg < :lowerbound AND deviceid = :deviceid",
            ExpressionAttributeNames={ 
                "#timestamp": "timestamp",
                "#avg": "avg" 
            },
            ExpressionAttributeValues={
                ':deviceid': {'S': str(deviceid)},
                ':datatype': {'S': str(datatype)},
                ':start': {'S': str(start)},
                ':end': {'S': str(end)},
                ':lowerbound': {'N': str(rule["avg_min"])},
            }
        )
        return lower_data

    def get_upperbound_inrange(self, deviceid, datatype, start, end, rule):
        upper_data = self.client.query(
            TableName="bsm_agg_data", 
            KeyConditionExpression='datatype = :datatype AND #timestamp BETWEEN :start AND :end',
            FilterExpression="#avg > :upperbound AND deviceid = :deviceid",
            ExpressionAttributeNames={ 
                "#timestamp": "timestamp",
                "#avg": "avg" 
            },
            ExpressionAttributeValues={
                ':deviceid': {'S': str(deviceid)},
                ':datatype': {'S': str(datatype)},
                ':start': {'S': str(start)},
                ':end': {'S': str(end)},
                ':upperbound': {'N': str(rule["avg_max"])},
            }
        )
        return upper_data
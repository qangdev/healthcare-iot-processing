import random
import datetime
import json

class RawData:

    def __init__(self, db, client):
        self.db = db
        self.client = client

    def add_random_heartrate(self, deviceid):
        message = {}
        message['deviceid'] = deviceid
        value = float(random.normalvariate(99, 1.5))
        value = round(value, 1)
        timestamp = str(datetime.datetime.now())
        message['timestamp'] = timestamp
        message['datatype'] = 'Temperature'
        message['value'] = value
        self.add_raw_data(message)
        return json.dumps(message)
        
    def add_random_spo2(self, deviceid):
        message = {}
        message['deviceid'] = deviceid
        value = int(random.normalvariate(90,3.0))
        timestamp = str(datetime.datetime.now())
        message['timestamp'] = timestamp
        message['datatype'] = 'SPO2'
        message['value'] = value
        self.add_raw_data(message)
        return json.dumps(message)
    
    def add_random_temperature(self, deviceid):
        message = {}
        message['deviceid'] = deviceid
        value = int(random.normalvariate(85,12))
        timestamp = str(datetime.datetime.now())
        message['timestamp'] = timestamp
        message['datatype'] = 'HeartRate'
        message['value'] = value
        self.add_raw_data(message)
        return json.dumps(message)

    def add_raw_data(self, item):
        format_item = {
            'deviceid': {'S': item['deviceid']},
            'timestamp': {'S': item['timestamp']},
            'datatype': {'S': item['datatype']},
            'value': {'N': str(item['value'])},
        }
        self.client.put_item(
            TableName="bsm_data",
            Item=format_item
        )
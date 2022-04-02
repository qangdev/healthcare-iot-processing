import json
import collections

from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta 

from Database import DynamoDatabase

rules_json = '''[
    {
        "name": "Rule-HeartRate",
        "check_type": "avg_range",
        "type": "HeartRate",
        "avg_min": 86,
        "avg_max": 88,
        "trigger_count": 2
    },
    {
        "name": "Rule-SPO2",
        "check_type": "avg_range",
        "type": "SPO2",
        "avg_min": 90,
        "avg_max": 91,
        "trigger_count": 2
    },
    {
        "name": "Rule-Temperature",
        "check_type": "avg_range",
        "type": "Temperature",
        "avg_min": 90,
        "avg_max": 91,
        "trigger_count": 2
    }
]'''


class Alert:

    def __init__(self, db:DynamoDatabase, client, devices, datatypes):
        self.db:DynamoDatabase = db
        self.client = client
        self.devices = devices
        self.datatypes = datatypes
        self.rules = []

    def parse(self, link_file='./rules.json'):
        with open(link_file, 'r') as rfile:
            self.rules = json.loads(rfile.read())

    def run(self):
        check_point = datetime.now().replace(minute=0, second=0, microsecond=0)
        start = check_point
        end = check_point + timedelta(hours=1)
        for device in self.devices:
            for datatype in self.datatypes:
                for rule in self.rules:
                    if rule["check_type"] == "avg_range":
                        print(f'Processing rules for device {device}; {datatype}; {str(start)}; {str(end)}, {rule}')
                        self.check_inrange(device, datatype, start, end, rule)

    def check_inrange(self, deviceid, datatype, start, end, rule):
        lower_data = self.db.get_lowerbound_inrange(deviceid, datatype, start, end, rule)
        upper_data = self.db.get_upperbound_inrange(deviceid, datatype, start, end, rule)
        # Make a new data list to check rule - sort by timestamp
        comb_data = collections.OrderedDict()
        for item in lower_data["Items"] + upper_data["Items"]:
            timestamp_obj = datetime.fromisoformat(item['timestamp']['S'])
            comb_data[timestamp_obj] = 1  # item['avg']['N']

        all_ts = list(comb_data.keys())
        ranges = sum((list(t) for t in zip(all_ts, all_ts[1:]) if t[0] + relativedelta(minutes=1) != t[1]), [])
        ts_ranges = iter(all_ts[0:1] + ranges + all_ts[-1:])
        
        date_ranges = [((n), (next(ts_ranges))) for n in ts_ranges]
        violations = []
        for start, end in date_ranges:
            obj = []
            while start <= end:
                obj.append(start)
                start = start + relativedelta(minutes=1)
            if len(obj) >= rule["trigger_count"]:
                violations.append({
                    'deviceid': deviceid, 
                    'datatype': datatype,
                    'timestamp': obj[0],
                    'rule': rule["name"]
                })
        # Save and print out alerts if any
        for obj in violations:
            self.print_alerts(obj)
            self.save_alerts(obj) 

    def print_alerts(self, obj):
        print(f"Alert for device_id {obj['deviceid']} violated {obj['datatype']} rule {obj['rule']} at {obj['timestamp']}")

    def save_alerts(self, obj):
        self.client.put_item(
            TableName='bsm_alerts',
            Item={
                'deviceid': {'S': str(obj['deviceid'])},
                'datatype': {'S': str(obj['datatype'])},
                'timestamp': {'S': str(obj['timestamp'])},
                'rule': {'S': str(obj['rule'])}
            }
        )
        print("Alert Added")

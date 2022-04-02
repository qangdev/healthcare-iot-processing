from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from Database import DynamoDatabase


class AggregateData:

    def __init__(self, db:DynamoDatabase, client, devices:list):
        self.db:DynamoDatabase = db
        self.client = client
        self.devices = []

    def aggregate_raw_data(self, datatype:str, start:datetime, end:datetime):
        point = start
        while start <= end:
            # ------------------------------------------
            point = start + relativedelta(minutes=1)
            data = self.db.get_raw_data_inrange(start, point)
            min_v, max_v, all_value = None, None, []
            for item in data["Items"]:
                value = float(item['payload']['M']["value"]["N"])
                all_value.append(value)
                
                if min_v is None:
                    min_v = value
                elif value < min_v:
                    min_v = value 
                
                if max_v is None:
                    max_v = value
                elif value > max_v:
                    max_v = value

            # ------------------------------------------
            if min_v is not None and max_v is not None and len(all_value) > 0:
                format_item = {
                    'deviceid': {'S': item['payload']['M']['deviceid']['S']},
                    'datatype': {'S': datatype},
                    'timestamp': {'S': str(start)},
                    'min': {'N': str(min_v)},
                    'max': {'N': str(max_v)},
                    'avg': {'N': str(float(round(sum(all_value) / len(all_value), 2))) if all_value else ""}
                }
                self.db.add_agg_data(format_item)
                print("Added", str(start), str(point), format_item)

            # ------------------------------------------
            start = point

    def agg_spo2(self):
        for device in self.devices:
            print(f"Aggregating data for device {device}")
            check_point = datetime.now().replace(minute=0, second=0, microsecond=0)
            self.aggregate_raw_data(
                datatype="SPO2",
                start=check_point,
                end=check_point + timedelta(hours=1)
            )

    def agg_heart_rate(self):
        for device in self.devices:
            print(f"Aggregating data for device {device}")
            check_point = datetime.now().replace(minute=0, second=0, microsecond=0)
            self.aggregate_raw_data(
                datatype="HeartRate",
                start=check_point,
                end=check_point + timedelta(hours=1)
            )
        
    def agg_temperature(self):
        for device in self.devices:
            print(f"Aggregating data for device {device}")
            check_point = datetime.now().replace(minute=0, second=0, microsecond=0)
            self.aggregate_raw_data(
                datatype="Temperature",
                start=check_point,
                end=check_point + timedelta(hours=1)
            )
      
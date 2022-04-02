from asyncio import events
import boto3
import schedule
import time

from datetime import datetime
from dateutil.relativedelta import relativedelta


def create_table_bsm_data(client):
    # Define `bms_data` table
    tables = client.list_tables()["TableNames"]
    if 'bsm_data' not in tables:
        client.create_table(
            TableName='bsm_data',
            KeySchema=[
                {
                    'AttributeName': 'datatype',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'timestamp',
                    'KeyType': 'RANGE'
                },
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'datatype',
                    'AttributeType': 'S'  # String
                },
                {
                    'AttributeName': 'timestamp',
                    'AttributeType': 'S'  # String
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        print("Table bsm_data created")


def create_table_bsm_agg_data(client):
    tables = client.list_tables()["TableNames"]
    if 'bsm_agg_data' not in tables:
        client.create_table(
            TableName='bsm_agg_data',
            KeySchema=[
                {
                    'AttributeName': 'datatype',
                    'KeyType': 'HASH'
                },
                {
                    'AttributeName': 'timestamp',
                    'KeyType': 'RANGE'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'datatype',
                    'AttributeType': 'S'  # String
                },
                {
                    'AttributeName': 'timestamp',
                    'AttributeType': 'S'  # String
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        print("Table bsm_agg_data created")


def create_table_bsm_alerts(client):
    tables = client.list_tables()["TableNames"]
    if 'bsm_alerts' not in tables:
        client.create_table(
            TableName='bsm_alerts',
            KeySchema=[
                {
                    'AttributeName': 'deviceid',
                    'KeyType': 'HASH'
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'deviceid',
                    'AttributeType': 'S'  # String
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        print("Table bsm_alerts created")


if __name__ == '__main__':
    # ------------------------------------------
    # DATABASE
    from Database import DynamoDatabase
    db = DynamoDatabase()
    # ------------------------------------------

    # ------------------------------------------
    # CREATE TABLES if not exists
    create_table_bsm_data(db.client)
    create_table_bsm_agg_data(db.client)
    create_table_bsm_alerts(db.client)
    # print(db.client.list_tables())
    # ------------------------------------------

    # ------------------------------------------
    # AGGREGATION
    from AggregateModel import AggregateData
    agg_obj = AggregateData(
        db=db,
        client=db.client,
        devices=["BSM_G101", "BSM_G201"],
    )
    # agg_obj.agg_spo2()
    # agg_obj.agg_heart_rate()
    # agg_obj.agg_temperature()
    schedule.every(1).minutes.do(agg_obj.agg_spo2)
    schedule.every(1).minutes.do(agg_obj.agg_heart_rate)
    schedule.every(1).minutes.do(agg_obj.agg_temperature)
    # ------------------------------------------

    # ------------------------------------------
    # ALERTS WATCHER
    from AlertDataModel import Alert
    alert_obj = Alert(
        db=db,
        client=db.client,
        devices=["BSM_G101", "BSM_G201"],
        datatypes=["HeartRate", "SPO2", "Temperature"]
    )
    alert_obj.parse()
    alert_obj.run()
    schedule.every(3).minutes.do(alert_obj.run)
    # ------------------------------------------

    while True:
        schedule.run_pending()
        time.sleep(1)

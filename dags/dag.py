import requests
import os
import json
import logging
from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago

POSTGRES_ID = "postgres_default"

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(dag_id='user-pipeline',
         start_date=datetime.now(),
         default_args=default_args,
         schedule_interval="@daily", catchup=False) as dag:

    @task()
    def get_data():
        res = requests.get("https://randomuser.me/api/")
        res_json = res.json()
        res_json = res_json['results'][0]
        return res_json

    @task()
    def parse_data(res):
        data = {}
        data['first_name'] = res['name']['first']
        data['last_name'] = res['name']['last']
        data['age'] = res['dob']['age']
        data['country'] = res['location']['country']
        data['email'] = res['email']
        data['username'] = res['login']['username']
        data['number'] = res['phone']
        data['profile_photo'] = res['picture']['medium']

        print(json.dumps(data, indent=4, sort_keys=False))

        logging.info(json.dumps(data, indent=4, sort_keys=False))

        return data

    @task()
    def load_data(data):
        pg = PostgresHook(postgres_conn_id=POSTGRES_ID)
        connection = pg.get_conn()
        logging.info("Connected to Postgres DB...")

        cursor = connection.cursor()

        cursor.execute("""CREATE TABLE IF NOT EXISTS USERS (
            FIRST_NAME VARCHAR(255),
            LAST_NAME VARCHAR(255),
            AGE VARCHAR(255),
            COUNTRY VARCHAR(255),
            EMAIL VARCHAR(255),
            USERNAME VARCHAR(255),
            NUMBER VARCHAR(255),
            PROFILE_PHOTO VARCHAR(255),
            TIMESTAMP TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)
        logging.info("Created table...")

        cursor.execute("""
        INSERT INTO USERS (FIRST_NAME, LAST_NAME, AGE, COUNTRY, EMAIL, USERNAME, NUMBER, PROFILE_PHOTO) 
        VALUES(%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            data['first_name'],
            data['last_name'],
            data['age'],
            data['country'],
            data['email'],
            data['username'],
            data['number'],
            data['profile_photo']
        ))
        logging.info("Populated DB...")

        connection.commit()
        cursor.close()

        logging.info("Closing Postgres DB...")

    # DAG Workflow
    raw_data = get_data()
    parsed_data = parse_data(raw_data)
    load_data(parsed_data)

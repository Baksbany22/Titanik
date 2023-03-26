from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.utils.dates import days_ago
import json
import csv
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine.url import URL


default_args = {
    'owner': 'admin',
    'start_date': days_ago(0),
    'depends_on_past': False,
}


def createJson():
    temp = {
        'data': []
    }
    try:
        with open('/opt/airflow/dags/test.csv') as test:
            data = csv.DictReader(test, delimiter=',')

            for i in data:
                temp['data'].append(
                    {'PassengerID': i['PassengerId'], 'Survived': i['Survived'], 'Pclass': i['Pclass'],
                     'Name': i['Name'], 'Sex': i['Sex'], 'Age': i['Age'], 'SibSP': i['SibSp'], 'Parch': i['Parch'],
                     'NumberTicket': i['Ticket'],
                     'Fare': i['Fare'], 'Cabin': i['Cabin'], 'Embarked': i['Embarked']})

    except Exception as e:
        print(e)

    with open('/opt/airflow/dags/testik.json', 'w') as file:
        json.dump(temp['data'], file, ensure_ascii=False)


def insertdate():
    try:
        data_insert = pd.read_json('/opt/airflow/dags/testik.json', orient = "records")

        data_insert = data_insert.groupby(['Pclass', 'Survived']).count()[['PassengerID']]

        url = URL.create(
            'postgresql',
            username='root',
            password='root',
            host='172.18.0.3',
            port='5432',
            database='postgres'
        )

        engine = create_engine(url)

        data_insert.to_sql('pandas', engine, if_exists='replace', index=True)
    except Exception as se:
        print(se)



with DAG(
    'mydag',
    default_args=default_args,
    schedule_interval='@once',
    catchup=False
) as dag:



    t1 = PythonOperator(
        task_id='parse_json',
        python_callable = createJson
    )
    t2 = PythonOperator(
        task_id='print_date',
        python_callable = insertdate
    )
    t1 >> t2


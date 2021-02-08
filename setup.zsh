#!/bin/zsh

# create virtual environment and install requirements

virtualenv -p /usr/local/opt/python@3.7/Frameworks/Python.framework/Versions/3.7/bin/python3 airflow-venv
source airflow-venv/bin/activate
export AIRFLOW_HOME=~/airflow-venv/airflow-course
pip install -r requirements.txt

# create necessary file

touch ~/run.txt

# create postgresql database and user

brew services start postgresql

psql postgres -c "CREATE USER airflow WITH PASSWORD 'airflow';"
psql postgres -c "CREATE DATABASE airflow_db OWNER airflow;"
psql postgres -c "GRANT ALL PRIVILEGES ON DATABASE airflow_db to airflow;"

# set configuration values

export AIRFLOW__CORE__LOAD_EXAMPLES=False
export AIRFLOW__CORE__EXECUTOR=LocalExecutor
export AIRFLOW__CORE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@localhost:5432/airflow__db
export AIRFLOW__SMART_SENSOR__USE_SMART_SENSOR=True
export AIRFLOW__SMART_SENSOR__SHARDS=1
export AIRFLOW__SMART_SENSOR__SENSORS_ENABLED=SmartFileSensor
export AIRFLOW_VAR_SLACK_TOKEN=xoxb-1727046194672-1709378985940-A6HDRTXvZKqhpy8OFC7aBBOf


# create airflow user

airflow db init
airflow users create -r Admin -u airflow -p airflow -e airflow@airflow.com -f airflow -l airflow

# import connections and variables

airflow variables import variables.json
airflow connections add 'postgres' --conn-uri 'postgres://airflow:airflow@localhost:5432/airflow_db'
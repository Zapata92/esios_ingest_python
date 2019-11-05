# -*- coding: utf-8 -*-
"""
Created on Mon Sep  9 17:04:53 2019

@author: jzapa
"""
import datetime
import pandas as pd
import urllib
import json
from urllib.error import HTTPError
from sqlalchemy import create_engine
import html
import re

from airflow.hooks.base_hook import BaseHook
from airflow.exceptions import AirflowException
from airflow.models import DAG, Variable
from airflow.operators.esios import (EsiosOperator,
                                     LatestTimestampOperator,
                                     EsiosIndicatorsOperator)
from airflow.hooks.esios import (EsiosHook,
                                 PostgresEsiosHook)

token = Variable.get('esios_token')
base_url = Variable.get('esios_base_url')
tables = Variable.get('tables', deserialize_json=True)
end_date = (datetime.datetime.now() + datetime.timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%S")


def create_dag(name, historical):
    default_dag_args = {
        'start_date': datetime.datetime(2019, 1, 1),
        # 'email': ['jzapata@opensistemas.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 0,
        'retry_delay': datetime.timedelta(minutes=1),
    }
    with DAG(name,
             schedule_interval=None if historical else "0 11 * * *",
             default_args=default_dag_args) as dag:

        if historical:
            load_esios_data = EsiosIndicatorsOperator(
                task_id='LoadEsiosData',
                token=token,
                base_url=base_url,
                table=name)
            load_esios_data
        else:
            # Comprobar si existe la tabla en BigQuery
            getmaxtimestamp = LatestTimestampOperator(
                task_id='GetMaxTimestamp',
                table=name)

            # Transferencia de S3 a GS
            load_esios_data = EsiosOperator(
                task_id='LoadEsiosData',
                token=token,
                base_url=base_url,
                table=name,
                start_date_esios='{{ ti.xcom_pull(key="max_date") }}',
                end_date_esios=end_date)
            
            getmaxtimestamp >> load_esios_data

    return dag


for name, params in tables.items():
    dag = create_dag(
        name,
        params['historical'])
    globals()['dag_' + name] = dag

del dag


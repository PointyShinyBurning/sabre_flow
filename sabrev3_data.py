from airflow import DAG
import zipfile
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import shutil
import cpgintegrate
from cpgintegrate.connectors import OpenClinica, XNAT
from cpgintegrate.processors import tanita_bioimpedance, epiq7_liverelast
import requests
import logging
from airflow.operators.cpg_plugin import CPGDatasetToCsv, CPGProcessorToCsv


def unzip_first_file(zip_path, destination):
    zip_file = zipfile.ZipFile(zip_path)
    destination_file = open(destination, "wb")
    shutil.copyfileobj(
        zip_file.open(zip_file.namelist()[0], "r"),
        destination_file
    )
    destination_file.close()


def push_to_ckan(push_csv_path, push_resource_id):
    conn = BaseHook.get_connection('ckan')
    file = open(push_csv_path, 'rb')
    res = requests.post(
        url=conn.host + '/api/3/action/resource_update',
        data={"id": push_resource_id},
        headers={"Authorization": conn.get_password()},
        files={"upload": file},
    )
    logging.info("HTTP Status Code: %s", res.status_code)
    assert res.status_code == 200

csv_dir = BaseHook.get_connection('temp_file_dir').extra_dejson.get("path")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['d.key@ucl.ac.uk'],
    'email_on_failure': True,
    'email_on_retry': True,
    'start_date': datetime(2017, 7, 20),
    'csv_dir': csv_dir,
}

dag = DAG('sabrev3', default_args=default_args)

oc_xml_path = csv_dir + "openclinica.xml"
oc_args = {"connector_class": OpenClinica, "connection_id": 'openclinica', "connector_args": ['S_SABREV3_4350'],
           "connector_kwargs": {"xml_path": oc_xml_path}, "dag": dag}

operators_resource_ids = [
    (CPGDatasetToCsv(task_id="F_ANTHROPO", **oc_args, dataset_args=['F_ANTHROPO']), '40aa2125-2132-473b-9a06-302ed97060a6'),
    (CPGDatasetToCsv(task_id="F_FALLSRISKSAB", **oc_args, dataset_args=['F_FALLSRISKSAB']),
     'fa39e257-897f-44d4-81a5-008f140305b0'),
    (CPGProcessorToCsv(task_id="I_ANTHR_BIOIMPEDANCEFILE", **oc_args,
                       iter_files_args=['I_ANTHR_BIOIMPEDANCEFILE'], processor=tanita_bioimpedance),
     'f1755dba-b898-4af4-bb4e-0c7977ef8a37')
]

unzip = PythonOperator(
    python_callable=unzip_first_file,
    op_args=[
        BaseHook.get_connection('openclinica_export_zip').extra_dejson.get("path"),
        oc_xml_path
    ],
    task_id='unzip', dag=dag,
)

previous = unzip

for operator, ckan_resource_id in operators_resource_ids:

    operator << previous

    push_dataset = PythonOperator(
        python_callable=push_to_ckan, op_args=[operator.csv_path, ckan_resource_id],
        task_id=operator.task_id + "_push_to_ckan", dag=dag,
    )

    push_dataset << operator

    # Run the OpenClinica extracts sequentially because its session management is stoopid
    if operator.connector_class == OpenClinica:
        previous = operator
    else:
        previous = unzip

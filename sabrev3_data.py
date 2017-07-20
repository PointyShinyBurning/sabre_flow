from airflow import DAG
import zipfile
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from datetime import datetime
import shutil
from cpgintegrate.connectors import OpenClinica
import requests
import logging

xml_dump_path = BaseHook.get_connection('temp_file_dir').extra_dejson.get("path")+"openclinica.xml"


def unzip_first_file(zip_path, destination):
    zip_file = zipfile.ZipFile(zip_path)
    destination_file = open(destination, "wb")
    shutil.copyfileobj(
        zip_file.open(zip_file.namelist()[0], "r"),
        destination_file
    )
    destination_file.close()


def save_form_to_csv(form_oid_prefix, save_path):
    OpenClinica("http://cmp.slms.ucl.ac.uk/OpenClinica", "S_SABREV3_4350")\
        .get_dataset(xml_dump_path, form_oid_prefix)\
        .to_csv(save_path)


def push_to_ckan(csv_path, resource_id):
    conn = BaseHook.get_connection('ckan')
    file = open(csv_path, 'rb')
    res = requests.post(
        url=conn.host + '/api/3/action/resource_update',
        data={"id": resource_id},
        headers=conn.extra_dejson,
        files={"upload": file},
    )
    logging.info("HTTP Status Code: %s", res.status_code)
    assert res.status_code == 200

def post_to_ckan(id, file_path):
    pass

forms_and_ids = {'F_ANTHROPO': '746f91c8-ab54-4476-95e3-a9da2dafdffc'}

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['d.key@ucl.ac.uk'],
    'email_on_failure': True,
    'email_on_retry': True,
    'start_date': datetime(2017, 7, 20),
}

dag = DAG('sabrev3', default_args=default_args)

unzip = PythonOperator(
    python_callable=unzip_first_file,
    op_args=[
        BaseHook.get_connection('openclinica_export_zip').extra_dejson.get("path"),
        xml_dump_path
    ],
    task_id='unzip',
    dag=dag,
)

for form_prefix, resource_id in forms_and_ids.items():
    csv_path = BaseHook.get_connection('temp_file_dir').extra_dejson.get("path")+form_prefix+".csv"
    pull_dataset = PythonOperator(
        python_callable=save_form_to_csv,
        op_args=[form_prefix, csv_path],
        task_id=form_prefix+"_export",
        dag=dag,
    )
    pull_dataset << unzip

    push_dataset = PythonOperator(
        python_callable=push_to_ckan,
        op_args=[csv_path, resource_id],
        task_id=form_prefix + "_push_to_ckan",
        dag=dag,
    )

    push_dataset << pull_dataset

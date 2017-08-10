from airflow import DAG
import zipfile
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import shutil
import cpgintegrate
from cpgintegrate.connectors import OpenClinica
from cpgintegrate.processors import tanita_bioimpedance, epiq7_liverelast
import requests
import logging
import numpy as np

xml_dump_path = BaseHook.get_connection('temp_file_dir').extra_dejson.get("path")+"openclinica.xml"
openclinica_conn = BaseHook.get_connection('openclinica')
openclinica_auth = openclinica_conn.login, openclinica_conn.password


def unzip_first_file(zip_path, destination):
    zip_file = zipfile.ZipFile(zip_path)
    destination_file = open(destination, "wb")
    shutil.copyfileobj(
        zip_file.open(zip_file.namelist()[0], "r"),
        destination_file
    )
    destination_file.close()


def save_form_to_csv(form_oid_prefix, save_path, **context):
    OpenClinica(openclinica_conn.host, "S_SABREV3_4350", xml_path=xml_dump_path)\
        .get_dataset(form_oid_prefix)\
        .to_csv(save_path)


def save_processed_files_to_csv(item_oid, save_path, processor=None, cols=None, **context):

    logging.info("Logging into OpenClinica")
    oc = OpenClinica(openclinica_conn.host, "S_SABREV3_4350", xml_path=xml_dump_path, auth=openclinica_auth)

    df = cpgintegrate.process_files(oc.iter_files(item_oid), processor)
    is_error = ~df.get("error", df.assign(error=np.NAN).error).isnull()
    df.loc[~is_error, cols+['Source', 'FileSubjectID'] if cols else df.columns].to_csv(save_path)
    context['task_instance'].xcom_push("processing_error", df.loc[is_error, ['Source', 'error']].to_dict())


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


forms_and_ids = {'F_ANTHROPO': (save_form_to_csv, '40aa2125-2132-473b-9a06-302ed97060a6', {}),
                 'I_ANTHR_BIOIMPEDANCEFILE': (save_processed_files_to_csv, 'f1755dba-b898-4af4-bb4e-0c7977ef8a37',
                                              {'cols': ['BMI_WEIGHT', 'BODYFAT_FATM', 'BODYFAT_FATP'],
                                               'processor': tanita_bioimpedance.to_frame}),
                 'I_LIVER_ELASTOGRAPHYFILE': (save_processed_files_to_csv, 'e751379f-2a2b-472c-b454-05cf83d8f099',
                                              {'processor': epiq7_liverelast.to_frame}),
                 }

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
    task_id='unzip', dag=dag,
)

previous = unzip

for form_prefix, (callee, resource_id, extra_args) in forms_and_ids.items():
    csv_path = BaseHook.get_connection('temp_file_dir').extra_dejson.get("path")+form_prefix+".csv"
    pull_dataset = PythonOperator(
        python_callable=callee, op_args=[form_prefix, csv_path], op_kwargs=extra_args,
        task_id=form_prefix+"_export", dag=dag, provide_context=True
    )
    pull_dataset << previous

    push_dataset = PythonOperator(
        python_callable=push_to_ckan, op_args=[csv_path, resource_id],
        task_id=form_prefix + "_push_to_ckan", dag=dag,
    )
    # Run the OpenClinica extracts sequentially because its session management is stoopid
    push_dataset << pull_dataset
    previous = pull_dataset


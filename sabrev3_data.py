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


def unzip_first_file(zip_path, destination):
    zip_file = zipfile.ZipFile(zip_path)
    destination_file = open(destination, "wb")
    shutil.copyfileobj(
        zip_file.open(zip_file.namelist()[0], "r"),
        destination_file
    )
    destination_file.close()


def process_files_and_save(save_path, connector=None, connector_args=None, iter_files_args=None,
                           processor=None, processor_args=None, cols=None, **context):
    connector_instance = connector(*connector_args)
    processor_instance = processor(*processor_args) if processor_args else processor
    (cpgintegrate
     .process_files(connector_instance.iter_files(iter_files_args), processor_instance.to_frame)
     .filter(axis='columns', **({"items": cols} if cols else {"regex": ".*"}))
     .to_csv(save_path))


def save_dataset(save_path, connector=None, connector_args=None, dataset_args=None, **context):
    connector(*connector_args)\
        .get_dataset(*dataset_args)\
        .to_csv(save_path)


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


xml_dump_path = BaseHook.get_connection('temp_file_dir').extra_dejson.get("path")+"openclinica.xml"
openclinica_conn = BaseHook.get_connection('openclinica')
openclinica_conn_args = (openclinica_conn.host, "S_SABREV3_4350", xml_dump_path,
                         (openclinica_conn.login, openclinica_conn.password))

xnat_conn = BaseHook.get_connection('xnat')
xnat_conn_args = (xnat_conn.host, "SABREv3", (xnat_conn.login, xnat_conn.password))

forms_and_ids = {'F_ANTHROPO': ('40aa2125-2132-473b-9a06-302ed97060a6', save_dataset,
                                [OpenClinica, openclinica_conn_args, ['F_ANTHROPO']]),
                 'F_FALLSRISKSAB': ('fa39e257-897f-44d4-81a5-008f140305b0', save_dataset,
                                    [OpenClinica, openclinica_conn_args, ['F_FALLSRISKSAB']]),
                 'I_ANTHR_BIOIMPEDANCEFILE': ('f1755dba-b898-4af4-bb4e-0c7977ef8a37', process_files_and_save,
                                              [OpenClinica, openclinica_conn_args,
                                               'I_ANTHR_BIOIMPEDANCEFILE', tanita_bioimpedance,
                                               None, ['BMI_WEIGHT', 'BODYFAT_FATM', 'BODYFAT_FATP']]),
                 'I_LIVER_ELASTOGRAPHYFILE': ('e751379f-2a2b-472c-b454-05cf83d8f099', process_files_and_save,
                                              [OpenClinica, openclinica_conn_args,
                                               'I_LIVER_ELASTOGRAPHYFILE', epiq7_liverelast]),
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

for resource_name_root, (ckan_resource_id, callee, callee_args) in forms_and_ids.items():
    csv_path = BaseHook.get_connection('temp_file_dir').extra_dejson.get("path")+resource_name_root+"_t3.csv"
    pull_dataset = PythonOperator(
        python_callable=callee, op_args=[csv_path]+callee_args,
        task_id=resource_name_root+"_export", dag=dag, provide_context=True
    )
    pull_dataset << previous

    push_dataset = PythonOperator(
        python_callable=push_to_ckan, op_args=[csv_path, ckan_resource_id],
        task_id=resource_name_root + "_push_to_ckan", dag=dag,
    )

    push_dataset << pull_dataset

    # Run the OpenClinica extracts sequentially because its session management is stoopid
    if callee_args[0] == OpenClinica:
        previous = pull_dataset
    else:
        previous = unzip

from airflow import DAG
import zipfile
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator, ShortCircuitOperator
from datetime import datetime
import shutil
from cpgintegrate.connectors import OpenClinica, XNAT
from cpgintegrate.processors import tanita_bioimpedance, epiq7_liverelast, dicom_sr
import requests
import logging
from airflow.operators.cpg_plugin import CPGDatasetToXCom, CPGProcessorToXCom, XComDatasetProcess, XComDatasetToCkan
import os
import re
import pandas
import cpgintegrate


def unzip_first_file(zip_path, destination):
    zip_file = zipfile.ZipFile(zip_path)
    destination_file = open(destination, "wb")
    shutil.copyfileobj(
        zip_file.open(zip_file.namelist()[0], "r"),
        destination_file
    )
    destination_file.close()


def dataset_freshness_check(source_task_id, **context):
    return context['ti'].xcom_pull(source_task_id, include_prior_dates=True)[cpgintegrate.TIMESTAMP_FIELD_NAME].max()\
           > context['execution_date'].timestamp()


def ult_sr_sats(df):
    sat_cols = [col for col in df.columns if re.search("^.SAT (Left|Right)_Distance\(mm\)_?\d?$", col)]
    filtered = df.dropna(how="all", subset=sat_cols, axis=0)
    out = filtered.loc[:, XComDatasetProcess.cols_always_present + ['study_date']]
    grouped = filtered.loc[:, sat_cols].apply(pandas.to_numeric).groupby(lambda x: x.split("_")[0], axis=1)
    aggs = pandas.concat([grouped.agg(func).rename(columns=lambda x: x+"_"+func)
                          for func in ["mean", "median", "std"]], axis=1).round(2)
    return pandas.concat([aggs, out], axis=1)


csv_dir = BaseHook.get_connection('temp_file_dir').extra_dejson.get("path")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['d.key@ucl.ac.uk'],
    'email_on_failure': True,
    'email_on_retry': True,
    'start_date': datetime(2017, 9, 6),
    'csv_dir': csv_dir,
}

with DAG('sabrev3', default_args=default_args) as dag:

    oc_xml_path = csv_dir + "openclinica.xml"
    oc_args = {"connector_class": OpenClinica, "connection_id": 'openclinica', "connector_args": ['S_SABREV3_4350'],
               "connector_kwargs": {"xml_path": oc_xml_path}, "pool": "OpenClinica", }
    xnat_args = {"connector_class": XNAT, "connection_id": 'xnat', "connector_args": ['SABREv3'],
                 "pool": "xnat"}

    dexa_selector_kwargs = {
        "experiment_selector": lambda x: x['xnat:imagesessiondata/scanner/manufacturer'] == 'HOLOGIC',
        "scan_selector": lambda x: x.xsiType in ["xnat:srScanData", "xnat:otherDicomScanData"]}

    operators_resource_ids = [
        (CPGProcessorToXCom(task_id="SR_ULT", **xnat_args, processor=dicom_sr.to_frame,
                            iter_files_kwargs={
                                "experiment_selector":
                                    lambda x: x[
                                                  'xnat:imagesessiondata/scanner/manufacturer'] == 'Philips Medical Systems'
                                              and x['xnat:imagesessiondata/scanner/model'] != 'Achieva',
                                "scan_selector": lambda x: x.xsiType in ["xnat:srScanData", "xnat:otherDicomScanData"]
                            }),
         [({"task_id": "SR_SAT", "post_processor": ult_sr_sats}, '1c0e5f95-5c95-4d57-bfb1-7b5e815461f2')]),
        (CPGProcessorToXCom(task_id="SR_DEXA", **xnat_args, processor=dicom_sr.to_frame,
                            iter_files_kwargs=dexa_selector_kwargs),
         [({"task_id": "SR_DEXA_HIP", "row_filter": lambda row: 'Hip' in str(row['Analysis Type'])},
           '1c0e5f95-5c95-4d57-bfb1-7b5e815461f2'),
          ({"task_id": "SR_DEXA_SPINE", "row_filter": lambda row: 'Spine' in str(row['Analysis Type'])},
           '1c0e5f95-5c95-4d57-bfb1-7b5e815461f2'),
          ({"task_id": "SR_DEXA_BODY", "row_filter": lambda row: 'Whole Body' in str(row['Analysis Type'])},
           '1c0e5f95-5c95-4d57-bfb1-7b5e815461f2')]),
        (CPGDatasetToXCom(task_id="F_ANTHROPOMETR", **oc_args, dataset_args=['F_ANTHROPOMETR']),
         '1c0e5f95-5c95-4d57-bfb1-7b5e815461f2'),
        (CPGDatasetToXCom(task_id="F_FALLSRISKSAB", **oc_args, dataset_args=['F_FALLSRISKSAB']),
         '1c0e5f95-5c95-4d57-bfb1-7b5e815461f2'),
        (CPGProcessorToXCom(task_id="I_ANTHR_BIOIMPEDANCEFILE_UNFILTERED", **oc_args,
                            iter_files_args=['I_ANTHR_BIOIMPEDANCEFILE'], processor=tanita_bioimpedance.to_frame),
         [({"task_id": "I_ANTHR_BIOIMPEDANCEFILE",
            "filter_cols": ['BMI_WEIGHT', 'TABC_FATP', 'TABC_FATM', 'TABC_FFM', 'TABC_TBW', 'TABC_PMM',
                            'TABC_IMP', 'TABC_BMI', 'TABC_VFATL', 'TABC_RLFATP',
                            'TABC_RLFATM', 'TABC_RLFFM', 'TABC_RLPMM', 'TABC_RLIMP', 'TABC_LLFATP',
                            'TABC_LLFATM', 'TABC_LLFFM', 'TABC_LLPMM', 'TABC_LLIMP', 'TABC_RAFATP',
                            'TABC_RAFATM', 'TABC_RAFFM', 'TABC_RAPMM', 'TABC_RAIMP', 'TABC_LAFATP',
                            'TABC_LAFATM', 'TABC_LAFFM', 'TABC_LAPMM', 'TABC_LAIMP', 'TABC_TRFATP',
                            'TABC_TRFATM', 'TABC_TRFFM', 'TABC_TRPMM']},
           '1c0e5f95-5c95-4d57-bfb1-7b5e815461f2')]),
        (CPGProcessorToXCom(task_id="I_LIVER_ELASTOGRAPHYFILE", **oc_args, iter_files_args=['I_LIVER_ELASTOGRAPHYFILE'],
                            processor=epiq7_liverelast.to_frame),
         '1c0e5f95-5c95-4d57-bfb1-7b5e815461f2'),
    ]

    unzip = PythonOperator(
        python_callable=unzip_first_file,
        op_args=[
            BaseHook.get_connection('openclinica_export_zip').extra_dejson.get("path"),
            oc_xml_path
        ],
        task_id='unzip',
    )

    previous = unzip

    for operator, outputs in operators_resource_ids:

        operator << unzip

        if isinstance(outputs, str):
            push_list = [(operator, outputs)]
        else:
            push_list = [CPGProcessorToXCom(souce_task_id=operator.task_id,**processor_args)
                for processor_args, package_id
                         in outputs
            ]
            [op << operator for op, _ in push_list]

        for branch_operator, package_id in push_list:

            check_file = ShortCircuitOperator(task_id=branch_operator.task_id+"_freshness_check",
                                              python_callable=dataset_freshness_check,
                                              op_args=[branch_operator.task_id], provide_context=True,)
            check_file << branch_operator

            push_dataset = XComDatasetToCkan(task_id=branch_operator.task_id+"_ckan_push",
                                             source_task_id=branch_operator.task_id, ckan_connection_id='ckan',
                                             ckan_package_id=package_id
            )
            push_dataset << check_file


import pandas
from airflow import DAG
from datetime import datetime
import cpgintegrate
from cpgintegrate.connectors import OpenClinica, XNAT, Teleform
from cpgintegrate.processors import tanita_bioimpedance, epiq7_liverelast, dicom_sr, omron_bp, mvo2_exercise
from airflow.operators.cpg_plugin import CPGDatasetToXCom, CPGProcessorToXCom, XComDatasetProcess, XComDatasetToCkan
import re
from datetime import timedelta


# ~~~~~~~~~~ Data processing functions ~~~~~~~~~~~~~~~~~~~~~~~~~


def ult_sr_sats(df):
    sat_cols = [col for col in df.columns if re.search("^.SAT (Left|Right)_Distance_?\d?$", col)]
    filtered = df.dropna(how="all", subset=sat_cols, axis=0)
    out = filtered.loc[:, XComDatasetProcess.cols_always_present + ['study_date']]
    grouped = filtered.loc[:, sat_cols].apply(pandas.to_numeric).groupby(lambda x: x.split("_")[0], axis=1)
    aggs = pandas.concat([grouped.agg(func).rename(columns=lambda x: x + "_" + func)
                          for func in ["mean", "median", "std"]], axis=1).round(2)
    return pandas.concat([aggs, out], axis=1)


def omron_bp_combine(bp_left, bp_right):
    bp_left['BP Arm'] = 'Left'
    bp_right['BP Arm'] = 'Right'

    # Use right arm BP if >10 difference in either measurement or left is missing
    bps_different = ((bp_right['SYS'] - bp_left['SYS']) > 10) | \
                    ((bp_right['DIA'] - bp_left['DIA']) > 10)

    select_right = bps_different | bps_different.index.isin(bp_right.index.difference(bp_left.index))

    # Pulse from left arm if available
    return (bp_right[select_right]
            .append(bp_left[~select_right])
            .assign(Pulse=bp_left.Pulse.combine_first(bp_right.Pulse).round().astype(int)))


def tango_measurement_num_assign(grips_crf, exercise_crf, tango_data):

    def widen_by_meaurement_sequence(data: pandas.DataFrame):

        subject_id = data.index.values[0][0]

        data.reset_index(inplace=True)

        mins, secs = exercise_crf.loc[subject_id, ['StepperMins', 'StepperSecs']].astype(int).values

        ex_time = timedelta(minutes=int(mins), seconds=int(secs))

        if ex_time > timedelta(minutes=5, seconds=20):
            ex_measures = 7
        elif ex_time > timedelta(minutes=3, seconds=20):
            ex_measures = 6
        elif ex_time > timedelta(minutes=1, seconds=20):
            ex_measures = 5
        elif ex_time > timedelta(minutes=0, seconds=0):
            ex_measures = 4
        else:
            ex_measures = 2

        # Work out if they did grip test or not
        try:
            grip_strength = grips_crf.loc[subject_id]
        except KeyError:
            grip_strength = None
        # Error codes to zero if blank
        data.loc[data.ErrorCode.isnull(), "ErrorCode"] = 0

        # Remove #less lines
        data = data[data["#"].notnull()]

        # Trim data according to measurement sequence
        data.index = range(0, len(data))
        data['seqNum'] = 0
        if grip_strength:
            skip_errs = [True] * 6
            skip_errs[2] = False
        else:
            skip_errs = [True] * 4
        skip_errs += [False] * ex_measures
        meas_num = 0
        for seqNum in range(0, len(skip_errs)):
            if skip_errs[seqNum]:
                while data.loc[meas_num].ErrorCode != 0:
                    meas_num += 1
            data.ix[meas_num, 'seqNum'] = int(seqNum + 1)
            meas_num += 1

        if data[data.seqNum == data.seqNum.max()].iloc[0].ErrorCode != 0:
            data.iloc[-1].seq_num = 14

        data = data[data.seqNum != 0]

        m = pandas.melt(data, id_vars=['seqNum'])

        m['variable'] = m['variable'] + '_' + m['seqNum'].astype(int).apply("{:0>2d}".format)
        m['ind'] = 0

        sheet = m.pivot(index='ind', columns='variable', values='value')
        sheet['StepperMins'] = mins
        sheet['StepperSecs'] = secs

        return sheet

    out_frame = \
        (tango_data
         .dropna(how="all", subset=['#'])
         .assign(**{cpgintegrate.SUBJECT_ID_FIELD_NAME: (lambda df: df.index)})
         .set_index([cpgintegrate.SUBJECT_ID_FIELD_NAME, '#'])
         .groupby(level=0)
         .apply(widen_by_meaurement_sequence)
         )
    return out_frame


# ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['d.key@ucl.ac.uk'],
    'email_on_failure': True,
    'email_on_retry': True,
}

with DAG('sabrev3', start_date=datetime(2017, 9, 6), schedule_interval='1 0 * * *', default_args=default_args) as dag:
    oc_args = {"connector_class": OpenClinica, "connection_id": 'sabrev3_openclinica', "pool": "openclinica"}
    xnat_args = {"connector_class": XNAT, "connection_id": 'sabrev3_xnat', "pool": "xnat"}
    teleform_args = {"connector_class": Teleform, "connection_id": 'teleform'}

    dexa_selector_kwargs = {
        "experiment_selector": lambda x: x['xnat:imagesessiondata/scanner/manufacturer'] == 'HOLOGIC',
        "scan_selector": lambda x: x.xsiType in ["xnat:srScanData", "xnat:otherDicomScanData"]}

    bp_combine = XComDatasetProcess(task_id='I_CLINI_CLINICBPFILE', post_processor=omron_bp_combine)
    for field_name in ['I_CLINI_CLINICBPFILE_LEFT', 'I_CLINI_CLINICBPFILE_RIGHT']:
        bp_combine << CPGProcessorToXCom(task_id=field_name, **oc_args,
                                         iter_files_args=[field_name], processor=omron_bp.to_frame)
    sr_dexa = CPGProcessorToXCom(task_id="SR_DEXA", **xnat_args, processor=dicom_sr.to_frame,
                                 iter_files_kwargs=dexa_selector_kwargs)
    tango_sequence = XComDatasetProcess(task_id='TANGO', post_processor=tango_measurement_num_assign)
    CPGDatasetToXCom(task_id='Grip_Strength', **oc_args, dataset_args=['F_GRIPSTRENGTH']) >> tango_sequence

    operators_resource_ids = [
        (CPGProcessorToXCom(task_id="SR_ULT", **xnat_args, processor=dicom_sr.to_frame,
                            iter_files_kwargs={
                                "experiment_selector":
                                    lambda x: x[
                                                  'xnat:imagesessiondata/scanner/manufacturer'] == 'Philips Medical Systems'
                                              and x['xnat:imagesessiondata/scanner/model'] != 'Achieva',
                                "scan_selector": lambda x: x.xsiType in ["xnat:srScanData", "xnat:otherDicomScanData"]
                            }) >> XComDatasetProcess(task_id='SR_SAT', post_processor=ult_sr_sats),
         '1c0e5f95-5c95-4d57-bfb1-7b5e815461f2'),
        (sr_dexa >>
         XComDatasetProcess(task_id='SR_DEXA_HIP', row_filter=lambda row: 'Hip' in str(row['Analysis Type'])),
         '1c0e5f95-5c95-4d57-bfb1-7b5e815461f2'),
        (sr_dexa >>
         XComDatasetProcess(task_id="SR_DEXA_SPINE", row_filter=lambda row: 'Spine' in str(row['Analysis Type'])),
         '1c0e5f95-5c95-4d57-bfb1-7b5e815461f2'),
        (sr_dexa >>
         XComDatasetProcess(task_id="SR_DEXA_BODY", row_filter=lambda row: 'Whole Body' in str(row['Analysis Type'])),
         '1c0e5f95-5c95-4d57-bfb1-7b5e815461f2'),
        (CPGDatasetToXCom(task_id="F_ANTHROPOMETR", **oc_args, dataset_args=['F_ANTHROPOMETR']),
         '1c0e5f95-5c95-4d57-bfb1-7b5e815461f2'),
        (CPGDatasetToXCom(task_id="F_FALLSRISKSAB", **oc_args, dataset_args=['F_FALLSRISKSAB']),
         '1c0e5f95-5c95-4d57-bfb1-7b5e815461f2'),
        (CPGDatasetToXCom(task_id="F_COGNITIVEQUE", **oc_args, dataset_args=['F_COGNITIVEQUE']) >>
         XComDatasetProcess(task_id='F_COGNITIVEQUE_DEPRESSION', filter_cols=
         ['Satisfied', 'DroppedInterests', 'LifeEmpty', 'AfraidBadThings', 'Happy', 'Helpless', 'MemoryProblems',
          'FullOfEnergy', 'HopelessSituation', 'MostBetterOff']),
         'cognitive'),
        (CPGProcessorToXCom(task_id="I_ANTHR_BIOIMPEDANCEFILE_UNFILTERED", **oc_args,
                            iter_files_args=['I_ANTHR_BIOIMPEDANCEFILE'], processor=tanita_bioimpedance.to_frame) >>
         XComDatasetProcess(task_id="I_ANTHR_BIOIMPEDANCEFILE",
                            filter_cols=['BMI_WEIGHT', 'TABC_FATP', 'TABC_FATM', 'TABC_FFM', 'TABC_TBW', 'TABC_PMM',
                                         'TABC_IMP', 'TABC_BMI', 'TABC_VFATL', 'TABC_RLFATP',
                                         'TABC_RLFATM', 'TABC_RLFFM', 'TABC_RLPMM', 'TABC_RLIMP', 'TABC_LLFATP',
                                         'TABC_LLFATM', 'TABC_LLFFM', 'TABC_LLPMM', 'TABC_LLIMP', 'TABC_RAFATP',
                                         'TABC_RAFATM', 'TABC_RAFFM', 'TABC_RAPMM', 'TABC_RAIMP', 'TABC_LAFATP',
                                         'TABC_LAFATM', 'TABC_LAFFM', 'TABC_LAPMM', 'TABC_LAIMP', 'TABC_TRFATP',
                                         'TABC_TRFATM', 'TABC_TRFFM', 'TABC_TRPMM'])
         , '1c0e5f95-5c95-4d57-bfb1-7b5e815461f2'),
        (CPGProcessorToXCom(task_id="I_LIVER_ELASTOGRAPHYFILE", **oc_args, iter_files_args=['I_LIVER_ELASTOGRAPHYFILE'],
                            processor=epiq7_liverelast.to_frame),
         '1c0e5f95-5c95-4d57-bfb1-7b5e815461f2'),
        (CPGDatasetToXCom(task_id="QUEST_1A", **teleform_args, dataset_args=['quest_1a']),
         'c104c2c5-0d8b-4cb5-a1f2-084d681dc3fe'),
        (CPGDatasetToXCom(task_id="QUEST_1B", **teleform_args, dataset_args=['quest_1b']),
         'c104c2c5-0d8b-4cb5-a1f2-084d681dc3fe'),
        (CPGDatasetToXCom(task_id="QUEST_2", **teleform_args, dataset_args=['quest_2']),
         'c104c2c5-0d8b-4cb5-a1f2-084d681dc3fe'),
        (tango_sequence << CPGDatasetToXCom(task_id="F_EXERCISESABR", **oc_args, dataset_args=['F_EXERCISESABR']),
         'exercise'),
        (CPGProcessorToXCom(task_id='TANGO_Data', **oc_args, iter_files_args=['I_EXERC_TANGO'],
                            processor=lambda file:
                            pandas.read_csv(file, header=None, skiprows=1, names=tango_cols)
                            if file.name.lower().endswith(".csv")
                            else pandas.read_excel(file, header=None, skiprows=1, parse_cols=8, names=tango_cols)
                            ) >> tango_sequence, 'exercise'),
        (CPGProcessorToXCom(task_id='I_EXERC_MVO2_XLSX', **oc_args, iter_files_args=['I_EXERC_MVO2_XLSX'],
                            processor=mvo2_exercise.to_frame),
         'exercise'),
        (bp_combine, 'vascular'),
    ]

    for operator, resource_id in operators_resource_ids:
        operator >> XComDatasetToCkan(task_id=operator.task_id + "_ckan_push",
                                      ckan_connection_id='ckan', ckan_package_id=resource_id)

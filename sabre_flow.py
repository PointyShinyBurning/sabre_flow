import pandas
from airflow import DAG
from datetime import datetime
import cpgintegrate
from airflow.operators.dummy_operator import DummyOperator
from cpgintegrate.connectors.openclinica import OpenClinica
from cpgintegrate.connectors.xnat import XNAT
from cpgintegrate.connectors.teleform import Teleform
from cpgintegrate.connectors.ckan import CKAN
from cpgintegrate.processors import tanita_bioimpedance, epiq7_liverelast, dicom_sr, omron_bp, mvo2_exercise
from airflow.operators.cpg_plugin import CPGDatasetToXCom, CPGProcessorToXCom, XComDatasetProcess, XComDatasetToCkan
import re
from datetime import timedelta
from cpgintegrate.processors.utils import edit_using, match_indices


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

        mins, secs = exercise_crf.loc[subject_id, ['StepperMins', 'StepperSecs']].fillna(0).astype(int).values

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
            grip_strength = grips_crf.loc[subject_id, "GripStrengthMax_kpa"]
        except KeyError:
            grip_strength = None

        data.reset_index(inplace=True)
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
        try:
            for seqNum in range(0, len(skip_errs)):
                if skip_errs[seqNum]:
                    while data.loc[meas_num].ErrorCode != 0:
                        meas_num += 1
                data.ix[meas_num, 'seqNum'] = int(seqNum + 1)
                meas_num += 1

            if data[data.seqNum == data.seqNum.max()].iloc[0].ErrorCode != 0:
                data.iloc[-1].seq_num = 14

        except KeyError:
            # Not enough measures
            pass

        m = pandas.melt(
            data.query('seqNum != 0')
                .drop([cpgintegrate.SOURCE_FIELD_NAME, cpgintegrate.SUBJECT_ID_FIELD_NAME], axis=1),
            id_vars=['seqNum'])

        m['variable'] = m['variable'] + '_' + m['seqNum'].astype(int).apply("{:0>2d}".format)

        sheet = m.set_index('variable').drop('seqNum', axis=1).T

        return sheet.assign(**{'Steppermins': mins, 'StepperSecs': secs,
                               cpgintegrate.SOURCE_FIELD_NAME: data.iloc[0].Source}).squeeze()

    out_frame = \
        (tango_data
         .dropna(how="all", subset=['#'])
         .assign(**{cpgintegrate.SUBJECT_ID_FIELD_NAME: (lambda df: df.index)})
         .set_index([cpgintegrate.SUBJECT_ID_FIELD_NAME, '#'])
         .groupby(level=0)
         .apply(widen_by_meaurement_sequence)
         .unstack()
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
    ckan_args = {"connector_class": CKAN, "connection_id": "ckan"}

    dexa_selector_kwargs = {
        "experiment_selector": lambda x: x['xnat:imagesessiondata/scanner/manufacturer'] == 'HOLOGIC',
        "scan_selector": lambda x: x.xsiType in ["xnat:srScanData", "xnat:otherDicomScanData"]}

    bp_combine = XComDatasetProcess(task_id='Clinic_BP', post_processor=omron_bp_combine)
    for field_name in ['I_CLINI_CLINICBPFILE_LEFT', 'I_CLINI_CLINICBPFILE_RIGHT']:
        bp_combine << CPGProcessorToXCom(task_id=field_name, **oc_args,
                                         iter_files_args=[field_name], processor=omron_bp.to_frame)
    CPGProcessorToXCom(task_id="SR_DEXA", **xnat_args, processor=dicom_sr.to_frame,
                       iter_files_kwargs=dexa_selector_kwargs) >> [
        XComDatasetProcess(task_id='DEXA_Hip', row_filter=lambda row: 'Hip' in str(row['Analysis Type'])),
        XComDatasetProcess(task_id="DEXA_Spine", row_filter=lambda row: 'Spine' in str(row['Analysis Type'])),
        XComDatasetProcess(task_id="DEXA_Body", row_filter=lambda row: 'Whole Body' in str(row['Analysis Type']))
    ]
    CPGDatasetToXCom(task_id="Anthropometrics_CRF", **oc_args, dataset_args=['F_ANTHROPOMETR'])

    tango_cols = ['#', 'Date', 'Time', 'SYS', 'DIA', 'HR', 'ErrorCode', 'BpType', 'Comments']
    [CPGDatasetToXCom(task_id='Grip_Strength', **oc_args, dataset_args=['F_GRIPSTRENGTH']),
     CPGDatasetToXCom(task_id="Exercise_CRF", **oc_args, dataset_args=['F_EXERCISESABR']),
     CPGProcessorToXCom(task_id='TANGO_Data', **oc_args, iter_files_args=['I_EXERC_TANGO'],
                        processor=lambda file:
                        pandas.read_csv(file, header=None, skiprows=1, names=tango_cols)
                        if file.name.lower().endswith(".csv")
                        else pandas.read_excel(file, header=None, skiprows=1, parse_cols=8, names=tango_cols)
                        )] >> \
        XComDatasetProcess(task_id='TANGO', post_processor=tango_measurement_num_assign)

    CPGProcessorToXCom(task_id="Ultrasound_SRs", **xnat_args, processor=dicom_sr.to_frame,
                       iter_files_kwargs={
                           "experiment_selector":
                               lambda x: x[
                                             'xnat:imagesessiondata/scanner/manufacturer'] == 'Philips Medical Systems'
                                         and x['xnat:imagesessiondata/scanner/model'] != 'Achieva',
                           "scan_selector": lambda x: x.xsiType in ["xnat:srScanData", "xnat:otherDicomScanData"]
                       }) >> XComDatasetProcess(task_id='Subcutaneous_Fat', post_processor=ult_sr_sats)

    CPGDatasetToXCom(task_id="Falls_Risk_CRF", **oc_args, dataset_args=['F_FALLSRISKSAB'])

    CPGDatasetToXCom(task_id="Cognitive_CRF", **oc_args, dataset_args=['F_COGNITIVEQUE']) >> XComDatasetProcess(
        task_id='Cognitive_CRF_Geriatric_Depression',
        filter_cols=['Satisfied', 'DroppedInterests', 'LifeEmpty', 'AfraidBadThings', 'Happy', 'Helpless',
                     'MemoryProblems', 'FullOfEnergy', 'HopelessSituation', 'MostBetterOff'])

    CPGProcessorToXCom(task_id="I_ANTHR_BIOIMPEDANCEFILE_UNFILTERED", **oc_args,
                       iter_files_args=['I_ANTHR_BIOIMPEDANCEFILE'], processor=tanita_bioimpedance.to_frame) >> \
        XComDatasetProcess(task_id="Bioimpedance",
                           filter_cols=['BMI_WEIGHT', 'TABC_FATP', 'TABC_FATM', 'TABC_FFM', 'TABC_TBW', 'TABC_PMM',
                                        'TABC_IMP', 'TABC_BMI', 'TABC_VFATL', 'TABC_RLFATP',
                                        'TABC_RLFATM', 'TABC_RLFFM', 'TABC_RLPMM', 'TABC_RLIMP', 'TABC_LLFATP',
                                        'TABC_LLFATM', 'TABC_LLFFM', 'TABC_LLPMM', 'TABC_LLIMP', 'TABC_RAFATP',
                                        'TABC_RAFATM', 'TABC_RAFFM', 'TABC_RAPMM', 'TABC_RAIMP', 'TABC_LAFATP',
                                        'TABC_LAFATM', 'TABC_LAFFM', 'TABC_LAPMM', 'TABC_LAIMP', 'TABC_TRFATP',
                                        'TABC_TRFATM', 'TABC_TRFFM', 'TABC_TRPMM'])

    CPGProcessorToXCom(task_id="Liver_Elastography", **oc_args, iter_files_args=['I_LIVER_ELASTOGRAPHYFILE'],
                       processor=epiq7_liverelast.to_frame)

    questionnaire_pulls = [CPGDatasetToXCom(task_id="Questionnaire_1A_raw", **teleform_args, dataset_args=['quest_1a']),
                           CPGDatasetToXCom(task_id="Questionnaire_1B_raw", **teleform_args, dataset_args=['quest_1b']),
                           CPGDatasetToXCom(task_id="Questionnaire_2_raw", **teleform_args, dataset_args=['quest_2'])]

    subject_basics = CPGDatasetToXCom(task_id='subject_basics', **ckan_args,
                                      dataset_kwargs={'dataset': 'basics-and-attendance', 'resource': 'subject_basics'})

    for task in questionnaire_pulls:
        index_match = XComDatasetProcess(task_id=task.task_id.replace("_raw", ""), post_processor=match_indices)
        [task, CPGDatasetToXCom(task_id=task.task_id.replace("_raw", "_edits"), **ckan_args,
                                dataset_kwargs={'dataset': 'questionnaires',
                                                'resource': task.task_id.replace("_raw", "_edits")})] >> \
            XComDatasetProcess(task_id=task.task_id.replace("_raw", "_edited"), post_processor=edit_using) >> \
            index_match
        subject_basics >> index_match

    CPGProcessorToXCom(task_id='MVO2', **oc_args, iter_files_args=['I_EXERC_MVO2_XLSX'],
                       processor=mvo2_exercise.to_frame)

    pushes = {'Ultrasound_SRs': '_sabret3admin',
              'Subcutaneous_Fat': 'anthropometrics',
              'DEXA_Hip': 'anthropometrics',
              'DEXA_Spine': 'anthropometrics',
              'DEXA_Body': 'anthropometrics',
              'Anthropometrics_CRF': 'anthropometrics',
              'Falls_Risk_CRF': 'anthropometrics',
              'Cognitive_CRF_Geriatric_Depression': 'cognitive',
              'Bioimpedance': 'anthropometrics',
              'Liver_Elastography': 'anthropometrics',
              'Questionnaire_1A': 'questionnaires',
              'Questionnaire_1B': 'questionnaires',
              'Questionnaire_2': 'questionnaires',
              'Exercise_CRF': 'exercise',
              'TANGO': 'exercise',
              'MVO2': 'exercise',
              'Clinic_BP': 'vascular',
              }

    for task_id, dataset in pushes.items():
        tasks = {task.task_id: task for task in dag.topological_sort()}
        tasks[task_id] >> XComDatasetToCkan(task_id=task_id + "_ckan_push",
                                            ckan_connection_id='ckan', ckan_package_id=dataset)

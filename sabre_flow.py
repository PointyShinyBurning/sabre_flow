import pandas
from airflow import DAG
from datetime import datetime
import cpgintegrate
from airflow.operators.dummy_operator import DummyOperator
from cpgintegrate.connectors.imap import IMAP
from cpgintegrate.connectors.openclinica import OpenClinica
from cpgintegrate.connectors.xnat import XNAT
from cpgintegrate.connectors.postgres import Postgres
from cpgintegrate.connectors.ckan import CKAN
from cpgintegrate.processors import tanita_bioimpedance, epiq7_liverelast, dicom_sr, omron_bp, mvo2_exercise,\
    doctors_lab_bloods, pulsecor_bp, microquark_spirometry
from airflow.operators.cpg_plugin import CPGDatasetToXCom, CPGProcessorToXCom, XComDatasetProcess, XComDatasetToCkan
import re
from datetime import timedelta
from cpgintegrate.processors.utils import edit_using, match_indices, replace_indices
import IPython
import numpy

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


def tubeloc_match(bloods_crf, tubelocs):

    blood_col_pairs = [('EDTA_WB1ml_1', 'EDTA_WB1ml_Rack1'),
                       ('EDTA_WB_1', 'EDTA_WB_Rack1_1'),
                       ('SALIVA_Grey_1', 'SALIVA_Grey_Rack1_1'),
                       ('PAXGENE_RNA_Light_Brown_1', 'PAXGENE_RNA_Light_Brown_Rack1_1'),
                       ('PAXGENE_DNA_Light_Blue_1', 'PAXGENE_DNA_Light_Blue_Rack1_1'),
                       ('Sodium_Heparin_PlusDMSO_Dark_Green_1', 'Sodium_Heparin_PlusDMSO_Rack1'),
                       ]

    multiples_prefixes = ['EDTA_Plasma_Purple', 'SST_serum_Orange', 'URINE_Yellow']

    extra_pairs = []
    for prefix in multiples_prefixes:
        i = 1
        suffix = "_%s" % i
        while prefix + suffix in bloods_crf.columns:
            extra_pairs.append((prefix + suffix, prefix + '_Rack1' + suffix))
            i += 1
            suffix = "_%s" % i

    blood_col_pairs.extend(extra_pairs)

    melted_bloods = pandas.concat(
        [bloods_crf.assign(SubjectID=bloods_crf.index)
            .melt(id_vars=["SubjectID", rack_var], value_vars=tube_var, var_name='product')
            .rename(columns={rack_var: 'openclinica_rack', 'value': 'tube_code'})
         for tube_var, rack_var in blood_col_pairs]
    )

    melted_bloods = melted_bloods[melted_bloods['tube_code'].notnull()]

    return (tubelocs
            .query('~tube_code.str.contains("No ")')
            .groupby(level=0).last()
            .merge(melted_bloods, how='outer', right_on='tube_code', left_index=True))

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
    teleform_args = {"connector_class": Postgres, "connection_id": 'teleform'}
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

    [CPGProcessorToXCom(task_id="Bioimpedance_raw", **oc_args,
                       iter_files_args=['I_ANTHR_BIOIMPEDANCEFILE'], processor=tanita_bioimpedance.to_frame) >> \
        XComDatasetProcess(task_id="Bioimpedance",
                           filter_cols=['BMI_WEIGHT', 'TABC_FATP', 'TABC_FATM', 'TABC_FFM', 'TABC_TBW', 'TABC_PMM',
                                        'TABC_IMP', 'TABC_BMI', 'TABC_VFATL', 'TABC_RLFATP',
                                        'TABC_RLFATM', 'TABC_RLFFM', 'TABC_RLPMM', 'TABC_RLIMP', 'TABC_LLFATP',
                                        'TABC_LLFATM', 'TABC_LLFFM', 'TABC_LLPMM', 'TABC_LLIMP', 'TABC_RAFATP',
                                        'TABC_RAFATM', 'TABC_RAFFM', 'TABC_RAPMM', 'TABC_RAIMP', 'TABC_LAFATP',
                                        'TABC_LAFATM', 'TABC_LAFFM', 'TABC_LAPMM', 'TABC_LAIMP', 'TABC_TRFATP',
                                        'TABC_TRFATM', 'TABC_TRFFM', 'TABC_TRPMM']),
     CPGDatasetToXCom(task_id="Anthropometrics_CRF_raw", **oc_args, dataset_args=['F_ANTHROPOMETR'])] >> \
        XComDatasetProcess(task_id="Anthropometrics_CRF", post_processor = lambda tanita, crf: crf.apply(
                           lambda row: row.set_value('Weight', tanita.loc[row.name, 'BMI_WEIGHT'])
                           if pandas.isnull(row['Weight']) else row, axis=1))

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
                       }) >> [XComDatasetProcess(task_id='Subcutaneous_Fat', post_processor=ult_sr_sats),
                              XComDatasetToCkan(task_id='Ultrasound_SRs_ckan_push', ckan_package_id='_sabret3admin',
                                                ckan_connection_id='ckan', push_data_dictionary=False, pool='ckan')]

    CPGDatasetToXCom(task_id="Falls_Risk_CRF", **oc_args, dataset_args=['F_FALLSRISKSAB'])

    CPGDatasetToXCom(task_id="Cognitive_CRF", **oc_args, dataset_args=['F_COGNITIVEQUE']) >> XComDatasetProcess(
        task_id='Cognitive_CRF_Geriatric_Depression',
        filter_cols=['Satisfied', 'DroppedInterests', 'LifeEmpty', 'AfraidBadThings', 'Happy', 'Helpless',
                     'MemoryProblems', 'FullOfEnergy', 'HopelessSituation', 'MostBetterOff'])

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

    def sort_report_dates(match_from, match_to):
        return (match_indices(match_from, match_to)
                .assign(report_date=
                        lambda df: pandas.to_datetime(df['Report Date:'], format='%d %B %Y %H:%M:%S', errors='coerce'))
                .sort_values('report_date')
                .drop(columns='report_date'))

    [CPGProcessorToXCom(task_id='Bloods_raw', connector_class=IMAP, connection_id='office365',
                       iter_files_args=['RMX0960', 'inbox'], processor=doctors_lab_bloods.to_frame),
        subject_basics] >> \
        XComDatasetProcess(task_id='Bloods_matched', post_processor=sort_report_dates, keep_duplicates='last') >> [
        XComDatasetProcess(task_id='Bloods_Ranges', filter_cols='.*_OutOfRange|_Range'),
        XComDatasetProcess(task_id='Bloods', filter_cols='^((?!_OutOfRange|_Range).)*$')
    ]

    CPGProcessorToXCom(task_id="Pulsecor_BP", **oc_args, processor=pulsecor_bp.to_frame,
                       iter_files_args=['I_PULSE_PULSECORFILE'])

    CPGDatasetToXCom(task_id="AGES_CRF", **oc_args, dataset_args=['F_AGEFILE'])

    CPGProcessorToXCom(task_id='Spirometry', **oc_args, iter_files_args=['I_SPIRO_SPIROFILE'],
                       processor=microquark_spirometry.to_frame)

    def samples_to_subjects(bloods_crf, sample_list):
        results = (bloods_crf
                   .assign(SubjectID=bloods_crf.index).melt(id_vars='SubjectID').dropna()
                   .pipe(lambda df: df[df.value.str.isdigit()])
                   .assign(value=lambda df: df.value.map(lambda val: int(val)))
                   .join(sample_list, on='value', rsuffix='_results', how='inner')
                   .rename(columns={'value': 'SampleID'})
                   )
        pandas.concat([
            results.loc[:, ['SampleID', 'variable']].rename(columns={'variable': 'value'}).assign(variable='tube_type'),
            results.loc[:, ['SampleID', 'variable_results', 'value_results']].rename(columns=lambda col: col.replace('_results','')),
            results.loc[:, ['SampleID', 'Source']].rename(columns={'Source': 'value'}).assign(variable='Source'),
        ])
        return results

    bloods_collection = CPGDatasetToXCom(task_id="Bloods_CRF", **oc_args, dataset_args=['F_BLOODSCOLLEC'])

    [bloods_collection,
     CPGDatasetToXCom(task_id='tubeloc', connector_class=Postgres, connection_id='tubeloc',dataset_args=['tubeloc'],
                      dataset_kwargs={'index_col': 'tube_code', 'index_col_is_subject_id': False})] >>\
    XComDatasetProcess(task_id='bloods_tubeloc', post_processor=tubeloc_match)


    [bloods_collection
        , CPGProcessorToXCom(task_id="External_bloods_samples", **ckan_args, iter_files_args=['_bloods_files'],
                             processor=lambda f: pandas.read_excel(f)
                             .pipe(lambda df: df.rename(columns={df.columns[0]: 'SampleID'}))
                             .melt(id_vars='SampleID')
                             .dropna().set_index('SampleID'))] >>\
        XComDatasetProcess(task_id='Bloods_external_results', post_processor=samples_to_subjects)

    CPGDatasetToXCom(task_id='xnat_sessions', **xnat_args)

    indexes_seen_as_partners = \
        CPGDatasetToXCom(task_id='indexes_seen_as_partners', **ckan_args, dataset_kwargs={'index_col': 'IndexID'},
                         dataset_args=['basics-and-attendance', 'indexes_seen_as_partners'],)

    [[CPGProcessorToXCom(task_id="cIMT_raw", **ckan_args, iter_files_args=['_cimt_files'],
                         processor=lambda f: pandas.read_csv(f, sep='\t')
                         .set_index("Patient's Name").rename_axis(cpgintegrate.SUBJECT_ID_FIELD_NAME)),
      indexes_seen_as_partners] >> \
        XComDatasetProcess(task_id='cIMT_edited', post_processor=replace_indices),
        subject_basics] >> \
        XComDatasetProcess(task_id='cIMT', post_processor=match_indices)

    CPGDatasetToXCom(task_id='DEXA_CRF', **oc_args, dataset_args=['F_DEXA'])

    pushes = {'External_bloods_samples': '_sabret3admin',
              'Bloods_external_results': '_sabret3admin',
              'SR_DEXA': '_sabret3admin',
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
              # 'Questionnaire_1A_raw': '_sabret3admin',
              # 'Questionnaire_1A_edited': '_sabret3admin',
              # 'Questionnaire_2_raw': '_sabret3admin',
              # 'Questionnaire_2_edited': '_sabret3admin',
              # 'Questionnaire_1B_raw': '_sabret3admin',
              # 'Questionnaire_1B_edited': '_sabret3admin',
              'Exercise_CRF': 'exercise',
              'TANGO': 'exercise',
              'MVO2': 'exercise',
              'Clinic_BP': 'vascular',
              'Pulsecor_BP': 'vascular',
              'Bloods_Ranges': '_sabret3admin',
              'Bloods': 'bloods-urine-saliva',
              'AGES_CRF': 'ages',
              'Spirometry': 'respiratory',
              'cIMT': 'vascular',
              'Bloods_CRF': '_sabret3admin',
              'xnat_sessions': '_sabret3admin',
              'bloods_tubeloc': '_sabret3admin',
              'DEXA_CRF': '_sabret3admin',
              }

    for task_id, dataset in pushes.items():
        tasks = {task.task_id: task for task in dag.topological_sort()}
        tasks[task_id] >> XComDatasetToCkan(task_id=task_id + "_ckan_push",
                                            ckan_connection_id='ckan', ckan_package_id=dataset, pool='ckan')


    def six_dof_report(**frames):
        def six_dof_count(df):
            df_num = df.apply(lambda col: pandas.to_numeric(col, errors='coerce'))
            return {col: (~df_num[col].between(mean - 6*std, mean + 6*std) & ~df_num[col].isna()).sum()
                    for col, mean, std
                    in zip(df_num.columns, df_num.mean(), df_num.std())
                    if not all(df_num[col].isna())
                    }

        return pandas.DataFrame(
            ({'frame': task_id, 'col': column, 'six_dof_removed': count, 'dataset': pushes[task_id]}
             for task_id, df in frames.items()
             for column, count in six_dof_count(df).items()
             )
        ).loc[lambda df: df.six_dof_removed > 0, ['dataset', 'frame', 'col', 'six_dof_removed']]\
            .sort_values('six_dof_removed', ascending=False)

    [task for task in dag.topological_sort() if task.task_id in pushes.keys() and pushes[task.task_id] != '_sabret3admin'] >> \
        XComDatasetProcess(task_id='six_dof_report', post_processor=six_dof_report, task_id_kwargs=True) >> \
        XComDatasetToCkan(task_id='six_dof_report_ckan_push', ckan_connection_id='ckan',
                          ckan_package_id='_sabret3admin', pool='ckan')
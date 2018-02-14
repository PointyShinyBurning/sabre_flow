import pandas
from airflow.operators.cpg_plugin import XComDatasetProcess
import re


def ult_sr_sats(df):
    sat_cols = [col for col in df.columns if re.search("^.SAT (Left|Right)_Distance_?\d?$", col)]
    filtered = df.dropna(how="all", subset=sat_cols, axis=0)
    out = filtered.loc[:, XComDatasetProcess.cols_always_present + ['study_date']]
    grouped = filtered.loc[:, sat_cols].apply(pandas.to_numeric).groupby(lambda x: x.split("_")[0], axis=1)
    aggs = pandas.concat([grouped.agg(func).rename(columns=lambda x: x+"_"+func)
                          for func in ["mean", "median", "std"]], axis=1).round(2)
    return pandas.concat([aggs, out], axis=1)


def omron_bp_combine(bp_left, bp_right):
    bp_left['Arm'] = 'Left'
    bp_right['Arm'] = 'Right'

    # Left arm BP unless >10 difference in either measurement, in which case right
    select = ((bp_right['SYS'] - bp_left['SYS']) > 10) | \
             ((bp_right['DIA'] - bp_left['DIA']) > 10)

    # Left arm pulses
    return (bp_right[select].append(bp_left[not select])
            .assign(Pulse=bp_left['Pulse']))

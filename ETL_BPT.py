import pyhdb
import numpy as np
import pandas as pd
from sqlalchemy import types,create_engine


connP = pyhdb.connect(host="bishdbp01", port=34015,user="ZMLF_FICO_COPATMA",password="Hanapod10")
hanaeng =create_engine('hana+pyhdb://ZMLF_FICO_COPATMA:Hanapod10@bishdbp01:34015')


def get_numeric_freq_value(db):
    freq_cols = ['Frequency per Time Period','Frequency per Time Period1']
    for col in freq_cols:
        db[col]   = db[col].astype(str)
        be_null   = db[col].str.contains('Multi-save|Trial EDLP|EDLP Lockdown until')
        is_summ   = db[col]=='Summer EDLP from P4-P8 only'
        db.loc[be_null, col]= np.nan
        db.loc[is_summ, col]= 28
        is_edlp = db[col].str.contains('EDLP', na=False)
        is_edlc = db[col].str.contains('EDLC', na=False)
        db.loc[is_edlp | is_edlc , col]= 52
        db[col]   = db[col].fillna('UNKNOWN')
        db[col]   = db[col].astype(str)
        has_digit     = db[col].str[0].str.isdigit()
        db.loc[~has_digit, col]=np.nan
        db[col] = db[col].str.extract('(\d+)')
        db[col] = db[col].fillna(0).astype(int)
        db.loc[db[col]>52, col]=0
        db[col] = db[col].astype(str).replace('0',np.nan)
    return db

years = ['2020','2019','2018','2017','2016']
for year in years:
    sql = """SELECT * FROM MIQ.MIQ_BPT_WEEKLY_TMP
    WHERE MLF_YEAR_WEEK LIKE '%"""+year+"""%'
    """
    db   = pd.read_sql(sql,connP)
    print('Data has been taken for',year)
    df = get_numeric_freq_value(db.copy())
    df.to_sql(name='MIQ_BPT_WEEKLY',schema='MIQ',con = hanaeng, index = False,if_exists='append')
    print('Data has been loaded for',year)

#df.to_csv('etl_bpt'+year+'.xlsx',index=False)

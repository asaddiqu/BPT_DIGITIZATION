import sys,os
import pandas as pd
import numpy as np
import pyhdb
import time
from sqlalchemy import types,create_engine
# PROD
hanaeng = create_engine('hana+pyhdb://ZMLF_FICO_COPATMA:Hanapod10@bishdbp01:34015')
conn    = pyhdb.connect(host="bishdbp01",port=34015,user="ZMLF_FICO_COPATMA",password="Hanapod10")
# DEV
#hanaeng =create_engine('hana+pyhdb://ZMLF_FICO_COPATMA:Hanapod10@bishdbd01:31015')
#conn = pyhdb.connect(host="bishdbd01", port=31015,user="ZMLF_FICO_COPATMA",password="Hanapod10")


def GetProdHier():
    sql_brand = """SELECT
               DISTINCT
               MATERIAL AS SAP_MATERIAL,
               PROD_HIER 
               FROM "_SYS_BIC"."zmlf-miq-pkg/CV_POS_MASTER_DATA"
               WHERE PROD_HIER IS NOT NULL
                    """
    dm = pd.read_sql(sql_brand,conn)
    dm['SAP_MATERIAL'] = dm['SAP_MATERIAL'].astype('int64')
    return dm

def explode(df, lst_cols, fill_value='', preserve_index=False):
    # make sure `lst_cols` is list-alike
    if (lst_cols is not None
        and len(lst_cols) > 0
        and not isinstance(lst_cols, (list, tuple, np.ndarray, pd.Series))):
        lst_cols = [lst_cols]
    # all columns except `lst_cols`
    idx_cols = df.columns.difference(lst_cols)
    # calculate lengths of lists
    lens = df[lst_cols[0]].str.len()
    # preserve original index values    
    idx = np.repeat(df.index.values, lens)
    # create "exploded" DF
    res = (pd.DataFrame({
                col:np.repeat(df[col].values, lens)
                for col in idx_cols},
                index=idx)
             .assign(**{col:np.concatenate(df.loc[lens>0, col].values)
                            for col in lst_cols}))
    # append those rows that have empty lists
    if (lens == 0).any():
        # at least one list in cells is empty
        res = (res.append(df.loc[lens==0, idx_cols], sort=False)
                  .fillna(fill_value))
    # revert the original index order
    res = res.sort_index()
    # reset index if requested
    if not preserve_index:        
        res = res.reset_index(drop=True)
    return res

def get_client_id(db):
    sql1="""
    SELECT DISTINCT BANNER AS POS_BANNER,BPT_REGION,BPT_BANNER 
    FROM MIQ.MIQ_BANNER_MAP_POS_BPT 
    """
    sql2="""
    SELECT DISTINCT ZCCLIENT AS CLIENT_ID, ZCCURBAN AS POS_BANNER 
    FROM MIQ.MIQ_ZOSCCU01 WHERE ZCCURSTOR=ZCSTORNUM
    """
    dmp = pd.read_sql(sql1,conn)
    dcl = pd.read_sql(sql2,conn) 
    dmap   = dmp.merge(dcl,on=['POS_BANNER'],how='left')
    dmap   = dmap.drop(['POS_BANNER'],axis=1)
    if len(dmap)==len(dmp):print(f'merge for client id was good with {len(dmp)} records')
    dmap = dmap.drop_duplicates(subset =['BPT_BANNER','BPT_REGION'], keep = 'first')
    dnew = db.merge(dmap, on=['BPT_BANNER','BPT_REGION'],how='left')
    dnew['BPT_BANNER']=dnew['BPT_BANNER'].fillna('UNKNOWN')
    dnew.loc[dnew['BPT_BANNER'].str.contains('FRILLS'),'CLIENT_ID']='LCL'
    dnew.loc[dnew['BPT_BANNER'].str.contains('RCSS'),'CLIENT_ID']='LCL'
    dnew.loc[dnew['BPT_BANNER'].str.contains('SUPER C'),'CLIENT_ID']='LCL'
    dnew.loc[dnew['BPT_BANNER'].str.contains('LOBLAW'),'CLIENT_ID']='LCL'
    dnew.loc[dnew['BPT_BANNER'].str.contains('RASS'),'CLIENT_ID']='LCL'
    dnew.loc[dnew['BPT_BANNER'].str.contains('LCL'),'CLIENT_ID']='LCL'
    dnew.loc[dnew['BPT_BANNER'].str.contains('COSTCO'),'CLIENT_ID']='COS'
    dnew.loc[dnew['BPT_BANNER'].str.contains('SOBEYS'),'CLIENT_ID']='SOB'
    dnew.loc[dnew['BPT_BANNER'].str.contains('IGA'),'CLIENT_ID']='SOB'
    dnew.loc[dnew['BPT_BANNER'].str.contains('OVERWAITEA'),'CLIENT_ID']='OFG'
    dnew.loc[dnew['BPT_BANNER'].str.contains('OFG'),'CLIENT_ID']='OFG'
    dnew.loc[dnew['BPT_BANNER'].str.contains('WALMART'),'CLIENT_ID']='WAL'
    dnew.loc[dnew['BPT_BANNER'].str.contains('SHOPPERS DRUG MART'),'CLIENT_ID']='SDM'
    dnew.loc[dnew['BPT_BANNER'].str.contains('CO-OP'),'CLIENT_ID']='FCL'
    dnew.loc[dnew['BPT_BANNER'].str.contains('FED COOP'),'CLIENT_ID']='FCL'
    dnew['CLIENT_ID'] = dnew['CLIENT_ID'].fillna('OTH')
    dnew = dnew.drop(['BPT_BANNER','BPT_REGION'],axis=1)
    if len(dnew)==len(db):print(f'merge was good having {len(db)} records')
    return dnew

def GetDigitizedBPT(year):
    start = time.time()
    sql_bpt = """SELECT *
    FROM "MIQ"."MIQ_BANNER_PRICING_TEMPLATE"
    WHERE MLF_YEAR_WEEK_FROM LIKE '"""+str(year)+"""%'
    """
    db = pd.read_sql(sql_bpt,conn)
    print(f'Raw BPT has {len(db)} records')
    db['BPT_BANNER']=db['BPT Banner Name'].str.upper()
    db['BPT_REGION']=db['REGION'].str.upper()
    db                  = get_client_id(db.copy())
    db['MLF_YEAR_WEEK'] = db.apply(lambda row: list(range(row['MLF_YEAR_WEEK_FROM'],row['MLF_YEAR_WEEK_TO']+1)),axis=1)
    dbpt = explode(db, ['MLF_YEAR_WEEK'], fill_value='')
    dbpt['MLF_YEAR_WEEK'] = dbpt['MLF_YEAR_WEEK'].astype(int)
    dbpt = dbpt.rename(columns={'SAP Sku#':'SAP_MATERIAL'})
    dbpt['SAP_MATERIAL'] = dbpt['SAP_MATERIAL'].astype(str)
    dbpt = dbpt[dbpt['SAP_MATERIAL'].apply(lambda x: x.isnumeric())]
    dbpt['SAP_MATERIAL'] = dbpt['SAP_MATERIAL'].astype('int64')
    dh  = GetProdHier()
    dhr = dbpt.merge(dh,on=['SAP_MATERIAL'],how='left')
    dhr = dhr.rename(columns={'SAP_MATERIAL':'SAP Sku#'})
    end = time.time()
    print(year,"BPT has",len(dhr)," records, obtained in",int(round((end-start)/60,0)),'min')
    return dhr

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

def DeleteExistingBPTData(year):
    sql_del="""
    delete FROM MIQ.MIQ_BPT_WEEKLY
    WHERE (MLF_YEAR_WEEK LIKE '"""+str(year)+"""%')
    """
    #hanaeng =create_engine('hana+pyhdb://ZMLF_FICO_COPATMA:Hanapod10@bishdbp01:34015')
    conn = hanaeng.connect()
    trans = conn.begin()
    hanaeng.execute(sql_del)
    trans.commit()

year = int(sys.argv[1])
dbpt = GetDigitizedBPT(year) 
#dbpt.to_csv('bpt2019.csv',index=False)
colb=['MLF_YEAR_WEEK','BPT Banner Name','REGION','SAP Sku#']
df = dbpt.drop_duplicates(colb,keep='first')
df = df[df['PROD_HIER'].notnull()]
db = get_numeric_freq_value(df.copy())
try:
    DeleteExistingBPTData(year)
    db.to_sql(name='MIQ_BPT_WEEKLY',schema="MIQ",con = hanaeng, index = False,if_exists='append')
    print('BPT Data Successfully loaded to Hana')
except:
    print(f"WARNING! Data isn't able to reload for year {year}")
    db.to_csv('bpt'+str(year)+'.csv',index=False)

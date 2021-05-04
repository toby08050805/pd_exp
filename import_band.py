import pandas as pd
import re
import sys
from sqlalchemy import create_engine
import pymysql

if __name__ == '__main__':
    pymysql.install_as_MySQLdb()
    lteBandSum=pd.Series(0)
    umtsBandSum=pd.Series(0)
    lteBandlist=[]
    umtsBandlist=[]
    excel_path='./lte.csv'
    lteBand = pd.read_csv(excel_path,engine='python',sep='\t',index_col='NAME')
    excel_path='./umts.csv'
    umtsBand = pd.read_csv(excel_path,engine='python',sep='\t',index_col='id')
    excel_path='./MSRPLUS20210408_band.csv'
    df = pd.read_csv(excel_path,engine='python',sep='|',nrows=100,index_col='TAC')
    #df = pd.read_csv(excel_path,engine='python',sep='|',index_col='TAC')
    #can observe memory usage
    #df.info(memory_usage='deep')
    #can  change to list
    #bands=df['Bands'].tolist()
    bands=df['Bands']
    # band_split.index...get all index , in this case is tac
    #string split


    for row in  bands.index:
        band_split=bands.str.split(",")
        #to sum band value
        #len(band_split.get(row) get all band name in data.
        for y in range(len(band_split.get(row))):
            #scan all the band data  and get tech
            isLteMatch=re.match('LTE',band_split.get(row)[y])
            if isLteMatch:
                #get last number
                bandName="band_"+re.findall("[0-9]+$", band_split.get(row)[y])[0]
                try:
                    #get mapping value from lteband by bnad name and conver it to int64
                    #lteBand.loc[bandName].astype('uint64')
                    lteBandSum=lteBandSum+(lteBand.loc[bandName].astype('uint64')[0])
                except:
                    continue
                lteBandSum=lteBandSum+(lteBand.loc[bandName].astype('uint64')[0])
            isUmtsMatch=re.match('WCDMA',band_split.get(row)[y])
            '''
            if isUmtsMatch:
                #get last number
                bandName="band_"+re.findall("[0-9]+$", band_split.get(row)[y])[0]
                try:
                    #get mapping value from lteband by bnad name and conver it to int64
                    #lteBand.loc[bandName].astype('uint64')
                    umtsBandSum=umtsBandSum+(umtsBand.loc[bandName].astype('uint64')[0])
                except:
                    continue
            '''    
        #values[0] means only get number
        lteBandlist.append(lteBandSum.astype('uint64').values[0])
        #lteBandlist.append(umtsBandSum.astype('uint64').values[0])
        lteBandSum=pd.Series(0)
    #appand to data frame
    df['lte_band']= lteBandlist
    df=df.drop(columns='Bands')
    #print(df[['Bands','lte_band']])
    print(df.columns)
####ready for import to mysql####
    engine = create_engine('mysql://covmo:covmo123@192.168.1.221:3322/gt_covmo')
    #sql = '''select * from `dim_handset_cap_band` limot 10;'''
    #pd.read_sql_query(sql, engine)
    with engine.connect() as con:
        con.execute('drop table if exists dim_handset_cap_band;')

    df.to_sql('dim_handset_cap_band', engine, index= True)
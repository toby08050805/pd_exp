import pandas as pd
import re
import sys
from sqlalchemy import create_engine
import pymysql
import time
import multiprocessing as mp
from queue import Queue

#def job(lteBand,umtsBand,tacinput,startrows,endrows):
def job(lteBand,umtsBand,tacinput,queue):
    lteBandSum=pd.Series(0)
    umtsBandSum=pd.Series(0)
    gsmBand=0
    lteBandlist=[]
    umtsBandlist=[]
    gsmBandlist=[]
    df = pd.DataFrame()
    df=tacinput
    #print(len(df.index))
    #df = pd.read_csv(excel_path,engine='python',sep='|',index_col='TAC')
    #can observe memory usage
    #df.info(memory_usage='deep')
    #can  change to list
    #bands=df['Bands'].tolist()
    bands=df['Bands']
    # band_split.index...get all index , in this case is tac
    #string split
    
    band_split=bands.str.split(",")
    #print(type(band_split))
    for row in  bands.index:tobyo
        #to sum band value
        #len(band_split.get(row) get all band name in data.
        for y in range(len(band_split.get(row))):
            #scan all the band data  and get tech
            band=band_split.get(row)[y]
            #isLteMatch=re.match('LTE',band)
            if re.match('LTE',band):
                #get last number
                try:
                    #get mapping value from lteband by bnad name and conver it to int64
                    #lteBand.loc[bandName].astype('uint64')
                    bandName="band_"+re.findall("[0-9]+$", band)[0]
                    lteBandSum=lteBandSum+(lteBand.loc[bandName].astype('uint64')[0])
                except:
                    continue
            elif re.match('WCDMA',band):
            #if isUmtsMatch:
                #get last number
                try:
                    #get mapping value from lteband by bnad name and conver it to int64
                    #lteBand.loc[bandName].astype('uint64')
                    bandName="band_"+re.findall("[0-9]+$", band)[0]
                    umtsBandSum=umtsBandSum+(umtsBand.loc[bandName].astype('uint64')[0])
                except:
                    continue
            elif re.match('GSM',band):
                gsmBand=12608
                continue
            else:
                continue


        #values[0] means only get number
        lteBandlist.append(lteBandSum.astype('uint64').values[0])
        gsmBandlist.append(gsmBand)
        umtsBandlist.append(umtsBandSum.astype('uint64').values[0])
        #lteBandlist.append(umtsBandSum.astype('uint64').values[0])
        lteBandSum=pd.Series(0)
        umtsBandSum=pd.Series(0)
        gsmBand=0
    #appand to data frame
    #df['lte_band']= lteBandlist
    #print(df)
    #print(lteBandlist)
    df['lte_band'] = lteBandlist
    df['gsm_band'] = gsmBandlist
    df['umts_band'] = umtsBandlist
    df=df.drop(columns='Bands')
    #print(df[['Bands','lte_band']])
    #print(df.columns)
    queue.put(df)
    print(df)
    #return df

#def job_test(lteBand,umtsBand,tacinput,queue):

if __name__ == '__main__':
    pymysql.install_as_MySQLdb()
    start = time.time()
    df_result = pd.DataFrame()
    datacount=0
    excel_path='./lte.csv'
    lteBand = pd.read_csv(excel_path,engine='python',sep='\t',index_col='NAME')
    excel_path='./umts.csv'
    umtsBand = pd.read_csv(excel_path,engine='python',sep='\t',index_col='id')

    if len(sys.argv) == 1:
        excel_path='./MSRPLUS20210408_band.csv'
    else:
        excel_path=sys.argv[1]
    #excel_path = './'+sys.argv[1]
    tacinput = pd.read_csv(excel_path,engine='python',sep='|',nrows=100,index_col='TAC')
    
 
    processes = [] 
    q = mp.Queue()

    bathcdata=50
    datalen = len(tacinput.index)
    threadNum = int(datalen/bathcdata)
    #print(lteBand)
    #print(umtsBand)
    #print(tacinput[0:20])
 
    while datacount < datalen:
        dataStart=datacount
        dataEnd=datacount+bathcdata
        datacount=datacount+bathcdata   
        #df_result=df_result.append(job(lteBand,umtsBand,tacinput[dataStart:dataEnd]))
        #threads.append(threading.Thread(target=job, args=(lteBand,umtsBand,tacinput[dataStart:dataEnd],q)))
        t = mp.Process(target=job, args=(lteBand,umtsBand,tacinput[dataStart:dataEnd],q))
        processes.append(t)
        t.start()
 

    for proc in processes:
        print(proc)
        proc.join()
        print(proc)

    for x in range(threadNum):
        print('get '+x)
        df_result=df_result.append(q.get()) # 取出 queue 裡面的資料
    print(len(df_result.index)) # 顯示執行後的結果
    #df_result=df_result.append(job(lteBand,umtsBand,tacinput[10:20]))

    done = time.time()
    elapsed = done - start
    print(elapsed)
####ready for import to mysql####
    ##engine = create_engine('mysql://covmo:covmo123@192.168.1.221:3322/gt_covmo')
    #sql = '''select * from `dim_handset_cap_band` limot 10;'''
    #pd.read_sql_query(sql, engine)
    ##with engine.connect() as con:
        ##con.execute('drop table if exists dim_handset_cap_band;')

    #df_result.to_sql('dim_handset_cap_band', engine, index= True)
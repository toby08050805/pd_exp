
from openpyxl import load_workbook
import pandas as pd

if __name__ == '__main__':
    #安裝mysql docker 測試
    #connect=mysqlConnect('192.168.1.220','3309','covmo','covmo123','schema_pack_iii').getConnectcursor()
    #cur=connect.cursor()
    #cur.execute('show engines')
    #records = cur.fetchall()
    #print("count：", cur.rowcount)
    '''
    excel_path='./CovMo_LTE_5G_NSA_S20201105_R20201208.xlsx'
    usecols_raw=['#','Column Name','CH Data Type','CH Key']
    df = pd.read_excel('./CovMo_LTE_5G_NSA_S20201105_R20201208.xlsx',engine='openpyxl',sheet_name='table_call_lte',nrows=21,usecols=usecols_raw)
    print(df)
    '''
    excel_path='./umtts.csv'
    df = pd.read_csv(excel_path,engine='python',sep='\t',nrows=21)
    print(df.columns)
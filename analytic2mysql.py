import pymysql
import pandas as pd

db = pymysql.connect(host='localhost',
                         user='root',
                         password='qazmlp00',
                         database='project', local_infile=1)

def handledb(s):
    list = []
    for i in range(1, 4):
        sql = "select * from " + s + str(i) + ';';
        data = pd.read_sql(sql, con = db)
        if(data is not None):
            df_data = pd.DataFrame(data)
            df_data = df_data.rename(columns={'New_cases': 'NewCases'})
            list.append(df_data)
    return list

def mapPartition(list):
    for i in range(len(list)):
        for idx, row in list[i].iterrows():
            list[i].loc[idx,'Date'] = str(list[i].loc[idx,'Date'])[0: 7]
    list_result = []
    for i in range(len(list)):
        df_data = list[i]
        df_data_partition = df_data.groupby(['Date']).agg({"NewCases":"sum", "Confirmed": "sum"})
        list_result.append(df_data_partition)
    return list_result

def reduce(l):
    df_new = l[0]
    for i in range(1, len(l)):
        for idx, row in l[i].iterrows():
            if idx in list(df_new.index):
                df_new.loc[idx,'NewCases'] = df_new.loc[idx,'NewCases'] + row['NewCases']
                df_new.loc[idx,'Confirmed'] = df_new.loc[idx,'Confirmed'] + row['Confirmed']
            else:
                df_new.loc[idx,'NewCases'] = row['NewCases']
                df_new.loc[idx,'Confirmed'] = row['Confirmed']
    df_new = df_new.reset_index()
    df_new = df_new[['Date', 'NewCases', 'Confirmed']]
    df_new = df_new.rename(columns={'Confirmed': 'TotalCases'})
    return df_new

def getResult():
    df_result = reduce(mapPartition(handledb("day_wise")))
    return df_result.to_dict('records')
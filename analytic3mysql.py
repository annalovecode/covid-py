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
            list.append(df_data)
    return list

def mapPartition(list):
    list_result = []
    for i in range(len(list)):
        df_data = list[i]
        df_data_partition = df_data.groupby(['CountryRegion']).agg({"Deaths":"sum", "Confirmed": "sum"})
        list_result.append(df_data_partition)
    return list_result

def reduce(l):
    df_new = l[0]
    for i in range(1, len(l)):
        for idx, row in l[i].iterrows():
            if idx in list(df_new.index):
                df_new.loc[idx,'Deaths'] = df_new.loc[idx,'Deaths'] + row['Deaths']
                df_new.loc[idx,'Confirmed'] = df_new.loc[idx,'Confirmed'] + row['Confirmed']
            else:
                df_new.loc[idx,'Deaths'] = row['Deaths']
                df_new.loc[idx,'Confirmed'] = row['Confirmed']
    for idx, row in df_new.iterrows():
        df_new.loc[idx,'Deaths/Confirmed'] = format((row['Deaths'] / row['Confirmed']), '.2%')
        df_new.loc[idx,'Deaths/ConfirmedHelper'] = row['Deaths'] / row['Confirmed']
    result = pd.DataFrame(df_new, columns = ['Deaths/Confirmed','Deaths/ConfirmedHelper']).sort_values(by = ['Deaths/ConfirmedHelper'], ascending=False)

    result = result.reset_index()
    result = result[['CountryRegion', 'Deaths/Confirmed']]
    result = result.rename(columns={'Deaths/Confirmed': 'Mortality Rate', 'CountryRegion': 'Country/Region'})
    return result

def getResult():
    df_result = reduce(mapPartition(handledb("full_grouped")))
    return df_result.to_dict('records')
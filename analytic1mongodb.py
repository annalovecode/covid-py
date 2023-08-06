import pymongo
import pandas as pd
import math
import json


def handledb(s):
    myclient = pymongo.MongoClient("mongodb://localhost:27017/vueProject")
    mydb = myclient["data"]
    mycol = mydb[s]
    list = []
    for x in mycol.find({}, {'_id': 0}):
        l = []
        for key in x:
            l.append(x[key])
        df_data = pd.DataFrame(l)
        list.append(df_data)
    return list

def mapPartition(list):
    list_result = []
    for i in range(len(list)):
        df_data = list[i]
        df_data_partition = df_data.groupby(['Continent']).agg({"TotalCases":"sum"}).sort_values(by = ['TotalCases'], ascending=False)
        list_result.append(df_data_partition)
    return list_result

# def reduce(list):
#     df_new = list[0]
#     for i in range(1, len(list)):
#         df_new = pd.merge(df_new, list[i], on = "Continent")
#     df_new['sum_TotalCases'] = df_new.iloc[:,0:len(list)].sum(axis=1)
#     result = pd.DataFrame(df_new, columns = ['sum_TotalCases'])
#     result = result.reset_index()
#     result = result.rename(columns={'sum_TotalCases':'TotalCases'})
#     return result

def reduce(l):
    df_new = l[0]
    for i in range(1, len(l)):
        for idx, row in l[i].iterrows():
            if idx in list(df_new.index):
                df_new.loc[idx,'TotalCases'] = df_new.loc[idx,'TotalCases'] + row['TotalCases']
            else:
                df_new.loc[idx,'TotalCases'] = row['TotalCases']
    result = df_new.reset_index()
    return result

def getResult():
    df_result = reduce(mapPartition(handledb("worldometer_data")))
    return df_result.to_dict('records')
import requests
import pandas as pd
import sys
import json
import pymysql

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

#print(handledb('country_wise_latest'))

#input: list = [df1, df2, df3 ...] (dfi represents the ith partition of the data)
#output: list = [df1, df2, df3 ...] (dfi represents the ith partition of the map result of the data)
#                                   (dfi columns : Continent, TotalCases)
def mapPartition(p,lo,hi):
    ret = []
    for i in range(len(p)):
        df = p[i]
        ret.append((df[(lo <= df['Confirmed']) & (df['Confirmed'] <= hi)]['CountryRegion']).tolist())

    if ret:
        return ret
    else:
        return 0


def getResult(lo,hi):
    ret = mapPartition(handledb('country_wise_latest'),lo,hi)
    if not ret:
        return "No counties found with the given range"
    list_ret = []
    for i in ret:
        for j in i:
            list_ret.append(j)

    list_ret = list(set(list_ret))
    return list_ret
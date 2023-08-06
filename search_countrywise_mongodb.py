import pymongo
import pandas as pd
import math
import json


def handledb(s):
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
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
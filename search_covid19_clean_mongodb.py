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


def mapPartition(p,lo,hi):
    country = []
    region = []
    for i in range(len(p)):
        df = p[i]
        country.append((df[(lo <= df['Deaths']) & (df['Deaths'] <= hi)]['CountryRegion']).tolist())
        region.append((df[(lo <= df['Deaths']) & (df['Deaths'] <= hi)]['ProvinceState']).tolist())
    return country,region

def getResult(lo,hi):
    country,region =  mapPartition(handledb('covid_19_clean_complete'),lo,hi)
    list_ret = []
    for i in range(len(country)):
        for j in range(len(country[i])):
            if type(region[i][j]) == str:
                list_ret.append(str(country[i][j]+' '+str(region[i][j])))
            else:
                list_ret.append(str(country[i][j]))
    list_ret = list(set(list_ret))

    return list_ret
import requests
import pandas as pd
import pymysql
#search for Deaths cases between lo to hi

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

#print(handledb('covid_19_clean_complete'))

#input: list = [df1, df2, df3 ...] (dfi represents the ith partition of the data)
#output: list = [df1, df2, df3 ...] (dfi represents the ith partition of the map result of the data)
#                                   (dfi columns : Continent, TotalCases)

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
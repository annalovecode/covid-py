import requests
import pandas as pd
#search for Deaths cases between lo to hi

def handledb(s):
    list = []
    base_url = 'https://dsci551-2d784-default-rtdb.firebaseio.com/'
    i = 1
    while(1):
        url = base_url + s + '/' + str(i) +'.json'
        data = requests.get(url).json()
        if(data is not None):
            df_data = pd.DataFrame(data)
            list.append(df_data)
        else:
            break
        i = i + 1
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
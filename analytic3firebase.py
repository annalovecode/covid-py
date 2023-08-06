import requests
import pandas as pd

def handledb(s):
    list = []
    base_url = 'https://dsci551-2d784-default-rtdb.firebaseio.com/'
    i = 1
    while(1):
        url = base_url + s +'/' + str(i) +'.json'
        data = requests.get(url).json()
        if(data is not None):
            df_data = pd.DataFrame(data)
            list.append(df_data)
        else:
            break
        i = i + 1
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
    # list_result = []
    # for idx, row in df_result.iterrows():
    #     list = []
    #     list.append(idx)
    #     list.append(row["Deaths/Confirmed"])
    #     list_result.append(list)
    # list_title = ['Country / Regeion', 'Mortality Rate']
    # return list_title, list_result
    return df_result.to_dict('records')
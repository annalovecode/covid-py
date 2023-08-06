import requests
import pandas as pd

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

def mapPartition(list):
    list_result = []
    for i in range(len(list)):
        df_data = list[i]
        df_data_partition = df_data.groupby(['Continent']).agg({"TotalCases":"sum"}).sort_values(by = ['TotalCases'], ascending=False)
        list_result.append(df_data_partition)
    return list_result

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
    # list_result = []
    # for idx, row in df_result.iterrows():
    #     list = []
    #     list.append(idx)
    #     list.append(row["sum_TotalCases"])
    #     list_result.append(list)
    # list_title = ['Continent', 'TotalCases']
    # return list_title, list_result
    return df_result.to_dict('records')

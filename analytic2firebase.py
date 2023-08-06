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
    for i in range(len(list)):
        for idx, row in list[i].iterrows():
            list[i].loc[idx,'Date'] = list[i].loc[idx,'Date'][0: 7]
    list_result = []
    for i in range(len(list)):
        df_data = list[i]
        df_data = df_data.rename(columns={'New cases': 'NewCases'})
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
    # list_result = []
    # for idx, row in df_result.iterrows():
    #     list = []
    #     list.append(idx)
    #     list.append(row["NewCases"])
    #     list.append(row["Confirmed"])
    #     list_result.append(list)
    # list_title = ['Date', 'NewCases', 'TotalCases']
    # return list_title, list_result
    return df_result.to_dict('records')
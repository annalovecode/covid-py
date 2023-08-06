import pandas as pd
import pymysql

db = pymysql.connect(host='localhost',
                     user='root',
                     password='qazmlp00',
                     database='project', local_infile=1)
cursor = db.cursor()
dir_sql = "create table if not exists directory(parent varchar(100), child varchar(100), primary key(parent, child))"
partition_sql = "create table if not exists parts(base varchar(100), part varchar(100), primary key(base, part))"
cursor.execute(dir_sql)
cursor.execute(partition_sql)
db.commit()


def getName(c, i):
    if i == 0:
        name = c.split('.')[0]
    else:
        name = c.split('.')[0] + str(i)
    return name


def divideCSV(c, num):
    file = pd.read_csv(c)
    for i in range(1, 4):
        data = file.iloc[(i - 1) * num: i * num]
        fileName = c.split('.')[0] + str(i) + '.csv'
        data.to_csv(fileName, index=False)


def loadSCV(c):
    for i in range(0, 4):
        load_sql = "load data local infile '/Users/annazhao/Downloads/covid 2/covid_sql/'" + getName(c, i) + ".csv' " + \
                   "into table " + getName(c, i) + " fields terminated by ','" \
                                                   "optionally enclosed by '''' lines terminated by '\n' ignore 1 lines"
        cursor.execute(load_sql)
    db.commit()

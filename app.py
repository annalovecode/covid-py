import pymysql as pymysql
from flask import Flask, render_template, request
import requests
import json
import re
import math
from flask import Flask, jsonify
from flask_cors import CORS
import analystic1fireBase, analytic1mysql
import analytic2firebase, analytic2mysql
import analytic3firebase, analytic3mysql
import mysql
import search_countrywise, search_countrywise_mysql
import search_covid19_clean, search_covid19_clean_mysql
import analytic1mongodb
import analytic2mongodb
import analytic3mongodb
import search_countrywise_mongodb, search_covid19_clean_mongodb
from mysql import *
from mongodb import *

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False
CORS(app, resources={r'/*': {'origins': '*'}})

root = 'https://dsci551-2d784-default-rtdb.firebaseio.com/'
directory = 'https://dsci551-2d784-default-rtdb.firebaseio.com/directory'
meta_d = 'https://dsci551-2d784-default-rtdb.firebaseio.com/metadata'


def make_url(x):
    dirc = x.split('/')
    url = ""
    d = {}
    for i in range(len(dirc)):
        if i != 0 and i + 1 < len(dirc):
            url += "/" + dirc[i]
        if i + 1 == len(dirc):
            d[dirc[i]] = ""
    return url, d

def make_mdurl(x,fname, k):
    dirc = x.split('/')
    url = ""
    d = {}
    for i in range(len(dirc)):
        if i != 0 and i + 1 < len(dirc):
            url += "/" + dirc[i]
        if i + 1 == len(dirc):
            url += "/" + dirc[i] + '/' + fname[:-4]
            d[k] = meta_d +url +'.json'
    return url, d

def get_nodes(node, l):
    d = {}
    d['label'] = node
    children = get_children(node, l)
    if children:
        d['children'] = [get_nodes(child, l) for child in children]
    return d

def get_children(node, l):
    return [x[1] for x in l if x[0] == node]

def getPC(i):
    listpc = []
    x = i.split('/')
    for i in range(2, len(x)):
        if x[1] != '':
                p = x[i-1]
                c = x[i]
                listpc.append((p, c))
    return listpc

def addRoot(links):
    if links != []:
        parents, children = zip(*links)
        root_nodes = {x for x in parents if x not in children}
        for node in root_nodes:
            if node != 'Directory':
                links.append(('Directory', node))


links = []
@app.route("/fireBase", methods=['GET', 'POST'])
def edfsfire():
    # files = request.files
    if request.method == 'POST':
        instr = request.form.get('instructions')
        if instr:
            input = instr
            if input.split(' ')[0] == "mkdir":
                url, d = make_url(input.split(' ')[1])
                requests.patch(directory + url + '.json', json.dumps(d))
                ans = "successful create dir!"
                # print(input.split(' ')[1], getPC(input.split(' ')[1]))
                if len(input.split(' ')[1].split('/')) == 2 and input != "mkdir /":
                    links.append(('Directory', input.split(' ')[1][1:]))
                for i in getPC(input.split(' ')[1]):
                            # print(i)
                        links.append(i)
                addRoot(links)
                # print(links)
                tree = get_nodes('Directory', links)
                return jsonify(tree)
            if input.split(' ')[0] == "ls":
                ul = input.split(' ')[1]
                ans = requests.get(directory + ul + '.json').json()
                res = {}
                for i in ans.keys():
                    res[i] = ""
                r = [i for i in ans.keys()]
                return jsonify(r)
            if input.split(' ')[0] == "cat":
                target = input.split(' ')[1]
                tlist = target.split('/')
                u, d = make_url(target[:-4])
                if tlist[-1][:-4] in requests.get(directory + u + '.json').json():  # check file if in directory
                    url = root + tlist[-1][:-4] + '.json'
                    ans = requests.get(url).json()
                return jsonify(ans)
            if re.split(r'[, ;(]+', input)[0] == "put":
                ans = "successfully upload!"
                fname = re.split(r'[), ;(]+', input)[1]  # day_wise.csv
                dirc = re.split(r'[), ;(]+', input)[2]  # /user/jack

                k = re.split(r'[), ;(]+', input)[3]  # 3
                new_d = {}
                new_d[fname[:-4]] = ""
                requests.patch(directory + dirc + '.json', json.dumps(new_d))  # create directory

                df_data = pd.read_csv(fname)
                df_data_list = []
                i = 0
                while (i < len(df_data)):
                    df_data_list.append(df_data[i:i + min(math.ceil(len(df_data) / int(k)), len(df_data) - i)])
                    i = i + min(math.ceil(len(df_data) / int(k)), len(df_data) - i)
                requests.delete(root + fname[:-4] + ".json")
                for i in range(len(df_data_list)):  # create file
                    new_url = root + fname[:-4] + "/" + str(i + 1) + ".json"
                    val = df_data_list[i].reset_index(drop=True)
                    json_str = val.to_json(orient='index')
                    requests.patch(new_url, json_str)
                for n in range(int(k)):
                    url, d = make_mdurl(dirc, fname, str(n+1))
                    requests.patch(meta_d + url + '.json', json.dumps(d))
                # add file to tree
                links.append((dirc.split('/')[-1], fname.split('.')[0]))
                tree = get_nodes('Directory', links)
                ans = "successfully upload!"
                return jsonify(ans)
            if input.split(' ')[0] == "rm":
                target = input.split(' ')[1]
                tlist = target.split('/')
                u, d = make_url(target[:-4])
                print(u, d, tlist[-2], tlist[-1][:-4])
                if tlist[-1][:-4] in requests.get(directory + u + '.json').json():
                    url = root + tlist[-1][:-4] + '.json'
                    d_url = directory + target[:-4] + '.json'
                    requests.delete(url)
                    requests.delete(d_url)
                    new_u, new_d = make_url(u)
                    response = requests.get(directory + u + '.json').json()
                    if response is None:
                        requests.patch(directory + new_u + '.json', json.dumps(new_d))

                links.remove((tlist[-2], tlist[-1][:-4]))
                tree = get_nodes('Directory', links)
                # print(json.dumps(tree, indent=4))
                ans = "successfully removed!"
                return jsonify(ans)
            if re.split(r'[()]+', input)[0] == "getPartitionLocations":
                fname = re.split(r'[()]+', input)[1][:-4]
                response = requests.get(root + fname + '.json').json()
                if response is not None:
                    ans = root + fname + '.json'
                else:
                    ans = None
                return jsonify(ans)
            if re.split(r'[(,) ]+', input)[0] == "readPartition":
                fname = re.split(r'[(,) ]+', input)[1][:-4]
                k = re.split(r'[(,) ]+', input)[2]
                ans = requests.get(root + fname + '/' + k + '.json').json()
                return jsonify(ans)

links_sql = []
@app.route("/mySQL", methods=['GET', 'POST'])
def edfsMY():
    if request.method == "POST":
        instr = request.form.get('instructions')
        if instr:
            input = instr
            insert_stmt = "insert ignore into directory(parent, child) values (%s, %s)"
            if input.split(' ')[0] == "mkdir":
                data = input.split(' ')[1].split('/')
                for i in range(1, len(data)):
                    val = (data[i - 1], data[i])
                    cursor.execute(insert_stmt, val)
                db.commit()

                # print(input.split(' ')[1], getPC(input.split(' ')[1]))
                if len(input.split(' ')[1].split('/')) == 2 and input != "mkdir /":
                    links_sql.append(('Directory', input.split(' ')[1][1:]))
                for i in getPC(input.split(' ')[1]):
                    print(i)
                    links_sql.append(i)
                addRoot(links_sql)
                # print(links)
                tree = get_nodes('Directory', links_sql)
                # ans = "successful create dir!"
                return tree
            if input.split(' ')[0] == "ls":
                data = input.split(' ')[1].split('/')
                sel_stmt = ("select child from directory where parent = %s")
                cursor.execute(sel_stmt, data[-1])
                ans = cursor.fetchall()
                ans1 = json.dumps(ans)
                return jsonify(json.loads(ans1))
            if input.split(' ')[0] == "cat":
                file = input.split(' ')[1].split('/')[-1]
                df = pd.read_csv(file)
                ans = df.to_json(orient="records")
                return jsonify(json.loads(ans))
            if re.split(r'[, ;(]+', input)[0] == "put":
                p = re.split(r'[, ;(]+', input)[2].split('/')[-1]
                c = re.split(r'[, ;(]+', input)[1]
                cursor.execute(insert_stmt, (p, c))
                insert_parts = "insert ignore into parts(base, part) values (%s, %s)"
                for i in range(1, 4):
                    cursor.execute(insert_parts, (c, c.split('.')[0] + str(i)))
                db.commit()
                if c == "day_wise.csv":
                    for i in range(0, 4):
                        create_sql = "create table if not exists " + getName(c, i) + \
                                     " (Date date, Confirmed int, Deaths int, Recovered int, Active int," \
                                     "New_cases int, New_deaths int, New_recovered int, Deaths_per_100Cases float, " \
                                     "Recovered_per_100Cases float, Deaths_per_100Recovered float, No_of_countries int)"
                        mysql.cursor.execute(create_sql)
                    mysql.divideCSV(c, 70)
                if c == "country_wise_latest.csv":
                    for i in range(0, 4):
                        create_sql = "create table if not exists " + getName(c, i) + \
                                     "(CountryRegion varchar(100), Confirmed int, Deaths int, Recovered int, Active int, " \
                                     "New_cases int, New_deaths int, New_recovered int,Deaths_per_100Cases float, " \
                                     "Recovered_per_100Cases float, Deaths_per_100Recovered float," \
                                     "Confirmed_last_week int, 1week_change int, 1week_increase float, WHO_Region varchar(100))"
                        cursor.execute(create_sql)
                    divideCSV(c, 70)
                if c == "full_grouped.csv":
                    for i in range(0, 4):
                        create_sql = "create table if not exists " + getName(c, i) + \
                                     "(Date date, CountryRegion varchar(100), Confirmed int, " \
                                     "Deaths int, Recovered int, Active int, New_cases int, New_deaths int, " \
                                     "New_recovered int, WHO_Region varchar(100))"
                        cursor.execute(create_sql)
                    divideCSV(c, 12000)
                if c == "worldometer_data.csv":
                    for i in range(0, 4):
                        create_sql = "create table if not exists " + getName(c, i) + \
                                     "(CountryRegion varchar(100), Continent varchar(100)," \
                                     "Population int, TotalCases int, NewCases int, TotalDeaths int, NewDeaths int, " \
                                     "TotalRecovered int, NewRecovered int, ActiveCases int, SeriousCritical int," \
                                     "TotCases1Mpop int, Deaths1Mpop int, TotalTests int, Tests1Mpop int, WHORegion varchar(100))"
                        cursor.execute(create_sql)
                    divideCSV(c, 70)
                if c == "covid_19_clean_complete.csv":
                    for i in range(0, 4):
                        create_sql = "create table if not exists " + getName(c, i) + \
                                     "(ProvinceState varchar(100), CountryRegion varchar(100), Latitude float, " \
                                     "Longitude float, Date date, Confirmed int, Deaths int, Recovered int, " \
                                     "Active int, WHORegion varchar(100))"
                        cursor.execute(create_sql)
                    divideCSV(c, 18000)
                loadSCV(c)
                db.commit()

                links_sql.append((p, c[:-4]))
                tree = get_nodes('Directory', links_sql)
                # print(json.dumps(tree, indent=4))
                return jsonify(tree)
            if input.split(' ')[0] == "rm":
                file = input.split(' ')[1].split('/')
                child = file[-1]
                delete_dir = "delete from directory where child=%s"
                delete_parts = "delete from parts where base = %s"
                cursor.execute(delete_dir, child)
                cursor.execute(delete_parts, child)
                drop_sql = "drop table "
                cursor.execute(drop_sql + child.split('.')[0])
                for i in range(1, 4):
                    cursor.execute(drop_sql + child.split('.')[0] + str(i))
                db.commit()
                links_sql.remove((file[-2], child[:-4]))
                tree = get_nodes('Directory', links_sql)
                return jsonify("successfully remove!")
            if re.split(r'[()]+', input)[0] == "getPartitionLocations":
                c = re.split(r'[()]+', input)[1]
                sel_sql = "select part from parts where base=%s"
                cursor.execute(sel_sql, c)
                ans = cursor.fetchall()
                return jsonify(ans)
            if re.split(r'[(,) ]+', input)[0] == "readPartition":
                c = re.split(r'[(,) ]+', input)[1].split('.')[0]
                k = re.split(r'[(,) ]+', input)[3]
                sql = "select parent from directory where child='" + re.split(r'[(,) ]+', input)[1] + "'"
                cursor.execute(sql)
                data = cursor.fetchall()
                if len(data) == 0:
                    ans = ""
                else:
                    df = pd.read_csv(c + str(k) + '.csv')
                    ans = df.to_json(orient='records')
                return jsonify(ans)
    # return render_template('index.html')

links_mongo = []
@app.route("/mongoDB", methods=['GET', 'POST'])
def edfsOne():
    if request.method == "POST":
        instr = request.form.get('instructions')
        #print(instr)
        if instr:
            input = instr
            if input.split(' ')[0] == "mkdir":
                dirc = input.split(' ')[1].split('/')
                # dirc = ['', 'user', 'deft', 'a']
                d = {}
                count = 0
                create_d(d, dirc, count)
                m_col.insert_one(d)
                ans = "successful create dir!"
                if len(input.split(' ')[1].split('/')) == 2 and input != "mkdir /":
                    links_mongo.append(('Directory', input.split(' ')[1][1:]))
                for i in getPC(input.split(' ')[1]):
                    # print(i)
                    links_mongo.append(i)
                addRoot(links_mongo)
                # print(links)
                tree = get_nodes('Directory', links_mongo)
                return jsonify({"code": 200, "msg": ans})
            if input.split(' ')[0] == "ls":
                index = ''
                dirc = input.split(' ')[1].split('/')[1:]
                for i in range(len(dirc)):
                    index += dirc[i]
                    if i + 1 < len(dirc):
                        index += '.'
                q = {index: {"$exists": True}}
                doc = m_col.find(q)
                s = dict()
                for i in doc:
                    dirc = input.split(' ')[1].split('/')
                    check_d(i, dirc, s)
                    # s = json.dumps(s)
                return jsonify({"code": 200, "msg": s})
            if input.split(' ')[0] == "cat":
                fname = input.split(' ')[1].split('/')[-1]
                mycol = mydb[fname[:-4]]
                cursor = list(mycol.find())
                ans = dumps(cursor)
                # return render_template('index.html', t=ans)

                return jsonify({"code": 200, "msg": ans})
            if input.split(' ')[0] == "rm":
                dirc = input.split(' ')[1].split('/')[1:]
                fname = dirc[-1]
                index = ''
                mycol = mydb[fname[:-4]]
                new_dirc = dirc[:-1]
                for i in range(len(new_dirc)):
                    index += new_dirc[i]
                    if i + 1 < len(new_dirc):
                        index += '.'
                q = {index: fname}
                m_col.delete_many(q)
                mycol.drop()
                ans = "successfully remove!"
                return jsonify({"code": 200, "msg": ans})
                # return render_template('index.html')
            if re.split(r'[), ;(]+', input)[0] == "put":
                fname = re.split(r'[), ;(]+', input)[1]
                dirc = re.split(r'[), ;(]+', input)[2].split('/')
                k = re.split(r'[), ;(]+', input)[3]
                d = {}
                count = 0
                create_file_d(d, dirc, count, fname)
                m_col.insert_one(d)  # create directory
                p_col = mydb["partition"]
                f_col = mydb["file"]
                f_col.insert_one({"name": fname[:-4]})
                for n in range(int(k)):
                    p_col.insert_one({'fileName': fname[:-4], 'partition': n, 'location': 'data.' + fname[:-4]})
                df_data = pd.read_csv(fname)
                df_data_list = []
                i = 0
                while (i < len(df_data)):
                    df_data_list.append(df_data[i:i + min(math.ceil(len(df_data) / int(k)), len(df_data) - i)])
                    i = i + min(math.ceil(len(df_data) / int(k)), len(df_data) - i)
                c_list = mydb.list_collection_names()
                if fname[:-4] not in c_list:  # check if existed
                    mycol = mydb[fname[:-4]]
                    for i in range(len(df_data_list)):
                        val = df_data_list[i].reset_index(drop=True)
                        json_str = val.to_json(orient='index')
                        json_data = json.loads(json_str)
                        mycol.insert_one(json_data)
                ans = "successfully upload!"
                return jsonify({"code": 200, "msg": ans})
            # if re.split(r'[), ;(]+', input)[0] == "getPartitionLocations":
            #     fname = re.split(r'[), ;(]+', input)[1]
            #     d_col = {}
            #     d_col['collection: ' + fname] = ""
            #     return jsonify({"code": 200, "msg": d_col})
            if re.split(r'[), ;(]+', input)[0] == "getPartitionLocations":
                fname = re.split(r'[), ;(]+', input)[1]
                c_list = mydb.list_collection_names()
                d_col = {}
                if fname[:-4] in c_list:
                    d_col['data.' + fname[:-4]] = ""
                return jsonify({"code": 200, "msg": d_col})
            if re.split(r'[), ;(]+', input)[0] == "readPartition":
                fname = re.split(r'[), ;(]+', input)[1]
                k = re.split(r'[), ;(]+', input)[2]
                mycol = mydb[fname[:-4]]
                cursor = list(mycol.find())
                count = 1
                for i in cursor:
                    if count == int(k):
                        ans = dumps(i, indent=4)
                    count += 1
                return jsonify({"code": 200, "msg": ans})



# def list_obj():
#     lables, content = analystic1fireBase.getResult()
#
#     print(lables)
#     print(content)
#     # return obj


@app.route('/analytics1firebase', methods=['POST', 'GET'])
def f_analytics1firebase():
    # lables, content = analystic1fireBase.getResult()
    # list_obj()
    # print(content[0])
    # # return jsonify({"code": 200, "msg": ans})
    # print(type(lables))
    # print()
    # c =content.tolist()
    # print(c)
    # return jsonify({"Continent": "North America", "TotalCases": 5919209}, {"Continent": "Asia", "TotalCases": 4689794},
    #                {"Continent": "South America", "TotalCases": 4543273},
    #                {"Continent": "Europe", "TotalCases": 2982576}, {"Continent": "Africa", "TotalCases": 1011867},
    #                {"Continent": "Australia/Oceania", "TotalCases": 21735})
    # return jsonify({"code":200,"lables": l , "content":c})
    result = analystic1fireBase.getResult()
    return jsonify(result)


@app.route('/analytics2firebase', methods=['POST', 'GET'])
def f_analytics2firebase():
    # lables, content = analytic2firebase.getResult()
    # return jsonify({"Date": "2020-01", "NewCases": 9372.0, "TotalCases": 38534.0},
    #                {"Date": "2020-01", "NewCases": 9372.0, "TotalCases": 38534.0})
    result = analytic2firebase.getResult()
    return jsonify(result)


@app.route('/analytics3firebase', methods=['POST', 'GET'])
def f_analytics3firebase():
    # lables, content = analytic3firebase.getResult()
    # # json.dumps()
    # return jsonify({"Country/Regeion": "Yemen", "Mortality Rate": 26.36},
    #                {"Country/Regeion": "Belgium", "Mortality Rate": 15.34})
    result = analytic3firebase.getResult()
    return jsonify(result)

@app.route('/analytics1mysql', methods=['POST', 'GET'])
def f_analytics1mysql():
    result = analytic1mysql.getResult()
    return jsonify(result)

@app.route('/analytics2mysql', methods=['POST', 'GET'])
def f_analytics2mysql():
    result = analytic2mysql.getResult()
    return jsonify(result)

@app.route('/analytics3mysql', methods=['POST', 'GET'])
def f_analytics3mysql():
    result = analytic3mysql.getResult()
    return jsonify(result)

@app.route('/analytics1mongodb', methods=['POST', 'GET'])
def f_analytics1mongodb():
    result = analytic1mongodb.getResult()
    return jsonify(result)

@app.route('/analytics2mongodb', methods=['POST', 'GET'])
def f_analytics2mongodb():
    result = analytic2mongodb.getResult()
    return jsonify(result)

@app.route('/analytics3mongodb', methods=['POST', 'GET'])
def f_analytics3mongodb():
    result = analytic3mongodb.getResult()
    return jsonify(result)


@app.route('/search1', methods=['POST', 'GET'])
def f_search1():
    lo, hi = 0, 0
    if request.values.get("lo"):
        lo = int(request.values.get("lo"))
    if request.values.get("hi"):
        hi = int(request.values.get("hi"))
    list_ret = search_countrywise.getResult(lo, hi)
    print(list_ret)
    # ans = dumps(content)
    # return jsonify({"code": 200, "msg": list_ret})
    return render_template("search1.html", content=list_ret)

@app.route('/search1mysql', methods=['POST', 'GET'])
def f_search1mysql():
    lo, hi = 0, 0
    if request.values.get("lo"):
        lo = int(request.values.get("lo"))
    if request.values.get("hi"):
        hi = int(request.values.get("hi"))
    list_ret = search_countrywise_mysql.getResult(lo, hi)
    print(list_ret)
    # ans = dumps(content)
    # return jsonify({"code": 200, "msg": list_ret})
    return render_template("search1.html", content=list_ret)


@app.route('/search2', methods=['POST', 'GET'])
def f_search2():
    lo, hi = -1, -1
    if request.values.get("lo"):
        lo = int(request.values.get("lo"))
    if request.values.get("hi"):
        hi = int(request.values.get("hi"))
    content = search_covid19_clean.getResult(lo, hi)
    return render_template("search2.html", content=content)

@app.route('/search2mysql', methods=['POST', 'GET'])
def f_search2mysql():
    lo, hi = -1, -1
    if request.values.get("lo"):
        lo = int(request.values.get("lo"))
    if request.values.get("hi"):
        hi = int(request.values.get("hi"))
    content = search_covid19_clean_mysql.getResult(lo, hi)
    return render_template("search2.html", content=content)

@app.route('/search1mongodb', methods=['POST', 'GET'])
def f_search1mongodb():
    lo, hi = 0, 0
    if request.values.get("lo"):
        lo = int(request.values.get("lo"))
    if request.values.get("hi"):
        hi = int(request.values.get("hi"))
    list_ret = search_countrywise_mongodb.getResult(lo, hi)
    print(list_ret)
    # ans = dumps(content)
    # return jsonify({"code": 200, "msg": list_ret})
    return render_template("search1.html", content=list_ret)

@app.route('/search2mongodb', methods=['POST', 'GET'])
def f_search2mongodb():
    lo, hi = -1, -1
    if request.values.get("lo"):
        lo = int(request.values.get("lo"))
    if request.values.get("hi"):
        hi = int(request.values.get("hi"))
    content = search_covid19_clean_mongodb.getResult(lo, hi)
    return render_template("search2.html", content=content)

if __name__ == '__main__':
    app.run(debug=True)

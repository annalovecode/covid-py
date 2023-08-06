import pymongo
from flask import Flask, render_template, request
import json
import re
import csv
from bson.json_util import dumps, loads
import math
import pandas as pd


app = Flask(__name__)

myclient = pymongo.MongoClient("mongodb://localhost:27017/vueProject")

m_db = myclient["metadata"]

mydb = myclient["data"]
m_col = mydb["directory"]


def create_d(d,dirc,count):
    count+=1
    if count+1 == len(dirc):
        d[dirc[count]] = ''
        return d
    else:
        d[dirc[count]]={}
        create_d(d[dirc[count]],dirc,count)


def check_d(d,l,s):
    l.pop(0)
    if len(l)==0:
        if type(d) is dict:
            for i in d:
                #i = 'a'
                s[i] = ""
        if type(d) is str and d != "":
            s[d] = ""
        return s
    else:
        check_d(d[l[0]],l,s)


def create_file_d(d,dirc,count,fname):
    count+=1
    if count+1 == len(dirc):
        d[dirc[count]] = fname
        return d
    else:
        d[dirc[count]]={}
        create_file_d(d[dirc[count]],dirc,count,fname)

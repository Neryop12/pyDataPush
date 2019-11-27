# -*- coding: UTF-8 -*-
import json
import requests
import sys
import re
import mysql.connector as mysql
from datetime import datetime
import time
import pandas as pd
import numpy as mp
from xml.etree import ElementTree
import io
import math
conn = None
#conn = mysql.connect(host='3.95.117.169',database='MediaPlatforms',user='omgdev',password='Sdev@2002!',autocommit=True)
ACCESS_TOKEN_URL = "https://auth.mediamath.com/oauth/token"
#Coneccion a la base de datos
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

host= '3.95.117.169'
name = 'MediaPlatforms'
user = 'omgdev'
password = 'Sdev@2002!'
autocommit= 'True'
def openConnection():
    global conn
    try:
        conn = mysql.connect(host=host, database=name,
                             user=user, password=password, autocommit=autocommit)
    except:
        print("ERROR: NO SE PUEDO ESTABLECER CONEXION MYSQL.")
        sys.exit()

def GetToken():
    global Token
    url='https://api.adform.com/Services/Security/Login'
    Token=requests.post(
                url,
                 headers={
                            'Content-Type': 'application/json',
                            },
               json={
                        "UserName": "JDELEON",
                        "Password": "Joseandre2019"
                    }
               

                )
    Token = Token.json()

def GetAdformCampaign(conn):
     global cur
     cur=conn.cursor(buffered=True)
     print (datetime.now())
     cuentas=[]
     campanas=[]
     campmetrics=[]
     #Querys a insertar a la base de datos
     
     sqlInsertCampaingMetrics = "INSERT INTO dailycampaing(CampaingID,Cost,impressions,clicks,frequency) VALUES (%s,%s,%s,%s,%s)"
     try:
         url='https://api.adform.com/v1/reportingstats/agency/reportdata'
         data=requests.post(
             url,
             #Headers: se coloca el token de autorizacion
             headers={
                 'Authorization':'Bearer '+ Token['Ticket'],
                 'Content-Type':'application/json'
             },
             #Para obtener los datos se realiza un POST con los datos de dimension, metricas y filtros, tiene que tener al menos uno de cada uno
             #Para enviarlo se tiene que guardar en formato Json.
             json={
                       "dimensions": [
                                    "campaignID",
                                    "campaign",
                                    "clientID",
                                    "client",
                                    "campaignStartDate",
                                    "campaignEndDate",
                                    "frequencyCampaign",
                                    "campaignType"
                                ],
                                "metrics": [
                                    "clicks",
                                    "impressions",
                                    "cost"
                                ],
                                "filter": {
                                    "date": {"from":"2019-10-01", "to": "{}".format(str(datetime.now()))}
                                }
                    }
         )
         data=data.json()
         for row in data['reportData']['rows']:
             if(row[0]!=''):
                 if row[7] != 'Non-campaign':
                    
                    if(row[6].isnumeric()):
                        campanametrica=[row[0],row[10],row[9],row[8],row[6]]
                    else:
                        campanametrica=[row[0],row[10],row[9],row[8],'0']
                    campmetrics.append(campanametrica)

         cur.execute("SET FOREIGN_KEY_CHECKS=0")
         cur.executemany(sqlInsertCampaingMetrics,campmetrics)
         cur.execute("SET FOREIGN_KEY_CHECKS=1")
         print('Success AF Campanas')
     except Exception as e:
         print(e)
     finally:
         print (datetime.now())


if __name__ == '__main__':
    openConnection()
    GetToken()
    #PushCampaingAdform(conn)
    GetAdformCampaign(conn)
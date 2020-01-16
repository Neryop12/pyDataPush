# -*- coding: UTF-8 -*-
import json
import requests
import sys
import re
import mysql.connector as mysql
from datetime import datetime,timedelta
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
                        "UserName": "SFRANCO",
                        "Password": "SFrancoomg2019"
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

     sqlInsertCampaingMetrics = "INSERT INTO dailycampaing(CampaingID,Cost,impressions,clicks,frequency,result) VALUES (%s,%s,%s,%s,%s,%s)"
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

                                    "campaignType",
                                    "bannerType"

                                ],
                                "metrics": [
                                    "clicks",
                                    "impressions",
                                    "cost",
                                    "conversions",
                                    "sales"
                                ],
                               "filter":{
                               				"date":{
										    	"from": str(datetime.now() - timedelta(days=5)),
										    	"to":str(datetime.now() + timedelta(days=10))
                               				}
                                   }
                                   ,
                                 "paging": {
                                    "offset": 0,
                                    "limit": 3000
                                }
             }
         )
         data=data.json()
         for row in data['reportData']['rows']:
             result = 0
             if(row[0]!=''):
                 if row[7] != 'Non-campaign':
                     searchObj = re.search(r'([0-9,.]+)_(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.+&]+)_([a-zA-Z0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&0-9]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&0-9]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&0-9]+)_([a-zA-Z-/.+]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ.+0-9]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(2019|19|20|2020)_([0-9,.]+)_(BA|AL|TR|TRRS|TRRRSS|IN|DES|RV|CO|MESAD|LE)_([0-9,.]+)_(CPM|CPMA|CPVi|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCO|CPCO)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([0-9,.-]+)?(_B-)?(_)?([0-9.,]+)?(_S-)?(_)?([0-9.,]+)?(\(([0-9.)])\))?(/[0-9].+)?', row[1], re.M | re.I)
                 if searchObj:
                    Result = (searchObj.group(15))
                    if str(Result).upper() == 'CPVI':
                        result = row[8]
                    elif str(Result).upper() == 'CPMA':
                        result = row[8]
                    elif str(Result).upper() == 'CPM':
                        result = row[9]
                    elif str(Result).upper() == 'CPCO':
                        result = row[9]
                    elif str(Result).upper() == 'CPC':
                        result = row[8]
                    elif str(Result).upper() == 'CPMA':
                        result = row[9]

                    if(row[6].isnumeric()):
                        campanametrica=[row[0],row[10],row[9],row[8],row[6],result]
                    else:
                        campanametrica=[row[0],row[10],row[9],row[8],'0',result]
                    campmetrics.append(campanametrica)

         cur.execute("SET FOREIGN_KEY_CHECKS=0")
         cur.executemany(sqlInsertCampaingMetrics,campmetrics)
         cur.execute("SET FOREIGN_KEY_CHECKS=1")
         print('Success AF Campanas')
         fechahoy = datetime.now()
         dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
         sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("pushAdsMovilCamps", "Success","post_resultados_diarios_adform.py","{}");'.format(dayhoy)
         cur.execute(sqlBitacora)
     except Exception as e:
         print(e)
         fechahoy = datetime.now()
         dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
         sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("pushAdsMovil", "{}","post_resultados_diarios_adform.py","{}");'.format(e,dayhoy)
         cur.execute(sqlBitacora)
     finally:
         print (datetime.now())


if __name__ == '__main__':
    openConnection()
    GetToken()
    #PushCampaingAdform(conn)
    GetAdformCampaign(conn)
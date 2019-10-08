# -*- coding: UTF-8 -*-
import json
import requests
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import re
import mysql.connector as mysql
from datetime import datetime, timedelta
import time
import logger
import pandas as pd
import numpy as mp
from xml.etree import ElementTree
import io
import math
import configparser
conn = None
config = configparser.ConfigParser()
config.read('config.ini')

host= config['TESTING']['HOST']
name = config['TESTING']['NAME']
user = config['TESTING']['USER']
password = config['TESTING']['PASSWORD']
autocommit= config['TESTING']['AUTOCOMMIT']

ACCESS_TOKEN_URL = "https://auth.mediamath.com/oauth/token"
#Coneccion a la base de datos
def openConnection():
    global conn
    try:
        conn = mysql.connect(host=host, database=name,
                             user=user, password=password, autocommit=autocommit)
    except:
        print("ERROR: NO SE PUEDO ESTABLECER CONEXION MYSQL.")
        sys.exit()

#API GET, obtiene el token de session para MediaMath
def GetToken():
    global Token
    #URL para la obtencion del Token
    url='https://auth.mediamath.com/oauth/token'
    Token=requests.post(
                ACCESS_TOKEN_URL,
                data={
                    "grant_type": "password",
                    "username": "sfranco@omg.com.gt",
                    "password": "SFomg2019",
                    "audience": "https://api.mediamath.com/",
                    "scope": "",
                    "client_id": "7Geve1fUt8luTYXCuB1KiVNjIDAcsGxl",
                    "client_secret": "gKDRia_oS-ChUinxxFNXou09DKLOFSaPTeaxQFfWhnA105NwK6BOXnoGgBh4FTfx"
                    }
                )
    Token=Token.json()


#API GET, obtiene el cookie de session para MediaMath
def GetSession():
    global session
    url='https://api.mediamath.com/api/v2.0/session'
    Result = requests.get(
                        url,
                        headers={
                            'Authorization': 'Bearer '+ Token['access_token'],
                            }
                    )
    tree = ElementTree.fromstring(Result.content)
    root = tree.getchildren()
    session = root[1].attrib

## Funcion para la insersion de informacion a la base de datos desde MediaMath, Dimensio:Campaing -> Campaing.
def GetMediaMathCampaing(conn):
     global cur
     cur=conn.cursor(buffered=True)
     fechahoy = datetime.now() - timedelta(1)
     ayer = fechahoy.strftime("%Y-%m-%d")
     campmetrics=[]
     #Querys a insertar a la base de datos
     sqlInsertCampaingMetrics = "INSERT INTO dailycampaing(CampaingID,Cost,impressions,clicks) VALUES (%s,%s,%s,%s)"
     try:
         #Direccion del API, las variable session se pasas com oun Cookie
        url=r'https://api.mediamath.com/reporting/v1/std/performance?filter=organization_id=101058&dimensions=advertiser_name%2cadvertiser_id%2ccampaign_id%2ccampaign_name%2ccampaign_budget&metrics=impressions%2cclicks%2ctotal_spend%2ctotal_spend_cpm%2ctotal_spend_cpa%2ctotal_spend_cpc%2cctr%2cvideo_third_quartile'
        #Request GET, para obtener el reporte de Performance de MediaMath
        Result2 = requests.get(
                            url,

                            headers={
                                'Content-Type': 'application/javascript',
                                'Cookie':'adama_session=' + session['sessionid']
                                },
                            params={
                                'start_date': '{}'.format(str(ayer)),
                                'time_rollup':'by_day',
                            }
                        )
        #Variable para guardar el contenido del request.
        s = Result2.content
        #Libreria Pandas, para extrare datos obtenidos del request (extension .csv)
        data=pd.read_csv(io.StringIO(s.decode('utf-8')))
        #Libreria Numpy, para conventir el formato csv a array
        data = data.to_numpy()
        for row in data:
            if row[3]!='':
                campanametrica=[row[4],row[9],row[8],row[7]]
                campmetrics.append(campanametrica)
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(sqlInsertCampaingMetrics,campmetrics)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        print('Success MM Campanas')
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("GetMediaMathCamp", "Success", "pushDataMediaMathDiario.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
     except Exception as e:
        print(e)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("GetMediaMathCamp", "{}", "pushDataMediaMathDiario.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
     finally:
        print (datetime.now())



def GetMediaMathADSets(conn):
    global cur
    cur=conn.cursor(buffered=True)
    fechahoy = datetime.now() - timedelta(1)
    ayer = fechahoy.strftime("%Y-%m-%d")
    print (datetime.now())
    adsets=[]
    adsetmetrics=[]
    #Querys
    sqlInsertAdsSetsMetrics = "INSERT INTO dailyadset(AdSetID,AdSetName,Impressions,Clicks) VALUES (%s,%s,%s,%s) ON DUPLICATE KEY UPDATE AdSetName=VALUES(AdSetName)"

    try:
         #Direccion del API, las variable session se pasas com oun Cookie
        url=r'https://api.mediamath.com/reporting/v1/std/performance?filter=organization_id=101058&dimensions=campaign_id%2cstrategy_id%2cstrategy_name%2cstrategy_budget%2cstrategy_start_date%2cstrategy_end_date%2cstrategy_type&metrics=impressions%2cclicks%2ctotal_spend%2ctotal_spend_cpm%2ctotal_spend_cpa%2ctotal_spend_cpc%2cctr%2cvideo_third_quartile'
         #Request GET, para obtener el reporte de Performance de MediaMath
        Result2 = requests.get(
                            url,

                            headers={
                                'Content-Type': 'application/javascript',
                                'Cookie':'adama_session=' + session['sessionid']
                                },
                            params={
                                'start_date': '{}'.format(str(ayer)),
                                'time_rollup':'by_day',
                            }
                        )
        #Variable para guardar el contenido del request.
        s = Result2.content
        #Libreria Pandas, para extrare datos obtenidos del request (extension .csv)
        data=pd.read_csv(io.StringIO(s.decode('utf-8')))
        #Libreria Numpy, para conventir el formato csv a array
        data = data.to_numpy()
        for row in data:
            if row[3]!='':
                #se verifica si la fecha es null, de serlo se toma el valor de la fecha
                adsetmetric=[row[3],row[4],row[9],row[10]]
                adsetmetrics.append(adsetmetric)
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(sqlInsertAdsSetsMetrics ,adsetmetrics)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        print('Success MM Adsets')
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("GetMediaMathADSets", "Success", "pushDataMediaMathDiario.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("GetMediaMathADSets", "{}", "pushDataMediaMathDiario.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print (datetime.now())

def GetMediaMathADs(conn):
    global cur
    cur=conn.cursor(buffered=True)
    print (datetime.now())
    ads=[]
    adsmetrics=[]
    #Querys
    sqlInsertMetricsAds = "INSERT INTO dailyads(AdID,Adname,Impressions,Clicks,Cost,Cpm,ctr, Videowatchesat75) VALUES (%s,%s,%s,%s,%s,%s,%s,%s);"

    try:
         #Direccion del API, las variable session se pasas com oun Cookie
        url=r'https://api.mediamath.com/reporting/v1/std/performance?filter=organization_id=101058&dimensions=strategy_id%2ccreative_id%2ccreative_name&metrics=impressions%2cclicks%2ctotal_spend%2ctotal_spend_cpm%2ctotal_spend_cpa%2ctotal_spend_cpc%2cctr%2cvideo_third_quartile'
         #Request GET, para obtener el reporte de Performance de MediaMath
        Result2 = requests.get(
                            url,

                            headers={
                                'Content-Type': 'application/javascript',
                                'Cookie':'adama_session=' + session['sessionid']
                                },
                            params={

                                'start_date': '2019-01-01',
                                'time_rollup':'by_day',
                            }
                        )
        #Variable para guardar el contenido del request.
        s = Result2.content
        #Libreria Pandas, para extrare datos obtenidos del request (extension .csv)
        data=pd.read_csv(io.StringIO(s.decode('utf-8')))
        #Libreria Numpy, para conventir el formato csv a array
        data = data.to_numpy()
        for row in data:
            if row[3]!='':
                admetric=[row[3],row[4],row[5],row[6],row[7],row[8],row[11],row[12]]
                adsmetrics.append(admetric)
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(sqlInsertMetricsAds ,adsmetrics)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        print('Success MM AD')
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("GetMediaMathADs", "Success", "pushDataMediaMathDiario.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("GetMediaMathADs", "{}", "pushDataMediaMathDiario.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print (datetime.now())


if __name__ == '__main__':
    openConnection()
    GetToken()
    GetSession()
    GetMediaMathCampaing(conn)
    GetMediaMathADSets(conn)
    GetMediaMathADs(conn)
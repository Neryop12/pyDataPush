# -*- coding: UTF-8 -*-
import config.db as db
import dbconnect as sql
import json
import requests
import sys
import re
import mysql.connector as mysql
from datetime import datetime, timedelta
import time
import pandas as pd
import numpy as mp
from xml.etree import ElementTree
import io
import math

now = datetime.now()
CreateDate = now.strftime("%Y-%m-%d %H:%M:%S")

ACCESS_TOKEN_URL = "https://auth.mediamath.com/oauth/token"

#API GET, obtiene el token de session para MediaMath
def GetToken():
    global Token
    #URL para la obtencion del Token
    url='https://auth.mediamath.com/oauth/token'
    Token=requests.post(
                ACCESS_TOKEN_URL,
                data={
                    "grant_type": "password",
                    "username": db.MM['username'],
                    "password": db.MM['password'],
                    "audience": "https://api.mediamath.com/",
                    "scope": "",
                    "client_id": db.MM['client_id'],
                    "client_secret": db.MM['client_secret']
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
def GetMediaMathCampaing(media,conn):

     fechahoy = datetime.now() - timedelta(1)
     ayer =(datetime.now() - timedelta(days=29))
     ayer = ayer.strftime("%Y-%m-%d")
     fechahoy=fechahoy.strftime("%Y-%m-%d")
     campmetrics=[]
     historico=[]
  
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
                                'start_date': ayer,
                                'end_date':fechahoy,
                                'time_rollup':'by_day',
                            }
                        )
        #Variable para guardar el contenido del request.
        s = Result2.content
        #Libreria Pandas, para extrare datos obtenidos del request (extension .csv)
        data=pd.read_csv(io.StringIO(s.decode('utf-8')))
        df=pd.DataFrame(data)
        df=df.fillna(0)
        
        for index, row in df.iterrows():
            result = 0
            CampaingID = row['campaign_name']
            Campaingname = row['campaign_name']
            Campaigndailybudget =''
            Campaignlifetimebudget = row['campaign_budget']
            Percentofbudgetused = 0
            StartDate = row['start_date']
            EndDate = row['end_date']
            result = 0
            Objetive = ''
            CampaignIDMFC = 0
            Cost = row['total_spend']
            Frequency = 0
            Reach = 0
            Postengagements=0
            Impressions = row['impressions']
            Clicks = row['clicks']
            Estimatedadrecalllift = 0
            Landingpageviews = 0
            Videowachesat75 = row['video_third_quartile']
            ThruPlay = 0
            Conversions = 0

            if row['campaign_name']!='':
                regex='([0-9,.]+)_(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&]+)_([a-zA-Z0-9-/.%+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.%+&0-9]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+%&0-9]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+%&0-9]+)_([a-zA-Z-/.+]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ.+0-9]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(2019|19|20|2020)_([0-9,.]+)_(BA|AL|TR|TRRS|TRRRSS|IN|DES|RV|CO|MESAD|LE)_([0-9,.]+)_(CPM|CPMA|CPVi|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCO|CPCO)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([0-9,.-]+)?(_B-)?(_)?([0-9.,]+)?(_S-)?(_)?([0-9.,]+)?(\(([0-9.)])\))?(/[0-9].+)?'
                searchObj = re.search(regex, row['campaign_name'])  
                if searchObj:
                    CampaingIDMFC = searchObj.group(1)
                    Result = (searchObj.group(15))
                    if str(Result).upper() == 'CPVI':
                        result = Impressions
                        Objetive='CPVI'
                    elif str(Result).upper() == 'CPM':
                        result = Clicks
                        Objetive='CPM'
                    elif str(Result).upper() == 'CPV':
                        result = Videowachesat75
                        Objetive='CPV'
                    elif str(Result).upper() == 'CPC':
                        result = Impressions
                        Objetive='CPC'
                    else:
                        result=0
                        Objetive=''

                
                if datetime.strptime(EndDate,'%Y-%m-%d') < datetime.now() - timedelta(days=1):
                    historia=[CampaingID,Campaingname,Campaigndailybudget,Campaignlifetimebudget,Percentofbudgetused,
                            StartDate,EndDate,result,Objetive,CampaignIDMFC,Cost,Frequency,Reach,Postengagements,Impressions,
                            Clicks,Estimatedadrecalllift,Landingpageviews,Videowachesat75,ThruPlay,Conversions,CreateDate]
                    historico.append(historia)

                campanametrica=[CampaingID,Campaingname,Campaigndailybudget,Campaignlifetimebudget,Percentofbudgetused,
                                StartDate,EndDate,result,Objetive,CampaignIDMFC,Cost,Frequency,Reach,Postengagements,Impressions,
                                Clicks,Estimatedadrecalllift,Landingpageviews,Videowachesat75,ThruPlay,Conversions,CreateDate]
                campmetrics.append(campanametrica)

        sql.connect.insertDiarioCampanas(campmetrics,media,conn)
        sql.connect.insertReportingDiarioCampanas(campmetrics,media,conn)
        sql.connect.insertHistoric(historico,media,conn)
     except Exception as e:
        print(e)


def GetMediaMathADSets(conn):
    global cur
    cur=conn.cursor(buffered=True)
    fechahoy = datetime.now() - timedelta(1)
    ayer =(datetime.now() - timedelta(days=29))
    ayer = ayer.strftime("%Y-%m-%d")
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
                                'start_date': ayer,
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
    except Exception as e:
        print(e)
    finally:
        print (datetime.now())

def GetMediaMathADs(conn):
    global cur
    cur=conn.cursor(buffered=True)
    print (datetime.now())
    ayer =(datetime.now() - timedelta(days=29))
    ayer = ayer.strftime("%Y-%m-%d")
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

                                'start_date': ayer,
                                'time_rollup':'by_week',
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
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("GetMediaMathADs", "Success", "post_resultados_diarios_mm.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("GetMediaMathADs", "{}", "post_resultados_diarios_mm.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print (datetime.now())


if __name__ == '__main__':
    conn = sql.connect.open(db.DB_DEV['host'],db.DB_DEV['user'],db.DB_DEV['password'],
                        db.DB_DEV['dbname'], db.DB_DEV['port'], db.DB_DEV['autocommit'])

    GetToken()
    GetSession()
    GetMediaMathCampaing('MediaMath',conn)

    sql.connect.close(conn)

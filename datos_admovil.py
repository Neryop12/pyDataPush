# -*- coding: UTF-8 -*-
import json
import requests
import sys
import re
import mysql.connector as mysql
from datetime import datetime, timedelta
import numpy as mp
import configparser
conn = None

config = configparser.ConfigParser()
config.read('config.ini')

host = '3.95.117.169'
# host='localhost'
name = 'MediaPlatforms'
user = 'omgdev'
password = 'Sdev@2002!'
autocommit = 'True'


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
    # URL para la obtencion del Token
    url = 'https://reportapi.adsmovil.com/api/login'
    Token = requests.post(
        url,
        data={

            "email": "rmarroquin@omg.com.gt",
            "password": "#rm4rr0qu1n",
        }
    )
    Token = Token.json()


def pushAdsMovil(conn):
    global cur
    fechaayer = datetime.now() - timedelta(days=1)
    # Formato de las fechas para aceptar en el GET
    dayayer = fechaayer.strftime("%Y-%m-%d")
    print(datetime.now())
    cur = conn.cursor(buffered=True)
    campanas = []
    sqlInsertCampaing = "INSERT INTO CampaingsAM (`CampaingID`, `Campaingname`, `ad`, `Impressions`, `Clicks`, `Ctr`, `Video_firstquartile`, `Video_midpoint`, `Video_thirdquartile`, `Video_completed`, `cost`, `CPM`, `AccountsID`, `StartDate`)  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE Campaingname=VALUES(Campaingname),ad=VALUES(ad), Impressions=VALUES(Impressions),Clicks=VALUES(Clicks),Ctr=VALUES(Ctr),Video_firstquartile=VALUES(Video_firstquartile),Video_midpoint=VALUES(Video_midpoint),Video_thirdquartile=VALUES(Video_thirdquartile),cost=VALUES(cost),CPM=VALUES(CPM) "
    sqlInsertCampaingMetrics = "INSERT INTO dailycampaing(CampaingID,Cost,impressions,clicks,result) VALUES (%s,%s,%s,%s,%s)"
    try:
        url = 'https://reportapi.adsmovil.com/api/campaign/details'
        Result2 = requests.get(
            url,

            headers={
                'Authorization': "'" + Token["result"]["token"] + "'",
            },
            params={
                'report': 'adsmovil_dsp',
                'startDate': dayayer,
                'endDate': dayayer,
            }
        )
        r = Result2.json()
        for row in r["result"]["queryResponseData"]["rows"]:
            result = 0
            for n, i in enumerate(row):
                if i == 'NaN':
                    row[n] = 0
            if row[1] != '':
                campaingName = row[2].split('(')
                campaingName = campaingName[0].replace(' ', '')
                searchObj = re.search(r'([0-9,.]+)_(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.+&]+)_([a-zA-Z0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&0-9]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&0-9]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&0-9]+)_([a-zA-Z-/.+]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ.+0-9]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(2019|19|20|2020)_([0-9,.]+)_(BA|AL|TR|TRRS|TRRRSS|IN|DES|RV|CO|MESAD|LE)_([0-9,.]+)_(CPM|CPMA|CPVI|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCO|CPCO)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([0-9,.-]+)?(_B-)?(_)?([0-9.,]+)?(_S-)?(_)?([0-9.,]+)?(\(([0-9.)])\))?(/[0-9].+)?', campaingName, re.M | re.I)
                if searchObj:
                    Result = (searchObj.group(15))
                    if str(Result).upper() == 'CPVI':
                        result = row[5]
                    elif str(Result).upper() == 'CPM':
                        result = row[4]
                    elif str(Result).upper() == 'CPV':
                        result = row[9]
                    elif str(Result).upper() == 'CPC':
                        result = row[5]

            campana = [row[1], row[11], row[4], row[5], result]
            campanas.append(campana)
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(sqlInsertCampaingMetrics, campanas)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        print('Success AdsMovil Campanas')
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("pushAdsMovil", "Success", "post_resultados_diarios_adsmovil.py","{}");'.format(
            dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("pushAdsMovil", "{}", "post_resultados_diarios_adsmovil.py","{}");'.format(
            e, dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print(datetime.now())


def pushAdsMovilPusgAds(conn):
    global cur
    fechaayer = datetime.now() - timedelta(days=1)
    # Formato de las fechas para aceptar en el GET
    dayayer = fechaayer.strftime("%Y-%m-%d")
    print(datetime.now())
    cur = conn.cursor(buffered=True)
    campanas = []
    sqlInsertCampaing = "INSERT INTO CampaingsAM (`CampaingID`, `Campaingname`, `ad`, `Impressions`, `Clicks`, `Ctr`, `Video_firstquartile`, `Video_midpoint`, `Video_thirdquartile`, `Video_completed`, `cost`, `CPM`, `AccountsID`, `StartDate`)  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE Campaingname=VALUES(Campaingname),ad=VALUES(ad), Impressions=VALUES(Impressions),Clicks=VALUES(Clicks),Ctr=VALUES(Ctr),Video_firstquartile=VALUES(Video_firstquartile),Video_midpoint=VALUES(Video_midpoint),Video_thirdquartile=VALUES(Video_thirdquartile),cost=VALUES(cost),CPM=VALUES(CPM) "
    sqlInsertCampaingMetrics = "INSERT INTO dailycampaing(CampaingID,Cost,impressions,clicks) VALUES (%s,%s,%s,%s)"
    try:
        url = 'https://reportapi.adsmovil.com/api/campaign/details'
        Result2 = requests.get(
            url,

            headers={
                'Authorization': "'" + Token["result"]["token"] + "'",
            },
            params={
                'report': 'pushads',
                'startDate': dayayer,
                'endDate': dayayer,
            }
        )
        r = Result2.json()
        for row in r["result"]["queryResponseData"]["rows"]:
            for n, i in enumerate(row):
                if i == 'NaN':
                    row[n] = 0
            campana = [row[1], row[11], row[4], row[5]]
            campanas.append(campana)
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(sqlInsertCampaingMetrics, campanas)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        print('Success AdsMovil Campanas')
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("pushAdsMovil", "Success", "post_resultados_diarios_adsmovil.py","{}");'.format(
            dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("pushAdsMovil", "{}", "post_resultados_diarios_adsmovil.py","{}");'.format(
            e, dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print(datetime.now())


if __name__ == '__main__':
    openConnection()
    GetToken()
    pushAdsMovil(conn)
    # pushAdsMovilPushAds(conn)
    conn.close()

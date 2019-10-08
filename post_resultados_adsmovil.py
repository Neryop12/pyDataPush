# -*- coding: UTF-8 -*-
import json
import requests
import sys
reload(sys)
sys.setdefaultencoding('utf8')
import re
import mysql.connector as mysql
from datetime import datetime, timedelta
import numpy as mp
import configparser
conn = None

config = configparser.ConfigParser()
config.read('config.ini')

host= config['TESTING']['HOST']
name = config['TESTING']['NAME']
user = config['TESTING']['USER']
password = config['TESTING']['PASSWORD']
autocommit= config['TESTING']['AUTOCOMMIT']

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
    #URL para la obtencion del Token
    url='https://reportapi.adsmovil.com/api/login'
    Token=requests.post(
                url,
                data={

                    "email": "rmarroquin@omg.com.gt",
                    "password": "#rm4rr0qu1n",
                    }
                )
    Token=Token.json()

def pushAdsMovil(conn):
    global cur
    fechaayer = datetime.now() - timedelta(days=1)
    #Formato de las fechas para aceptar en el GET
    dayayer = fechaayer.strftime("%Y-%m-%d")
    print (datetime.now())
    cur=conn.cursor(buffered=True)
    campanas=[]
    sqlInsertCampaing = "INSERT INTO CampaingsAM (`CampaingID`, `Campaingname`, `ad`, `Impressions`, `Clicks`, `Ctr`, `Video_firstquartile`, `Video_midpoint`, `Video_thirdquartile`, `Video_completed`, `cost`, `CPM`, `AccountsID`, `StartDate`)  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE Campaingname=VALUES(Campaingname),ad=VALUES(ad), Impressions=VALUES(Impressions),Clicks=VALUES(Clicks),Ctr=VALUES(Ctr),Video_firstquartile=VALUES(Video_firstquartile),Video_midpoint=VALUES(Video_midpoint),Video_thirdquartile=VALUES(Video_thirdquartile),cost=VALUES(cost),CPM=VALUES(CPM) "

    try:
        url='https://reportapi.adsmovil.com/api/campaign/details'
        Result2 = requests.get(
                            url,

                            headers={
                                'Authorization': "'" + Token["result"]["token"] + "'" ,
                                },
                            params={
                                'report':'adsmovil_dsp',
                                'startDate': dayayer,
                                'endDate':dayayer,
                            }
                        )
        r=Result2.json()
        for row  in r["result"]["queryResponseData"]["rows"]:
            for n, i in enumerate(row):
                if i =='NaN':
                    row[n]=0
            campana=[row[1],row[2],row[3],row[4],row[5],row[6],row[7],row[8],row[9],row[10],row[11],row[12],'AdsMovil',row[0]]
            campanas.append(campana)
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(sqlInsertCampaing,campanas)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        print('Success MM Campanas')
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("pushAdsMovil", "Success", "pushDataAdsMovil.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("pushAdsMovil", "{}", "pushDataAdsMovil.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print (datetime.now())

if __name__ == '__main__':
    openConnection()
    GetToken()
    pushAdsMovil(conn)
    conn.close()
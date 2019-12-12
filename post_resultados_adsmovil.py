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

# host= '3.95.117.169'
host= 'localhost'
name = 'MediaPlatforms'
user = 'omgdev'
password = 'Sdev@2002!'
autocommit= 'True'
def openConnection():
    global conn
    try:
        conn = mysql.connect(host=host, database=name,port=8889,
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
    accounts=[]
    campanas=[]
    conjuntos=[]
    cmetrics=[]
    ametrics=[]
    anuncios=[]

    sqlInsertCampaing = "INSERT INTO Campaings (`CampaingID`, `Campaingname`, `AccountsID`,`Campaignstatus`)  VALUES (%s,%s,%s,'ACTIVE') ON DUPLICATE KEY UPDATE Campaingname=VALUES(Campaingname),StartDate=VALUES(StartDate)"

    AdsMetrics = "INSERT INTO metricsads (`AdID`,`Adname`,`Impressions`, `Clicks`, `Videowatchesat75`,`Videowatchesat100`, `Ctr`, `Cpm`, `cost`)  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    sqlConjunto = "INSERT INTO adsets (`CampaingID`, `AdSetID`,`Adsetname`,`Status`)  VALUES (%s,%s,%s,'ACTIVE') ON DUPLICATE KEY UPDATE Adsetname=(Adsetname)"

    sqlAnuncio="INSERT INTO ads (`AdSetID`,`AdID`,`Adname`,`Status`,`Media`) VALUES(%s,%s,%s,'ACTIVE','AM') ON DUPLICATE KEY UPDATE Adname=(Adname)"

    sqlInsertAccounts="INSERT INTO `MediaPlatforms`.`Accounts` (`AccountsID`, `Account`, `Media`, `State`) VALUES (%s, %s, %s, %s)ON DUPLICATE KEY UPDATE Account=VALUES(Account);"

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

            searchObj = re.search(r'(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.+&]+)_([a-zA-Z0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)', str(row[2]), re.M | re.I)
            if searchObj:
                if searchObj.group(2)=='CLARO':
                    AccountID=searchObj.group(1)+searchObj.group(2)+searchObj.group(3)+searchObj.group(4)
                else:
                    AccountID=searchObj.group(2)+searchObj.group(3)+searchObj.group(4)
                adid=row[1]+row[3]
                account=[AccountID,AccountID,'AM',1]
                campana=[row[1],row[2],AccountID]
                conjunto=[row[1],AccountID,row[2]]
                anuncio=[AccountID, adid,row[3]]
                ametric=[adid,row[3],row[4],row[5],row[9],row[10],row[6],row[12],row[11]]

            accounts.append(account)
            campanas.append(campana)


            conjuntos.append(conjunto)
            anuncios.append(anuncio)
            ametrics.append(ametric)

        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(sqlInsertAccounts,accounts)
        cur.executemany(sqlInsertCampaing,campanas)
        cur.executemany(sqlConjunto,conjuntos)
        cur.executemany(sqlAnuncio,anuncios)
        cur.executemany(AdsMetrics,ametrics)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        print('Success AdsMovil Campanas')
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

def resultsCamps(conn):
    global cur
    print (datetime.now())
    cmetrics=[]
    cur=conn.cursor(buffered=True)
    sqlMetricsAds="SELECT  a.AdID,SUM(b.Clicks) as Clicks, SUM(b.Impressions) as Impressions, SUM(b.cost) as cost FROM ads a INNER JOIN metricsads b on a.AdID=b.AdID GROUP BY a.AdID"
    sqlInsertMetrics=""
    cur.execute(sqlMetricsAds,)
    resultscon = cur.fetchall()
    for row in resultscon:
        cmetric=[row[0],row[1],row[2],row[3]]

if __name__ == '__main__':
    openConnection()
    GetToken()
    pushAdsMovil(conn)
    # resultsCamps(conn)
    conn.close()
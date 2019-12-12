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

host= '3.95.117.169'
# host= 'localhost'
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

    AdsMetrics = "INSERT INTO MetricsAds (`AdID`,`Adname`,`Impressions`, `Clicks`, `Videowatchesat75`,`Videowatchesat100`, `Ctr`, `Cpm`, `cost`)  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    sqlConjunto = "INSERT INTO adsets (`CampaingID`, `AdSetID`,`Adsetname`,`Status`)  VALUES (%s,%s,%s,'ACTIVE') ON DUPLICATE KEY UPDATE Adsetname=VALUES(Adsetname)"

    sqlAnuncio="INSERT INTO Ads (`AdSetID`,`AdID`,`Adname`,`Status`,`Media`) VALUES(%s,%s,%s,'ACTIVE','AM') ON DUPLICATE KEY UPDATE Adname=(Adname)"

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
            adsetid=row[1]+AccountID
            account=[AccountID,AccountID,'AM',1]
            campana=[row[1],row[2],AccountID]
            conjunto=[row[1],adsetid,row[2]]
            anuncio=[adsetid, adid,row[3]]
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
    resultado=''
    cur=conn.cursor(buffered=True)
    sqlMetricsAds="""select c.CampaingID,c.Campaingname,sum(m.Clicks),sum(m.Impressions),SUM(m.cost)  from MetricsAds m
                    inner join Ads a on a.AdID = m.AdID
                    inner join Adsets ad on ad.AdSetID = a.AdSetID
                    INNER join Campaings c on c.CampaingID = ad.CampaingID
                    GROUP by c.CampaingID;
                    """
    sqlInsertMetrics="INSERT INTO CampaingMetrics(CampaingID,Clicks,Impressions,Cost,Diario,Result)VALUE(%s,%s,%s,%s,1,%s)"
    try:
        cur.execute(sqlMetricsAds,)
        resultscon = cur.fetchall()
        for row in resultscon:
            searchObj = re.search(r'(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.+&]+)_([a-zA-Z0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-Z-/.+]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ.+]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(19|2019)_([0-9,.]+)_(BA|AL|TR|TRRS|TRRRSS|IN|DES|RV|CO|MESAD|LE)_([0-9,.]+)_(CPM|CPMA|CPVi|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCO|CPCO)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&]+)_([0-9,.-]+)?(_B-)?(_)?([0-9]+)?(_S-)?(_)?([0-9]+)?(\(([0-9.)]+)\))?', str(row[1]), re.M | re.I)
            if searchObj:
                if searchObj.group(14)=='CPM':
                    resultado=row[3]
                elif searchObj.group(14)=='CPC':
                    resultado=row[2]
                elif searchObj.group(14)=='CPVi':
                    resultado=row[2]
            cmetric=[row[0],row[2],row[3],row[4],resultado]
            cmetrics.append(cmetric)
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(sqlInsertMetrics,cmetrics)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        print('Success AdsMovil Resultador Diario Campanas')
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("pushAdsMovilCamps", "Success","pushDataAdsMovil.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("pushAdsMovil", "{}","pushDataAdsMovil.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print (datetime.now())

if __name__ == '__main__':
    openConnection()
    # GetToken()
    # pushAdsMovil(conn)
    resultsCamps(conn)
    conn.close()
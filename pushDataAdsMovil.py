# -*- coding: UTF-8 -*-
import json
import requests
import sys
import re
import mysql.connector as mysql
from datetime import datetime, timedelta
import numpy as mp
conn = None

#Coneccion a la base de datos de mfcgt
def openConnection():
    global conn
    try:
        conn = mysql.connect(host='3.95.117.169', database='MediaPlatforms',
                             user='omgdev', password='Sdev@2002!', autocommit=True)
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
    cuentas=[]
    campanas=[]
    sqlInsertAccount = "INSERT INTO Accounts(AccountsID, Account,Media) values(%s,%s,%s) ON DUPLICATE KEY UPDATE Account=VALUES(Account)"
    sqlInsertCampaing = "INSERT INTO CampaingsAM(`StartDate`,`CampaingID`, `Campaingname`, `Advertiser`, `Impressions`, `Clicks`, `Ctr`, `Spend`, `AccountsID`)  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE Campaingname=VALUES(Campaingname), Advertiser=VALUES(Advertiser), Impressions=VALUES(Impressions),Clicks=VALUES(Clicks),Ctr=VALUES(Ctr),Spend=VALUES(Spend) "
    
    try:
        url='https://reportapi.adsmovil.com/api/campaign/details'
        Result2 = requests.get(
                            url,

                            headers={
                                'Authorization': "'" + Token["result"]["token"] + "'" ,
                                },
                            params={
                                'report':'pushads',
                                'startDate': dayayer,
                                'endDate':dayayer,
                            }
                        )
        r=Result2.json()
        for row  in r["result"]["queryResponseData"]["rows"]:
            accountId = str(row[2]).strip() + '/AM'
            cuenta=[accountId,row[2],'AM']
            cuentas.append(cuenta)
            campana=[row[0],accountId,row[2],row[1],row[4],row[5],row[6],row[7],accountId]
            campanas.append(campana)
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(sqlInsertAccount ,cuentas)
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
# -*- coding: UTF-8 -*-
import json
import requests
import sys
import re
import mysql.connector as mysql
from datetime import datetime
import configparser

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
        conn = mysql.connect(host=host, database=name,user=user, password=password, autocommit=autocommit)
    except:
        print("ERROR: NO SE PUEDO ESTABLECER CONEXION MYSQL.")
        sys.exit()

def SuperMetricsConnect(URL,MEDIA):
    print (datetime.now())
    global data
    global temp_k
    try:
        r=requests.get('https://spreadsheets.google.com/feeds/list/'+URL+'/od6/public/values?alt=json')
        data=r.json()
        temp_k=data['feed']['entry']
    except:
        print("ERROR: CONEXION CON SUPERMETRICS",MEDIA)
        sys.exit()

def fbPaused(conn):
    cur=conn.cursor(buffered=True)
    SuperMetricsConnect('1TZfbnL_ETZF77S80kK14rzUEuZmsv-us_aYLzsU2CgI','FB')
    StatusCampaings=[]
    StatusAdsets=[]
    StatusAds=[]
    SErrors=[]
    StatusErrorsCampaings=[]
    try:
        cur = conn.cursor()
        for atr in temp_k:
            campaingid=atr['gsx$campaignid']['$t']
            adsetid=atr['gsx$adsetid']['$t']
            adid=atr['gsx$adid']['$t']
            campstatus=atr['gsx$campaignconfiguredstatus']['$t']
            adsetstatus=atr['gsx$adsetconfiguredstatus']['$t']
            adstatus=atr['gsx$adconfiguredstatus']['$t']
            Campstatus='UPDATE Campaings set Campaignstatus=%s,CreateDate=%s  where CampaingID=%s'
            Adsetsstatus='UPDATE Adsets set Status=%s,CreateDate=%s where AdSetID=%s'
            Ads='UPDATE Ads set Adstatus=%s,CreateDate=%s where AdID=%s'
            ErrorsCamping='UPDATE ErrorsCampaings set StatusCampaing=%s,CreateDate=%s where CampaingID=%s'

            if campstatus!='PAUSED':
                if adsetstatus!='PAUSED':
                    if adstatus!='PAUSED':
                        Estatus=adstatus
                    else:
                        Estatus=adstatus
                else:
                    Estatus=adsetstatus
            else:
                Estatus=campstatus

            SErrors=(Estatus, campaingid,datetime.now())

            SCamp=(campstatus, campaingid,datetime.now())
            SAdsets=(adsetstatus, adsetstatus,datetime.now())
            SAds=(adstatus,adid,datetime.now())
            SErrors=(campstatus, campaingid,datetime.now())
            StatusAds.append(SAds)
            StatusAdsets.append(SAdsets)
            StatusCampaings.append(SCamp)
            StatusErrorsCampaings.append(SErrors)
        cur.executemany(Campstatus,StatusCampaings)
        cur.executemany(ErrorsCamping,StatusErrorsCampaings)
        print('SUCCESS')
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("fbPaused", "Success", "updateInactiveCamps.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
    except mysql.Error as error:
        print("Failed to update record to database: {}".format(error))
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("fbPaused", "{}", "updateInactiveCamps.py","{}");'.format(error,dayhoy)
        cur.execute(sqlBitacora)

def goPaused(conn):
    cur=conn.cursor(buffered=True)
    SuperMetricsConnect('1ZU8H_83KLMX8PZKlTwkWOsApzB3f8XUABttCuWsCEpA','GO')
    StatusCampaings=[]
    StatusAdsets=[]
    StatusAds=[]
    StatusErrorsCampaings=[]
    try:
        cur = conn.cursor()
        for atr in temp_k:
            campaingid=atr['gsx$campaignid']['$t']
            adsetid=atr['gsx$adgroupid']['$t']
            adid=atr['gsx$adid']['$t']
            campstatus=atr['gsx$campaignstatus']['$t']
            adsetstatus=atr['gsx$adgroupstatus']['$t']
            adstatus=atr['gsx$adstatus']['$t']
            Campstatus='UPDATE Campaings set Campaignstatus=%s,CreateDate=%s where CampaingID=%s'
            Adsetsstatus='UPDATE Adsets set Status=%s,CreateDate=%s where AdSetID=%s'
            Ads='UPDATE Ads set Adstatus=%s,CreateDate=%s where AdID=%s'
            ErrorsCamping='UPDATE ErrorsCampaings set StatusCampaing=%s,CreateDate=%s where CampaingID=%s'
            if campstatus!='enabled':
                if adsetstatus!='enabled':
                    if SAds!='enabled':
                        Estatus=SAds
                    else:
                        Estatus=SAds
                else:
                    Estatus=adsetstatus
            else:
                Estatus=campstatus
            SCamp=(campstatus, campaingid,datetime.now())
            SAdsets=(adsetstatus, adsetstatus,datetime.now())
            SAds=(adstatus,adid,datetime.now())
            SErrors=(campstatus, campaingid,datetime.now())
            StatusAds.append(SAds)
            StatusAdsets.append(SAdsets)
            StatusCampaings.append(SCamp)
            StatusErrorsCampaings.append(SErrors)
        cur.executemany(Campstatus,StatusCampaings)
        cur.executemany(ErrorsCamping,StatusErrorsCampaings)
        print('SUCCESS')
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("goPaused", "Success", "updateInactiveCamps.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
    except mysql.Error as error:
        print("Failed to update record to database: {}".format(error))
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("goPaused", "{}", "updateInactiveCamps.py","{}");'.format(error, dayhoy)
        cur.execute(sqlBitacora)

if __name__ == '__main__':
   openConnection()
   fbPaused(conn)
   goPaused(conn)
   conn.close()
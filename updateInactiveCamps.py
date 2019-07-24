# -*- coding: UTF-8 -*-
import json
import requests
import sys
import re
import mysql.connector as mysql
from datetime import datetime

def openConnection():
    global conn
    try:
        conn = mysql.connect(host='3.95.117.169',database='MediaPlatforms',user='omgdev',password='Sdev@2002!',autocommit=True)
    except:
        logger.error("ERROR: NO SE PUEDO ESTABLECER CONEXION MYSQL.")
        sys.exit()

def SuperMetricsConnect(URL,MEDIA):
    startTime = datetime.now()
    print (datetime.now())
    global data,temp_k
    try:
        r=requests.get('https://spreadsheets.google.com/feeds/list/'+URL+'/od6/public/values?alt=json')
        data=r.json()
        temp_k=data['feed']['entry']
    except:
        logger.error("ERROR: CONEXION CON SUPERMETRICS",MEDIA)
        sys.exit()

def fbPaused(conn):
    cur=conn.cursor(buffered=True)
    SuperMetricsConnect('1TZfbnL_ETZF77S80kK14rzUEuZmsv-us_aYLzsU2CgI','FB')
    StatusCampaings=[]
    StatusAdsets=[]
    StatusAds=[]
    StatusErrorsCampaings=[]
    Estatus=''
    try:
        cursor = conn.cursor()
        for atr in temp_k:
            campaingid=atr['gsx$campaignid']['$t']
            adsetid=atr['gsx$adsetid']['$t']
            adid=atr['gsx$adid']['$t']
            campstatus=atr['gsx$campaignconfiguredstatus']['$t']
            adsetstatus=atr['gsx$adsetconfiguredstatus']['$t']
            adstatus=atr['gsx$adconfiguredstatus']['$t']
            Campstatus='UPDATE Campaings set Campaignstatus=%s  where CampaingID=%s'
            Adsetsstatus='UPDATE Adsets set Status=%s where AdSetID=%s'
            Ads='UPDATE Ads set Adstatus=%s where AdID=%s'
            ErrorsCamping='UPDATE ErrorsCampaings set StatusCampaing=%s where CampaingID=%s'
            SCamp=(campstatus, campaingid)
            SAdsets=(adsetstatus, adsetstatus)
            SAds=(adstatus,adid)
            if campstatus!='PAUSED':
                if adsetstatus!='PAUSED':
                    if SAds!='PAUSED':
                        Estatus=SAds
                    else:
                        Estatus=SAds
                else:
                    Estatus=adsetstatus
            else:
                Estatus=campstatus

            SErrors=(Estatus, campaingid)

            StatusAds.append(SAds)
            StatusAdsets.append(SAdsets)
            StatusCampaings.append(SCamp)
            StatusErrorsCampaings.append(SErrors)

        cur.executemany(Campstatus,StatusCampaings)
        cur.executemany(ErrorsCamping,StatusErrorsCampaings)
        print('SUCCESS')
    except mysql.Error as error:
        print("Failed to update record to database: {}".format(error))

def goPaused(conn):
    cur=conn.cursor(buffered=True)
    SuperMetricsConnect('1ZU8H_83KLMX8PZKlTwkWOsApzB3f8XUABttCuWsCEpA','GO')
    StatusCampaings=[]
    StatusAdsets=[]
    StatusAds=[]
    StatusErrorsCampaings=[]
    try:
        cursor = conn.cursor()
        for atr in temp_k:
            campaingid=atr['gsx$campaignid']['$t']
            adsetid=atr['gsx$adgroupid']['$t']
            adid=atr['gsx$adid']['$t']
            campstatus=atr['gsx$campaignstatus']['$t']
            adsetstatus=atr['gsx$adgroupstatus']['$t']
            adstatus=atr['gsx$adstatus']['$t']
            Campstatus='UPDATE Campaings set Campaignstatus=%s where CampaingID=%s'
            Adsetsstatus='UPDATE Adsets set Status=%s where AdSetID=%s'
            Ads='UPDATE Ads set Adstatus=%s where AdID=%s'
            ErrorsCamping='UPDATE ErrorsCampaings set StatusCampaing=%s where CampaingID=%s'
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
            SCamp=(campstatus, campaingid)
            SAdsets=(adsetstatus, adsetstatus)
            SAds=(adstatus,adid)
            SErrors=(campstatus, campaingid)
            StatusAds.append(SAds)
            StatusAdsets.append(SAdsets)
            StatusCampaings.append(SCamp)
            StatusErrorsCampaings.append(SErrors)
        cur.executemany(Campstatus,StatusCampaings)
        cur.executemany(ErrorsCamping,StatusErrorsCampaings)
        print('SUCCESS')
    except mysql.Error as error:
        print("Failed to update record to database: {}".format(error))



if __name__ == '__main__':
   openConnection()
   fbPaused(conn)
   goPaused(conn)
   conn.close()
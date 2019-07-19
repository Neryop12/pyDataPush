# -*- coding: UTF-8 -*-
import json
import requests
import sys
import re
import mysql.connector as mysql
from datetime import datetime
import logger

def openConnection():
    global conn
    try:
        conn = mysql.connect(host='localhost', port=3306,database='mediaplatafoms',user='root',password='1234',autocommit=True)
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
    SuperMetricsConnect('1TZfbnL_ETZF77S80kK14rzUEuZmsv-us_aYLzsU2CgI','FB')
    try:
        cursor = conn.cursor()
        for atr in temp_k:
            campaingid=atr['gsx$campaignid']['$t']
            adsetid=atr['gsx$adsetid']['$t']
            adid=atr['gsx$adid']['$t']
            campstatus=atr['gsx$campaignconfiguredstatus']['$t']
            adsetstatus=atr['gsx$adsetconfiguredstatus']['$t']
            adstatus=atr['gsx$adconfiguredstatus']['$t']
            Campstatus='UPDATE Campaings set Campaignstatus="%s"  where CampaingID="%s"' % (campstatus, campaingid)
            Adsetsstatus='UPDATE Adsets set Status="%s" where AdSetID="%s"' % (adsetstatus, adsetid)
            Ads='UPDATE Ads set Adstatus="%s" where AdID="%s"' % (adstatus,adid)    
            ErrorsCamping='UPDATE errorscampaings set StatusCampaing="%s" where CampaingID="%s"' % (campstatus, campaingid)    
            cursor.execute(Campstatus)
            cursor.execute(Adsetsstatus)
            cursor.execute(Ads)
            cursor.execute(ErrorsCamping)
            conn.commit()
        print('SUCCESS')
    except mysql.Error as error:
        print("Failed to update record to database: {}".format(error))

def goPaused(conn):
    SuperMetricsConnect('1ZU8H_83KLMX8PZKlTwkWOsApzB3f8XUABttCuWsCEpA','GO')
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


if __name__ == '__main__':
   openConnection()
   fbPaused(conn)
   conn.close()
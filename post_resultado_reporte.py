# -*- coding: UTF-8 -*-
import json
import requests
import sys
import re
import mysql.connector as mysql
from datetime import datetime
import schedule
import time
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

host= '3.95.117.169'
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


def fb_camp(conn):
    cur=conn.cursor(buffered=True)
    fechahoy = datetime.now()
    dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
    print (datetime.now())
    r=requests.get("https://spreadsheets.google.com/feeds/list/1RkgTADZ8AQCnMiImY_onb61ix3ALd_H7R0oNKd5u1D4/od6/public/values?alt=json")
    data=r.json()
    #ACCEDER AL OBJETO ENTRY CON LOS DATOS DE LAS CAMPANAS
    temp_k=data['feed']['entry']
    #CONEXION
    try:
        #QUERYS
      
        GuardarCampMetrics="INSERT  INTO CampaingMetrics(CampaingID,Reach,Frequency,Impressions,Placement,Clicks,cost,CreateDate,Diario,Result) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        GuardarCampDisplays="INSERT  INTO CampaingDisplay(CampaingID,publisherplatform,placement) VALUES (%s,%s,%s) ON DUPLICATE KEY UPDATE CampaingID=VALUES(CampaingID),publisherplatform=VALUES(publisherplatform),placement=VALUES(placement)"
        cuentas=[]
        campanas=[]
        campmetrics=[]
        campdisplays=[]
        for atr in temp_k:
            result = 0
            #ACCOUNT
            accountid=atr['gsx$accountid']['$t'].encode('utf-8')
            account=atr['gsx$account']['$t']
            account2=atr['gsx$account']['$t'].encode('utf-8')
            #CAMPAING
            campaingid=atr['gsx$campaignid']['$t']
            campaingname=atr['gsx$campaignname']['$t'].encode('utf-8')
            campaignspendinglimit=atr['gsx$campaignspendinglimit']['$t']
            campaigndailybudget=atr['gsx$campaigndailybudget']['$t']
            campaignlifetimebudget=atr['gsx$campaignlifetimebudget']['$t']
            campaignobjective=atr['gsx$campaignobjective']['$t'].encode('utf-8')
            campaingbuyingtypee=atr['gsx$campaignbuyingtype']['$t'].encode('utf-8')
            campaignstatus=atr['gsx$campaignstatus']['$t'].encode('utf-8')
            reach=atr['gsx$reach']['$t']
            impressions=atr['gsx$impressions']['$t']
            frequency=atr['gsx$frequency']['$t']
            placement='' #atr['gsx$placement']['$t'].encode('utf-8')
            publisherplatform=atr['gsx$publisherplatform']['$t']
            startdate=atr['gsx$campaignstartdate']['$t']
            enddate=atr['gsx$campaignenddate']['$t']
            clicks=atr['gsx$outboundclicks']['$t']
            cost=atr['gsx$cost']['$t']
            videowatch=atr['gsx$videowatchesat75']['$t']
            leads=atr['gsx$leadsform']['$t']
            mess=atr['gsx$newmessagingconversations']['$t']
            if campaignspendinglimit == '':
                campaignspendinglimit = 0
            if campaigndailybudget == '':
                campaigndailybudget = 0
            if campaignlifetimebudget == '':
                campaignlifetimebudget = 0
            if clicks == '':
                clicks = 0
            if frequency == '':
                frequency = 0
            if reach == '':
                reach = 0
            if impressions == '':
                impressions = 0
            if cost == '':
                cost = 0
            #FIN VARIABLES
            if accountid!='' or accountid!=0:
                searchObj = re.search(r'(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.+&]+)_([a-zA-Z0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-Z-/.+]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ.+]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(19|2019)_([0-9,.]+)_(BA|AL|TR|TRRS|TRRRSS|IN|DES|RV|CO|MESAD|LE)_([0-9,.]+)_(CPM|CPMA|CPVi|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCO|CPCO)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&]+)_([0-9,.-]+)?(_B-)?(_)?([0-9]+)?(_S-)?(_)?([0-9]+)?(\(([0-9.)]+)\))?', campaingname, re.M | re.I)
                if searchObj:
                    Result = (searchObj.group(14))
                    objcon = (searchObj.group(8))
                    if str(Result).upper() == 'CPVI':
                        result = clicks
                    elif str(Result).upper() == 'CPMA':
                        result = reach
                    elif str(Result).upper() == 'CPM':
                       result = impressions
                    elif str(Result).upper() == 'CPV':
                        result = videowatch
                    elif str(Result).upper() == 'CPCO':
                        if str(objcon).upper() == 'MESAD':
                            result = mess
                        elif str(objcon).upper() == 'LE':
                            result = leads
                        else:
                            result = clicks
                    elif str(Result).upper() == 'CPI':
                        result = leads
                    elif str(Result).upper() == 'CPMA':
                        result = reach
                    elif str(Result).upper() == 'CPC':
                        result = clicks
                    elif str(Result).upper() == 'CPMA':
                        result = reach
                campmetric=(campaingid,reach,frequency,impressions,placement,clicks,cost,dayhoy,1,result)
                campmetrics.append(campmetric)
            #FIN CICLOx
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(GuardarCampMetrics,campmetrics)
        
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("fb_camp", "Success", "pushDataMedia.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("fb_camp", "{}", "pushDataMedia.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print('Success Facebook Camp')
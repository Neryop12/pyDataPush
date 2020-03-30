# -*- coding: UTF-8 -*-
import json
import requests
import sys
import re
import mysql.connector as mysql
from datetime import datetime
import time
import configparser

config = configparser.ConfigParser()
config.read('config.ini')
host= '3.95.117.169'
name = 'MediaPlatforms'
user = 'omgdev'
password = 'Sdev@2002!'
autocommit= 'True'

# host= 'localhost'
# name = 'MediaPlatforms'
# user = 'omgdev'
# password = 'Sdev@2002!'
# autocommit= 'True'

def openConnection():
    global conn
    try:
        conn = mysql.connect(host=host, database=name,
                             user=user, password=password, autocommit=autocommit)
    except Exception as e:
        print("ERROR: NO SE PUEDO ESTABLECER CONEXION MYSQL.")
        print(e)

def fb_ads(conn):
    cur=conn.cursor(buffered=True)
    fechahoy = datetime.now()
    print (datetime.now())
    dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
    r=requests.get("https://spreadsheets.google.com/feeds/list/1WkJhD6-jmyCTqY5LfQOA__stGEh6Rgwph3X8q4tGiaI/od6/public/values?alt=json")
    data=r.json()
    #ACCEDER AL OBJETO ENTRY CON LOS DATOS DE LAS CAMPANAS

    try:
        temp_k=data['feed']['entry']
        #SQLS
        #Verificar si existe
        #Guardar Datos
        sqlInsertAd = "INSERT INTO Ads(AdID,Adname,Country,Adstatus,AdSetID) VALUES (%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE AdID=VALUES(AdID),Adname=VALUES(Adname);"
        sqlInsertCreativeAds = "INSERT INTO CreativeAds(AdcreativeID, Creativename, Linktopromotedpost, AdcreativethumbnailURL, AdcreativeimageURL, ExternaldestinationURL, Adcreativeobjecttype, PromotedpostID, Promotedpostname, PromotedpostInstagramID, Promotedpostmessage, Promotedpostcaption, PromotedpostdestinationURL, PromotedpostimageURL, LinktopromotedInstagrampost, AdID,Adname,Country) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE AdcreativeID=VALUES(AdcreativeID),Creativename=VALUES(Creativename);"
        sqlInsertMetricsAds = "INSERT INTO MetricsAds(Impressions, Ctr, Cpm, Cro,Cost, Frequency, Reach, Pagelikes, Peopletakingaction, Postreactions, Postshares, Photoviews, Clickstoplayvideo, Outboundclicks, Leads, Eventresponses, Messagingreplies, Videowatchesat75, Videowatchesat100, Websiteleads, Desktopappinstalls, Mobileappinstalls, AdID, Country, Adname,Clicks) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);"
        #results = cur.fetchall()  Mostrar datos de una consulta
        ads=[]
        adscreatives=[]
        adsmetrics=[]
        for atr in temp_k:
            #ADSET
            adsetid=atr['gsx$adsetid']['$t']
            adsetname=atr['gsx$adsetname']['$t'].encode('utf-8')
            adsetlifetimebudget=atr['gsx$adsetlifetimebudget']['$t']
            adsetdailybudget=atr['gsx$adsetdailybudget']['$t']
            adsettargeting=atr['gsx$adsettargeting']['$t']
            adsetend=atr['gsx$adsetendtime']['$t']
            adsetstart=atr['gsx$adsetstarttime']['$t']
            #Ads
            adid=atr['gsx$adid']['$t']
            adname=atr['gsx$adname']['$t'].encode('unicode_escape')
            country=atr['gsx$country']['$t'].encode('utf-8')
            adstatus=atr['gsx$adstatus']['$t'].encode('utf-8')
            #CREATIVE
            adcreativeid=atr['gsx$adcreativeid']['$t']
            creativename=atr['gsx$creativename']['$t'].encode('unicode_escape')
            linktopromotedpost=atr['gsx$linktopromotedpost']['$t']
            adcreativethumbnailurl=atr['gsx$adcreativethumbnailurl']['$t']
            adcreativeimageurl=atr['gsx$adcreativeimageurl']['$t']
            externaldestinationurl=atr['gsx$externaldestinationurl']['$t']
            adcreativeobjecttype=atr['gsx$adcreativeobjecttype']['$t'].encode('utf-8')
            promotedpostid=atr['gsx$promotedpostid']['$t']
            promotedpostname=atr['gsx$promotedpostname']['$t'].encode('unicode_escape')
            promotedpostinstagramid=atr['gsx$promotedpostinstagramid']['$t']
            promotedpostmessage=atr['gsx$promotedpostmessage']['$t'].encode('unicode_escape')
            promotedpostcaption=atr['gsx$promotedpostcaption']['$t'].encode('unicode_escape')
            promotedpostdestinationurl=atr['gsx$promotedpostdestinationurl']['$t']
            promotedpostimageurl=atr['gsx$promotedpostimageurl']['$t']
            linktopromotedinstagrampost=atr['gsx$linktopromotedinstagrampost']['$t']
            #MetricsAds
            impressions=atr['gsx$impressions']['$t']
            ctr=atr['gsx$outboundctr']['$t']
            cpm=atr['gsx$cpmcostper1000impressions']['$t']
            cost=atr['gsx$cost']['$t']
            cro=atr['gsx$websiteconversionrate']['$t']
            frequency=atr['gsx$frequency']['$t']
            reach=atr['gsx$reach']['$t']
            pagelikes=atr['gsx$pagelikes']['$t']
            peopletakingaction=atr['gsx$peopletakingaction']['$t']
            postreactions=atr['gsx$postreactions']['$t']
            postshares=atr['gsx$postshares']['$t']
            photoviews=atr['gsx$photoviews']['$t']
            clickstoplayvideo=atr['gsx$clickstoplayvideo']['$t']
            outboundclicks=atr['gsx$outboundclicks']['$t']
            leads=atr['gsx$leadsform']['$t']
            eventresponses=atr['gsx$eventresponses']['$t']
            messagingreplies=atr['gsx$messagingreplies']['$t']
            videowatchesat75=atr['gsx$videowatchesat75']['$t']
            videowatchesat100=atr['gsx$videowatchesat100']['$t']
            websiteleads=atr['gsx$websiteleads']['$t']
            desktopappinstalls=atr['gsx$desktopappinstalls']['$t']
            mobileappinstalls=atr['gsx$mobileappinstalls']['$t']
            #ACCOUT
            #AD
            if adid!='':
                ad=(adid,adname,country,adstatus,adsetid)
                ads.append(ad)
                adcrea=(adcreativeid, creativename, linktopromotedpost, adcreativethumbnailurl, adcreativeimageurl, externaldestinationurl, adcreativeobjecttype, promotedpostid, promotedpostname, promotedpostinstagramid, promotedpostmessage, promotedpostcaption, promotedpostdestinationurl, promotedpostimageurl, linktopromotedinstagrampost, adid,adname,country)
                adscreatives.append(adcrea)
                adme=(impressions, ctr, cpm, cro,cost, frequency, reach, pagelikes, peopletakingaction, postreactions, postshares, photoviews, clickstoplayvideo, outboundclicks, leads, eventresponses, messagingreplies, videowatchesat75, videowatchesat100, websiteleads, desktopappinstalls, mobileappinstalls, adid, country, adname,outboundclicks)
                adsmetrics.append(adme)
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(sqlInsertAd,ads)
        cur.executemany(sqlInsertCreativeAds,adscreatives)
        cur.executemany(sqlInsertMetricsAds,adsmetrics)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("fb_ads", "Success", "post_resultado_fb_go_tw.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("fb_ads", "{}", "post_resultado_fb_go_tw.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print('Success Facebook Ads')
#FIN VISTA
def fb_camp(conn):
    cur=conn.cursor(buffered=True)
    fechahoy = datetime.now()
    dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
    print (datetime.now())
    r=requests.get("https://spreadsheets.google.com/feeds/list/1fqS12Wc1UIo7v9Ma7OUjY00AdyAuBWnRuY0wx9wrVo4/od6/public/values?alt=json")
    data=r.json()
    #ACCEDER AL OBJETO ENTRY CON LOS DATOS DE LAS CAMPANAS
    temp_k=data['feed']['entry']
    #CONEXION
    try:
        #QUERYS
        GuardarCuentas="""INSERT INTO  Accounts (AccountsID, Account,Media) values(%s,%s,%s) ON DUPLICATE KEY UPDATE Account=VALUES(Account)"""
        GuardarCampaing="""INSERT INTO Campaings(CampaingID,Campaingname,Campaignspendinglimit,Campaigndailybudget,Campaignlifetimebudget,Campaignobjective,Campaignstatus,AccountsID,StartDate,EndDate,Campaingbuyingtype) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE Campaingname=VALUES(Campaingname),Campaigndailybudget=VALUES(Campaigndailybudget), Campaignlifetimebudget=VALUES(Campaignlifetimebudget),Campaignspendinglimit=VALUES(Campaignspendinglimit),Campaignstatus=VALUES(Campaignstatus),StartDate=VALUES(StartDate),EndDate=VALUES(EndDate),Campaingbuyingtype=VALUES(Campaingbuyingtype) """
        GuardarCampMetrics="INSERT  INTO CampaingMetrics(CampaingID,Reach,Frequency,Impressions,Placement,Clicks,cost,CreateDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
        GuardarCampDisplays="INSERT  INTO CampaingDisplay(CampaingID,publisherplatform,placement) VALUES (%s,%s,%s) ON DUPLICATE KEY UPDATE CampaingID=VALUES(CampaingID),publisherplatform=VALUES(publisherplatform),placement=VALUES(placement)"
        cuentas=[]
        campanas=[]
        campmetrics=[]
        campdisplays=[]
        for atr in temp_k:
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
                cuenta=[accountid,account,'FB']
                cuentas.append(cuenta)
                if enddate!='':
                    campana=[campaingid,campaingname,campaignspendinglimit,campaigndailybudget,campaignlifetimebudget,campaignobjective,campaignstatus,accountid,startdate,enddate,campaingbuyingtypee]
                    campanas.append(campana)
                campdisplay=(campaingid,publisherplatform,placement)
                campdisplays.append(campdisplay)
                campmetric=(campaingid,reach,frequency,impressions,placement,clicks,cost,dayhoy)
                campmetrics.append(campmetric)
            #FIN CICLOx
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(GuardarCuentas,cuentas)
        cur.executemany(GuardarCampaing,campanas)
        cur.executemany(GuardarCampMetrics,campmetrics)
        cur.executemany(GuardarCampDisplays,campdisplays)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("fb_camp", "Success", "post_resultado_fb_go_tw.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("fb_camp", "{}", "post_resultado_fb_go_tw.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print('Success Facebook Camp')

def fb_adsets(conn):
    cur=conn.cursor(buffered=True)
    #CONEXION
    fechahoy = datetime.now()
    dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
    r=requests.get("https://spreadsheets.google.com/feeds/list/1jtlJhQJVW0sPvLaFNYoNPwXMnbhdV5M30sX1jt3C1Rc/od6/public/values?alt=json")
    data=r.json()
    #ACCEDER AL OBJETO ENTRY CON LOS DATOS DE LAS CAMPANAS
    temp_k=data['feed']['entry']
    try:
            #QUERYS
        adsets=[]
        adsetmetrics=[]
        sqlInsertAdsSetsMetrics = "INSERT INTO AdSetMetrics(AdSetID,AdSetName,Country,Reach,Frequency,Impressions,Clicks,CreateDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE AdSetName=VALUES(AdSetName)"
        sqlSelectAdSet = "SELECT count(*) FROM Adsets where AdSetID=%s"
        sqlInsertAdSet = "INSERT INTO Adsets(AdSetID,Adsetname,Adsetlifetimebudget,Adsetdailybudget,Adsettargeting,Adsetend,Adsetstart,CampaingID,Status) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE AdSetName=VALUES(AdSetName),Adsetlifetimebudget=VALUES(Adsetlifetimebudget),Adsetdailybudget=VALUES(Adsetdailybudget),Status=VALUES(Status)"
        for atr in temp_k:
            #ACCOUNT
            adsetid=atr['gsx$adsetid']['$t']
            adsetname=atr['gsx$adsetname']['$t']
            reach=atr['gsx$reach']['$t']
            frequency=atr['gsx$frequency']['$t']
            country=atr['gsx$country']['$t']
            #CAMPAING
            campaingid=atr['gsx$campaignid']['$t']
            #ADSET
            adsetid=atr['gsx$adsetid']['$t']
            adsetname=atr['gsx$adsetname']['$t'].encode('utf-8')
            adsetlifetimebudget=atr['gsx$adsetlifetimebudget'   ]['$t']
            adsetdailybudget=atr['gsx$adsetdailybudget']['$t']
            adsettargeting=atr['gsx$adsettargeting']['$t']
            adsetend=atr['gsx$adsetendtime']['$t']
            adsetstart=atr['gsx$adsetstarttime']['$t']
            impressions=atr['gsx$impressions']['$t']
            frequency=atr['gsx$frequency']['$t']
            clicks=atr['gsx$outboundclicks']['$t']
            status=atr['gsx$adsetconfiguredstatus']['$t']
            #ADSET
            if adsetid!='':
                adset=(adsetid,adsetname,adsetlifetimebudget,adsetdailybudget,adsettargeting,adsetend,adsetstart,campaingid,status)
                adsets.append(adset)
                adsetmetric=(adsetid,adsetname,country,reach,frequency,impressions,clicks,dayhoy)
                adsetmetrics.append(adsetmetric)
            #FIN CICLO
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(sqlInsertAdSet,adsets)
        cur.executemany(sqlInsertAdsSetsMetrics,adsetmetrics)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("fb_adsets", "Success", "post_resultado_fb_go_tw.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("fb_ads", {}, "post_resultado_fb_go_tw.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print('Success Facebook AdSets')
#FIN VISTA
def go_camp(conn):
    cur=conn.cursor(buffered=True)
    fechahoy = datetime.now()
    dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
    r=requests.get("https://spreadsheets.google.com/feeds/list/13bmX2G2PX7MHd49bHMRJtkeJZXb0vHClzBJw4_GmvjA/od6/public/values?alt=json")
    #FB CAMPAINGS   https://docs.google.com/spreadsheets/d/1fqS12Wc1UIo7v9Ma7OUjY00AdyAuBWnRuY0wx9wrVo4/edit?usp=sharing
    data=r.json()
    #ACCEDER AL OBJETO ENTRY CON LOS DATOS DE LAS CAMPANAS
    temp_k=data['feed']['entry']
    #CONEXION
    try:
        cuentas=[]
        campanas=[]
        campmetrics=[]
        campdisplays=[]
        #QUERYS
        sqlInsertCampaing = "INSERT INTO Campaings(CampaingID,Campaingname,Campaigndailybudget,Campaignlifetimebudget,Campaignobjective,Campaignstatus,AccountsID,StartDate,EndDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE Campaingname=VALUES(Campaingname),Campaigndailybudget=VALUES(Campaigndailybudget), Campaignlifetimebudget=VALUES(Campaignlifetimebudget),Campaignstatus=VALUES(Campaignstatus)"
        sqlInsertAccount = "INSERT INTO Accounts(AccountsID, Account,Media) values(%s,%s,%s) ON DUPLICATE KEY UPDATE Account=VALUES(Account)"
        sqlInsertCampaingMetrics = "INSERT INTO CampaingMetrics(CampaingID,Percentofbudgetused,impressions,placement,clicks,cost,CreateDate) VALUES (%s,%s,%s,%s,%s,%s,%s)"
        sqlInsertCampaingDisplay = "INSERT INTO CampaingDisplay(CampaingID,publisherplatform,placement) VALUES (%s,%s,%s) ON DUPLICATE KEY UPDATE CampaingID=VALUES(CampaingID),publisherplatform=VALUES(publisherplatform),placement=VALUES(placement)"
        for atr in temp_k:
            #ACCOUNT
            accountid=atr['gsx$accountid']['$t']
            account=atr['gsx$account']['$t'].encode('utf-8')
            #CAMPAING
            campaingid=atr['gsx$campaignid']['$t']
            campaingname=atr['gsx$campaignname']['$t']
            campaigndailybudget=atr['gsx$dailybudget']['$t']
            campaignlifetimebudget=atr['gsx$budget']['$t']
            percentofbudgetused=atr['gsx$percentofbudgetused']['$t']
            startdate=atr['gsx$startdate']['$t']
            enddate=atr['gsx$enddate']['$t']
            campaignobjective=''
            impressions=atr['gsx$impressions']['$t']
            clicks=atr['gsx$clicks']['$t']
            cost=atr['gsx$cost']['$t']
            campaignstatus=atr['gsx$campaignstatus']['$t'].encode('utf-8')
            placement=atr['gsx$advertisingchanneltype']['$t'].encode('utf-8')
            publisherplatform=atr['gsx$advertisingchannelsub-type']['$t'].encode('utf-8')

            if accountid!='':
                cuenta=[accountid,account,'GO']
                cuentas.append(cuenta)
                campana=[campaingid,campaingname,campaigndailybudget,campaignlifetimebudget,campaignobjective,campaignstatus,accountid,startdate,enddate]
                campanas.append(campana)
                campdisplay=(campaingid,publisherplatform,placement)
                campdisplays.append(campdisplay)
                campmetric=(campaingid,percentofbudgetused,impressions,placement,clicks,cost,dayhoy)
                campmetrics.append(campmetric)
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(sqlInsertAccount ,cuentas)
        cur.executemany(sqlInsertCampaing,campanas)
        cur.executemany(sqlInsertCampaingMetrics,campmetrics)
        cur.executemany(sqlInsertCampaingDisplay,campdisplays)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("go_camp", "Success", "post_resultado_fb_go_tw.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("go_ads", "Success", "post_resultado_fb_go_tw.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print (datetime.now())
        print('Success GOOGLE Campanas')
#FIN VISTA


def go_camp_mos(conn):
    cur=conn.cursor(buffered=True)
    fechahoy = datetime.now()
    dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
    r=requests.get("https://spreadsheets.google.com/feeds/list/1tcET43KvjNYaSOpBpLtI-qJ8lqeOTsjfRrpw-yLri7k/od6/public/values?alt=json")
    #FB CAMPAINGS   https://docs.google.com/spreadsheets/d/1fqS12Wc1UIo7v9Ma7OUjY00AdyAuBWnRuY0wx9wrVo4/edit?usp=sharing
    data=r.json()
    #ACCEDER AL OBJETO ENTRY CON LOS DATOS DE LAS CAMPANAS
    temp_k=data['feed']['entry']
    #CONEXION
    try:
        cuentas=[]
        campanas=[]
        campmetrics=[]
        campdisplays=[]
        #QUERYS
        sqlInsertCampaing = "INSERT INTO Campaings(CampaingID,Campaingname,Campaigndailybudget,Campaignlifetimebudget,Campaignobjective,Campaignstatus,AccountsID,StartDate,EndDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE Campaingname=VALUES(Campaingname),Campaigndailybudget=VALUES(Campaigndailybudget), Campaignlifetimebudget=VALUES(Campaignlifetimebudget),Campaignstatus=VALUES(Campaignstatus)"
        sqlInsertAccount = "INSERT INTO Accounts(AccountsID, Account,Media) values(%s,%s,%s) ON DUPLICATE KEY UPDATE Account=VALUES(Account)"
        sqlInsertCampaingMetrics = "INSERT INTO CampaingMetrics(CampaingID,Percentofbudgetused,impressions,placement,clicks,cost,CreateDate) VALUES (%s,%s,%s,%s,%s,%s,%s)"
        sqlInsertCampaingDisplay = "INSERT INTO CampaingDisplay(CampaingID,publisherplatform,placement) VALUES (%s,%s,%s) ON DUPLICATE KEY UPDATE CampaingID=VALUES(CampaingID),publisherplatform=VALUES(publisherplatform),placement=VALUES(placement)"
        for atr in temp_k:
            #ACCOUNT
            accountid=atr['gsx$accountid']['$t']
            account=atr['gsx$account']['$t'].encode('utf-8')
            #CAMPAING
            campaingid=atr['gsx$campaignid']['$t']
            campaingname=atr['gsx$campaignname']['$t']
            campaigndailybudget=atr['gsx$dailybudget']['$t']
            campaignlifetimebudget=atr['gsx$budget']['$t']
            percentofbudgetused=atr['gsx$percentofbudgetused']['$t']
            startdate=atr['gsx$startdate']['$t']
            enddate=atr['gsx$enddate']['$t']
            campaignobjective=''
            impressions=atr['gsx$impressions']['$t']
            clicks=atr['gsx$clicks']['$t']
            cost=atr['gsx$cost']['$t']
            campaignstatus=atr['gsx$campaignstatus']['$t'].encode('utf-8')
            placement=atr['gsx$advertisingchanneltype']['$t'].encode('utf-8')
            publisherplatform=atr['gsx$advertisingchannelsub-type']['$t'].encode('utf-8')

            if accountid!='':
                cuenta=[accountid,account,'GO']
                cuentas.append(cuenta)
                campana=[campaingid,campaingname,campaigndailybudget,campaignlifetimebudget,campaignobjective,campaignstatus,accountid,startdate,enddate]
                campanas.append(campana)
                campdisplay=(campaingid,publisherplatform,placement)
                campdisplays.append(campdisplay)
                campmetric=(campaingid,percentofbudgetused,impressions,placement,clicks,cost,dayhoy)
                campmetrics.append(campmetric)
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(sqlInsertAccount ,cuentas)
        cur.executemany(sqlInsertCampaing,campanas)
        cur.executemany(sqlInsertCampaingMetrics,campmetrics)
        cur.executemany(sqlInsertCampaingDisplay,campdisplays)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("go_camp", "Success", "post_resultado_fb_go_tw.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("go_ads", "Success", "post_resultado_fb_go_tw.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print (datetime.now())
        print('Success GOOGLE Campanas MOSCA')
#FIN VISTA

def go_camp_house(conn):
    cur=conn.cursor(buffered=True)
    fechahoy = datetime.now()
    dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
    r=requests.get("https://spreadsheets.google.com/feeds/list/1fCfMi1hu5Ba4TNtWsx0h7Nx5szjLnhkkY2cLAucorr4/od6/public/values?alt=json")
    #FB CAMPAINGS   https://docs.google.com/spreadsheets/d/1fqS12Wc1UIo7v9Ma7OUjY00AdyAuBWnRuY0wx9wrVo4/edit?usp=sharing
    data=r.json()
    #ACCEDER AL OBJETO ENTRY CON LOS DATOS DE LAS CAMPANAS
    temp_k=data['feed']['entry']
    #CONEXION
    try:
        cuentas=[]
        campanas=[]
        campmetrics=[]
        campdisplays=[]
        #QUERYS
        sqlInsertCampaing = "INSERT INTO Campaings(CampaingID,Campaingname,Campaigndailybudget,Campaignlifetimebudget,Campaignobjective,Campaignstatus,AccountsID,StartDate,EndDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE Campaingname=VALUES(Campaingname),Campaigndailybudget=VALUES(Campaigndailybudget), Campaignlifetimebudget=VALUES(Campaignlifetimebudget),Campaignstatus=VALUES(Campaignstatus)"
        sqlInsertAccount = "INSERT INTO Accounts(AccountsID, Account,Media) values(%s,%s,%s) ON DUPLICATE KEY UPDATE Account=VALUES(Account)"
        sqlInsertCampaingMetrics = "INSERT INTO CampaingMetrics(CampaingID,Percentofbudgetused,impressions,placement,clicks,cost,CreateDate) VALUES (%s,%s,%s,%s,%s,%s,%s)"
        sqlInsertCampaingDisplay = "INSERT INTO CampaingDisplay(CampaingID,publisherplatform,placement) VALUES (%s,%s,%s) ON DUPLICATE KEY UPDATE CampaingID=VALUES(CampaingID),publisherplatform=VALUES(publisherplatform),placement=VALUES(placement)"
        for atr in temp_k:
            #ACCOUNT
            accountid=atr['gsx$accountid']['$t']
            account=atr['gsx$account']['$t'].encode('utf-8')
            #CAMPAING
            campaingid=atr['gsx$campaignid']['$t']
            campaingname=atr['gsx$campaignname']['$t']
            campaigndailybudget=atr['gsx$dailybudget']['$t']
            campaignlifetimebudget=atr['gsx$budget']['$t']
            percentofbudgetused=atr['gsx$percentofbudgetused']['$t']
            startdate=atr['gsx$startdate']['$t']
            enddate=atr['gsx$enddate']['$t']
            campaignobjective=''
            impressions=atr['gsx$impressions']['$t']
            clicks=atr['gsx$clicks']['$t']
            cost=atr['gsx$cost']['$t']
            campaignstatus=atr['gsx$campaignstatus']['$t'].encode('utf-8')
            placement=atr['gsx$advertisingchanneltype']['$t'].encode('utf-8')
            publisherplatform=atr['gsx$advertisingchannelsub-type']['$t'].encode('utf-8')

            if accountid!='':
                cuenta=[accountid,account,'GO']
                cuentas.append(cuenta)
                campana=[campaingid,campaingname,campaigndailybudget,campaignlifetimebudget,campaignobjective,campaignstatus,accountid,startdate,enddate]
                campanas.append(campana)
                campdisplay=(campaingid,publisherplatform,placement)
                campdisplays.append(campdisplay)
                campmetric=(campaingid,percentofbudgetused,impressions,placement,clicks,cost,dayhoy)
                campmetrics.append(campmetric)
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(sqlInsertAccount ,cuentas)
        cur.executemany(sqlInsertCampaing,campanas)
        cur.executemany(sqlInsertCampaingMetrics,campmetrics)
        cur.executemany(sqlInsertCampaingDisplay,campdisplays)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("go_camp", "Success", "post_resultado_fb_go_tw.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("go_ads", "Success", "post_resultado_fb_go_tw.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print (datetime.now())
        print('Success GOOGLE Campanas HOUSE')
#FIN VISTA


def go_adsets(conn):
    cur=conn.cursor(buffered=True)
    fechahoy = datetime.now()
    dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
    print (datetime.now())
    r=requests.get("https://spreadsheets.google.com/feeds/list/1LfQVv-DbT7Nit_7Q32kN59-slUJhK6UL0tG0KAnkFxY/od6/public/values?alt=json")
    #FB CAMPAINGS   https://docs.google.com/spreadsheets/d/1fqS12Wc1UIo7v9Ma7OUjY00AdyAuBWnRuY0wx9wrVo4/edit?usp=sharing
    data=r.json()
    #ACCEDER AL OBJETO ENTRY CON LOS DATOS DE LAS CAMPANAS
    temp_k=data['feed']['entry']
    #CONEXION
    try:
        #QUERYS
        adsets=[]
        adsetmetrics=[]
        sqlInsertAdsSets = "INSERT INTO Adsets(AdSetID,Adsetname,Adsetlifetimebudget,CampaingID,Status) VALUES (%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE AdSetName=VALUES(AdSetName),Adsetlifetimebudget=VALUES(Adsetlifetimebudget),Status=VALUES(Status)"
        sqlInsertAdsSetsMetrics = "INSERT INTO AdSetMetrics(AdSetID,AdSetName,Impressions,Clicks,CreateDate) VALUES (%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE AdSetName=VALUES(AdSetName)"

        for atr in temp_k:
            #ACCOUNT
            adsetid=atr['gsx$adgroupid']['$t']
            adsetname=atr['gsx$adgroupname']['$t']
            campaingid=atr['gsx$campaignid']['$t']
            cost=atr['gsx$cost']['$t']
            impressions=atr['gsx$impressions']['$t']
            clicks=atr['gsx$clicks']['$t']
            status=atr['gsx$adgroupstatus']['$t']
            if adsetid!='':
                adset=(adsetid,adsetname,cost,campaingid,status)
                adsets.append(adset)
                adsetmetric=(adsetid,adsetname,impressions,clicks,dayhoy)
                adsetmetrics.append(adsetmetric)
            #FIN CICLO
            cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(sqlInsertAdsSets,adsets)
        cur.executemany(sqlInsertAdsSetsMetrics,adsetmetrics)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("go_adsets", "Success", "post_resultado_fb_go_tw.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("go_adsets", "{}", "post_resultado_fb_go_tw.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print('Success Google Adsets')
#FIN VISTA
def go_ads(conn):
    cur=conn.cursor(buffered=True)
    startTime = datetime.now()
    fechahoy = datetime.now()
    print (datetime.now())
    r=requests.get("https://spreadsheets.google.com/feeds/list/1repEph-Yd6IzQATtYOFajq3GDUkJ4GVNaH2HNT9_HMw/od6/public/values?alt=json")
    #FB CAMPAINGS   https://docs.google.com/spreadsheets/d/1fqS12Wc1UIo7v9Ma7OUjY00AdyAuBWnRuY0wx9wrVo4/edit?usp=sharing
    data=r.json()
    #ACCEDER AL OBJETO ENTRY CON LOS DATOS DE LAS CAMPANAS
    temp_k=data['feed']['entry']
    #CONEXION
    try:
        ads=[]
        adscreatives=[]
        adsmetrics=[]
        #QUERYS
        sqlInsertAds = "INSERT INTO Ads(AdID,Adstatus,AdSetID) VALUES (%s,%s,%s)ON DUPLICATE KEY UPDATE AdID=VALUES(AdID)"
        sqlInsertMetricsAds = "INSERT INTO MetricsAds(Cost,Ctr,Cpm,Convertions,Videowatchesat100,Videowatchesat75,AdID,Impressions,Clicks) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        for atr in temp_k:
            #ACCOUNT
            adsetid=atr['gsx$adgroupid']['$t']
            adid=atr['gsx$adid']['$t']
            adstatus=atr['gsx$adstatus']['$t']
            descripcion=atr['gsx$description']['$t'].encode('unicode_escape')
            cost=atr['gsx$cost']['$t']
            convertions=atr['gsx$conversions']['$t']
            ctr=atr['gsx$ctr']['$t']
            cpm=atr['gsx$cpm']['$t']
            cpc=atr['gsx$cpc']['$t']
            impressions=atr['gsx$impressions']['$t']
            clicks=atr['gsx$clicks']['$t']
            videowatchesat75=atr['gsx$watch75rate']['$t']
            videowatchesat100=atr['gsx$watch100rate']['$t']
            if adid!='':
                ad=(adid,adstatus,adsetid)
                ads.append(ad)
                adme=(cost,ctr,cpm,convertions,videowatchesat100,videowatchesat75,adid,impressions,clicks)
                adsmetrics.append(adme)
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(sqlInsertAds,ads)
        cur.executemany(sqlInsertMetricsAds,adsmetrics)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("go_ads", "Success", "post_resultado_fb_go_tw.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("go_ads", "{}", "post_resultado_fb_go_tw.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print('Success Google Ads')
#FIN VISTA
def tw_camp(conn):
    #FB CAMPAINGS   https://docs.google.com/spreadsheets/d/1fqS12Wc1UIo7v9Ma7OUjY00AdyAuBWnRuY0wx9wrVo4/edit?usp=sharing
    cur=conn.cursor(buffered=True)
    fechahoy = datetime.now()
    dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
    print (datetime.now())
    r=requests.get("https://spreadsheets.google.com/feeds/list/1oVsLY6IdOY4AvFz4BMLlSvlHDmG1rtw9HMZrsZzxqJY/od6/public/values?alt=json")
    #FB CAMPAINGS   https://docs.google.com/spreadsheets/d/1fqS12Wc1UIo7v9Ma7OUjY00AdyAuBWnRuY0wx9wrVo4/edit?usp=sharing
    data=r.json()
    #ACCEDER AL OBJETO ENTRY CON LOS DATOS DE LAS CAMPANAS

    #CONEXION

    #QUERYS
    try:
        cuentas=[]
        campanas=[]
        campmetrics=[]
        campdisplays=[]
        temp_k=data['feed']['entry']
        sqlInsertCampaing = "INSERT INTO Campaings(CampaingID,Campaingname,Campaigndailybudget,Campaignlifetimebudget,Campaignobjective,AccountsID,StartDate,EndDate,Campaignstatus) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'ACTIVE') ON DUPLICATE KEY UPDATE Campaingname=VALUES(Campaingname),Campaigndailybudget=VALUES(Campaigndailybudget), Campaignlifetimebudget=VALUES(Campaignlifetimebudget),Campaignstatus=VALUES(Campaignstatus)"
        sqlInsertAccount = "INSERT INTO Accounts(AccountsID,Account,Media) VALUES (%s,%s,%s) ON DUPLICATE KEY UPDATE Account=VALUES(Account)"
        sqlInsertCampaingMetrics = "INSERT INTO CampaingMetrics(CampaingID,Impressions,Clicks,cost,CreateDate) VALUES (%s,%s,%s,%s,%s)"
        sqlInsertCampaingDisplay = "INSERT INTO CampaingDisplay(CampaingID,Placement) VALUES (%s,%s)ON DUPLICATE KEY UPDATE CampaingID=VALUES(CampaingID),publisherplatform=VALUES(publisherplatform),placement=VALUES(placement)"
        for atr in temp_k:
            #ACCOUNT
            accountid=atr['gsx$accountid']['$t'].encode('utf-8')
            account=atr['gsx$account']['$t'].encode('utf-8')
            #CAMPAING
            campaingid=atr['gsx$campaignid']['$t'].encode('utf-8')
            campaingname=atr['gsx$campaign']['$t'].encode('utf-8')
            campaigndailybudget=atr['gsx$campaigndailybudget']['$t']
            campaignlifetimebudget=atr['gsx$campaigntotalbudget']['$t']
            startdate=atr['gsx$campaignstarttime']['$t']
            enddate=atr['gsx$campaignendtime']['$t']
            campaignobjective=atr['gsx$campaignobjective']['$t'].encode('utf-8')
            placement=atr['gsx$lineitemplacements']['$t'].encode('utf-8')
            impressions=atr['gsx$impressions']['$t']
            clicks=atr['gsx$clicks']['$t']
            cost=atr['gsx$cost']['$t']
            if accountid!='':
                cuenta=[accountid,account,'TW']
                cuentas.append(cuenta)
                campana=[campaingid,campaingname,campaigndailybudget,campaignlifetimebudget,campaignobjective,accountid,startdate,enddate]
                campanas.append(campana)
                campdisplay=(campaingid,placement)
                campdisplays.append(campdisplay)
                campmetric=(campaingid,impressions,clicks,cost,dayhoy)
                campmetrics.append(campmetric)
            #FIN CICLOx
            cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(sqlInsertAccount ,cuentas)
        cur.executemany(sqlInsertCampaing ,campanas)
        cur.executemany(sqlInsertCampaingMetrics,campmetrics)
        cur.executemany(sqlInsertCampaingDisplay,campdisplays)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("tw_camp", "Success", "post_resultado_fb_go_tw.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("tw_camp", "{}", "post_resultado_fb_go_tw.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print('Success Campanas Twitter')
#FIN VISTA
def tw_adsets(conn):
    cur=conn.cursor(buffered=True)
    fechahoy = datetime.now()
    dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
    print (datetime.now())
    r=requests.get("https://spreadsheets.google.com/feeds/list/1ZDP70FMghz9fu2yDpRzEhZw4_JNnBzWSU5pWi42E3lA/od6/public/values?alt=json")
    #FB CAMPAINGS   https://docs.google.com/spreadsheets/d/1fqS12Wc1UIo7v9Ma7OUjY00AdyAuBWnRuY0wx9wrVo4/edit?usp=sharing
    try:
        data=r.json()
        #ACCEDER AL OBJETO ENTRY CON LOS DATOS DE LAS CAMPANAS
        temp_k=data['feed']['entry']
        #CONEXION

        adsets=[]
        adsetmetrics=[]
        #QUERYS
        sqlInsertAdsSetsMetrics = "INSERT INTO AdSetMetrics(AdSetID,AdSetName,Impressions,Clicks,CreateDate) VALUES (%s,%s,%s,%s,%s) "
        sqlInsertAdsSets = "INSERT INTO Adsets(AdSetID,Adsetname,Adsetlifetimebudget,CampaingID,Status) VALUES (%s,%s,%s,%s,'ACTIVE') ON DUPLICATE KEY UPDATE AdSetName=VALUES(AdSetName), Status=VALUES(Status)"
        for atr in temp_k:
            #ACCOUNT
            adsetid=atr['gsx$campaignid']['$t']+atr['gsx$fundinginstrumentid']['$t']
            adsetname=atr['gsx$campaignid']['$t']+atr['gsx$fundinginstrumentid']['$t']
            impressions=atr['gsx$impressions']['$t']
            #CAMPAING
            campaingid=atr['gsx$campaignid']['$t']
            campaingname=atr['gsx$campaign']['$t'].encode('utf-8')
            #ADSET
            adsetlifetimebudget=atr['gsx$lineitemtotalbudget']['$t']
            clicks=atr['gsx$clicks']['$t']

            if adsetid!='':
                adset=(adsetid,adsetname,adsetlifetimebudget,campaingid)
                adsets.append(adset)
                adsetmetric=(adsetid,adsetname,impressions,clicks,dayhoy)
                adsetmetrics.append(adsetmetric)
            #ADSET
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(sqlInsertAdsSets,adsets)
        cur.executemany(sqlInsertAdsSetsMetrics,adsetmetrics)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("tw_adsets", "Success", "post_resultado_fb_go_tw.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("tw_adsets", "{}", "post_resultado_fb_go_tw.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print('Success Twitter Adsets')
#FIN VISTA
def tw_ads(conn):
    cur=conn.cursor(buffered=True)
    startTime = datetime.now()
    fechahoy = datetime.now()
    dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
    print (datetime.now())
    r=requests.get("https://spreadsheets.google.com/feeds/list/1eHGuoB4gxqASR-I9RijMNtEax2xFWIAo4xqAyRhCfcM/od6/public/values?alt=json")
    #FB CAMPAINGS   https://docs.google.com/spreadsheets/d/1fqS12Wc1UIo7v9Ma7OUjY00AdyAuBWnRuY0wx9wrVo4/edit?usp=sharing
    data=r.json()
    #ACCEDER AL OBJETO ENTRY CON LOS DATOS DE LAS CAMPANAS
    temp_k=data['feed']['entry']
    #CONEXION
    try:
        ads=[]
        adscreatives=[]
        adsmetrics=[]
        #QUERYS
        sqlInsertAds = "INSERT INTO Ads(AdID,AdSetID,Adname,Adstatus) VALUES (%s,%s,%s,'ACTIVE') ON DUPLICATE KEY UPDATE Adname=VALUES(Adname),Adstatus=VALUES(Adstatus)"
        sqlInsertMetricsAds = "INSERT INTO MetricsAds(Impressions,Cost,Ctr,Cpm,AdID) VALUES (%s,%s,%s,%s,%s)"
        sqlSelectAds = "SELECT count(*) FROM Ads where AdID=%s and AdSetID=%s"
        for atr in temp_k:
            #ACCOUNT
            adid=atr['gsx$campaignid']['$t']+atr['gsx$fundinginstrumentid']['$t']
            adsetid=atr['gsx$campaignid']['$t']+atr['gsx$lineitem']['$t']
            adname=atr['gsx$lineitem']['$t'].encode('unicode_escape')
            impressions=atr['gsx$impressions']['$t']
            #CAMPAING
            #ADSET
            cost=atr['gsx$cost']['$t']
            ctr=atr['gsx$ctr']['$t']
            cpm=atr['gsx$cpm']['$t']
            cpc=atr['gsx$cpc']['$t']
            if adid!='':
                ad=(adid,adsetid,adname)
                ads.append(ad)

                adme=(impressions, cost,ctr,cpm, adsetid)
                adsmetrics.append(adme)
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(sqlInsertAds,ads)
        cur.executemany(sqlInsertMetricsAds,adsmetrics)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("tw_ads", "Success", "post_resultado_fb_go_tw.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)

    except Exception as e:
        print(e)
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("tw_ads", "{}", "post_resultado_fb_go_tw.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print('Success Ads Twittter')
#FIN VISTA
def push_camps(conn):
    # fb_camp(conn)
    go_camp(conn)
    go_camp_mos(conn)
    go_camp_house(conn)
    # go_camp_mosca(conn)
    # tw_camp(conn)

def push_adsets(conn):
    # fb_adsets(conn)
    go_adsets(conn)
    # tw_adsets(conn)

def push_ads(conn):
    fb_ads(conn)
    go_ads(conn)
    tw_ads(conn)

if __name__ == '__main__':
    openConnection()
    push_camps(conn)
    push_adsets(conn)
    push_ads(conn)
    conn.close()

# -*- coding: UTF-8 -*-
import json
import requests
import sys
import re
import mysql.connector as mysql
from datetime import datetime
import time
import logger
conn = None
#3.95.117.169
def openConnection():
    global conn
    try:
        conn = mysql.connect(host='3.95.117.169',database='MediaPlatforms',user='omgdev',password='Sdev@2002!',autocommit=True)
    except:
        logger.error("ERROR: NO SE PUEDO ESTABLECER CONEXION MYSQL.")
        sys.exit()

def fb_ads(conn):
    cur=conn.cursor(buffered=True)
    print (datetime.now())
    r=requests.get("https://spreadsheets.google.com/feeds/list/1ZL49TIgJU9qJsqx_7qhghZ6XaGh_QEyzHkQXH6JagBI/od6/public/values?alt=json")
    data=r.json()
    #ACCEDER AL OBJETO ENTRY CON LOS DATOS DE LAS CAMPANAS
    temp_k=data['feed']['entry']
    try:
        #SQLS
        #Verificar si existe
        #Guardar Datos
        sqlInsertAd = "INSERT INTO Ads(AdID,Adname,Country,Adstatus,AdSetID) VALUES (%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE AdID=VALUES(AdID),Adname=VALUES(Adname);"
        sqlInsertCreativeAds = "INSERT INTO CreativeAds(AdcreativeID, Creativename, Linktopromotedpost, AdcreativethumbnailURL, AdcreativeimageURL, ExternaldestinationURL, Adcreativeobjecttype, PromotedpostID, Promotedpostname, PromotedpostInstagramID, Promotedpostmessage, Promotedpostcaption, PromotedpostdestinationURL, PromotedpostimageURL, LinktopromotedInstagrampost, AdID,Adname,Country) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE Creativename=VALUES(Creativename);"
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
        cur.executemany(sqlInsertAd,ads)
        cur.executemany(sqlInsertCreativeAds,adscreatives)
        cur.executemany(sqlInsertMetricsAds,adsmetrics)

    except Exception as e:
        print(e)
    finally:
        print('Success Facebook Ads')
#FIN VISTA
def fb_camp(conn):
    cur=conn.cursor(buffered=True)
    startTime = datetime.now()
    print (datetime.now())
    r=requests.get("https://spreadsheets.google.com/feeds/list/1fqS12Wc1UIo7v9Ma7OUjY00AdyAuBWnRuY0wx9wrVo4/od6/public/values?alt=json")
    data=r.json()
    #ACCEDER AL OBJETO ENTRY CON LOS DATOS DE LAS CAMPANAS
    temp_k=data['feed']['entry']
    #CONEXION
    try:
        #QUERYS
        GuardarCuentas="""INSERT INTO  Accounts (AccountsID, Account,Media) values(%s,%s,%s) ON DUPLICATE KEY UPDATE Account=VALUES(Account)"""
        GuardarCampaing="""INSERT INTO Campaings(CampaingID,Campaingname,Campaignspendinglimit,Campaigndailybudget,Campaignlifetimebudget,Campaignobjective,Campaignstatus,AccountsID,StartDate,EndDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)ON DUPLICATE KEY UPDATE Campaingname=VALUES(Campaingname),Campaigndailybudget=VALUES(Campaigndailybudget), Campaignlifetimebudget=VALUES(Campaignlifetimebudget),Campaignspendinglimit=VALUES(Campaignspendinglimit),Campaignstatus=VALUES(Campaignstatus)"""
        GuardarCampMetrics="INSERT  INTO CampaingMetrics(CampaingID,Reach,Frequency,Impressions,Placement,Clicks) VALUES (%s,%s,%s,%s,%s,%s)"
        GuardarCampDisplays="INSERT  INTO CampaingDisplay(CampaingID,publisherplatform,placement) VALUES (%s,%s,%s)"
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
            campaignstatus=atr['gsx$campaignstatus']['$t'].encode('utf-8')
            reach=atr['gsx$reach']['$t']
            impressions=atr['gsx$impressions']['$t']
            frequency=atr['gsx$frequency']['$t']
            placement=atr['gsx$placement']['$t'].encode('utf-8')
            publisherplatform=atr['gsx$publisherplatform']['$t']
            startdate=atr['gsx$campaignstartdate']['$t']
            enddate=atr['gsx$campaignenddate']['$t']
            clicks=atr['gsx$outboundclicks']['$t']
            #FIN VARIABLES
            if accountid!='' or accountid!=0:
                cuenta=[accountid,account,'FB']
                cuentas.append(cuenta)
                campana=[campaingid,campaingname,campaignspendinglimit,campaigndailybudget,campaignlifetimebudget,campaignobjective,campaignstatus,accountid,startdate,enddate]
                campanas.append(campana)
                campdisplay=(campaingid,publisherplatform,placement)
                campdisplays.append(campdisplay)
                campmetric=(campaingid,reach,frequency,impressions,placement,clicks)
                campmetrics.append(campmetric)
            #FIN CICLOx
        cur.executemany(GuardarCuentas,cuentas)
        cur.executemany(GuardarCampaing,campanas)
        cur.executemany(GuardarCampMetrics,campmetrics)
        cur.executemany(GuardarCampDisplays,campdisplays)

    except Exception as e:
        print(e)
    finally:
        print('Success Facebook Camp')
#FIN VISTA
def fb_adsets(conn):
    cur=conn.cursor(buffered=True)
    #CONEXION
    startTime = datetime.now()
    r=requests.get("https://spreadsheets.google.com/feeds/list/1jtlJhQJVW0sPvLaFNYoNPwXMnbhdV5M30sX1jt3C1Rc/od6/public/values?alt=json")
    data=r.json()
    #ACCEDER AL OBJETO ENTRY CON LOS DATOS DE LAS CAMPANAS
    temp_k=data['feed']['entry']
    try:
            #QUERYS
        adsets=[]
        adsetmetrics=[]
        sqlInsertAdsSetsMetrics = "INSERT INTO AdSetMetrics(AdSetID,AdSetName,Country,Reach,Frequency,Impressions,Clicks) VALUES (%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE AdSetName=VALUES(AdSetName)"
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
                adsetmetric=(adsetid,adsetname,country,reach,frequency,impressions,clicks)
                adsetmetrics.append(adsetmetric)
            #FIN CICLO

        cur.executemany(sqlInsertAdSet,adsets)
        cur.executemany(sqlInsertAdsSetsMetrics,adsetmetrics)


    except Exception as e:
        print(e)
    finally:
        print('Success Facebook AdSets')
#FIN VISTA
def go_camp(conn):
    cur=conn.cursor(buffered=True)
    startTime = datetime.now()
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
        sqlSelectCampaing = "SELECT count(*) FROM Campaings where CampaingID=%s and AccountsID=%s"
        sqlSelectCampaingDisplay = "SELECT count(*) FROM CampaingDisplay where CampaingID=%s  and Publisherplatform=%s and Placement=%s"
        sqlInsertCampaing = "INSERT INTO Campaings(CampaingID,Campaingname,Campaigndailybudget,Campaignlifetimebudget,Campaignobjective,Campaignstatus,AccountsID,StartDate,EndDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE Campaingname=VALUES(Campaingname),Campaigndailybudget=VALUES(Campaigndailybudget), Campaignlifetimebudget=VALUES(Campaignlifetimebudget),Campaignstatus=VALUES(Campaignstatus)"
        sqlInsertAccount = "INSERT INTO Accounts(AccountsID, Account,Media) values(%s,%s,%s) ON DUPLICATE KEY UPDATE Account=VALUES(Account)"
        sqlInsertCampaingMetrics = "INSERT INTO CampaingMetrics(CampaingID,Percentofbudgetused,impressions,placement,clicks) VALUES (%s,%s,%s,%s,%s)"
        sqlInsertCampaingDisplay = "INSERT INTO CampaingDisplay(CampaingID,publisherplatform,placement) VALUES (%s,%s,%s)"
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
                campmetric=(campaingid,percentofbudgetused,impressions,placement,clicks)
                campmetrics.append(campmetric)

        cur.executemany(sqlInsertAccount ,cuentas)
        cur.executemany(sqlInsertCampaing,campanas)
        cur.executemany(sqlInsertCampaingMetrics,campmetrics)
        cur.executemany(sqlInsertCampaingDisplay,campdisplays)

    except Exception as e:
        print(e)
    finally:
        print (datetime.now())
        print('Success GOOGLE Campanas')
#FIN VISTA
def go_adsets(conn):
    cur=conn.cursor(buffered=True)
    startTime = datetime.now()
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
        sqlInsertAdsSetsMetrics = "INSERT INTO AdSetMetrics(AdSetID,AdSetName,Impressions,Clicks) VALUES (%s,%s,%s,%s) ON DUPLICATE KEY UPDATE AdSetName=VALUES(AdSetName)"

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
                adsetmetric=(adsetid,adsetname,impressions,clicks)
                adsetmetrics.append(adsetmetric)
            #FIN CICLO
        cur.executemany(sqlInsertAdsSets,adsets)
        cur.executemany(sqlInsertAdsSetsMetrics,adsetmetrics)

    except Exception as e:
        print(e)
    finally:
        print('Success Google Adsets')
#FIN VISTA
def go_ads(conn):
    cur=conn.cursor(buffered=True)
    startTime = datetime.now()
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
        cur.executemany(sqlInsertAds,ads)
        cur.executemany(sqlInsertMetricsAds,adsmetrics)

    except Exception as e:
        print(e)
    finally:
        print('Success Google Ads')
#FIN VISTA
def tw_camp(conn):
    cur=conn.cursor(buffered=True)
    startTime = datetime.now()
    print (datetime.now())
    r=requests.get("https://spreadsheets.google.com/feeds/list/1oVsLY6IdOY4AvFz4BMLlSvlHDmG1rtw9HMZrsZzxqJY/od6/public/values?alt=json")
    #FB CAMPAINGS   https://docs.google.com/spreadsheets/d/1fqS12Wc1UIo7v9Ma7OUjY00AdyAuBWnRuY0wx9wrVo4/edit?usp=sharing
    data=r.json()
    #ACCEDER AL OBJETO ENTRY CON LOS DATOS DE LAS CAMPANAS
    temp_k=data['feed']['entry']
    #CONEXION

    cuentas=[]
    campanas=[]
    campmetrics=[]
    campdisplays=[]
    #QUERYS
    try:
        sqlInsertCampaing = "INSERT INTO Campaings(CampaingID,Campaingname,Campaigndailybudget,Campaignlifetimebudget,Campaignobjective,AccountsID,StartDate,EndDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE Campaingname=VALUES(Campaingname),Campaigndailybudget=VALUES(Campaigndailybudget), Campaignlifetimebudget=VALUES(Campaignlifetimebudget)"
        sqlInsertAccount = "INSERT INTO Accounts(AccountsID,Account,Media) VALUES (%s,%s,%s) ON DUPLICATE KEY UPDATE Account=VALUES(Account)"
        sqlInsertCampaingMetrics = "INSERT INTO CampaingMetrics(CampaingID,Impressions,Clicks) VALUES (%s,%s,%s)"
        sqlInsertCampaingDisplay = "INSERT INTO CampaingDisplay(CampaingID,Placement) VALUES (%s,%s)"
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
            if accountid!='':
                cuenta=[accountid,account,'TW']
                cuentas.append(cuenta)
                campana=[campaingid,campaingname,campaigndailybudget,campaignlifetimebudget,campaignobjective,accountid,startdate,enddate]
                campanas.append(campana)
                campdisplay=(campaingid,placement)
                campdisplays.append(campdisplay)
                campmetric=(campaingid,impressions,clicks)
                campmetrics.append(campmetric)
            #FIN CICLOx
        cur.executemany(sqlInsertAccount ,cuentas)
        cur.executemany(sqlInsertCampaing ,campanas)
        cur.executemany(sqlInsertCampaingMetrics,campmetrics)
        cur.executemany(sqlInsertCampaingDisplay,campdisplays)

    except Exception as e:
        print(e)
    finally:
        print('Success Campanas Twitter')
#FIN VISTA
def tw_adsets(conn):
    cur=conn.cursor(buffered=True)
    startTime = datetime.now()
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
        sqlInsertAdsSetsMetrics = "INSERT INTO AdSetMetrics(AdSetID,AdSetName,Impressions,Clicks) VALUES (%s,%s,%s,%s) "
        sqlInsertAdsSets = "INSERT INTO Adsets(AdSetID,Adsetname,Adsetlifetimebudget,CampaingID) VALUES (%s,%s,%s,%s) ON DUPLICATE KEY UPDATE AdSetName=VALUES(AdSetName)"
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
                adsetmetric=(adsetid,adsetname,impressions,clicks)
                adsetmetrics.append(adsetmetric)
            #ADSET

        cur.executemany(sqlInsertAdsSets,adsets)
        cur.executemany(sqlInsertAdsSetsMetrics,adsetmetrics)

    except Exception as e:
        print(e)
    finally:
        print('Success Twitter Adsets')
#FIN VISTA
def tw_ads(conn):
    cur=conn.cursor(buffered=True)
    startTime = datetime.now()
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
        sqlInsertAds = "INSERT INTO Ads(AdID,AdSetID,Adname) VALUES (%s,%s,%s) ON DUPLICATE KEY UPDATE Adname=VALUES(Adname)"
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

        cur.executemany(sqlInsertAds,ads)
        cur.executemany(sqlInsertMetricsAds,adsmetrics)


    except Exception as e:
        print(e)
    finally:
        print('Success Ads Twittter')
#FIN VISTA
def errors_fb_inv(conn):
    cur=conn.cursor(buffered=True)
    startTime = datetime.now()
    print (datetime.now())
    Comentario=''
    TipoErrorID=0
    a=0
    Estatus=''
    try:
        sqlConjuntosFB="select a.AccountsID,a.Account, b.CampaingID,b.Campaingname, b.Campaignspendinglimit,b.Campaigndailybudget,b.Campaignlifetimebudget,c.AdSetID,c.Adsetname,c.Adsetlifetimebudget,SUM(c.Adsetlifetimebudget) as tlotalconjungo,c.Adsetdailybudget,a.Media,b.Campaignstatus,b.Campaignstatus,c.Status from Accounts a INNER JOIN Campaings b on a.AccountsID=b.AccountsID INNER JOIN  Adsets c on b.CampaingID=c.CampaingID where a.Media='FB' group by b.CampaingID  desc "
        sqlInserErrors = "INSERT INTO ErrorsCampaings(Error,Comentario,Media,TipoErrorID,CampaingID,Impressions,StatusCampaing) VALUES (%s,%s,%s,%s,%s,%s,%s)"
        sqlSelectErrors = "SELECT COUNT(*) FROM ErrorsCampaings where CampaingID=%s and TipoErrorID=%s and Media=%s"
        cur.execute(sqlConjuntosFB,)
        resultscon=cur.fetchall()
        Errores=[]
        Impressions=0
        for result in resultscon:
            StatusCampaing=result[13]
            StatusAdsets=result[14]
            if StatusCampaing!='PAUSED':
                if StatusAdsets=='PAUSED':
                    Estatus=StatusAdsets
                else:
                    Estatus=StatusAdsets
            else:
                Estatus=StatusCampaing

            Nomenclatura=result[3]
            CampaingID=result[2]
            Media=result[12]
            searchObj = re.search( r'^(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/&]+)_([a-zA-Z0-9-/&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/&]+)_([a-zA-Z-/]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(19)_([0-9,.]+)_(BA|AL|TR|TRRS|IN|DES|RV|CO)_([0-9,.]+)_(CPM|CPMA|CPVi|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCo)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ]+)_([0-9,.-]+)_(B-)?([0-9]+)_(S-)?([0-9]+).*', Nomenclatura, re.M|re.I)
            if searchObj:
                NomInversion= searchObj.group(11)
                if result[4]==0:
                    if result[6]>NomInversion:
                        Error=result[6]
                        Comentario="Cuidado error de inversion diaria verifica la plataforma"
                        rescampaing=cur.fetchone()
                        if rescampaing[0]==0:
                            if CampaingID!='':
                                nuevoerror=(Error,Comentario,'FB',2,CampaingID,0,Estatus)
                                Errores.append(nuevoerror)
                    if result[10]>NomInversion:
                        Error=result[10]
                        Comentario="Cuidado error de inversion diaria de conjunto de anuncios verifica la plataforma "
                        cur.execute(sqlSelectErrors,(CampaingID,4,'FB'))
                        rescampaing=cur.fetchone()
                        if rescampaing[0]==0:
                            if CampaingID!='':
                                nuevoerror=(Error,Comentario,'FB',4,CampaingID,0,Estatus)
                                Errores.append(nuevoerror)
                    if result[5]>0:
                        Error=result[5]
                        Comentario="Error de inversion no debe ser mayor a la planificada"
                        cur.execute(sqlSelectErrors,(CampaingID,3,'FB'))
                        rescampaing=cur.fetchone()
                        if rescampaing[0]==0:
                            if CampaingID!='':
                                nuevoerror=(Error,Comentario,'FB',3,CampaingID,0,Estatus)
                                Errores.append(nuevoerror)
                    if result[11]>0:
                        Error=result[11]
                        Comentario="Error de inversion de conjunto de anuncios no debe ser mayor a la planificada"
                        cur.execute(sqlSelectErrors,(CampaingID,5,'FB'))
                        rescampaing=cur.fetchone()
                        if rescampaing[0]==0:
                            if CampaingID!='':
                                nuevoerror=(Error,Comentario,'FB',5,CampaingID,0,Estatus)
                                Errores.append(nuevoerror)
            else:
                Comentario="Error de nomenclatura verifica cada uno de sus elementos"
                cur.execute(sqlSelectErrors,(CampaingID,1,'FB'))
                rescampaing=cur.fetchone()
                if rescampaing[0]<1:
                    if CampaingID!='':
                        nuevoerror=(Nomenclatura,Comentario,'FB',1,CampaingID,0,Estatus)
                        Errores.append(nuevoerror)

        cur.executemany(sqlInserErrors,Errores)


    #ANALISIS IMPRESIONES Y
        #print(m.groups())
    except Exception as e:
        print(e)
    finally:
        print('Success Errores FB Inversion Comprobados')

def errors_fb_pais(conn):
    cur=conn.cursor(buffered=True)
    startTime = datetime.now()
    print (datetime.now())
    Comentario=''
    TipoErrorID=0
    Estatus=''
    a=0
    try:
        sqlCampaingsFB="select a.AccountsID,a.Account,a.Media,b.CampaingID,b.Campaingname,b.Campaignspendinglimit,b.Campaigndailybudget,b.Campaignlifetimebudget, c.AdSetID,c.Adsetname,c.Adsetlifetimebudget,c.Adsetdailybudget, d.AdID,d.Adname,d.country,d.CreateDate,e.Impressions,b.Campaignstatus,d.Adstatus from Accounts a INNER JOIN Campaings b on a.AccountsID=b.AccountsID INNER JOIN  Adsets c on b.CampaingID=c.CampaingID INNER JOIN Ads d on d.AdSetID=c.AdSetID INNER JOIN MetricsAds e on e.AdID=d.AdID where a.Media='FB' group by d.Adname, d.Country ORDER BY d.CreateDate desc"
        sqlInserErrors = "INSERT INTO ErrorsCampaings(Error,Comentario,Media,TipoErrorID,CampaingID,Impressions,StatusCampaing) VALUES (%s,%s,%s,%s,%s,%s,%s)"
        sqlSelectErrors = "SELECT COUNT(*) FROM ErrorsCampaings where CampaingID=%s and TipoErrorID=%s and Media=%s"

        Errores=[]
        Impressions=0
        cur.execute(sqlCampaingsFB,)
        results=cur.fetchall()
        for result in results:
            Nomenclatura=result[4].encode('utf-8')
            Media=result[2]
            CampaingIDS=result[3]
            Impressions=result[16]
            StatusCampaing=result[17]
            StatusAdsets=result[18]
            if StatusCampaing!='PAUSED':
                if StatusAdsets=='PAUSED':
                    Estatus='PAUSED'
                else:
                    Estatus=StatusAdsets
            else:
                Estatus=StatusCampaing
            #VALORES NOMENCLATURA
            searchObj = re.search( r'^(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/&]+)_([a-zA-Z0-9-/&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/&]+)_([a-zA-Z-/]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(19)_([0-9,.]+)_(BA|AL|TR|TRRS|IN|DES|RV|CO)_([0-9,.]+)_(CPM|CPMA|CPVi|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCo)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ]+)_([0-9,.-]+)_(B-)?([0-9]+)_(S-)?([0-9]+).*', Nomenclatura, re.M|re.I)
            if searchObj:
                NomPais= searchObj.group(1)
                NomCliente= searchObj.group(2)
                NomMarca= searchObj.group(3)
                NomProducto= searchObj.group(4)
                NomCampana= searchObj.group(5)
                NomVersion= searchObj.group(6)
                NomMedio= searchObj.group(7)
                NomFormato= searchObj.group(8)
                NomMes= searchObj.group(9)
                NomAno= searchObj.group(10)
                NomInversion= searchObj.group(11)
                NomObjetivo= searchObj.group(12)
                NomMetObjetivo= searchObj.group(13)
                NomTipoKpi= searchObj.group(14)
                NomCostoKpi= searchObj.group(15)
                NomFormato= searchObj.group(16)
                NomEstrategia= searchObj.group(17)
                NomTactica= searchObj.group(18)
                NomOrden= searchObj.group(19)
                NomBono= searchObj.group(20)
                NomSaldo= searchObj.group(21)
                if NomCliente=='CCPRADERA':
                        if NomProducto=='HUE':
                            if result[14]=='MX':
                                a+=1
                        if NomProducto=='CHIQ':
                            if result[14]=='GT':
                                a+=1
                            elif result[14]=='HN':
                                a+=1
                        if a==0:
                            Error=result[14]
                            TipoErrorID=6
                            Comentario="Error de paises se estan imprimiendo anuncios en otros paises"
                            cur.execute(sqlSelectErrors,(CampaingIDS,TipoErrorID,Media))
                            rescampaing=cur.fetchone()
                            if rescampaing[0]<1:
                                if CampaingIDS!='':
                                    nuevoerror=(Error,Comentario,Media,TipoErrorID,CampaingIDS,Impressions,Estatus)
                                    Errores.append(nuevoerror)
                if NomCliente!='CCR' and NomCliente!='CCPRADERA':
                    if result[14]!=NomPais:
                        Error=result[14]
                        TipoErrorID=6
                        Comentario="Error de paises se estan imprimiendo anuncios en otros paises"
                        cur.execute(sqlSelectErrors,(CampaingIDS,TipoErrorID,Media))
                        rescampaing=cur.fetchone()
                        if rescampaing[0]<1:
                            if CampaingIDS!='':
                                nuevoerror=(Error,Comentario,Media,TipoErrorID,CampaingIDS,Impressions,Estatus)
                                Errores.append(nuevoerror)

        cur.executemany(sqlInserErrors,Errores)
    #ANALISIS IMPRESIONES Y
        #print(m.groups())
    except Exception as e:
        print(e)
    finally:
        print('Success Errores FB Pais Comprobados')

def errors_go(conn):
    cur=conn.cursor(buffered=True)
    startTime = datetime.now()
    print (datetime.now())
    Comentario=''
    TipoErrorID=0
    a=0
    try:
        sqlCampaingsGO="select a.AccountsID,a.Account,a.Media,b.CampaingID,b.Campaingname,b.Campaignspendinglimit,b.Campaigndailybudget,b.Campaignlifetimebudget, c.AdSetID,c.Adsetname,c.Adsetlifetimebudget,c.Adsetdailybudget, d.AdID,d.Adname,d.CreateDate,b.Campaignstatus,b.Campaignstatus,c.Status from Accounts a INNER JOIN Campaings b on a.AccountsID=b.AccountsID  INNER JOIN  Adsets c on b.CampaingID=c.CampaingID INNER JOIN Ads d on d.AdSetID=c.AdSetID where a.Media='GO'  group by d.AdID ORDER BY d.CreateDate desc LIMIT 50000000000"
        sqlInserErrors = "INSERT INTO ErrorsCampaings(Error,Comentario,Media,TipoErrorID,CampaingID,Impressions) VALUES (%s,%s,%s,%s,%s,%s)"
        sqlSelectErrors = "SELECT COUNT(*) FROM ErrorsCampaings where CampaingID=%s and TipoErrorID=%s and Media=%s"

        Errores=[]
        Impressions=0
        Estatus=''
        cur.execute(sqlCampaingsGO,)
        results=cur.fetchall()
        for result in results:
            Nomenclatura=result[4]
            Media=result[2]
            CampaingID=result[3]
            #VALORES NOMENCLATURA
            StatusCampaing=result[15]
            StatusAdsets=result[16]
            if StatusCampaing!='enabled':
                if StatusAdsets!='enabled':
                    Estatus=StatusAdsets
                else:
                    Estatus=StatusAdsets
            else:
                Estatus=StatusCampaing


            searchObj = re.search( r'^(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/&]+)_([a-zA-Z0-9-/&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/&]+)_([a-zA-Z-/]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(19)_([0-9,.]+)_(BA|AL|TR|TRRS|IN|DES|RV|CO)_([0-9,.]+)_(CPM|CPMA|CPVi|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCo)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ]+)_([0-9,.-]+)_(B-)?([0-9]+)_(S-)?([0-9]+).*', Nomenclatura, re.M|re.I)
            if searchObj:
                a=1
            else:
                Error=Nomenclatura
                TipoErrorID=1
                Comentario="Error de nomenclatura verifica cada uno de sus elementos"
                cur.execute(sqlSelectErrors,(CampaingID,TipoErrorID,Media))
                rescampaing=cur.fetchone()
                if rescampaing[0]<1:
                    if CampaingIDS!='':
                        nuevoerror=(Error,Comentario,Media,TipoErrorID,CampaingID,0)
                        Errores.append(nuevoerror)

        cur.executemany(sqlInserErrors,Errores)


    #ANALISIS IMPRESIONES Y
        #print(m.groups())
    except Exception as e:
        print(e)
    finally:
        print('Success Errores GO Comprobados')
#FIN VISTA
def errors_tw(conn):
    cur=conn.cursor(buffered=True)
    startTime = datetime.now()
    print (datetime.now())
    Comentario=''
    TipoErrorID=0
    a=0
    Errores=[]
    Impressions=0
    try:
        sqlCampaingsTW="select a.AccountsID,a.Account,a.Media,b.CampaingID,b.Campaingname,b.Campaignspendinglimit,b.Campaigndailybudget,b.Campaignlifetimebudget,e.Impressions,b.Campaignstatus from Accounts a INNER JOIN Campaings b on a.AccountsID=b.AccountsID  INNER JOIN CampaingMetrics e on b.CampaingID = e.CampaingID where a.Media='TW'  group by b.CampaingID ORDER BY a.CreateDate desc LIMIT 50000000000"
        sqlInserErrors = "INSERT INTO ErrorsCampaings(Error,Comentario,Media,TipoErrorID,CampaingID,Impressions) VALUES (%s,%s,%s,%s,%s,%s)"
        sqlSelectErrors = "SELECT COUNT(*) FROM ErrorsCampaings where CampaingID=%s and TipoErrorID=%s and Media=%s"
        cur.execute(sqlCampaingsTW,)
        results=cur.fetchall()
        for result in results:
            Nomenclatura=result[4]
            Media=result[2]
            CampaingID=result[3]
            #Impressions=result[16]
            #VALORES NOMENCLATURA
            if result[8]>0:

                searchObj = re.search( r'^(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/&]+)_([a-zA-Z0-9-/&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/&]+)_([a-zA-Z-/]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(19)_([0-9,.]+)_(BA|AL|TR|TRRS|IN|DES|RV|CO)_([0-9,.]+)_(CPM|CPMA|CPVi|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCo)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ]+)_([0-9,.-]+)_(B-)?([0-9]+)_(S-)?([0-9]+).*', Nomenclatura, re.M|re.I)
                if searchObj:
                    a=1
                else:
                    Error=Nomenclatura
                    TipoErrorID=1
                    cur.execute(sqlSelectErrors,(CampaingID,TipoErrorID,Media))
                    rescampaing=cur.fetchone()
                    if rescampaing[0]<1:
                        if CampaingIDS!='':
                            nuevoerror=(Error,Comentario,Media,TipoErrorID,CampaingID,0)
                            Comentario="Error de nomenclatura verifica cada uno de sus elementos"
                            Errores.append(nuevoerror)

        cur.executemany(sqlInserErrors,Errores)


    #ANALISIS IMPRESIONES Y
        #print(m.groups())
    except Exception as e:
        print(e)
    finally:
        print('Success Errores TW Comprobados')

def reviewerrors(conn):
    cur=conn.cursor(buffered=True)
    startTime = datetime.now()
    print (datetime.now())
    try:
        berror="SELECT * FROM ErrorsCampaings"
        bcampaings='SELECT CampaingID,Campaingname,Campaignstatus  FROM Campaings where CampaingID=%s'
        btcampaings='SELECT CampaingID,Campaingname,Campaignstatus FROM Campaings'
        bupdate="UPDATE ErrorsCampaings SET estado=0 where CampaingID=%s"
        cupdate="UPDATE ErrorsCampaings SET error=%s where CampaingID=%s"

        cur.execute(btcampaings,)
        rest=cur.fetchall()
        for res in rest:
            if res[2]=='PAUSED':
                cur.execute(bupdate,(res[0],))
        cur.execute(berror,)
        resultscon=cur.fetchall()
        #SELECIONAMOS TODOS LOS ERRORES ACTUALES
        for res in resultscon:
            ##SI EL ERROR ES TIPO NOMENCLATURA
            if res[3]>0 and res[5]==1:
                rs=res[6]
                cur.execute(bcampaings,(rs,))
                ncampanas=cur.fetchall()
                for res in ncampanas:
                    ID=res[0]
                    Nomenclatura=res[1]
                    searchObj = re.search( r'^(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/&]+)_([a-zA-Z0-9-/&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/&]+)_([a-zA-Z-/]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(19)_([0-9,.]+)_(BA|AL|TR|TRRS|IN|DES|RV|CO)_([0-9,.]+)_(CPM|CPMA|CPVi|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCo)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ]+)_([0-9,.-]+)_(B-)?([0-9]+)_(S-)?([0-9]+).*', Nomenclatura, re.M|re.I)
                    if searchObj:
                        cur.execute(bupdate,(ID,))
                    else:
                        cur.execute(cupdate,(Nomenclatura,ID))
    except Exception as e:
        print(e)
    finally:
        print('Actualización Errores OK')

def push_camps(conn):
    fb_camp(conn)
    go_camp(conn)
    tw_camp(conn)

def push_adsets(conn):
    fb_adsets(conn)
    go_adsets(conn)
    tw_adsets(conn)

def push_ads(conn):
    fb_ads(conn)
    go_ads(conn)
    tw_ads(conn)

def push_errors(conn):
    errors_fb_inv(conn)
    errors_fb_pais(conn)
    errors_tw(conn)
    errors_go(conn)

if __name__ == '__main__':
   openConnection()
   push_errors(conn)
   #errors_fb_inv(conn)
   conn.close()
    #fb_ads()
   #reviewerrors()
   #reviewerrors()
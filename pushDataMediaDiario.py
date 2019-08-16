# -*- coding: UTF-8 -*-
import json
import requests
import sys
import re
import mysql.connector as mysql
from datetime import datetime
import schedule
import time
#3.95.117.169
def openConnection():
    global conn
    try:
        conn = mysql.connect(host='3.95.117.169',database='MediaPlatforms',user='omgdev',password='Sdev@2002!',autocommit=True)
    except:
        print("ERROR: NO SE PUEDO ESTABLECER CONEXION MYSQL.")
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
        sqlInsertDailyAd = "INSERT INTO dailyads(AdID, Adname, Status, Impressions, Ctr, Cpm, Cro, Cost, Frequency, Reach, Pagelikes, Peopletakingaction, Postreactions, Postshares, Photoviews, Clickstoplayvideo, Leads, Eventresponses, Messagingreplies, Videowatchesat75, Videowatchesat100, Websiteleads, Desktopappinstalls, Mobileappinstalls, Clicks) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        #results = cur.fetchall()  Mostrar datos de una consulta
        Dailyads=[]
        for atr in temp_k:
          
            #Ads
            adid=atr['gsx$adid']['$t']
            adname=atr['gsx$adname']['$t'].encode('unicode_escape')
            country=atr['gsx$country']['$t'].encode('utf-8')
            adstatus=atr['gsx$adstatus']['$t'].encode('utf-8')
            #CREATIVE
            
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
                dailyad=(adid,adname,adstatus,impressions,ctr,cpm,cro,cost,frequency,reach,pagelikes,peopletakingaction,postreactions,postshares,photoviews,clickstoplayvideo,leads,eventresponses,messagingreplies,videowatchesat75,videowatchesat100,websiteleads,desktopappinstalls,mobileappinstalls,outboundclicks)
                Dailyads.append(dailyad)
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(sqlInsertDailyAd,Dailyads)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        print('Success Facebook Ads')
    except Exception as e:
        print(e)
    finally:
        print(datetime.now())
#FIN VISTA

def fb_camp(conn):
    cur=conn.cursor(buffered=True)
    print (datetime.now())
    r = requests.get(
        "https://spreadsheets.google.com/feeds/list/1sJcYtuYMZvtD_MxIbwVkRIeBXBOAQXCRxsuc3_UHOYQ/od6/public/values?alt=json")
    data=r.json()
    #ACCEDER AL OBJETO ENTRY CON LOS DATOS DE LAS CAMPANAS
    temp_k=data['feed']['entry']
    #CONEXION
    try:
        #QUERYS
        GuardarDailyCampaing="INSERT INTO Dailycampaing(CampaingID,Reach,Frequency,Impressions,Placement,Clicks,cost) VALUES (%s,%s,%s,%s,%s,%s,%s)"
        campanas=[]
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
            cost=atr['gsx$cost']['$t']

            #FIN VARIABLES
            if campaingid!='':
                campana=(campaingid,reach,frequency,impressions,placement,clicks,cost)
                campanas.append(campana)
            #FIN CICLOx
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(GuardarDailyCampaing,campanas)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        print('Success Facebook Camp')
    except Exception as e:
        print(e)
    finally:
        print(datetime.now())
#FIN VISTA
def fb_adsets(conn):
    cur=conn.cursor(buffered=True)
    #CONEXION
    startTime = datetime.now()
    r = requests.get(
        "https://spreadsheets.google.com/feeds/list/1BgB9EvWYKX0j8xYW1_jjPDjN-OTyO1pUKW_kIyr95mc/od6/public/values?alt=json")
    data=r.json()
    #ACCEDER AL OBJETO ENTRY CON LOS DATOS DE LAS CAMPANAS
    temp_k=data['feed']['entry']
    try:
            #QUERYS
        adsets=[]
        sqlInsertDailyAdsets = "INSERT INTO dailyadset(AdSetID,AdSetName,Country,Reach,Frequency,Impressions,Clicks) VALUES (%s,%s,%s,%s,%s,%s,%s) "
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
                adset=(adsetid,adsetname,country,reach,frequency,impressions,clicks)
                adsets.append(adset)
            #FIN CICLO
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(sqlInsertDailyAdsets,adsets)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")


    except Exception as e:
        print(e)
    finally:
        print('Success Facebook AdSets')
#FIN VISTA
def go_camp(conn):
    cur=conn.cursor(buffered=True)
    print (datetime.now())
    r = requests.get(
        "https://spreadsheets.google.com/feeds/list/1CxZPB3uAGTFH3j8RAAfEcEorL1r6Iy-eWBY_X4mpXRw/od6/public/values?alt=json")
    data=r.json()
    #ACCEDER AL OBJETO ENTRY CON LOS DATOS DE LAS CAMPANAS
    temp_k=data['feed']['entry']
    #CONEXION
    try:
        #QUERYS
        GuardarDailyCampaing="INSERT INTO Dailycampaing(CampaingID,Impressions,Clicks,cost,Percentofbudgetused) VALUES (%s,%s,%s,%s,%s)"
        campanas=[]
        for atr in temp_k:
            #ACCOUNT
            accountid=atr['gsx$accountid']['$t'].encode('utf-8')
            account=atr['gsx$account']['$t']
            account2=atr['gsx$account']['$t'].encode('utf-8')
            #CAMPAING
            campaingid=atr['gsx$campaignid']['$t']
            campaingname=atr['gsx$campaignname']['$t'].encode('utf-8')

            impressions=atr['gsx$impressions']['$t']
            cost=atr['gsx$cost']['$t']
            clicks=atr['gsx$clicks']['$t']
            #Percent of budget used
            percentofbudgetused=atr['gsx$percentofbudgetused']['$t']
            #FIN VARIABLES
            if campaingid!='':
                if percentofbudgetused!='':
                    campana=(campaingid,impressions,clicks,cost,percentofbudgetused)
                    campanas.append(campana)
                else:
                    campana=(campaingid,impressions,clicks,cost,0)
                    campanas.append(campana)
            #FIN CICLOx
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(GuardarDailyCampaing,campanas)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        print('Success GOOGLE Campanas')
    except Exception as e:
        print(e)
    finally:
        print(datetime.now())
        
#FIN VISTA
def go_adsets(conn):
    cur=conn.cursor(buffered=True)
    startTime = datetime.now()
    print (datetime.now())
    r=requests.get("https://spreadsheets.google.com/feeds/list/1gLKwKf5NWo09HmgadIVG_FLeJFrAKLYEKBMo93HEgJk/od6/public/values?alt=json")
    #FB CAMPAINGS   https://docs.google.com/spreadsheets/d/1fqS12Wc1UIo7v9Ma7OUjY00AdyAuBWnRuY0wx9wrVo4/edit?usp=sharing
    data=r.json()
    #ACCEDER AL OBJETO ENTRY CON LOS DATOS DE LAS CAMPANAS
    temp_k=data['feed']['entry']
    #CONEXION
    try:
        #QUERYS
        adsets=[]
        sqlInsertDailyAdsets = "INSERT INTO dailyadset(AdSetID,AdSetName,Impressions,Clicks) VALUES (%s,%s,%s,%s) "

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
                adset=(adsetid,adsetname,impressions,clicks)
                adsets.append(adset)
            #FIN CICLO
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(sqlInsertDailyAdsets,adsets)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        print('Success Google Adsets')

    except Exception as e:
        print(e)
    finally:
        print(datetime.now())
#FIN VISTA
def go_ads(conn):
    cur=conn.cursor(buffered=True)
    startTime = datetime.now()
    print (datetime.now())
    r=requests.get("https://spreadsheets.google.com/feeds/list/1XAeBrJIyPB2n6Jz3pMGph5IazSCqZBS7PjsNVfGWt3w/od6/public/values?alt=json")
    #FB CAMPAINGS   https://docs.google.com/spreadsheets/d/1fqS12Wc1UIo7v9Ma7OUjY00AdyAuBWnRuY0wx9wrVo4/edit?usp=sharing
    data=r.json()
    #ACCEDER AL OBJETO ENTRY CON LOS DATOS DE LAS CAMPANAS
    temp_k=data['feed']['entry']
    #CONEXION
    try:
        ads=[]
        #QUERYS
        sqlInsertDailyAd = "INSERT INTO dailyads(Cost,Ctr,Cpm,Convertions,Videowatchesat100,Videowatchesat75,AdID,Adname,Impressions,Clicks) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
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
                ad=(cost,ctr,cpm,convertions,videowatchesat100,videowatchesat75,adid,descripcion,impressions,clicks)
                ads.append(ad)
        
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(sqlInsertDailyAd,ads)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        print('Success Google Ads')

    except Exception as e:
        print(e)
    finally:
        print(datetime.now())
#FIN VISTA
def tw_camp(conn):
    #FB CAMPAINGS   https://docs.google.com/spreadsheets/d/1fqS12Wc1UIo7v9Ma7OUjY00AdyAuBWnRuY0wx9wrVo4/edit?usp=sharing
    cur=conn.cursor(buffered=True)
    startTime = datetime.now()
    print (datetime.now())
    r=requests.get("https://spreadsheets.google.com/feeds/list/10UMdRoZal--BLcZYtq63PSvv6AStQ9Dye-dMgtdPUs0/od6/public/values?alt=json")
    #FB CAMPAINGS   https://docs.google.com/spreadsheets/d/1fqS12Wc1UIo7v9Ma7OUjY00AdyAuBWnRuY0wx9wrVo4/edit?usp=sharing
    data=r.json()
    #ACCEDER AL OBJETO ENTRY CON LOS DATOS DE LAS CAMPANAS
    temp_k=data['feed']['entry']
    #CONEXION
    
    campanas=[]
    #QUERYS
    try:
        GuardarDailyCampaing="INSERT INTO Dailycampaing(CampaingID,Impressions,Clicks,cost) VALUES (%s,%s,%s,%s)"
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
                campana=(campaingid,impressions,clicks,cost)
                campanas.append(campana)
            #FIN CICLOx
       
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(GuardarDailyCampaing ,campanas)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        print('Success Campanas Twitter')
    except Exception as e:
        print(e)
    finally:
        print(datetime.now())
#FIN VISTA
def tw_adsets(conn):
    cur=conn.cursor(buffered=True)
    print (datetime.now())
    r=requests.get("https://spreadsheets.google.com/feeds/list/1dpabfqdQkD6GAZ1pFRQlvNpcsjdVA_scEWTF90eBEBg/od6/public/values?alt=json")
    #FB CAMPAINGS   https://docs.google.com/spreadsheets/d/1fqS12Wc1UIo7v9Ma7OUjY00AdyAuBWnRuY0wx9wrVo4/edit?usp=sharing
    try:
        data=r.json()
        #ACCEDER AL OBJETO ENTRY CON LOS DATOS DE LAS CAMPANAS
        temp_k=data['feed']['entry']
        #CONEXION

        adsets=[]
        #QUERYS
        sqlInsertDailyAdsets = "INSERT INTO dailyadset(AdSetID,AdSetName,Impressions,Clicks) VALUES (%s,%s,%s,%s) "
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
                adset=(adsetid,adsetname,impressions,clicks)
                adsets.append(adset)
            #ADSET
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(sqlInsertDailyAdsets,adsets)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        print('Success Twitter Adsets')
    except Exception as e:
        print(e)
    finally:
        print(datetime.now())
#FIN VISTA
def tw_ads(conn):
    cur=conn.cursor(buffered=True)
    startTime = datetime.now()
    print (datetime.now())
    r=requests.get("https://spreadsheets.google.com/feeds/list/1gJYMnVO7MCey1LC5goqW0Igksq6GN_vs358JKMYzI4I/od6/public/values?alt=json")
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
        sqlInsertDailyAd = "INSERT INTO dailyads(Impressions,Cost,Ctr,Cpm,AdID) VALUES (%s,%s,%s,%s,%s)"
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
            if adid!='' and impressions != '':
                if cost!='':
                    adme=(impressions, cost,ctr,cpm, adsetid)
                    ads.append(adme)
                else:
                    adme=(impressions, 0,ctr,cpm, adsetid)
                    ads.append(adme)
               
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(sqlInsertDailyAd,ads)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        print('Success Ads Twittter')

    except Exception as e:
        print(e)
    finally:
        print(datetime.now())
#FIN VISTA
def push_camps(conn):
    fb_camp(conn)
    #go_camp(conn)
    #tw_camp(conn)

def push_adsets(conn):
    fb_adsets(conn)
    #go_adsets(conn)
    #tw_adsets(conn)

def push_ads(conn):
    fb_ads(conn)
    #go_ads(conn)
    #tw_ads(conn)

if __name__ == '__main__':
   openConnection()
   fb_ads(conn)
   fb_camp(conn)
   fb_adsets(conn)
   go_camp(conn)
   go_adsets(conn)
   go_ads(conn)
   tw_camp(conn)
   tw_adsets(conn)
   tw_ads(conn)
   conn.close()
 

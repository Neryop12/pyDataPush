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

host= config['TESTING']['HOST']
name = config['TESTING']['NAME']
user = config['TESTING']['USER']
password = config['TESTING']['PASSWORD']
autocommit= config['TESTING']['AUTOCOMMIT']

def openConnection():
    global conn
    try:
        conn = mysql.connect(host=host, database=name,
                             user=user, password=password, autocommit=autocommit)
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
        sqlInsertDailyAd = "INSERT INTO DailyAds(AdID, Adname, Status, Impressions, Ctr, Cpm, Cro, Cost, Frequency, Reach, Pagelikes, Peopletakingaction, Postreactions, Postshares, Photoviews, Clickstoplayvideo, Leads, Eventresponses, Messagingreplies, Videowatchesat75, Videowatchesat100, Websiteleads, Desktopappinstalls, Mobileappinstalls, Clicks) VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
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
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("fb_ads", "Success", "pushDataMediaDiario.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("fb_ads", "{}", "pushDataMediaDiario.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
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
        GuardarDailycampaing="INSERT INTO Dailycampaing(CampaingID,Reach,Frequency,Impressions,Clicks,cost,Campaignlifetimebudget,EndDate,Placement,VideoWatches75,PostReaccion,Result) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        campanas=[]
        for atr in temp_k:
            #ACCOUNT

            #CAMPAING
            campaingid=atr['gsx$campaignid']['$t']
            campaingname=atr['gsx$campaignname']['$t']
            campaignspendinglimit=atr['gsx$campaignspendinglimit']['$t']
            campaigndailybudget=atr['gsx$campaigndailybudget']['$t']
            campaignlifetimebudget=atr['gsx$campaignlifetimebudget']['$t']
            reach=atr['gsx$reach']['$t']
            impressions=atr['gsx$impressions']['$t']
            frequency=atr['gsx$frequency']['$t']
            startdate=atr['gsx$campaignstartdate']['$t']
            enddate=atr['gsx$campaignenddate']['$t']
            clicks=atr['gsx$outboundclicks']['$t']
            cost=atr['gsx$cost']['$t']
            budget=atr['gsx$campaignlifetimebudget']['$t']
            enddate=atr['gsx$campaignenddate']['$t']
            placement=atr['gsx$placement']['$t']
            videowatch=atr['gsx$videowatchesat75']['$t']
            postreaccion=atr['gsx$postreactions']['$t']
            result = 0
            #FIN VARIABLES
            if campaingid!='':
                searchObj = re.search(r'(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.+&]+)_([a-zA-Z0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-Z-/.+]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ.+]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(19|2019)_([0-9,.]+)_(BA|AL|TR|TRRS|TRRRSS|IN|DES|RV|CO)_([0-9,.]+)_(CPM|CPMA|CPVi|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCO|CPCO)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([0-9,.-]+)?(_B-)?(_)?([0-9]+)?(_S-)?(_)?([0-9]+)?(\(([0-9.)]+)\))?$', campaingname, re.M | re.I)
                if searchObj:
                    Result = (searchObj.group(14))
                    if str(Result).upper() == 'CPVI':
                        result = clicks
                    elif str(Result).upper() == 'CPMA':
                        result = reach
                    elif str(Result).upper() == 'CPM':
                       result = impressions
                    elif str(Result).upper() == 'CPV':
                        result = videowatch
                    elif str(Result).upper() == 'CPCO':
                        result = clicks
                    elif str(Result).upper() == 'CPI':
                        result = postreaccion
                if budget!='':
                    campana=(campaingid,reach,frequency,impressions,clicks,cost,budget,enddate,placement,videowatch,postreaccion,result)
                    campanas.append(campana)
                else:
                    campana=(campaingid,reach,frequency,impressions,clicks,cost,0,enddate,placement,videowatch,postreaccion,result)
                    campanas.append(campana)
            #FIN CICLOx
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(GuardarDailycampaing,campanas)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        print('Success Facebook Camp')
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("fb_camp", "Success", "pushDataMediaDiario.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("fb_camp", "{}", "pushDataMediaDiario.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
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
        sqlInsertDailyAdsets = "INSERT INTO DailyAdset(AdSetID,AdSetName,Country,Reach,Frequency,Impressions,Clicks) VALUES (%s,%s,%s,%s,%s,%s,%s) "
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
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("fb_adsets", "Success", "pushDataMediaDiario.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("fb_adsets", "{}", "pushDataMediaDiario.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
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
        GuardarDailycampaing="INSERT INTO Dailycampaing(CampaingID,Impressions,Clicks,cost,Percentofbudgetused,Campaigndailybudget,Campaignlifetimebudget,EndDate,Placement,SubPlacement) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
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
            dailybudget=atr['gsx$dailybudget']['$t']
            budget=atr['gsx$budget']['$t']
            enddate=atr['gsx$enddate']['$t']
            advertising=atr['gsx$advertisingchanneltype']['$t']
            subadvertising=atr['gsx$advertisingchannelsub-type']['$t']
            Nomenclatura=atr['gsx$campaignname']['$t']
            searchObj = re.search(r'^(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.+&]+)_([a-zA-Z0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-Z-/.+]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ.+]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(19|2019)_([0-9,.]+)_(BA|AL|TR|TRRS|IN|DES|RV|CO)_([0-9,.]+)_(CPM|CPMA|CPVi|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCo)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([0-9,.-]+)(_B-)?([0-9]+)?(_S-)?([0-9]+)?(\(([0-9.)]+)\))?', (Nomenclatura), re.M | re.I)
            if enddate=='' or enddate ==' --':
                enddate = None
            if searchObj:
                NomInversion = float(searchObj.group(11))
            #FIN VARIABLES
            if campaingid!='':
                if percentofbudgetused!='':
                    campana=(campaingid,impressions,clicks,cost,percentofbudgetused,dailybudget,NomInversion,enddate,advertising,subadvertising)
                    campanas.append(campana)
                else:
                    campana=(campaingid,impressions,clicks,cost,0,dailybudget,NomInversion,enddate,advertising,subadvertising)
                    campanas.append(campana)


            #FIN CICLOx
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(GuardarDailycampaing,campanas)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        print('Success GOOGLE Campanas')
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("go_camp", "Success", "pushDataMediaDiario.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("go_camp", "{}", "pushDataMediaDiario.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
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
        sqlInsertDailyAdsets = "INSERT INTO DailyAdset(AdSetID,AdSetName,Impressions,Clicks) VALUES (%s,%s,%s,%s) "

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
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("go_adsets", "Success", "pushDataMediaDiario.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("go_adsets", "{}", "pushDataMediaDiario.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
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
        sqlInsertDailyAd = "INSERT INTO DailyAds(Cost,Ctr,Cpm,Convertions,Videowatchesat100,Videowatchesat75,AdID,Adname,Impressions,Clicks) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
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
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("go_ads", "Success", "pushDataMediaDiario.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("go_ads", "{}", "pushDataMediaDiario.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
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
        GuardarDailycampaing="INSERT INTO Dailycampaing(CampaingID,Impressions,Clicks,cost,enddate) VALUES (%s,%s,%s,%s,%s)"
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
            enddate=atr['gsx$campaignendtime']['$t']
            if enddate=='':
                enddate = None
            if accountid!='':
                campana=(campaingid,impressions,clicks,cost,enddate)
                campanas.append(campana)
            #FIN CICLOx

        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(GuardarDailycampaing ,campanas)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        print('Success Campanas Twitter')
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("tw_camp", "Success", "pushDataMediaDiario.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("tw_camp", "{}", "pushDataMediaDiario.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
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
        sqlInsertDailyAdsets = "INSERT INTO DailyAdset(AdSetID,AdSetName,Impressions,Clicks) VALUES (%s,%s,%s,%s) "
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
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("tw_adsets", "Success", "pushDataMediaDiario.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("tw_adsets", "{}", "pushDataMediaDiario.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
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
        sqlInsertDailyAd = "INSERT INTO DailyAds(Impressions,Cost,Ctr,Cpm,AdID) VALUES (%s,%s,%s,%s,%s)"
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
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("tw_ads", "Success", "pushDataMediaDiario.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("tw_ads", "{}", "pushDataMediaDiario.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print(datetime.now())

def fb_reach(conn):
    cur=conn.cursor(buffered=True)
    print (datetime.now())
    r = requests.get(
        "https://spreadsheets.google.com/feeds/list/1Lt2XJgxQQyi5iylaQjGScZcqeefVObJ2mljlXHZLteg/od6/public/values?alt=json")
    data=r.json()
    #ACCEDER AL OBJETO ENTRY CON LOS DATOS DE LAS CAMPANAS
    temp_k=data['feed']['entry']
    #CONEXION
    try:
        #QUERYS
        UpdateCampaing="UPDATE Dailycampaing SET Reach = %s, Frequency = %s WHERE (CampaingID = %s and id > 0);"
        campanas=[]
        UpdateCampaingReach="UPDATE Dailycampaing SET Reach = %s, Frequency = %s, Result = %s WHERE (CampaingID = %s and id > 0);"
        reachs=[]
        for atr in temp_k:

            #CAMPAING
            campaingid=atr['gsx$campaignid']['$t']
            campaingname=atr['gsx$campaignname']['$t']
            reach=atr['gsx$reach']['$t']
            frequency=atr['gsx$frequency']['$t']
            if campaingid!='':
                searchObj = re.search(r'(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.+&]+)_([a-zA-Z0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-Z-/.+]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ.+]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(19|2019)_([0-9,.]+)_(BA|AL|TR|TRRS|TRRRSS|IN|DES|RV|CO)_([0-9,.]+)_(CPM|CPMA|CPVi|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCO|CPCO)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([0-9,.-]+)?(_B-)?(_)?([0-9]+)?(_S-)?(_)?([0-9]+)?(\(([0-9.)]+)\))?$', campaingname, re.M | re.I)
                if searchObj:
                    Metrica = (searchObj.group(14))
                    if str(Metrica).upper() == 'CPMA':
                        res=(reach,frequency,reach,campaingid)
                        reachs.append(res)
                    else:
                        campana=(reach,frequency,campaingid)
                        campanas.append(campana)
                        #FIN CICLOx
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(UpdateCampaing,campanas)
        cur.executemany(UpdateCampaingReach,reachs)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        print('Success Facebook Camp')
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("fb_camp", "Success", "pushDataMediaDiario.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("fb_camp", "{}", "pushDataMediaDiario.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print(datetime.now())


#FIN VISTA
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

#Funciones
if __name__ == '__main__':
   openConnection()
   fb_ads(conn)
   fb_camp(conn)
   fb_reach(conn)
   fb_adsets(conn)
   go_camp(conn)
   go_adsets(conn)
   go_ads(conn)
   tw_camp(conn)
   tw_adsets(conn)
   tw_ads(conn)
   conn.close()

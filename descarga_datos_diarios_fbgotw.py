# -*- coding: UTF-8 -*-
import json
import requests
import sys
import re
import mysql.connector as mysql
from datetime import datetime, timedelta
import time

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
    r = requests.get(
        "https://spreadsheets.google.com/feeds/list/1sJcYtuYMZvtD_MxIbwVkRIeBXBOAQXCRxsuc3_UHOYQ/od6/public/values?alt=json")
    data=r.json()
    #ACCEDER AL OBJETO ENTRY CON LOS DATOS DE LAS CAMPANAS
    temp_k=data['feed']['entry']
    #CONEXION
    try:
        #QUERYS
        GuardarDailycampaing="INSERT INTO dailycampaing(CampaingID,Reach,Frequency,Impressions,Clicks,cost,Campaignlifetimebudget,EndDate,Placement,VideoWatches75,PostReaccion,Result) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        GuardarHistorico="INSERT INTO HistoricCampaings(CampaingID,Campaingname,Cost,Result,Impressions,Clicks,Campaigndailybudget,Campaignlifetimebudget,Campaignspendinglimit,EndDate,Leads,VideoWatches75,PostReaccion,Reach,Frequency) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE Cost=VALUES(Cost), Result=Values(Result),Impressions=Values(Impressions),Clicks=Values(Clicks),Campaigndailybudget=Values(Campaigndailybudget),Campaignlifetimebudget=Values(Campaignlifetimebudget),Campaignspendinglimit=Values(Campaignspendinglimit),EndDate=Values(EndDate),Leads=Values(Leads),VideoWatches75=Values(VideoWatches75),PostReaccion=Values(PostReaccion),Reach=Values(Reach),Frequency=Values(Frequency);"
        campanas=[]
        historico=[]
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
            videowatch=atr['gsx$videowatchesat75']['$t']
            postreaccion=atr['gsx$postreactions']['$t']
            leads=0
            mess=atr['gsx$newmessagingconversations']['$t']
            result = 0
            #FIN VARIABLES
            if campaingid!='':
                searchObj = re.search(r'([0-9,.]+)_(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.+&]+)_([a-zA-Z0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&0-9]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&0-9]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&0-9]+)_([a-zA-Z-/.+]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ.+0-9]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(2019|19|20|2020)_([0-9,.]+)_(BA|AL|TR|TRRS|TRRRSS|IN|DES|RV|CO|MESAD|LE)_([0-9,.]+)_(CPM|CPMA|CPVi|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCO|CPCO)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([0-9,.-]+)?(_B-)?(_)?([0-9.,]+)?(_S-)?(_)?([0-9.,]+)?(\(([0-9.)])\))?(/[0-9].+)?', campaingname, re.M | re.I)
                if searchObj:
                    Result = (searchObj.group(15))
                    objcon = (searchObj.group(13))
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

                    if enddate == '':
                        enddate = '2019-01-01'
                    if datetime.strptime(enddate,'%Y-%m-%d') > datetime.now() - timedelta(days=1):

                        if budget!='':
                            campana=(campaingid,reach,frequency,impressions,clicks,cost,budget,enddate,'',videowatch,postreaccion,result)
                            campanas.append(campana)
                        else:
                            campana=(campaingid,reach,frequency,impressions,clicks,cost,0,enddate,'',videowatch,postreaccion,result)
                            campanas.append(campana)
                    else:
                        historia=(campaingid,campaingname,cost,result,impressions,clicks,campaigndailybudget,campaignlifetimebudget,campaignspendinglimit,enddate,leads,videowatch,postreaccion,reach,frequency)
                        historico.append(historia)
            #FIN CICLOx

        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(GuardarDailycampaing,campanas)
        cur.executemany(GuardarHistorico,historico)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        print('Success Facebook Camp')
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("Resultado diario Facebook", "Success", "post_resultados_diarios_fb_go_tw.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("Resultado diario Facebook", "{}", "post_resultados_diarios_fb_go_tw.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print(datetime.now())
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
        kpi=0
        NomInversion = 0
        #QUERYS
        GuardarDailycampaing="INSERT INTO dailycampaing(CampaingID,Impressions,Clicks,cost,Percentofbudgetused,Campaigndailybudget,Campaignlifetimebudget,EndDate,Placement,SubPlacement,Result) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        GuardarHistorico="INSERT INTO HistoricCampaings(CampaingID,Campaingname,Cost,Result,Impressions,Clicks,Percentofbudgetused,Campaigndailybudget,Campaignlifetimebudget,EndDate,Leads) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE Cost=VALUES(Cost), Result=Values(Result),Impressions=Values(Impressions),Clicks=Values(Clicks),Percentofbudgetused=Values(Percentofbudgetused),Campaigndailybudget=Values(Campaigndailybudget),Campaignlifetimebudget=Values(Campaignlifetimebudget),EndDate=Values(EndDate),Leads=Values(Leads);"
        campanas=[]
        historico=[]

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
            videowatch=atr['gsx$watch75views']['$t']
            conversion=atr['gsx$conversions']['$t']
            searchObj = re.search(r'([0-9,.]+)_(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.+&]+)_([a-zA-Z0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&0-9]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&0-9]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&0-9]+)_([a-zA-Z-/.+]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ.+0-9]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(2019|19|20|2020)_([0-9,.]+)_(BA|AL|TR|TRRS|TRRRSS|IN|DES|RV|CO|MESAD|LE)_([0-9,.]+)_(CPM|CPMA|CPVi|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCO|CPCO)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([0-9,.-]+)?(_B-)?(_)?([0-9.,]+)?(_S-)?(_)?([0-9.,]+)?(\(([0-9.)])\))?(/[0-9].+)?', (Nomenclatura), re.M | re.I)
            if enddate=='' or enddate ==' --':
                enddate = None
            if searchObj:
                NomInversion = float(searchObj.group(12))
                Result = (searchObj.group(15))
                if str(Result).upper() == 'CPVI':
                    kpi = clicks
                elif str(Result).upper() == 'CPM':
                    kpi = impressions
                elif str(Result).upper() == 'CPV':
                    kpi = videowatch
                elif str(Result).upper() == 'CPCO':
                    kpi = conversion
                elif str(Result).upper() == 'CPC':
                    kpi = clicks
                elif str(Result).upper() == 'CPD':
                    kpi = conversion

            #FIN VARIABLES
                if campaingid!='':
                    if enddate is None:
                        enddate = '2019-01-01'
                    if datetime.strptime(enddate,'%Y-%m-%d') > datetime.now() - timedelta(days=1):
                        if percentofbudgetused!='':
                            campana=(campaingid,impressions,clicks,cost,percentofbudgetused,dailybudget,NomInversion,enddate,advertising,subadvertising,kpi)
                            campanas.append(campana)
                        else:
                            campana=(campaingid,impressions,clicks,cost,0,dailybudget,NomInversion,enddate,advertising,subadvertising,kpi)
                            campanas.append(campana)
                    else:
                        historia=(campaingid,campaingname,cost,kpi,impressions,clicks,percentofbudgetused,dailybudget,budget,enddate,conversion)
                        historico.append(historia)
            #FIN CICLOx
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(GuardarDailycampaing,campanas)
        cur.executemany(GuardarHistorico,historico)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        print('Success GOOGLE Campanas')
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("Resultado Diario Google", "Success", "post_resultados_diarios_fb_go_tw.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("Resultado Diario Google", "{}", "post_resultados_diarios_fb_go_tw.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print(datetime.now())

def go_camp_mo(conn):
    cur=conn.cursor(buffered=True)
    print (datetime.now())
    r = requests.get(
        "https://spreadsheets.google.com/feeds/list/1SStEvt-n83Se2HWg3b2f4gJpFI7qUZy1VNKntqy36Bw/od6/public/values?alt=json")
    data=r.json()
    #ACCEDER AL OBJETO ENTRY CON LOS DATOS DE LAS CAMPANAS
    temp_k=data['feed']['entry']
    #CONEXION
    try:
        kpi=0
        NomInversion = 0
        #QUERYS
        GuardarDailycampaing="INSERT INTO dailycampaing(CampaingID,Impressions,Clicks,cost,Percentofbudgetused,Campaigndailybudget,Campaignlifetimebudget,EndDate,Placement,SubPlacement,Result) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        GuardarHistorico="INSERT INTO HistoricCampaings(CampaingID,Campaingname,Cost,Result,Impressions,Clicks,Percentofbudgetused,Campaigndailybudget,Campaignlifetimebudget,EndDate,Leads) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE Cost=VALUES(Cost), Result=Values(Result),Impressions=Values(Impressions),Clicks=Values(Clicks),Percentofbudgetused=Values(Percentofbudgetused),Campaigndailybudget=Values(Campaigndailybudget),Campaignlifetimebudget=Values(Campaignlifetimebudget),EndDate=Values(EndDate),Leads=Values(Leads);"
        campanas=[]
        historico=[]

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
            videowatch=atr['gsx$watch75views']['$t']
            conversion=atr['gsx$conversions']['$t']
            searchObj = re.search(r'([0-9,.]+)_(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.+&]+)_([a-zA-Z0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&0-9]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&0-9]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&0-9]+)_([a-zA-Z-/.+]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ.+0-9]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(2019|19|20|2020)_([0-9,.]+)_(BA|AL|TR|TRRS|TRRRSS|IN|DES|RV|CO|MESAD|LE)_([0-9,.]+)_(CPM|CPMA|CPVi|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCO|CPCO)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([0-9,.-]+)?(_B-)?(_)?([0-9.,]+)?(_S-)?(_)?([0-9.,]+)?(\(([0-9.)])\))?(/[0-9].+)?', (Nomenclatura), re.M | re.I)
            if enddate=='' or enddate ==' --':
                enddate = None
            if searchObj:
                NomInversion = float(searchObj.group(12))
                Result = (searchObj.group(15))
                if str(Result).upper() == 'CPVI':
                    kpi = clicks
                elif str(Result).upper() == 'CPM':
                    kpi = impressions
                elif str(Result).upper() == 'CPV':
                    kpi = videowatch
                elif str(Result).upper() == 'CPCO':
                    kpi = conversion
                elif str(Result).upper() == 'CPC':
                    kpi = clicks
                elif str(Result).upper() == 'CPD':
                    kpi = conversion

            #FIN VARIABLES
                if campaingid!='':
                    if enddate is None:
                        enddate = '2019-01-01'
                    if datetime.strptime(enddate,'%Y-%m-%d') > datetime.now() - timedelta(days=1):
                        if percentofbudgetused!='':
                            campana=(campaingid,impressions,clicks,cost,percentofbudgetused,dailybudget,NomInversion,enddate,advertising,subadvertising,kpi)
                            campanas.append(campana)
                        else:
                            campana=(campaingid,impressions,clicks,cost,0,dailybudget,NomInversion,enddate,advertising,subadvertising,kpi)
                            campanas.append(campana)
                    else:
                        historia=(campaingid,campaingname,cost,kpi,impressions,clicks,percentofbudgetused,dailybudget,budget,enddate,conversion)
                        historico.append(historia)
            #FIN CICLOx
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(GuardarDailycampaing,campanas)
        cur.executemany(GuardarHistorico,historico)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        print('Success GOOGLE Campanas Mosca')
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("Resultado Diario Google Mosca", "Success", "post_resultados_diarios_fb_go_tw.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("Resultado Diario Google Mosca", "{}", "post_resultados_diarios_fb_go_tw.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print(datetime.now())

def go_camp_house(conn):
    cur=conn.cursor(buffered=True)
    print (datetime.now())
    r = requests.get(
        "https://spreadsheets.google.com/feeds/list/137bLgysmPORksk0ruErrcvvG_Y33RHGRkd7TRZXlE4E/od6/public/values?alt=json")
    data=r.json()
    #ACCEDER AL OBJETO ENTRY CON LOS DATOS DE LAS CAMPANAS
    temp_k=data['feed']['entry']
    #CONEXION
    try:
        kpi=0
        NomInversion = 0
        #QUERYS
        GuardarDailycampaing="INSERT INTO dailycampaing(CampaingID,Impressions,Clicks,cost,Percentofbudgetused,Campaigndailybudget,Campaignlifetimebudget,EndDate,Placement,SubPlacement,Result) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        GuardarHistorico="INSERT INTO HistoricCampaings(CampaingID,Campaingname,Cost,Result,Impressions,Clicks,Percentofbudgetused,Campaigndailybudget,Campaignlifetimebudget,EndDate,Leads) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE Cost=VALUES(Cost), Result=Values(Result),Impressions=Values(Impressions),Clicks=Values(Clicks),Percentofbudgetused=Values(Percentofbudgetused),Campaigndailybudget=Values(Campaigndailybudget),Campaignlifetimebudget=Values(Campaignlifetimebudget),EndDate=Values(EndDate),Leads=Values(Leads);"
        campanas=[]
        historico=[]

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
            videowatch=atr['gsx$watch75views']['$t']
            conversion=atr['gsx$conversions']['$t']
            searchObj = re.search(r'([0-9,.]+)_(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.+&]+)_([a-zA-Z0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&0-9]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&0-9]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&0-9]+)_([a-zA-Z-/.+]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ.+0-9]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(2019|19|20|2020)_([0-9,.]+)_(BA|AL|TR|TRRS|TRRRSS|IN|DES|RV|CO|MESAD|LE)_([0-9,.]+)_(CPM|CPMA|CPVi|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCO|CPCO)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([0-9,.-]+)?(_B-)?(_)?([0-9.,]+)?(_S-)?(_)?([0-9.,]+)?(\(([0-9.)])\))?(/[0-9].+)?', (Nomenclatura), re.M | re.I)
            if enddate=='' or enddate ==' --':
                enddate = None
            if searchObj:
                NomInversion = float(searchObj.group(12))
                Result = (searchObj.group(15))
                if str(Result).upper() == 'CPVI':
                    kpi = clicks
                elif str(Result).upper() == 'CPM':
                    kpi = impressions
                elif str(Result).upper() == 'CPV':
                    kpi = videowatch
                elif str(Result).upper() == 'CPCO':
                    kpi = conversion
                elif str(Result).upper() == 'CPC':
                    kpi = clicks
                elif str(Result).upper() == 'CPD':
                    kpi = conversion

            #FIN VARIABLES
                if campaingid!='':
                    if enddate is None:
                        enddate = '2019-01-01'
                    if datetime.strptime(enddate,'%Y-%m-%d') > datetime.now() - timedelta(days=1):
                        if percentofbudgetused!='':
                            campana=(campaingid,impressions,clicks,cost,percentofbudgetused,dailybudget,NomInversion,enddate,advertising,subadvertising,kpi)
                            campanas.append(campana)
                        else:
                            campana=(campaingid,impressions,clicks,cost,0,dailybudget,NomInversion,enddate,advertising,subadvertising,kpi)
                            campanas.append(campana)
                    else:
                        historia=(campaingid,campaingname,cost,kpi,impressions,clicks,percentofbudgetused,dailybudget,budget,enddate,conversion)
                        historico.append(historia)
            #FIN CICLOx
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(GuardarDailycampaing,campanas)
        cur.executemany(GuardarHistorico,historico)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        print('Success GOOGLE Campanas House')
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("Resultado Diario Google House", "Success", "post_resultados_diarios_fb_go_tw.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("Resultado Diario Google House", "{}", "post_resultados_diarios_fb_go_tw.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print(datetime.now())
#FIN VISTA
def tw_camp(conn):

    cur=conn.cursor(buffered=True)
    startTime = datetime.now()
    print (datetime.now())
    r=requests.get("https://spreadsheets.google.com/feeds/list/10UMdRoZal--BLcZYtq63PSvv6AStQ9Dye-dMgtdPUs0/od6/public/values?alt=json")

    data=r.json()
    #ACCEDER AL OBJETO ENTRY CON LOS DATOS DE LAS CAMPANAS

    #CONEXION
    campanas=[]
    GuardarHistorico="INSERT INTO HistoricCampaings(CampaingID,Campaingname,Cost,Result) VALUES (%s,%s,%s,%s) ON DUPLICATE KEY UPDATE Cost=VALUES(Cost), Result=Values(Result);"
    historico=[]
    #QUERYS
    try:
        temp_k=data['feed']['entry']
        GuardarDailycampaing="INSERT INTO dailycampaing(CampaingID,Impressions,Clicks,cost,enddate) VALUES (%s,%s,%s,%s,%s)"
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
                enddate = '2019-01-01'
            if accountid!='':
                if datetime.strptime(enddate,'%Y-%m-%d') > datetime.now() - timedelta(days=1):
                    campana=(campaingid,impressions,clicks,cost,enddate)
                    campanas.append(campana)
                else:
                    historia=(campaingid,campaingname,cost,0)
                    historico.append(historia)
            #FIN CICLOx

        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(GuardarDailycampaing ,campanas)
        cur.executemany(GuardarHistorico ,historico)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        print('Success Campanas Twitter')
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("Resultado Diario Twitter", "Success", "post_resultados_diarios_fb_go_tw.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("Resultado Diario Twitter", "{}", "post_resultados_diarios_fb_go_tw.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print(datetime.now())
#FIN VISTA
def truncateAllCamp(conn):
    try:
        cur=conn.cursor(buffered=True)
        print (datetime.now())
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.execute('TRUNCATE dailycampaing;')
        cur.execute("SET FOREIGN_KEY_CHECKS=1")

    except Exception as e:
        print(e)
    finally:
        print(datetime.now())
#Funciones
if __name__ == '__main__':
   openConnection()
   truncateAllCamp(conn)
   fb_camp(conn)
   go_camp(conn)
   go_camp_mo(conn)
   go_camp_house(conn)
   tw_camp(conn)
   conn.close()


# -*- coding: UTF-8 -*-
import config.db as db
import dbconnect as sql
import json
import requests
import sys
import re
import mysql.connector as mysql
from datetime import datetime, timedelta
import time
import pandas as pd
import numpy as mp
from xml.etree import ElementTree
import io
import math

now = datetime.now()
CreateDate = now.strftime("%Y-%m-%d %H:%M:%S")

ayer = (datetime.now() - timedelta(1))
ayer = ayer.strftime("%Y-%m-%d")

cuentas = []
campanas = []
adsets = []
ads = []
metricas = []
creatives = []
media = 'AM'
metricasads = []
metricasadsets = []
historico = []
diarios = []


def GetToken():
    global Token
    # URL para la obtencion del Token
    url = 'https://reportapi.adsmovil.com/api/login'
    Token = requests.post(
        url,
        data={

            "email": db.AM['username'],
            "password": db.AM['password']
        }
    )
    Token = Token.json()


def Camapanas(df, media, conn):
    try:
        url = 'https://reportapi.adsmovil.com/api/campaign/details'
        Result2 = requests.get(
            url,

            headers={
                'Authorization': "'" + Token["result"]["token"] + "'",
            },
            params={
                'report': 'adsmovil_dsp',
                'startDate': ayer,
                'endDate': ayer,
            }
        )
        r = Result2.json()
        for row in r["result"]["queryResponseData"]["rows"]:
            for n, i in enumerate(row):
                if i == 'NaN':
                    row[n] = 0
            result = 0
            CampaingIDMFC = 0
            Campaigndailybudget = 0
            AccountsID = row[1]
            Account = row['advertiser_name']
            CampaingID = row['campaign_id']
            Campaignspendinglimit = 0
            Campaignobjective = ''
            Campaingbuyingtype = ''
            Campaignbudgetremaining = 0
            Campaignlifetimebudget = row['campaign_budget']
            Percentofbudgetused = 0
            Campaignstatus = 'ACTIVE'
            StartDate = row['start_date']
            EndDate = row['end_date']
            CampaignIDMFC = 0
            Cost = row['total_spend']
            Frequency = 0
            Reach = 0
            Postengagements = 0
            Impressions = row['impressions']
            Clicks = row['clicks']
            Landingpageviews = 0
            Videowachesat75 = row['video_third_quartile']
            ThruPlay = 0
            Conversions = row['total_conversions']

            Campaingname = row[2].split('(')
            Campaingname = Campaingname[0].replace(' ', '')
            regex = '([0-9,.]+)_(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&]+)_([a-zA-Z0-9-/.%+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.%+&0-9]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+%&0-9]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+%&0-9]+)_([a-zA-Z-/.+]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ.+0-9]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(2019|19|20|2020)_([0-9,.]+)_(BA|AL|TR|TRRS|TRRRSS|IN|DES|RV|CO|MESAD|LE)_([0-9,.]+)_(CPM|CPMA|CPVI|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCO|CPCO)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([0-9,.-]+)?(_B-)?(_)?([0-9.,]+)?(_S-)?(_)?([0-9.,]+)?(\(([0-9.)])\))?(/[0-9].+)?'
            searchObj = re.search(regex, Campaingname)
            if searchObj != None:
                CampaingIDMFC = searchObj.group(1)
                if searchObj.group(3) == 'CLARO':
                    AccountID = searchObj.group(
                        1)+searchObj.group(2)+searchObj.group(3)+searchObj.group(4)
                else:
                    AccountID = searchObj.group(
                        2)+searchObj.group(3)+searchObj.group(4)

                Result = (searchObj.group(15))
                objcon = (searchObj.group(13))
                if str(Result).upper() == 'CPVI':
                    result = Clicks
                    Objetive = 'CPVI'
                elif str(Result).upper() == 'CPMA':
                    result = Reach
                    Objetive = 'CPMA'
                elif str(Result).upper() == 'CPM':
                    result = Impressions
                    Objetive = 'CPM'
                elif str(Result).upper() == 'CPV':
                    result = Videowachesat75
                    Objetive = 'CPV'
                elif str(Result).upper() == 'CPCO':
                    if str(objcon).upper() == 'MESAD':
                        result = 0
                        Objetive = 'MESAD'
                    elif str(objcon).upper() == 'LE':
                        result = 0
                        Objetive = 'LE'
                    else:
                        result = Clicks
                        Objetive = 'CPCO'
                elif str(Result).upper() == 'CPI':
                    result = 0
                    Objetive = 'CPI'
                elif str(Result).upper() == 'CPMA':
                    result = Reach
                    Objetive = 'CPMA'
                elif str(Result).upper() == 'CPC':
                    result = Clicks
                    Objetive = 'CPC'
                elif str(Result).upper() == 'CPMA':
                    result = Reach
                    Objetive = 'CPMA'

            else:
                CampaingIDMFC = 0

            if EndDate == 0 or EndDate == '':
                EndDate = '2019-01-01'
            if datetime.strptime(EndDate, '%Y-%m-%d') < datetime.now() - timedelta(days=1):
                historia = [CampaingID, Campaingname, Campaigndailybudget,
                            Campaignlifetimebudget, Percentofbudgetused,
                            StartDate, EndDate, result, Objetive, CampaignIDMFC,
                            Cost, Frequency,
                            Reach, Postengagements, Impressions,
                            Clicks,  Landingpageviews,
                            Videowachesat75, ThruPlay, Conversions, CreateDate]

                historico.append(historia)

            campana = [CampaingID, Campaingname, Campaignspendinglimit,
                       Campaigndailybudget, Campaignlifetimebudget, Campaignobjective,
                       Campaignstatus, AccountID, StartDate,
                       EndDate, Campaingbuyingtype, Campaignbudgetremaining,
                       Percentofbudgetused, Cost, CampaingIDMFC, CreateDate]
            cuenta = [AccountID, AccountID, media, CreateDate]
            metrica = [CampaingID, Cost, Frequency, Reach, Postengagements, Impressions, Clicks,
                       Landingpageviews, Videowachesat75, ThruPlay, Conversions, CreateDate]
            diario = [CampaingID, Campaingname, Campaigndailybudget,
                      Campaignlifetimebudget, Percentofbudgetused,
                      StartDate, EndDate, result, Objetive, CampaignIDMFC,
                      Cost, Frequency,
                      Reach, Postengagements, Impressions,
                      Clicks,  Landingpageviews,
                      Videowachesat75, ThruPlay, Conversions, CreateDate]

            diarios.append(diario)

            campanas.append(campana)
            cuentas.append(cuenta)
            metricas.append(metrica)

        sql.connect.insertCuentas(cuentas, 'AM', conn)
        sql.connect.insertCampanas(campanas, 'AM', conn)
        sql.connect.insertMetricasCampanas(metricas, 'AM', conn)
        sql.connect.insertDiarioCampanas(diarios, 'AM', conn)
        sql.connect.insertHistoric(historico, 'AM', conn)
    except Exception as e:
        print(e)


def pushAdsMovil(conn):
    global cur
    fechaayer = datetime.now() - timedelta(days=1)
    # Formato de las fechas para aceptar en el GET
    dayayer = fechaayer.strftime("%Y-%m-%d")
    print(datetime.now())
    cur = conn.cursor(buffered=True)
    accounts = []
    campanas = []
    conjuntos = []
    cmetrics = []
    ametrics = []
    anuncios = []

    sqlInsertCampaing = "INSERT INTO Campaings (`CampaingID`, `Campaingname`, `AccountsID`,`Campaignstatus`)  VALUES (%s,%s,%s,'ACTIVE') ON DUPLICATE KEY UPDATE Campaingname=VALUES(Campaingname),StartDate=VALUES(StartDate)"

    AdsMetrics = "INSERT INTO MetricsAds (`AdID`,`Adname`,`Impressions`, `Clicks`, `Videowatchesat75`,`Videowatchesat100`, `Ctr`, `Cpm`, `cost`)  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    sqlConjunto = "INSERT INTO Adsets (`CampaingID`, `AdSetID`,`Adsetname`,`Status`)  VALUES (%s,%s,%s,'ACTIVE') ON DUPLICATE KEY UPDATE Adsetname=VALUES(Adsetname)"

    sqlAnuncio = "INSERT INTO Ads (`AdSetID`,`AdID`,`Adname`,`Status`,`Media`) VALUES(%s,%s,%s,'ACTIVE','AM') ON DUPLICATE KEY UPDATE Adname=(Adname)"

    sqlInsertAccounts = "INSERT INTO `MediaPlatforms`.`Accounts` (`AccountsID`, `Account`, `Media`, `State`) VALUES (%s, %s, %s, %s)ON DUPLICATE KEY UPDATE Account=VALUES(Account);"

    try:
        url = 'https://reportapi.adsmovil.com/api/campaign/details'
        Result2 = requests.get(
            url,

            headers={
                'Authorization': "'" + Token["result"]["token"] + "'",
            },
            params={
                'report': 'adsmovil_dsp',
                'startDate': dayayer,
                'endDate': dayayer,
            }
        )
        r = Result2.json()
        for row in r["result"]["queryResponseData"]["rows"]:
            for n, i in enumerate(row):
                if i == 'NaN':
                    row[n] = 0
            campaingName = row[2].split('(')
            campaingName = campaingName[0].replace(' ', '')
            regex = '([0-9,.]+)_(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&]+)_([a-zA-Z0-9-/.%+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.%+&0-9]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+%&0-9]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+%&0-9]+)_([a-zA-Z-/.+]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ.+0-9]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(2019|19|20|2020)_([0-9,.]+)_(BA|AL|TR|TRRS|TRRRSS|IN|DES|RV|CO|MESAD|LE)_([0-9,.]+)_(CPM|CPMA|CPVI|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCO|CPCO)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([0-9,.-]+)?(_B-)?(_)?([0-9.,]+)?(_S-)?(_)?([0-9.,]+)?(\(([0-9.)])\))?(/[0-9].+)?'
            searchObj = re.search(regex, Campaingname)
            if searchObj:
                if searchObj.group(3) == 'CLARO':
                    AccountID = searchObj.group(
                        1)+searchObj.group(2)+searchObj.group(3)+searchObj.group(4)
                else:
                    AccountID = searchObj.group(
                        2)+searchObj.group(3)+searchObj.group(4)
            adid = row[1]+row[3]
            adsetid = row[1]+AccountID
            account = [AccountID, AccountID, 'AM', 1]
            campana = [row[1], campaingName, AccountID]
            conjunto = [row[1], adsetid, row[2]]
            anuncio = [adsetid, adid, row[3]]
            ametric = [adid, row[3], row[4], row[5],
                       row[9], row[10], row[6], row[12], row[11]]
            accounts.append(account)
            campanas.append(campana)

            conjuntos.append(conjunto)
            anuncios.append(anuncio)
            ametrics.append(ametric)

        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(sqlInsertAccounts, accounts)
        cur.executemany(sqlInsertCampaing, campanas)
        cur.executemany(sqlConjunto, conjuntos)
        cur.executemany(sqlAnuncio, anuncios)
        cur.executemany(AdsMetrics, ametrics)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        print('Success AdsMovil Accouts')
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(sqlInsertCampaing, campanas)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        print('Success AdsMovil Campanas')
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("pushAdsMovil", "Success", "post_resultados_adsmovil.py","{}");'.format(
            dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("pushAdsMovil", "{}", "post_resultados_adsmovil.py","{}");'.format(
            e, dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print(datetime.now())


def pushAdsMovilPusads(conn):
    global cur
    fechaayer = datetime.now() - timedelta(days=1)
    anteayer = datetime.now() - timedelta(days=2)
    # Formato de las fechas para aceptar en el GET
    dayayer = fechaayer.strftime("%Y-%m-%d")
    print(datetime.now())
    cur = conn.cursor(buffered=True)
    accounts = []
    campanas = []
    conjuntos = []
    cmetrics = []
    ametrics = []
    anuncios = []

    sqlInsertCampaing = "INSERT INTO Campaings (`CampaingID`, `Campaingname`, `AccountsID`,`Campaignstatus`)  VALUES (%s,%s,%s,'ACTIVE') ON DUPLICATE KEY UPDATE Campaingname=VALUES(Campaingname),StartDate=VALUES(StartDate)"

    AdsMetrics = "INSERT INTO MetricsAds (`AdID`,`Adname`,`Impressions`, `Clicks`, `Videowatchesat75`,`Videowatchesat100`, `Ctr`, `Cpm`, `cost`)  VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    sqlConjunto = "INSERT INTO adsets (`CampaingID`, `AdSetID`,`Adsetname`,`Status`)  VALUES (%s,%s,%s,'ACTIVE') ON DUPLICATE KEY UPDATE Adsetname=VALUES(Adsetname)"

    sqlAnuncio = "INSERT INTO Ads (`AdSetID`,`AdID`,`Adname`,`Status`,`Media`) VALUES(%s,%s,%s,'ACTIVE','AM') ON DUPLICATE KEY UPDATE Adname=(Adname)"

    sqlInsertAccounts = "INSERT INTO `MediaPlatforms`.`Accounts` (`AccountsID`, `Account`, `Media`, `State`) VALUES (%s, %s, %s, %s)ON DUPLICATE KEY UPDATE Account=VALUES(Account);"

    try:
        url = 'https://reportapi.adsmovil.com/api/campaign/details'
        Result2 = requests.get(
            url,

            headers={
                'Authorization': "'" + Token["result"]["token"] + "'",
            },
            params={
                'report': 'adsmovil_dsp',
                'startDate': anteayayer,
                'endDate': dayayer,
            }
        )
        r = Result2.json()
        for row in r["result"]["queryResponseData"]["rows"]:
            for n, i in enumerate(row):
                if i == 'NaN':
                    row[n] = 0

            searchObj = re.search(r'([0-9,.]+)_(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.+&]+)_([a-zA-Z0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&0-9]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&0-9]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&0-9]+)_([a-zA-Z-/.+]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ.+0-9]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(2019|19|20|2020)_([0-9,.]+)_(BA|AL|TR|TRRS|TRRRSS|IN|DES|RV|CO|MESAD|LE)_([0-9,.]+)_(CPM|CPMA|CPVi|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCO|CPCO)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([0-9,.-]+)?(_B-)?(_)?([0-9.,]+)?(_S-)?(_)?([0-9.,]+)?(\(([0-9.)]+)\))?(/[0-9]+)?', str(row[2]), re.M | re.I)
            if searchObj:
                if searchObj.group(3) == 'CLARO':
                    AccountID = searchObj.group(
                        1)+searchObj.group(2)+searchObj.group(3)+searchObj.group(4)
                else:
                    AccountID = searchObj.group(
                        2)+searchObj.group(3)+searchObj.group(4)
            adid = row[1]+row[3]
            adsetid = row[1]+AccountID
            account = [AccountID, AccountID, 'AMP', 1]
            campana = [row[1], row[2], AccountID]
            conjunto = [row[1], adsetid, row[2]]
            anuncio = [adsetid, adid, row[3]]
            ametric = [adid, row[3], row[4], row[5],
                       row[9], row[10], row[6], row[12], row[11]]
            accounts.append(account)
            campanas.append(campana)

            conjuntos.append(conjunto)
            anuncios.append(anuncio)
            ametrics.append(ametric)

        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(sqlInsertAccounts, accounts)
        cur.executemany(sqlInsertCampaing, campanas)
        cur.executemany(sqlConjunto, conjuntos)
        cur.executemany(sqlAnuncio, anuncios)
        cur.executemany(AdsMetrics, ametrics)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        print('Success AdsMovil PushAds Campanas')
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("pushAdsMovil", "Success", "post_resultados_adsmovil.py","{}");'.format(
            dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("pushAdsMovil", "{}", "post_resultados_adsmovil.py","{}");'.format(
            e, dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print(datetime.now())


def resultsCamps(conn):
    global cur
    print(datetime.now())
    cmetrics = []
    resultado = ''
    cur = conn.cursor(buffered=True)
    sqlMetricsAds = """select c.CampaingID,c.Campaingname,sum(m.Clicks),sum(m.Impressions),SUM(m.cost)  from MetricsAds m
                    inner join Ads a on a.AdID = m.AdID
                    inner join Adsets ad on ad.AdSetID = a.AdSetID
                    INNER join Campaings c on c.CampaingID = ad.CampaingID
                    GROUP by c.CampaingID;
                    """
    sqlInsertMetrics = "INSERT INTO CampaingMetrics(CampaingID,Clicks,Impressions,Cost,Diario,Result)VALUE(%s,%s,%s,%s,1,%s)"
    try:
        cur.execute(sqlMetricsAds,)
        resultscon = cur.fetchall()
        for row in resultscon:
            searchObj = re.search(r'([0-9,.]+)_(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.+&]+)_([a-zA-Z0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&0-9]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&0-9]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&0-9]+)_([a-zA-Z-/.+]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ.+0-9]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(2019|19|20|2020)_([0-9,.]+)_(BA|AL|TR|TRRS|TRRRSS|IN|DES|RV|CO|MESAD|LE)_([0-9,.]+)_(CPM|CPMA|CPVi|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCO|CPCO)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([0-9,.-]+)?(_B-)?(_)?([0-9.,]+)?(_S-)?(_)?([0-9.,]+)?(\(([0-9.)]+)\))?(/[0-9]+)?', str(row[1]), re.M | re.I)
            if searchObj:
                if searchObj.group(14) == 'CPM':
                    resultado = row[3]
                elif searchObj.group(14) == 'CPC':
                    resultado = row[2]
                elif searchObj.group(14) == 'CPVi':
                    resultado = row[2]
            cmetric = [row[0], row[2], row[3], row[4], resultado]
            cmetrics.append(cmetric)
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(sqlInsertMetrics, cmetrics)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        print('Success AdsMovil Resultador Diario Campanas')
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("pushAdsMovilCamps", "Success","post_resultados_adsmovil.py","{}");'.format(
            dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("pushAdsMovil", "{}","post_resultados_adsmovil.py","{}");'.format(
            e, dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print(datetime.now())


if __name__ == '__main__':
    openConnection()
    GetToken()
    pushAdsMovil(conn)
    # pushAdsMovilPusads(conn)
    resultsCamps(conn)
    conn.close()

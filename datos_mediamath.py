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
metricasads = []
metricasadsets = []
historico = []
estados = []
diarios = []
media = 'MM'

ACCESS_TOKEN_URL = "https://auth.mediamath.com/oauth/token"

# API GET, obtiene el token de session para MediaMath


def GetToken():
    global Token
    # URL para la obtencion del Token
    url = 'https://auth.mediamath.com/oauth/token'
    Token = requests.post(
        ACCESS_TOKEN_URL,
        data={
            "grant_type": "password",
            "username": db.MM['username'],
            "password": db.MM['password'],
            "audience": "https://api.mediamath.com/",
            "scope": "",
            "client_id": db.MM['client_id'],
            "client_secret": db.MM['client_secret']
        }
    )
    Token = Token.json()

# API GET, obtiene el cookie de session para MediaMath


def GetSession():
    global session
    url = 'https://api.mediamath.com/api/v2.0/session'
    Result = requests.get(
        url,
        headers={
            'Authorization': 'Bearer ' + Token['access_token'],
        }
    )
    tree = ElementTree.fromstring(Result.content)
    root = tree.getchildren()
    session = root[1].attrib


def CuentasCampanas(conn):
    try:
        url = r'https://api.mediamath.com/reporting/v1/std/performance?filter=organization_id=101058&dimensions=advertiser_name%2cadvertiser_id%2ccampaign_id%2ccampaign_name%2ccampaign_budget&metrics=impressions%2cclicks%2ctotal_spend%2cvideo_third_quartile%2ctotal_conversions%2cvideo_complete'
        # Request GET, para obtener el reporte de Performance de MediaMath
        Result2 = requests.get(
            url,

            headers={
                'Content-Type': 'application/javascript',
                                'Cookie': 'adama_session=' + session['sessionid']
            },
            params={
                'start_date': '2019-12-31',
                'time_rollup': 'by_week',
            }
        )
        # Variable para guardar el contenido del request.
        s = Result2.content
        # Libreria Pandas, para extrare datos obtenidos del request (extension .csv)
        data = pd.read_csv(io.StringIO(s.decode('utf-8')))
        df = pd.DataFrame(data)
        df = df.fillna(0)
        for index, row in df.iterrows():
            result = 0
            CampaingIDMFC = 0
            Campaigndailybudget = 0
            AccountID = row['advertiser_id']
            Account = row['advertiser_name']
            CampaingID = row['campaign_id']
            Campaignspendinglimit = 0
            Campaignobjective = ''
            Campaingbuyingtype = ''
            Campaignbudgetremaining = 0
            Campaingname = row['campaign_name']
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
            Estimatedadrecalllift = 0
            Landingpageviews = 0
            Videowachesat75 = row['video_third_quartile']
            ThruPlay = 0
            Conversions = row['total_conversions']

            if Campaingname != 0 or Campaingname != '':
                regex = '([0-9,.]+)_(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&]+)_([a-zA-Z0-9-/.%+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.%+&0-9]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+%&0-9]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+%&0-9]+)_([a-zA-Z-/.+]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ.+0-9]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(2019|19|20|2020)_([0-9,.]+)_(BA|AL|TR|TRRS|TRRRSS|IN|DES|RV|CO|MESAD|LE)_([0-9,.]+)_(CPM|CPMA|CPVI|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCO|CPCO)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([0-9,.-]+)?(_B-)?(_)?([0-9.,]+)?(_S-)?(_)?([0-9.,]+)?(\(([0-9.)])\))?(/[0-9].+)?'
                match = re.search(regex, Campaingname)
                if match != None:
                    CampaignIDMFC = match.group(1)
                    Result = (match.group(15))
                    objcon = (match.group(13))
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
            if EndDate == 0 or EndDate == '':
                EndDate = '2019-01-01'
            if datetime.strptime(EndDate, '%Y-%m-%d') < datetime.now() - timedelta(days=1):
                historia = [CampaingID, Campaingname, Campaigndailybudget,
                            Campaignlifetimebudget, Percentofbudgetused,
                            StartDate, EndDate, result, Objetive, CampaignIDMFC,
                            Cost, Frequency,
                            Reach, Postengagements, Impressions,
                            Clicks, Estimatedadrecalllift, Landingpageviews,
                            Videowachesat75, ThruPlay, Conversions, CreateDate]

                historico.append(historia)

            campana = [CampaingID, Campaingname, Campaignspendinglimit,
                       Campaigndailybudget, Campaignlifetimebudget, Campaignobjective,
                       Campaignstatus, AccountID, StartDate,
                       EndDate, Campaingbuyingtype, Campaignbudgetremaining,
                       Percentofbudgetused, Cost, CampaingIDMFC, CreateDate]
            cuenta = [AccountID, Account, media, CreateDate]
            metrica = [CampaingID, Cost, Frequency, Reach, Postengagements, Impressions, Clicks,
                       Estimatedadrecalllift, Landingpageviews, Videowachesat75, ThruPlay, Conversions, CreateDate]
            diario = [CampaingID, Campaingname, Campaigndailybudget,
                      Campaignlifetimebudget, Percentofbudgetused,
                      StartDate, EndDate, result, Objetive, CampaignIDMFC,
                      Cost, Frequency,
                      Reach, Postengagements, Impressions,
                      Clicks, Estimatedadrecalllift, Landingpageviews,
                      Videowachesat75, ThruPlay, Conversions, CreateDate]

            diarios.append(diario)

            campanas.append(campana)
            cuentas.append(cuenta)
            metricas.append(metrica)

        sql.connect.insertCuentas(cuentas, 'MM', conn)
        sql.connect.insertCampanas(campanas, 'MM', conn)
        sql.connect.insertMetricasCampanas(metricas, 'MM', conn)
        sql.connect.insertDiarioCampanas(diarios, 'MM', conn)
        sql.connect.insertReportingDiarioCampanas(diarios, 'MM', conn)
        sql.connect.insertHistoric(historico, 'MM', conn)

    except Exception as e:
        print(e)


def Adsets(conn):
    try:
        url = r'https://api.mediamath.com/reporting/v1/std/performance?filter=organization_id=101058&dimensions=campaign_id%2cstrategy_id%2cstrategy_name%2cstrategy_budget%2cstrategy_start_date%2cstrategy_end_date%2cstrategy_type&metrics=impressions%2cclicks%2ctotal_spend%2ctotal_spend_cpm%2ctotal_spend_cpa%2ctotal_spend_cpc%2cctr%2cvideo_third_quartile%2ctotal_conversions%2cvideo_complete'
        # Request GET, para obtener el reporte de Performance de MediaMath
        Result2 = requests.get(
            url,

            headers={
                'Content-Type': 'application/javascript',
                                'Cookie': 'adama_session=' + session['sessionid']
            },
            params={
                'start_date': ayer,
                'time_rollup': 'by_week',
            }
        )
        # Variable para guardar el contenido del request.
        s = Result2.content
        # Libreria Pandas, para extrare datos obtenidos del request (extension .csv)
        data = pd.read_csv(io.StringIO(s.decode('utf-8')))
        df = pd.DataFrame(data)
        df = df.fillna(0)
        for index, row in df.iterrows():
            AdSetID = row['strategy_id']
            Adsetname = row['strategy_name']
            CampaingID = row['campaign_id']
            Adsetlifetimebudget = row['strategy_budget']
            Adsettargeting = ''
            Adsetdailybudget = 0
            Cost = row['total_spend']
            Frequency = 0
            Reach = 0
            Postengagements = 0
            Impressions = row['impressions']
            Clicks = row['clicks']
            Estimatedadrecalllift = 0
            Landingpageviews = 0
            Videowachesat75 = row['video_third_quartile']
            ThruPlay = 0
            Conversions = row['total_conversions']
            Adsetend = row['strategy_end_date']
            Adsetstart = row['strategy_start_date']
            Status = 'ACTIVE'
            Country = ''
            Referer = 'MediaMath'
            Media = 'MM'

            adset = [AdSetID, Adsetname, Adsetlifetimebudget, Adsetdailybudget, Adsettargeting,
                     Adsetend, Adsetstart, CampaingID, Status, CreateDate, Referer, Media]
            metrica = [AdSetID, Cost, Frequency,
                       Reach, Postengagements, Impressions, Clicks, Estimatedadrecalllift, Landingpageviews,
                       Videowachesat75, ThruPlay, Conversions, Country, CreateDate]

            metricasadsets.append(metrica)
            adsets.append(adset)

        sql.connect.insertAdsets(adsets, 'MM', conn)
        sql.connect.insertMetricasAdSet(metricasadsets, 'MM', conn)

    except Exception as e:
        print(e)


def Ads(conn):
    try:
        url = r'https://api.mediamath.com/reporting/v1/std/performance?filter=organization_id=101058&dimensions=strategy_id%2cstrategy_end_date%2cstrategy_start_date%2cstrategy_budget%2cstrategy_name%2ccreative_id%2ccreative_name&metrics=impressions%2cclicks%2ctotal_spend%2ctotal_spend_cpm%2ctotal_spend_cpa%2ctotal_spend_cpc%2cctr%2cvideo_third_quartile%2ctotal_conversions%2cvideo_complete'
        # Request GET, para obtener el reporte de Performance de MediaMath
        Result2 = requests.get(
            url,

            headers={
                'Content-Type': 'application/javascript',
                                'Cookie': 'adama_session=' + session['sessionid']
            },
            params={
                'start_date': ayer,
                'time_rollup': 'by_week',
            }
        )
        # Variable para guardar el contenido del request.
        s = Result2.content
        # Libreria Pandas, para extrare datos obtenidos del request (extension .csv)
        data = pd.read_csv(io.StringIO(s.decode('utf-8')))
        df = pd.DataFrame(data)
        df = df.fillna(0)
        for index, row in df.iterrows():
            AdSetID = row['strategy_id']
            Adname = row['strategy_name']
            AdID = row['strategy_id']
            Adsetlifetimebudget = row['strategy_budget']
            Adsettargeting = ''
            Adsetdailybudget = 0
            Cost = row['total_spend']
            Frequency = 0
            Reach = 0
            Postengagements = 0
            Impressions = row['impressions']
            Clicks = row['clicks']
            Estimatedadrecalllift = 0
            Landingpageviews = 0
            Videowachesat75 = row['video_third_quartile']
            ThruPlay = 0
            Conversions = row['total_conversions']
            Adsetend = row['strategy_end_date']
            Adsetstart = row['strategy_start_date']
            Adstatus = 'ACTIVE'
            Country = ''
            Referer = 'MediaMath'
            Media = 'MM'

            ad = [AdID, Adname, Country, Adstatus,
                  AdSetID, CreateDate, Referer, Media]
            metrica = [AdID, Cost, Frequency,
                       Reach, Postengagements, Impressions, Clicks, Estimatedadrecalllift, Landingpageviews,
                       Videowachesat75, ThruPlay, Conversions, Country, CreateDate]

            ads.append(ad)
            metricasads.append(metrica)

        sql.connect.insertAds(ads, 'MM', conn)
        sql.connect.insertMetricasAd(metricasads, 'MM', conn)

    except Exception as e:
        print(e)
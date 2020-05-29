import numpy as np
import config.db as db
import dbconnect as sql
import sys
import re
import mysql.connector as mysql
from datetime import datetime, timedelta
import time
import configparser
import pandas as pd


now = datetime.now()
CreateDate = now.strftime("%Y-%m-%d %H:%M:%S")
# CONEXION A SPREADSHEETS


def Spreadsheet(Spreadsheet, media, hoja):
    url = 'https://docs.google.com/spreadsheet/ccc?key=%s&output=csv&gid=%s' % (
        Spreadsheet, hoja)
    try:

        df = pd.read_csv(url)

        df = pd.DataFrame(df)
        df = df.fillna(0)

        return df
    except Exception as e:
        raise ValueError(
            'Ocurrio un error al conectarse con la hoja de SpreadSheets!')


def cuentas(df, media, conn):
    # Obtener los datos del Spreasheet
    cuentas = []
    df = df

    for index, row in df.iterrows():
        if media == 'FB' or media == 'GO':
            AccountID = int(row['Account ID'])
        else:
            AccountID = row['Account ID']

        cuenta = [row['Account ID'], row['Account'], media, CreateDate]

        if AccountID != '' or AccountID != 0:
            cuentas.append(cuenta)
    sql.connect.insertCuentas(cuentas, media, conn)


def campanas(df, media, conn):
    campanas = []
    Campaingname = ''
    # Obtener los datos del Spreasheet
    df = df
    for index, row in df.iterrows():
        # Metricas y dimenciones FB
        if media == 'FB':
            if int(row['Campaign ID']) < 1:
                continue

            CampaingID = int(row['Campaign ID'])
            Campaingname = row['Campaign name']
            Campaignspendinglimit = row['Campaign spending limit']
            Campaigndailybudget = row['Campaign daily budget']
            Campaignlifetimebudget = row['Campaign lifetime budget']
            Campaignobjective = row['Campaign objective']
            Campaignstatus = row['Campaign status']
            AccountsID = str(row['Account ID'])
            StartDate = row['Campaign start date']
            EndDate = row['Campaign end date']
            if EndDate == 0:
                EndDate = None
            Campaingbuyingtype = row['Campaign buying type']
            Campaignbudgetremaining = row['Campaign budget remaining']
            Percentofbudgetused = 0
            Cost = row['Cost']

        # Metricas y dimenciones GO
        elif media == 'GO':
            if int(row['Campaign ID']) < 1:
                continue
            EndDate = None
            CampaingID = row['Campaign ID']
            Campaingname = row['Campaign name']
            Campaignspendinglimit = 0
            Campaigndailybudget = row['Daily budget']
            Campaignlifetimebudget = row['Budget']
            Campaignobjective = 0
            Campaignstatus = row['Campaign status']
            AccountsID = row['Account ID']
            StartDate = row['Start date']
            EndDate = row['End date']
            if EndDate == 0 or EndDate == None:
                EndDate = None
            else:
                EndDate = row['End date']
            Campaingbuyingtype = 0
            Campaignbudgetremaining = 0
            Percentofbudgetused = row['Percent of budget used']
            Cost = row['Cost']

        elif media == 'TW':
            if row['Campaign ID'] == '':
                continue
            CampaingID = row['Campaign ID']
            Campaingname = row['Campaign']
            Campaignspendinglimit = 0
            Campaigndailybudget = row['Campaign daily budget']
            Campaignlifetimebudget = row['Campaign total budget']
            Campaignobjective = 'NULL'
            Campaignstatus = row['Campaign servable']
            if Campaignstatus == True:
                Campaignstatus = 'Active'
            else:
                Campaignstatus = 'Disabled'
            AccountsID = row['Account ID']
            StartDate = row['Campaign start time']
            EndDate = row['Campaign end time']
            if EndDate == 0 or EndDate == None:
                EndDate = None
            Campaingbuyingtype = 0
            Campaignbudgetremaining = 0
            Percentofbudgetused = 0
            Cost = row['Cost']

        if Campaingname != 0:
            regex = '([0-9,.]+)_(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&]+)_([a-zA-Z0-9-/.%+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.%+&0-9]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+%&0-9]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+%&0-9]+)_([a-zA-Z-/.+]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ.+0-9]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(2019|19|20|2020)_([0-9,.]+)_(BA|AL|TR|TRRS|TRRRSS|IN|DES|RV|CO|MESAD|LE)_([0-9,.]+)_(CPM|CPMA|CPVI|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCO|CPCO)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([0-9,.-]+)?(_B-)?(_)?([0-9.,]+)?(_S-)?(_)?([0-9.,]+)?(\(([0-9.)])\))?(/[0-9].+)?'
            match = re.search(regex, Campaingname)
            if match != None:
                CampaingIDMFC = match.group(1)
            else:
                CampaingIDMFC = 0

            campana = [CampaingID, Campaingname, Campaignspendinglimit,
                       Campaigndailybudget, Campaignlifetimebudget, Campaignobjective,
                       Campaignstatus, AccountsID, StartDate,
                       EndDate, Campaingbuyingtype, Campaignbudgetremaining,
                       Percentofbudgetused, Cost, CampaingIDMFC, CreateDate]

        campanas.append(campana)
    sql.connect.insertCampanas(campanas, media, conn)


def metricas_campanas(df, media, conn):
    metricas = []
    # Obtener los datos del Spreasheet
    df = df
    for index, row in df.iterrows():

        if media == 'FB':
            if int(row['Campaign ID']) < 1:
                continue
            CampaingID = int(row['Campaign ID'])
            Cost = row['Cost']
            Frequency = row['Frequency']
            Reach = row['Reach']
            Postengagements = row['Post engagements']
            Impressions = int(row['Impressions'])
            Clicks = int(row['Link clicks'])
            Landingpageviews = int(row['Landing page views'])
            Videowachesat75 = int(row['Video watches at 75%'])
            ThruPlay = row['ThruPlay actions']
            Conversions = 0

        elif media == 'GO':
            if int(row['Campaign ID']) < 1:
                continue
            CampaingID = int(row['Campaign ID'])
            Cost = row['Cost']
            Frequency = 0
            Reach = 0
            Postengagements = 0
            Impressions = row['Impressions']
            Clicks = int(row['Clicks'])
            Landingpageviews = 0
            Videowachesat75 = int(row['Watch 75% views'])
            ThruPlay = int(row['Video views'])
            Conversions = int(row['Conversions'])

        if media == 'TW':

            CampaingID = row['Campaign ID']
            Cost = row['Cost']
            Frequency = 0
            Reach = 0
            Postengagements = 0
            Impressions = row['Impressions']
            Clicks = int(row['Clicks'])
            Landingpageviews = 0
            Videowachesat75 = int(row['Video views (75% complete)'])
            ThruPlay = int(row['Video views'])
            Conversions = int(row['Conversions'])

        metrica = [CampaingID, Cost, Frequency, Reach, Postengagements, Impressions,
                   Clicks, Landingpageviews, Videowachesat75, ThruPlay, Conversions, CreateDate]

        metricas.append(metrica)

    sql.connect.insertMetricasCampanas(metricas, media, conn)


def adsets(df, media, conn):
    adsets = []
    # Obtener los datos del Spreasheet
    df = df
    for index, row in df.iterrows():

        if media == 'FB':
            CampaingID = int(row['Ad set ID'])
            AdSetID = row['Ad set ID']
            Adsetname = row['Ad set name']
            Adsetlifetimebudget = row['Ad set lifetime budget']
            Adsetdailybudget = row['Ad set daily budget']
            Adsettargeting = row['Ad set targeting']
            Adsetend = row['Ad set end time']
            Adsetstart = row['Ad set start time']
            CampaingID = int(row['Campaign ID'])
            Status = row['Ad set status']
            Referer = 'Facebook'
            Media = media

        elif media == 'GO':
            CampaingID = int(row['Campaign ID'])
            AdSetID = row['Ad group ID']
            Adsetname = row['Ad group name']
            Adsetlifetimebudget = 0
            Adsetdailybudget = 0
            Adsettargeting = ''
            Adsetend = None
            Adsetstart = None
            CampaingID = int(row['Campaign ID'])
            Status = row['Ad group status']
            Referer = 'Google'
            Media = media

        elif media == 'TW':
            CampaingID = row['Campaign ID']
            AdSetID = CampaingID+row['Funding instrument ID']
            Adsetname = AdSetID
            Adsetlifetimebudget = 0
            Adsetdailybudget = 0
            Adsettargeting = ''
            Adsetend = None
            Adsetstart = None
            CampaingID = row['Campaign ID']
            Status = 'ACTIVE'
            Referer = 'Twitter'
            Media = media

        adset = [AdSetID, Adsetname, Adsetlifetimebudget, Adsetdailybudget, Adsettargeting,
                 Adsetend, Adsetstart, CampaingID, Status, CreateDate, Referer, Media]

        adsets.append(adset)

    sql.connect.insertAdsets(adsets, media, conn)


def ads(df, media, conn):
    ads = []
    # Obtener los datos del Spreasheet
    df = df
    for index, row in df.iterrows():

        if media == 'FB':
            AdID = int(row['Ad ID'])
            Adname = row['Ad name']
            Country = row['Country']
            Adstatus = row['Ad status']
            AdSetID = int(row['Ad set ID'])
            Referer = 'Facebook'
            Media = media

        elif media == 'GO':
            AdID = int(row['Ad ID'])
            Adname = int(row['Ad ID'])
            Country = ''
            Adstatus = row['Ad status']
            AdSetID = int(row['Ad group ID'])
            Referer = 'Google'
            Media = media

        elif media == 'TW':
            AdID = row['Line item ID']
            Adname = row['Line item']
            Country = ''
            Adstatus = 'ACTIVE'
            AdSetID = row['Campaign ID']+row['Funding instrument ID']
            Referer = 'Twitter'
            Media = media

        ad = [AdID, Adname, Country, Adstatus,
              AdSetID, CreateDate, Referer, Media]

        ads.append(ad)

    sql.connect.insertAds(ads, media, conn)


def metricas_adsets(df, media, conn):
    metricas = []
    # Obtener los datos del Spreasheet
    df = df
    for index, row in df.iterrows():

        if media == 'FB':
            AdSetID = row['Ad set ID']
            Cost = row['Cost']
            Frequency = row['Frequency']
            Reach = row['Reach']
            Postengagements = row['Post engagements']
            Impressions = int(row['Impressions'])
            Clicks = int(row['Link clicks'])
            Landingpageviews = int(row['Landing page views'])
            Videowachesat75 = int(row['Video watches at 75%'])
            ThruPlay = row['ThruPlay actions']
            Conversions = 0
            Country = row['Country']

        elif media == 'GO':

            AdSetID = row['Ad group ID']
            Cost = row['Cost']
            Frequency = 0
            Reach = 0
            Postengagements = 0
            Impressions = row['Impressions']
            Clicks = int(row['Clicks'])
            Landingpageviews = 0
            Videowachesat75 = int(row['Watch 75% views'])
            ThruPlay = int(row['Video views'])
            Conversions = int(row['Conversions'])
            Country = ''

        if media == 'TW':

            AdSetID = row['Campaign ID']+row['Funding instrument ID']
            Cost = row['Cost']
            Frequency = 0
            Reach = 0
            Postengagements = 0
            Impressions = row['Impressions']
            Clicks = int(row['Clicks'])
            Landingpageviews = 0
            Videowachesat75 = int(row['Video views (75% complete)'])
            ThruPlay = int(row['Video views'])
            Conversions = int(row['Conversions'])
            Country = ''

        metrica = [AdSetID, Cost, Frequency, Reach, Postengagements, Impressions, Clicks,
                   Landingpageviews, Videowachesat75, ThruPlay, Conversions, Country, CreateDate]

        metricas.append(metrica)

    sql.connect.insertMetricasAdSet(metricas, media, conn)


def metricas_ads(df, media, conn):
    metricas = []
    # Obtener los datos del Spreasheet
    df = df
    for index, row in df.iterrows():

        if media == 'FB':
            AdID = int(row['Ad ID'])
            Cost = row['Cost']
            Frequency = row['Frequency']
            Reach = row['Reach']
            Postengagements = row['Post engagements']
            Impressions = int(row['Impressions'])
            Clicks = int(row['Link clicks'])
            Landingpageviews = int(row['Landing page views'])
            Videowachesat75 = int(row['Video watches at 75%'])
            ThruPlay = row['ThruPlay actions']
            Conversions = 0
            Country = row['Country']

        elif media == 'GO':

            AdID = int(row['Ad ID'])
            Cost = row['Cost']
            Frequency = 0
            Reach = 0
            Postengagements = 0
            Impressions = row['Impressions']
            Clicks = int(row['Clicks'])
            Landingpageviews = 0
            Videowachesat75 = int(row['Watch 75% views'])
            ThruPlay = int(row['Video views'])
            Conversions = int(row['Conversions'])
            Country = ''

        if media == 'TW':

            AdID = row['Line item ID']
            Cost = row['Cost']
            Frequency = 0
            Reach = 0
            Postengagements = 0
            Impressions = row['Impressions']
            Clicks = int(row['Clicks'])
            Landingpageviews = 0
            Videowachesat75 = int(row['Video views (75% complete)'])
            ThruPlay = int(row['Video views'])
            Conversions = int(row['Conversions'])
            Country = ''

        metrica = [AdID, Cost, Frequency, Reach, Postengagements, Impressions, Clicks,
                   Landingpageviews, Videowachesat75, ThruPlay, Conversions, Country, CreateDate]

        metricas.append(metrica)

    sql.connect.insertMetricasAd(metricas, media, conn)


def creative_ads(df, media, conn):
    creatives = []
    # Obtener los datos del Spreasheet
    df = df
    for index, row in df.iterrows():
        if media == 'FB':
            AdcreativeID = row['Ad creative ID']
            Creativename = row['Creative title']
            Linktopromotedpost = ''
            AdcreativethumbnailURL = row['Ad creative thumbnail URL']
            AdcreativeimageURL = row['Ad creative image URL']
            ExternaldestinationURL = ''
            Adcreativeobjecttype = row['Ad creative object type']
            PromotedpostID = ''
            Promotedpostname = ''
            PromotedpostInstagramID = ''
            Promotedpostmessage = ''
            Promotedpostcaption = ''
            PromotedpostdestinationURL = ''
            PromotedpostimageURL = ''
            LinktopromotedInstagrampost = row['Link to promoted Instagram post']
            AdID = int(row['Ad ID'])
            Adname = row['Ad name']
            Country = row['Country']

        creative = [AdcreativeID, Creativename, Linktopromotedpost, AdcreativethumbnailURL, AdcreativeimageURL, ExternaldestinationURL, Adcreativeobjecttype, PromotedpostID, Promotedpostname,
                    PromotedpostInstagramID, Promotedpostmessage, Promotedpostcaption, PromotedpostdestinationURL, PromotedpostimageURL, LinktopromotedInstagrampost, AdID, Adname, Country, CreateDate]

        creatives.append(creative)

    sql.connect.insertCreativesAd(creatives, media, conn)


def diario_campanas(df, media, conn):
    metricas = []
    historico = []
    Campaingname = ''
    result = 0
    Objetive = ''
    Percentofbudgetused = 0
    # Obtener los datos del Spreasheet
    df = df
    for index, row in df.iterrows():
        CampaignIDMFC = 0
        if media == 'FB':
            if int(row['Campaign ID']) < 1:
                continue
            CampaingID = int(row['Campaign ID'])
            Campaingname = row['Campaign name']
            Campaigndailybudget = row['Campaign daily budget']
            Campaignlifetimebudget = row['Campaign lifetime budget']
            Percentofbudgetused = 0
            StartDate = row['Campaign start date']
            EndDate = row['Campaign end date']
            Cost = row['Cost']
            Frequency = row['Frequency']
            Reach = row['Reach']
            Postengagements = row['Post engagements']
            Impressions = int(row['Impressions'])
            Clicks = int(row['Link clicks'])
            Landingpageviews = int(row['Landing page views'])
            Videowachesat75 = int(row['Video watches at 75%'])
            ThruPlay = row['ThruPlay actions']
            Conversions = 0

        elif media == 'GO':
            if int(row['Campaign ID']) < 1:
                continue
            CampaingID = int(row['Campaign ID'])
            Campaingname = row['Campaign name']
            Campaignspendinglimit = 0
            Campaigndailybudget = row['Daily budget']
            Campaignlifetimebudget = row['Budget']
            Percentofbudgetused = row['Percent of budget used']
            StartDate = row['Start date']
            EndDate = row['End date']
            if EndDate == '' or EndDate == ' --' or EndDate == 0:
                EndDate = '2019-01-01'
            Cost = row['Cost']
            Frequency = 0
            Reach = 0
            Postengagements = 0
            Impressions = row['Impressions']
            Clicks = int(row['Clicks'])
            Landingpageviews = 0
            Videowachesat75 = int(row['Watch 75% views'])
            ThruPlay = int(row['Video views'])
            Conversions = int(row['Conversions'])

        if media == 'TW':

            if row['Campaign ID'] == '':
                continue
            CampaingID = row['Campaign ID']
            Campaingname = row['Campaign']
            Campaignspendinglimit = 0
            Campaigndailybudget = row['Campaign daily budget']
            Campaignlifetimebudget = row['Campaign total budget']
            StartDate = row['Campaign start time']
            EndDate = row['Campaign end time']
            Cost = row['Cost']
            Frequency = 0
            Reach = 0
            Postengagements = 0
            Impressions = row['Impressions']
            Clicks = int(row['Clicks'])
            Landingpageviews = 0
            Videowachesat75 = int(row['Video views (75% complete)'])
            ThruPlay = int(row['Video views'])
            Conversions = int(row['Conversions'])

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
                            Clicks,  Landingpageviews,
                            Videowachesat75, ThruPlay, Conversions, CreateDate]

                historico.append(historia)

        metrica = [CampaingID, Campaingname, Campaigndailybudget,
                   Campaignlifetimebudget, Percentofbudgetused,
                   StartDate, EndDate, result, Objetive, CampaignIDMFC,
                   Cost, Frequency,
                   Reach, Postengagements, Impressions,
                   Clicks,  Landingpageviews,
                   Videowachesat75, ThruPlay, Conversions, CreateDate]

        metricas.append(metrica)

    sql.connect.insertDiarioCampanas(metricas, media, conn)
    sql.connect.insertHistoric(historico, media, conn)


def actualizarestado(df, media, conn):
    campanas = []
    Campaingname = ''
    # Obtener los datos del Spreasheet
    df = df
    for index, row in df.iterrows():
        # Metricas y dimenciones FB
        if media == 'FB':
            if int(row['Campaign ID']) < 1:
                continue
            CampaingID = int(row['Campaign ID'])
            Campaignstatus = row['Campaign status']
        # Metricas y dimenciones GO
        elif media == 'GO':
            if int(row['Campaign ID']) < 1:
                continue
            EndDate = None
            CampaingID = row['Campaign ID']
            Campaignstatus = row['Campaign status']

        elif media == 'TW':
            if row['Campaign ID'] == '':
                continue
            CampaingID = row['Campaign ID']
            Campaignstatus = row['Campaign servable']
            if Campaignstatus == True:
                Campaignstatus = 'Active'
            else:
                Campaignstatus = 'Disabled'
        campana = [CampaingID, Campaignstatus]
        campanas.append(campana)
    sql.connect.ActualizarEstado(campanas, media, conn)


def extrametrics(df, media, conn):
    campanas = []
    # Obtener los datos del Spreasheet
    df = df
    for index, row in df.iterrows():
        # Metricas y dimenciones FB
        if int(row['Campaign ID']) < 1:
            continue
        CampaingID = int(row['Campaign ID'])
        Estimatedadrecalllift = int(row['Estimated ad recall lift (people)'])
        # Metricas y dimenciones GO
        campana = [CampaingID, Estimatedadrecalllift, CreateDate]
        campanas.append(campana)
    sql.connect.insertExtraMetrics(campanas, media, conn)

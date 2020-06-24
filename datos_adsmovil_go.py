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
CreateDate = "2020-05-31"
# CONEXION A SPREADSHEETS


def Spreadsheet(Spreadsheet, hoja):
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


def cuentas(df, conn):
    # Obtener los datos del Spreasheet
    cuentas = []
    df = df

    for index, row in df.iterrows():
        media = row['Plataforma']

        cuenta = [row['Cuenta'], row['Cuenta'], media, CreateDate]
        cuentas.append(cuenta)
    sql.connect.insertCuentas(cuentas, media, conn)


def campanas(df, conn):
    campanas = []
    Campaingname = ''
    # Obtener los datos del Spreasheet
    df = df
    for index, row in df.iterrows():
        # Metricas y dimenciones FB
        CampaingID = int(row['ID Campana'])
        Campaingname = row['Nomenclatura']
        Campaignspendinglimit = 0
        Campaigndailybudget = 0
        Campaignlifetimebudget = row['Inversion']
        Campaignobjective = ''
        Campaignstatus = ''
        AccountsID = str(row['Cuenta'])
        StartDate = row['Fecha inicio']
        EndDate = row['Fecha Fin']
        Campaingbuyingtype = 0
        Campaignbudgetremaining = 0
        Percentofbudgetused = 0
        Cost = row['Costo']

        if Campaingname != 0:
            regex = '([0-9,.]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(2019|19|20|2020)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([0-9, .]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([0-9., ]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([0-9, .-]+)?(_B-)?(_)?([0-9., ]+)?(_S-)?(_)?([0-9., ]+)?(\(([0-9.)])\))?(/[0-9].+)?'
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
    sql.connect.insertCampanas(campanas, 'Extras', conn)


def metricas_campanas(df, conn):
    metricas = []
    # Obtener los datos del Spreasheet
    df = df
    for index, row in df.iterrows():
        Result = 0
        result = 0
        Objetive = ''
        objcon = ''
        CampaignIDMFC = 0
        CampaingID = row['ID Campana']
        Campaingname = row['Nombre Campana']
        Cost = row['Costo']
        Frequency = 0
        Reach = 0
        Postengagements = 0
        Impressions = row['Impresiones']
        Clicks = row['Clicks']
        Landingpageviews = 0
        Videowachesat75 = row['Reproducciones']
        ThruPlay = row['Reproducciones']
        Conversions = row['Conversiones']
        if Campaingname != 0 or Campaingname != '':
            regex = '([0-9,.]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(2019|19|20|2020)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([0-9, .]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([0-9., ]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([0-9, .-]+)?(_B-)?(_)?([0-9., ]+)?(_S-)?(_)?([0-9., ]+)?(\(([0-9.)])\))?(/[0-9].+)?'
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
                        result = Conversions
                        Objetive = 'MESAD'
                    elif str(objcon).upper() == 'LE':
                        result = Conversions
                        Objetive = 'LE'
                    else:
                        result = Clicks
                        Objetive = 'CPCO'
                elif str(Result).upper() == 'CPI':
                    result = Conversions
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

        metrica = [CampaingID, Cost, Frequency,
                   Reach, Postengagements, Impressions, Clicks, Landingpageviews,
                   Videowachesat75, ThruPlay, Conversions, result, Objetive, CampaignIDMFC, CreateDate]

        metricas.append(metrica)

    sql.connect.insertMetricasCampanas(metricas, 'Extras', conn)


def diario_campanas(df, conn):
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
        CampaingID = row['ID Campana']
        Campaingname = row['Nombre Campana']
        Cost = row['Costo']
        Frequency = 0
        Reach = 0
        Postengagements = 0
        Impressions = row['Impresiones']
        Clicks = row['Clicks']
        Landingpageviews = 0
        Videowachesat75 = row['Reproducciones']
        ThruPlay = 0
        Conversions = row['Conversiones']
        StartDate = row['Fecha inicio']
        EndDate = row['Fecha Fin']
        Campaignlifetimebudget = row['Inversion']
        Campaigndailybudget = 0

        if Campaingname != 0 or Campaingname != '':
            regex = '([0-9,.]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(2019|19|20|2020)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([0-9, .]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([0-9., ]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([0-9, .-]+)?(_B-)?(_)?([0-9., ]+)?(_S-)?(_)?([0-9., ]+)?(\(([0-9.)])\))?(/[0-9].+)?'
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
                        result = Conversions
                        Objetive = 'MESAD'
                    elif str(objcon).upper() == 'LE':
                        result = Conversions
                        Objetive = 'LE'
                    else:
                        result = Clicks
                        Objetive = 'CPCO'
                elif str(Result).upper() == 'CPI':
                    result = Conversions
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

    sql.connect.insertDiarioCampanas(metricas, 'Extras', conn)
    sql.connect.insertHistoric(historico, 'Extras', conn)

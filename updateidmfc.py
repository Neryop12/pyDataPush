import config.db as db
import dbconnect as sql
import sys
import re
import mysql.connector as mysql
from datetime import datetime, timedelta
import time
import configparser
import pandas as pd
import numpy as np

now = datetime.now()
CreateDate = now.strftime("%Y-%m-%d %H:%M:%S")


def delete_ads_metrics(conn):
    cur = conn.cursor()
    query = """Select a.Campaingname,a.CampaingID,c.Media from  Campaings a
                inner join CampaingMetrics b on a.CampaingID=b.CampaingID
                inner join Accounts c on c.AccountsID=a.AccountsID
                where a.Campaingname LIKE "%_ADS_%" and b.CreateDate<"2020-06-01" and c.Media='AM'"""
    query2 = """Delete from CampaingMetrics  where CampaingID = %s """
    try:
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.execute(query, )
        resultscon = cur.fetchall()
        Errores = []
        campanas = []
        for row in resultscon:
            print(row[1])
            campana = [row[1]]
            campanas.append(campana)
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(query2, campanas)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        cur.close()

        cur.close()
    except Exception as e:
        print(e)


def results_metrics(conn):
    cur = conn.cursor()
    query = """Select a.Campaingname, a.CampaingID, b.Reach, b.Frequency, b.Clicks, b.Impressions, b.Postengagements, b.Estimatedadrecalllift, b.Landingpageviews, b.Videowachesat75, b.'Fecha fin', b.Conversions from Campaings a, CampaingMetrics b where a.CampaingID = b.CampaingID  """
    query2 = """Update CampaingMetrics set Result = %s, Objetive = %s, CampaignIDMFC=%s where CampaingID = %s """
    try:
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.execute(query, )
        resultscon = cur.fetchall()
        Errores = []
        campanas = []
        for row in resultscon:
            CampaignIDMFC = 0
            Campaingname = row[0]
            Result = 0
            objcon = ''
            Objetive = ''
            result = 0
            Reach = row[2]
            Frequency = row[3]
            Clicks = row[4]
            Impressions = row[5]
            Videowachesat75 = row[9]
            Conversions = row[11]
            regex = '([0-9,.]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(2019|19|20|2020)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([0-9, .]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([0-9., ]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([0-9, .-]+)?(_B-)?(_)?([0-9., ]+)?(_S-)?(_)?([0-9., ]+)?(\(([0-9.)])\))?(/[0-9].+)?'
            match = re.search(regex, row[0])
            if match != None:
                CampaignIDMFC = (match.group(1))
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
                campana = [result, Objetive, CampaignIDMFC, row[1]]
                campanas.append(campana)
                print(campana)
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(query2, campanas)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        cur.close()
    except Exception as e:
        print(e)


def results_hismetrics(conn):
    cur = conn.cursor()
    query = """Select a.Campaingname, a.CampaingID, b.Reach, b.Frequency, b.Clicks, b.Impressions, b.Postengagements, b.Estimatedadrecalllift, b.Landingpageviews, b.Videowatches75, b.'Fecha fin', b.Conversions from Campaings a, HistoricCampaings b where a.CampaingID = b.CampaingID and Result = 0"""
    query2 = """Update HistoricCampaings set Result = %s, Objetive = %s where CampaingID = %s """
    try:
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.execute(query, )
        resultscon = cur.fetchall()
        Errores = []
        campanas = []
        for row in resultscon:
            Campaingname = row[0]
            Result = 0
            objcon = ''
            Objetive = ''
            result = 0
            Reach = row[2]
            Frequency = row[3]
            Clicks = row[4]
            Impressions = row[5]
            Videowachesat75 = row[9]
            Conversions = row[11]
            regex = '([0-9,.]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(2019|19|20|2020)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([0-9, .]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([0-9., ]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([0-9, .-]+)?(_B-)?(_)?([0-9., ]+)?(_S-)?(_)?([0-9., ]+)?(\(([0-9.)])\))?(/[0-9].+)?'
            match = re.search(regex, row[0])
            if match != None:
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
            campana = [result, Objetive, row[1]]
            campanas.append(campana)
            print(campana)
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(query2, campanas)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        cur.close()
    except Exception as e:
        print(e)


def update_metrics(conn):
    cur = conn.cursor()
    query = """Select a.Campaingname, a.CampaingID from Accounts b, Campaings a where a.AccountsID = b.AccountsID and a.CampaingIDMFC < 1"""
    query2 = """Update Campaings set CampaingIDMFC = %s where CampaingID = %s """
    try:
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.execute(query, )
        resultscon = cur.fetchall()
        Errores = []
        campanas = []
        for row in resultscon:
            CampaignIDMFC = 0
            regex = '([0-9,.]+)_.*'
            match = re.search(regex, row[0])
            if match != None:
                CampaignIDMFC = match.group(1)
            campana = [CampaignIDMFC, row[1]]
            campanas.append(campana)
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(query2, campanas)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        cur.close()
    except Exception as e:
        print(e)


def update_historic(conn):

    cur = conn.cursor()
    query = """Select a.Campaingname, a.CampaingID HistoricCampaings a where  a.CampaingIDMFC < 1"""
    query2 = """Update HistoricCampaings set CampaingIDMFC = %s where CampaingID = %s """
    try:
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.execute(query, )
        resultscon = cur.fetchall()
        Errores = []
        campanas = []
        for row in resultscon:
            CampaignIDMFC = 0
            regex = '([0-9,.]+)_.*'
            match = re.search(regex, row[0])
            if match != None:
                CampaignIDMFC = match.group(1)
            campana = [CampaignIDMFC, row[1]]
            campanas.append(campana)
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(query2, campanas)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        cur.close()
    except Exception as e:
        print(e)


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


def update_cammetrics(df, conn):
    cur = conn.cursor()
    cuentas = []
    Campanas = []
    Metricas = []
    df = df
    query = """Update Campaings set Campaignlifetimebudget=%s,Cost=%s where CampaingIDMFC= %s """
    query2 = """Update CampaingMetrics set
                Cost = %s,
                Reach=%s,
                Clicks=%s,
                Impressions=%s,
                Postengagements=%s,
                ThruPlay=%s,
                Conversions=%s,
                Result=%s
                where CampaignIDMFC = %s """
    for index, row in df.iterrows():
        CampaingIDMFC = 0
        Campaingname = row['Nomenclatura']
        Inversion = row['Inversion']
        Costo = row['Costo']
        Alcance = row['Alcance']
        Clicks = row['Clicks']
        Impresiones = row['Impresiones']
        Interacciones = row['Interacciones']
        Reproducciones = row['Reproducciones']
        Conversiones = row['Conversiones']
        if row['Descargas'] > 0:
            Conversiones = row['Descargas']
        if Campaingname != 0:
            regex = '([0-9,.]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(2019|19|20|2020)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([0-9, .]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([0-9., ]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([0-9, .-]+)?(_B-)?(_)?([0-9., ]+)?(_S-)?(_)?([0-9., ]+)?(\(([0-9.)])\))?(/[0-9].+)?'
            match = re.search(regex, Campaingname)
            if match != None:
                CampaingIDMFC = match.group(1)
                Result = (match.group(15))
                objcon = (match.group(13))
                Objetive = ''
                result = 0
                if str(Result).upper() == 'CPVI':
                    result = Clicks
                    costo_KPI = Cost / Clicks
                    Objetive = 'CPVI'
                elif str(Result).upper() == 'CPMA':
                    result = Alcance
                    costo_KPI = Cost / (Alcance * 1000)
                    Objetive = 'CPMA'
                elif str(Result).upper() == 'CPM':
                    result = Impresiones
                    costo_KPI = Cost / (Impresiones * 1000)
                    Objetive = 'CPM'
                elif str(Result).upper() == 'CPV':
                    result = Reproducciones
                    costo_KPI = Cost/Reproducciones
                    Objetive = 'CPV'
                elif str(Result).upper() == 'CPCO':
                    if str(objcon).upper() == 'MESAD':
                        result = Conversiones
                        costo_KPI = Cost/Conversiones
                        Objetive = 'MESAD'
                    elif str(objcon).upper() == 'LE':
                        result = Conversiones
                        costo_KPI = Cost/Conversiones
                        Objetive = 'LE'
                    else:
                        result = Clicks
                        costo_KPI = Cost / Clicks
                        Objetive = 'CPCO'
                elif str(Result).upper() == 'CPI':
                    result = Interacciones
                    costo_KPI = Cost/Interacciones
                    Objetive = 'CPI'
                elif str(Result).upper() == 'CPC':
                    if (str(objcon).upper() == 'BA' or str(objcon).upper() == 'TR') and media == 'FB':
                        result = Interacciones
                        costo_KPI = Cost/Interacciones
                        Objetive = 'CPC'
                    else:
                        result = Clicks
                        costo_KPI = Cost/Clicks
                        Objetive = 'CPC'
                elif str(Result).upper() == 'CPD':
                    result = Conversiones
                    costo_KPI = Cost / Conversiones
                    Objetive = 'CPD'
            else:
                CampaingIDMFC = 0
        Metrica = (Costo,  Alcance, Clicks, Impresiones, Interacciones,
                   Reproducciones, Conversiones, result, CampaingIDMFC)
        Campana = (Inversion, Costo, CampaingIDMFC)
        Campanas.append(Campana)
        Metricas.append(Metrica)
    cur.execute("SET FOREIGN_KEY_CHECKS=0")
    cur.executemany(query, Campanas)
    cur.executemany(query2, Metricas)
    cur.execute("SET FOREIGN_KEY_CHECKS=1")
    cur.close()

def update_cammetrics_kpi(conn):
    cur = conn.cursor()
    Metricas = []
    #query = """Update Campaings set Campaignlifetimebudget=%s,Cost=%s where CampaingIDMFC= %s """
    query = """select Campaingname Nomenclatura,CAMP.Cost, METRICS.Reach Alcance, METRICS.Clicks, METRICS.Impressions Impresiones, METRICS.Postengagements Interacciones,
		        METRICS.Videowachesat75 Reproducciones, METRICS.Conversions Conversiones, ACC.Media, METRICS.id
				from Campaings CAMP
                INNER JOIN CampaingMetrics METRICS on METRICS.CampaignIDMFC = CAMP.CampaingIDMFC
                Inner JOIN Accounts ACC on ACC.AccountsID = CAMP.AccountsID
                WHERE Result <=0
                GROUP BY CAMP.CampaingID;
                """
    query2 = """Update CampaingMetrics set
                Result=%s
                Objetive=%s
                KPICost=%s
                where Id = %s """

    try:
        result = 0
        Objetive = ''
        costo_KPI = 0
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.execute(query, )
        resultscon = cur.fetchall()
        CampaingIDMFC = 0
        for row in resultscon:

            regex = '([0-9,.]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(2019|19|20|2020)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([0-9, .]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([0-9., ]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([0-9, .-]+)?(_B-)?(_)?([0-9., ]+)?(_S-)?(_)?([0-9., ]+)?(\(([0-9.)])\))?(/[0-9].+)?'
            match = re.search(regex, str(row[0]))
            if match != None:
                Campaingname = str(row[0])
                Cost = float(row[1])
                Alcance = int(row[2])
                Clicks = int(row[3])
                Impresiones = int(row[4])
                Interacciones = int(row[5])
                Reproducciones = int(row[6])
                Conversiones = int(row[7])
                media = str(row[8])
                CampaingIDMFC = int(row[10])
                Result = (match.group(15))
                objcon = (match.group(13))
                costo_KPI = 0
                if str(Result).upper() == 'CPVI':
                    result = Clicks
                    costo_KPI = Cost / Clicks
                    Objetive = 'CPVI'
                elif str(Result).upper() == 'CPMA':
                    result = Alcance
                    costo_KPI = Cost / (Alcance * 1000)
                    Objetive = 'CPMA'
                elif str(Result).upper() == 'CPM':
                    result = Impresiones
                    costo_KPI = Cost / (Impresiones * 1000)
                    Objetive = 'CPM'
                elif str(Result).upper() == 'CPV':
                    result = Reproducciones
                    costo_KPI = Cost/Reproducciones
                    Objetive = 'CPV'
                elif str(Result).upper() == 'CPCO':
                    if str(objcon).upper() == 'MESAD':
                        result = Conversiones
                        costo_KPI = Cost/Conversiones
                        Objetive = 'MESAD'
                    elif str(objcon).upper() == 'LE':
                        result = Conversiones
                        costo_KPI = Cost/Conversiones
                        Objetive = 'LE'
                    else:
                        result = Clicks
                        costo_KPI = Cost / Clicks
                        Objetive = 'CPCO'
                elif str(Result).upper() == 'CPI':
                    result = Interacciones
                    costo_KPI = Cost/Interacciones
                    Objetive = 'CPI'
                elif str(Result).upper() == 'CPC':
                    if (str(objcon).upper() == 'BA' or str(objcon).upper() == 'TR') and media == 'FB':
                        result = Interacciones
                        costo_KPI = Cost/Interacciones
                        Objetive = 'CPC'
                    else:
                        result = Clicks
                        costo_KPI = Cost/Clicks
                        Objetive = 'CPC'
                elif str(Result).upper() == 'CPD':
                    result = Conversiones
                    costo_KPI = Cost / Conversiones
                    Objetive = 'CPD'
                Metrica = (result, Objetive, costo_KPI,CampaingIDMFC)
                Metricas.append(Metrica)
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(query2, Metricas)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        cur.close()
    except Exception as e:
        print(e)




if __name__ == '__main__':

    # Iniciamos la conexion
    conn = sql.connect.open(db.DB['host'], db.DB['user'], db.DB['password'],
                            db.DB['dbname'], db.DB['port'], db.DB['autocommit'])

    try:
        #dfni = Spreadsheet(db.CLARO['key'],  db.CLARO['SV'])

        #update_cammetrics(dfni, conn)
        # mediosextras.diario_campanas(dfni, conn)
        update_cammetrics_kpi(conn)
        print('termino todo')
    except Exception as e:
        print('Error on line {}'.format(
            sys.exc_info()[-1].tb_lineno), type(e).__name__, e)

    sql.connect.close(conn)

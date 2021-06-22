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
CreateDate = "2020-05-30"
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


def Campanas(media, conn):
    try:

        url = 'https://reportapi.adsmovil.com/api/campaign/details'
        Result2 = requests.get(
            url,

            headers={
                'Authorization': "'" + Token["result"]["token"] + "'",
            },
            params={
                'report': 'adsmovil_dsp',
                'startDate': "2020-01-01",
                'endDate': "2020-05-31",
            }
        )
        r = Result2.json()
        for row in r["result"]["queryResponseData"]["rows"]:
            for n, i in enumerate(row):
                if i == 'NaN':
                    row[n] = 0
                result = 0
                CampaignIDMFC = 0
                Campaigndailybudget = 0
                AccountID = 0
                Account = 0
                CampaingID = row[1]
                Campaignspendinglimit = 0
                Campaignobjective = ''
                Campaingbuyingtype = ''
                Campaignbudgetremaining = 0
                Campaignlifetimebudget = 0
                Percentofbudgetused = 0
                Campaignstatus = 'ACTIVE'
                StartDate = None
                EndDate = None
                CampaignIDMFC = 0
                Cost = row[11]
                Frequency = 0
                Reach = 0
                Postengagements = 0
                Impressions = 0
                Clicks = row[5]
                Landingpageviews = 0
                Videowachesat75 = row[10]
                ThruPlay = 0
                Conversions = 0
                Objetive = ''
                result = 0
                Campaingname = row[2].split('(')
                Campaingname = Campaingname[0].replace(' ', '')
                regex = '([0-9,.]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(2019|19|20|2020|2021|21)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([0-9, .]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([0-9., ]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([0-9, .-]+)?(_B-)?(_)?([0-9., ]+)?(_S-)?(_)?([0-9., ]+)?(\(([0-9.)])\))?(/[0-9].+)?'
                searchObj = re.search(regex, Campaingname)
                if searchObj != None:
                    CampaignIDMFC = searchObj.group(1)
                    print(CampaignIDMFC)
                    if searchObj.group(3) == 'CLARO':
                        AccountID = searchObj.group(
                            2)+searchObj.group(3)
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

                campana = [CampaingID, Campaingname, Campaignspendinglimit,
                           Campaigndailybudget, Campaignlifetimebudget, Campaignobjective,
                           Campaignstatus, AccountID, StartDate,
                           EndDate, Campaingbuyingtype, Campaignbudgetremaining,
                           Percentofbudgetused, Cost, CampaingIDMFC, CreateDate]
                cuenta = [AccountID, AccountID, media, CreateDate]
                metrica = [CampaingID, Cost, Frequency, Reach, Postengagements, Impressions, Clicks,
                           Landingpageviews, Videowachesat75, ThruPlay, Conversions, result, Objetive, CampaignIDMFC, CreateDate]
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
    except Exception as e:
        print(e)

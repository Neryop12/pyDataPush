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
media = 'AF'
metricasads = []
metricasadsets = []
historico = []
diarios = []


def GetAouth():
    global Token
    url = 'https://id.adform.com/sts/connect/token'
    Token = requests.post(
        url,
        headers={
            'Content-Type': 'application/x-www-form-urlencoded',
        },
        data={
            "client_id": "reporting.jdeleon.gt.es@clients.adform.com",
            "client_secret": "vUkmHq-G6q6tBW_mDRaaf886hOyaYz3Ik1yfYg8u",
            "grant_type": "client_credentials",
            "scope": "https://api.adform.com/scope/buyer.campaigns.api",
        }


    )
    Token = Token.json()


def GetToken():
    global Token
    url = 'https://api.adform.com/Services/Security/Login'
    Token = requests.post(
        url,
        headers={
            'Content-Type': 'application/json',
        },
        json={
            "UserName": db.ADF['username'],
            "Password": db.ADF['password']
        }


    )
    Token = Token.json()


def CuentasCampanas(conn):
    try:
        url = 'https://api.adform.com/v1/reportingstats/agency/reportdata'
        data = requests.post(
            url,
            # Headers: se coloca el token de autorizacion
            headers={
                'Authorization': 'Bearer ' + Token['Ticket'],
                'Content-Type': 'application/json'
            },
            # Para obtener los datos se realiza un POST con los datos de dimension, metricas y filtros, tiene que tener al menos uno de cada uno
            # Para enviarlo se tiene que guardar en formato Json.
            json={
                "dimensions": [
                    "campaignID",
                    "campaign",
                    "clientID",
                    "client",
                    "campaignStartDate",
                    "campaignEndDate",

                    "campaignType",
                    "bannerType"

                ],
                "metrics": [
                    "clicks",
                    "impressions",
                    "cost",
                    "conversions",
                    "sales"
                ],
                "filter": {
                    "date": {
                        "from":  str(datetime.now() - timedelta(days=1)),
                        "to": str(datetime.now() - timedelta(days=1))
                    }
                },
                "paging": {
                    "offset": 0,
                    "limit": 3000
                }
            }
        )
        data = data.json()
        for row in data['reportData']['rows']:
            if(row[0] != ''):
                AccountID = row[2]
                Account = row[3]
                Campaignstatus = ''
                CampaingID = row[0]
                Campaingname = row[1]
                Campaingbuyingtype = 0
                Campaignbudgetremaining = 0
                Campaignspendinglimit = 0
                result = 0
                Campaigndailybudget = 0
                Percentofbudgetused = 0
                CampaignIDMFC = 0
                Frequency = 0
                Reach = 0
                Postengagements = 0
                Landingpageview = 0
                Videowachesat75 = 0
                ThruPlay = 0
                Landingpageviews = 0
                Objetive = ''
                Campaignlifetimebudget = 0.00
                StartDate = row[4]
                EndDate = row[5]
                Cost = row[10]
                Conversions = row[11]
                Clicks = row[8]
                Impressions = row[9]
                Campaignobjective = ''
                if EndDate > "2020-01-01":
                    Campaignstatus == 'Active'
                else:
                    Campaignstatus == 'disabled'
                if row[1] != 0 or row[1] != '':
                    regex = '([0-9,.]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(2019|19|20|2020|2021|21)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([0-9, .]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([0-9., ]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([ a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.%+&!"#$%&()*+,/=@-]+)_([0-9, .-]+)?(_B-)?(_)?([0-9., ]+)?(_S-)?(_)?([0-9., ]+)?(\(([0-9.)])\))?(/[0-9].+)?'
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

                campana = [CampaingID, Campaingname, Campaignspendinglimit,
                           Campaigndailybudget, Campaignlifetimebudget, Campaignobjective,
                           Campaignstatus, AccountID, StartDate,
                           EndDate, Campaingbuyingtype, Campaignbudgetremaining,
                           Percentofbudgetused, Cost, CampaignIDMFC, CreateDate]

                # Se verifica si Frequency es numerico

                cuenta = [AccountID, Account, media, CreateDate]
                diario = [CampaingID, Campaingname, Campaigndailybudget,
                          Campaignlifetimebudget, Percentofbudgetused,
                          StartDate, EndDate, result, Objetive, CampaignIDMFC,
                          Cost, Frequency,
                          Reach, Postengagements, Impressions,
                          Clicks,  Landingpageviews,
                          Videowachesat75, ThruPlay, Conversions, CreateDate]

                campanas.append(campana)
                cuentas.append(cuenta)
                diarios.append(diario)

        sql.connect.insertCuentas(cuentas, media, conn)
        sql.connect.insertCampanas(campanas, media, conn)
        sql.connect.insertDiarioCampanas(diarios, media, conn)
        sql.connect.insertHistoric(historico, media, conn)

    except Exception as e:
        print(e)


def Adsets(conn):
    try:
        url = 'https://api.adform.com/v1/reportingstats/agency/reportdata'
        data = requests.post(
            url,
            # Headers: se coloca el token de autorizacion
            headers={
                'Authorization': 'Bearer ' + Token['Ticket'],
                'Content-Type': 'application/json'
            },
            # Para obtener los datos se realiza un POST con los datos de dimension, metricas y filtros, tiene que tener al menos uno de cada uno
            # Para enviarlo se tiene que guardar en formato Json.
            json={
                "dimensions": [
                    "campaignID",
                    "lineItemID",
                    "lineItem",
                    "lineItemStartDate",
                    "lineItemEndDate",
                    "frequencyLineItem",
                    "media"
                ],
                "metrics": [
                    "clicks",
                    "impressions",
                    "cost"
                ],
                "filter": {
                    "date": {
                        "from": str(datetime.now() - timedelta(days=1)),
                        "to": str(datetime.now() - timedelta(days=1))
                    }
                }
            }
        )
        data = data.json()
        for row in data['reportData']['rows']:
            if(row[0] > 0):
                Status = ''
                Frequency = 0
                AdSetID = row[1]
                Adsetname = row[2]
                CampaingID = row[0]
                Adsetlifetimebudget = 0
                Adsettargeting = ''
                Adsetdailybudget = 0
                Cost = row[8]
                Reach = 0
                Postengagements = 0
                Impressions = row[8]
                Clicks = row[7]
                Landingpageviews = 0
                Videowachesat75 = 0
                ThruPlay = 0
                Conversions = 0
                Adsetend = row[4]
                Adsetstart = row[3]
                Country = ''
                Media = row[6]
                Referer = media
                if Adsetend > "2020-01-01":
                    Status = 'Active'
                else:
                    Status = 'disabled'
                adset = [AdSetID, Adsetname, Adsetlifetimebudget, Adsetdailybudget,
                         Adsettargeting, Adsetend, Adsetstart, CampaingID, Status, CreateDate, Referer, Media]

                adsets.append(adset)

        sql.connect.insertAdsets(adsets, media, conn)

    except Exception as e:
        print(e)


def Ads(conn):
    try:
        url = 'https://api.adform.com/v1/reportingstats/agency/reportdata'
        data = requests.post(
            url,
            # Headers: se coloca el token de autorizacion
            headers={
                'Authorization': 'Bearer ' + Token['Ticket'],
                'Content-Type': 'application/json'
            },
            # Para obtener los datos se realiza un POST con los datos de dimension, metricas y filtros, tiene que tener al menos uno de cada uno
            # Para enviarlo se tiene que guardar en formato Json.
            json={
                "dimensions": [
                    "lineItemID",
                    "bannerID",
                    "banner",
                    "referrerType",
                    "media"
                ],
                "metrics": [
                    "clicks",
                    "impressions",
                    "cost",
                    "ctr",
                    "ecpm",
                    "conversions"
                ],
                "filter": {
                    "date": {
                        "from": str(datetime.now() - timedelta(days=1)),
                        "to": str(datetime.now() - timedelta(days=1))
                    }
                }
            }
        )
        data = data.json()
        for row in data['reportData']['rows']:
            if(row[0] > 0):
                AdSetID = row[0]
                Adname = row[2]
                AdID = row[1]
                Adsetlifetimebudget = 0
                Adsettargeting = ''
                Adsetdailybudget = 0
                Cost = row[7]
                Frequency = 0
                Reach = 0
                Postengagements = 0
                Impressions = row[6]
                Clicks = row[5]
                Landingpageviews = 0
                Videowachesat75 = 0
                ThruPlay = 0
                Conversions = row[10]
                Adsetend = ''
                Adsetstart = ''
                Adstatus = 'ACTIVE'
                Country = ''
                Media = row[4]
                Referer = media

                ad = [AdID, Adname, Country, Adstatus,
                      AdSetID,  CreateDate, Referer, Media]

                ads.append(ad)

        sql.connect.insertAds(ads, media, conn)

    except Exception as e:
        print(e)


def CreativesAds(conn):
    try:
        url = 'https://api.adform.com/v1/reportingstats/agency/reportdata'
        data = requests.post(
            url,
            # Headers: se coloca el token de autorizacion
            headers={
                'Authorization': 'Bearer ' + Token['Ticket'],
                'Content-Type': 'application/json'
            },
            # Para obtener los datos se realiza un POST con los datos de dimension, metricas y filtros, tiene que tener al menos uno de cada uno
            # Para enviarlo se tiene que guardar en formato Json.
            json={
                "dimensions": [
                    "bannerID",
                    "banner",
                    "bannerSize",
                    "bannerType",
                    "bannerWeight",
                    "bannerFormat",
                    "bannerAdMessage",
                    "adCreativeType"
                ],
                "metrics": [
                    "clicks",
                    "impressions"
                ],
                "filter": {
                    "date": {
                        "from": str(datetime.now() - timedelta(days=1)),
                        "to": str(datetime.now() - timedelta(days=1))
                    }
                }
            }
        )
        data = data.json()
        for row in data['reportData']['rows']:
            if(row[0] > 0):
                AdID = row[0]
                Weight = 0
                Adname = row[1]
                Size = row[2]
                AdType = row[3]
                Format = row[5]
                AdMessage = row[6]
                CreativeType = row[7]
                creative = [AdID, Adname, Size,
                            AdType, Weight, Format,
                            AdMessage, CreativeType]
                creatives.append(creative)
        sql.connect.CreativeAdf(creatives, media, conn)
    except Exception as e:
        print(e)

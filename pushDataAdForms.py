# -*- coding: UTF-8 -*-
import json
import requests
import sys
import re
import mysql.connector as mysql
from datetime import datetime
import time
import pandas as pd
import numpy as mp
from xml.etree import ElementTree
import io
import math
conn = None
#conn = mysql.connect(host='3.95.117.169',database='MediaPlatforms',user='omgdev',password='Sdev@2002!',autocommit=True)
ACCESS_TOKEN_URL = "https://auth.mediamath.com/oauth/token"
#Coneccion a la base de datos
def openConnection():
    global conn
    try:
        conn = mysql.connect(host='localhost', database='MediaPlatforms',
                             user='root', password='1234', autocommit=True)
    except:
        logger.error("ERROR: NO SE PUEDO ESTABLECER CONEXION MYSQL.")
        sys.exit()

def GetToken():
    global Token
    url='https://api.adform.com/Services/Security/Login'
    Token=requests.post(
                url,
                 headers={
                            'Content-Type': 'application/json',
                            },
                json={
                        "UserName": "JDELEON",
                        "Password": "Joseandre2019"
                    }

                )
    Token=Token.json()
#GET ReportStas, primero se debe generar el Token de session, solo se pueden obtener 8 dimensioines por Requests
#Falta agregar el budget, aun no se sabe como obtenerlo,
def GetAdformCampaign(conn):
     global cur
     cur=conn.cursor(buffered=True)
     print (datetime.now())
     cuentas=[]
     campanas=[]
     campmetrics=[]
     #Querys a insertar a la base de datos
     sqlInsertAccount = "INSERT INTO Accounts(AccountsID, Account,Media) values(%s,%s,%s) ON DUPLICATE KEY UPDATE Account=VALUES(Account)"
     sqlInsertCampaing = "INSERT INTO Campaings(`CampaingID`,`Campaingname`,`Campaignlifetimebudget`,`Cost`,`AccountsID`,`StartDate`,`EndDate`,`Campaignstatus`) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE Campaingname=VALUES(Campaingname), Campaignlifetimebudget=VALUES(Campaignlifetimebudget), Campaignstatus=VALUES(Campaignstatus)"
     sqlInsertCampaingMetrics = "INSERT INTO CampaingMetrics(CampaingID,Cost,impressions,clicks,frequency) VALUES (%s,%s,%s,%s,%s)"
     try:
         url='https://api.adform.com/v1/reportingstats/agency/reportdata'
         data=requests.post(
             url,
             #Headers: se coloca el token de autorizacion
             headers={
                 'Authorization':'Bearer '+ Token['Ticket'],
                 'Content-Type':'application/json'
             },
             #Para obtener los datos se realiza un POST con los datos de dimension, metricas y filtros, tiene que tener al menos uno de cada uno
             #Para enviarlo se tiene que guardar en formato Json.
             json={
                       "dimensions": [
                                    "campaignID",
                                    "campaign",
                                    "clientID",
                                    "client",
                                    "campaignStartDate",
                                    "campaignEndDate",
                                    "frequencyCampaign"
                                ],
                                "metrics": [
                                    "clicks",
                                    "impressions",
                                    "cost"
                                ],
                                "filter": {
                                    "date": {"from":"2019-01-01", "to": "{}".format(str(datetime.now()))}
                                }
                    }
         )
         data=data.json()
         for row in data['reportData']['rows']:
             if(row[0]!=''):
                 cuenta=[row[2],row[3],'AF']
                 cuentas.append(cuenta)
                 campana=[row[0],row[1],'0',row[9],row[2],row[4],row[5],'ACTIVE']
                 campanas.append(campana)
                 #Se verifica si Frequency es numerico
                 if(row[6].isnumeric()):
                    campanametrica=[row[0],row[9],row[8],row[7],row[6]]
                 else:
                    campanametrica=[row[0],row[9],row[8],row[7],'0']
                 campmetrics.append(campanametrica)
         cur.execute("SET FOREIGN_KEY_CHECKS=0")
         cur.executemany(sqlInsertAccount ,cuentas)
         cur.executemany(sqlInsertCampaing,campanas)
         cur.executemany(sqlInsertCampaingMetrics,campmetrics)
         cur.execute("SET FOREIGN_KEY_CHECKS=1")
         print('Success AF Campanas')
     except Exception as e:
         print(e)
     finally:
         print (datetime.now())


def GetAdformAdsets(conn):
    global cur
    cur=conn.cursor(buffered=True)
    print (datetime.now())
    adsets=[]
    adsetmetrics=[]
    #Querys
    sqlInsertAdsSetsMetrics = "INSERT INTO AdSetMetrics(AdSetID,AdSetName,Impressions,Clicks,frequency) VALUES (%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE AdSetName=VALUES(AdSetName)"
    sqlInsertAdSet = "INSERT INTO Adsets(AdSetID,AdSetName,Adsetlifetimebudget,Adsetend,Adsetstart,CampaingID,Status) VALUES (%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE AdSetName=VALUES(AdSetName),Adsetlifetimebudget=VALUES(Adsetlifetimebudget),Status=VALUES(Status);"
    try:
         url='https://api.adform.com/v1/reportingstats/agency/reportdata'
         data=requests.post(
             url,
             #Headers: se coloca el token de autorizacion
             headers={
                 'Authorization':'Bearer '+ Token['Ticket'],
                 'Content-Type':'application/json'
             },
             #Para obtener los datos se realiza un POST con los datos de dimension, metricas y filtros, tiene que tener al menos uno de cada uno
             #Para enviarlo se tiene que guardar en formato Json.
             json={
                       "dimensions": [
                                    "campaignID",
                                    "lineItemID",
                                    "lineItem",
                                    "lineItemStartDate",
                                    "lineItemEndDate",
                                    "frequencyLineItem",
                                ],
                                "metrics": [
                                    "clicks",
                                    "impressions",
                                    "cost"
                                ],
                                "filter": {
                                    "date": {"from":"2019-01-01", "to": '' + str(datetime.now())}
                                }
                    }
         )
         data=data.json()
         for row in data['reportData']['rows']:
             if(row[1]>0):
                 adset=[row[1],row[2],'0',row[3],row[4],row[0],'ACTIVE']
                 adsets.append(adset)
                 if(row[5].isnumeric()):
                    adsetmetric=[row[1],row[2],row[7],row[6],row[5]]
                 else:
                    adsetmetric=[row[1],row[2],row[7],row[6],'0']
                 adsetmetrics.append(adsetmetric)
         cur.execute("SET FOREIGN_KEY_CHECKS=0")
         cur.executemany(sqlInsertAdSet ,adsets)
         cur.executemany(sqlInsertAdsSetsMetrics ,adsetmetrics)
         cur.execute("SET FOREIGN_KEY_CHECKS=1")
         print('Success AF Adsets')
    except Exception as e:
        print(e)
    finally:
        print (datetime.now())

def GetAdformAds(conn):
    global cur
    cur=conn.cursor(buffered=True)
    print (datetime.now())
    ads=[]
    adsmetrics=[]
    #Querys
    sqlInsertAd = "INSERT INTO Ads(AdID,Adname,AdSetID,Media,Status) VALUES (%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE AdID=VALUES(AdID),Adname=VALUES(Adname),Media=VALUES(Media),Status=VALUES(Status);"
    sqlInsertMetricsAds = "INSERT INTO MetricsAds(AdID,Adname,Clicks,Impressions,Cost,ctr,cpm,convertions) VALUES (%s,%s,%s,%s,%s,%s,%s,%s);"
    try:
        url='https://api.adform.com/v1/reportingstats/agency/reportdata'
        data=requests.post(
             url,
             #Headers: se coloca el token de autorizacion
             headers={
                 'Authorization':'Bearer '+ Token['Ticket'],
                 'Content-Type':'application/json'
             },
             #Para obtener los datos se realiza un POST con los datos de dimension, metricas y filtros, tiene que tener al menos uno de cada uno
             #Para enviarlo se tiene que guardar en formato Json.
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
                            "date": {"from":"2019-01-01", "to": '' + str(datetime.now())}
                        }
                    }
         )
        data=data.json()
        for row in data['reportData']['rows']:
            if(row[1]>0):
                ad=[row[1],row[2],row[0],row[4],'ACTIVE']
                ads.append(ad)
                admetric=[row[1],row[2],row[5],row[6],row[7],row[8],row[9],row[10]]
                adsmetrics.append(admetric)
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(sqlInsertAd ,ads)
        cur.executemany(sqlInsertMetricsAds ,adsmetrics)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        print('Success AF AD')
    except Exception as e:
        print(e)
    finally:
        print (datetime.now())


def GetAdFormCreativeAds(conn):
    global cur
    cur=conn.cursor(buffered=True)
    print (datetime.now())
    creativeAds=[]
    #Query
    sqlInsertCreativeAds = 'INSERT INTO creativeads(adid,adname,size,adtype,weight,format,admessage,creativetype) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)'
    try:
        url='https://api.adform.com/v1/reportingstats/agency/reportdata'
        data=requests.post(
             url,
             #Headers: se coloca el token de autorizacion
             headers={
                 'Authorization':'Bearer '+ Token['Ticket'],
                 'Content-Type':'application/json'
             },
             #Para obtener los datos se realiza un POST con los datos de dimension, metricas y filtros, tiene que tener al menos uno de cada uno
             #Para enviarlo se tiene que guardar en formato Json.
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
                        "date": {"from":"2019-01-01", "to": '' + str(datetime.now())}
                    }
                    }
         )
        data=data.json()
        for row in data['reportData']['rows']:
            if(row[0]>0):
                if(row[3].isnumeric()):
                    creativead = [row[0],row[1],row[2],row[3],row[4],row[5],row[6],row[7]]
                else:
                    creativead = [row[0],row[1],row[2],row[3],'0',row[5],row[6],row[7]]
                creativeAds.append(creativead)
        cur.executemany(sqlInsertCreativeAds ,creativeAds)
        print('Success AF AD')
    except Exception as e:
        print(e)
    finally:
        print (datetime.now())


if __name__ == '__main__':
    openConnection()
    GetToken()
    GetAdformCampaign(conn)
    #GetAdformAdsets(conn)
    #GetAdformAds(conn)
    #GetAdFormCreativeAds(conn)

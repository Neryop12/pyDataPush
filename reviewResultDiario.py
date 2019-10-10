# -*- coding: UTF-8 -*-
import json
import requests
import sys
import re
import mysql.connector as mysql
from datetime import datetime, timedelta
import numpy as mp
conn = None

def openConnection():
    global conn
    try:
        conn = mysql.connect(host='3.95.117.169', database='MediaPlatforms',
                             user='omgdev', password='Sdev@2002!', autocommit=True)
    except:
        print("ERROR: NO SE PUEDO ESTABLECER CONEXION MYSQL.")
        sys.exit()

def InsertResutl(conn):
    cur=conn.cursor(buffered=True)
    Campanas=[]
    #Query para insertar los datos, Media  -> OC
    sqlInserErrors = "UPDATE `dailycampaing` SET `Result` = %s WHERE (`id` = %s);"
    sqlCamping = "select id,Campaingname,d.Impressions, d.Reach, d.Clicks, d.VideoWatches75, d.PostReaccion from Campaings c  inner join dailycampaing d on d.CampaingID = c.CampaingID where d.Result is Null limit 20000 ;"
    try:
        print(datetime.now())
        cur.execute(sqlCamping,)
        resultscon = cur.fetchall()
        #SELECIONAMOS TODOS LOS ERRORES ACTUALES
        for res in resultscon:
            Nomenclatura = res[1]
            ID = res[0]
            searchObj = re.search(r'(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.+&]+)_([a-zA-Z0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-Z-/.+]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ.+]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(19|2019)_([0-9,.]+)_(BA|AL|TR|TRRS|TRRRSS|IN|DES|RV|CO)_([0-9,.]+)_(CPM|CPMA|CPVi|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCO|CPCO)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([0-9,.-]+)?(_B-)?(_)?([0-9]+)?(_S-)?(_)?([0-9]+)?(\(([0-9.)]+)\))?$', Nomenclatura, re.M | re.I)
            if searchObj:
                Result = (searchObj.group(14))
                if str(Result).upper() == 'CPVI':
                    campana=(res[4],ID)
                    Campanas.append(campana)
                elif str(Result).upper() == 'CPM':
                    campana=(res[2],ID)
                    Campanas.append(campana)
                elif str(Result).upper() == 'CPV':
                    campana=(res[5],ID)
                    Campanas.append(campana)
                elif str(Result).upper() == 'CPCO':
                    campana=(res[4],ID)
                    Campanas.append(campana)
                elif str(Result).upper() == 'CPI':
                    campana=(res[6],ID)
                    Campanas.append(campana)
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(sqlInserErrors,Campanas)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        print('Success Facebook Camp')

    except Exception as e:
        print(e)
    finally:
        print(datetime.now())

if __name__ == '__main__':
    openConnection()
    InsertResutl(conn)
    conn.close()
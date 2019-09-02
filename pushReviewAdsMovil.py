# -*- coding: UTF-8 -*-
import json
import requests
import sys
import re
import mysql.connector as mysql
from datetime import datetime, timedelta

#Coneccion a la base de datos de mfcgt
def openConnection():
    global conn
    try:
        conn = mysql.connect(host='3.95.117.169', database='MediaPlatforms',
                             user='omgdev', password='Sdev@2002!', autocommit=True)
    except:
        print("ERROR: NO SE PUEDO ESTABLECER CONEXION MYSQL.")
        sys.exit()


def CampAM(conn):
    global cur
    cur = conn.cursor()
    print (datetime.now())
    slqCampana = "select CampaingID, Campaingname, Advertiser from CampaingsAM;"
    sqlInserErrors = "INSERT INTO ErrorsCampaings(Error,Comentario,Media,TipoErrorID,CampaingID,Impressions,StatusCampaing) VALUES (%s,%s,%s,%s,%s,%s,%s)"
    try:
        cur.execute(slqCampana,)
        resultscon = cur.fetchall()
        Errores = []
        for result in resultscon:
            Nomenclatura = result[1]
            CampaingID = result[0]
            searchObj = re.search(r'^(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.+&]+)_([a-zA-Z0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-Z-/.+]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ.+]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(19|2019)_([0-9,.]+)_(BA|AL|TR|TRRS|IN|DES|RV|CO)_([0-9,.]+)_(CPM|CPMA|CPVi|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCo)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([0-9,.-]+)(_B-)?([0-9]+)?(_S-)?([0-9]+)?(\(([0-9.)]+)\))?', Nomenclatura, re.M | re.I)
            if not searchObj:
                Error = result[1]
                Comentario = 'Error de nomenclatura verifica cada uno de sus elementos'
                nuevo=[Error,Comentario,'AM','1',CampaingID,'0','ACTIVE']
                Errores.append(nuevo)
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(sqlInserErrors,Errores)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
    except Exception as e:
        print(e)
    finally:
        print(datetime.now())



if __name__ == '__main__':
    openConnection()
    CampAM(conn)
    conn.close()
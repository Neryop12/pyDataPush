# -*- coding: UTF-8 -*-
import json
import requests
import sys
import re
import mysql.connector as mysql
from datetime import datetime, timedelta
import numpy as mp
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

host= config['TESTING']['HOST']
name = config['TESTING']['NAME']
user = config['TESTING']['USER']
password = config['TESTING']['PASSWORD']
autocommit= config['TESTING']['AUTOCOMMIT']

def openConnection():
    global conn
    try:
        conn = mysql.connect(host=host, database=name,
                             user=user, password=password, autocommit=autocommit)
    except:
        print("ERROR: NO SE PUEDO ESTABLECER CONEXION MYSQL.")
        sys.exit()
def PresupusetoCamp(conn):
    cur=conn.cursor(buffered=True)
    Errores=[]
    fechahoy = datetime.now()
    dayhoy = fechahoy.strftime("%Y-%m-%d")
    #Query para insertar los datos, Media  -> OC
    sqlInserErrors = "UPDATE `MediaPlatforms`.`ErrorsCampaings` SET `Estado` = '0' WHERE CampaingID=%s);"
    sqlCamping = "select Distinct c.CampaingID , sum(d.cost), c.Campaingname, date_format(c.Enddate,'%Y-%m-%d'),ifnull((select m.cost from CampaingMetrics m where m.CampaingID = c.CampaingID group by m.id desc limit 1  ),0) costo,a.Media from Campaings c inner join dailycampaing d on d.CampaingID = c.CampaingID inner join Accounts a on a.AccountsID = c.AccountsID inner join ErrorsCampaings ec on ec.CampaingID = c.CampaingID where ec.TipoErrorID = 15 and ec.Estado =1  group by c.CampaingID;"
    try:
        print(datetime.now())
        cur.execute(sqlCamping)
        camp = cur.fetchall()
        for result in camp:
            Nomenclatura = result[2]
            searchObj = re.search(r'^(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.+&]+)_([a-zA-Z0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-Z-/.+]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ.+]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(19|2019)_([0-9,.]+)_(BA|AL|TR|TRRS|IN|DES|RV|CO)_([0-9,.]+)_(CPM|CPMA|CPVi|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCo)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([0-9,.-]+)(_B-)?([0-9]+)?(_S-)?([0-9]+)?(\(([0-9.)]+)\))?', Nomenclatura, re.M | re.I)
            if searchObj:
                NomInversion = float(searchObj.group(11))
                Costo = float(result[1]) + float(result[4])
                if searchObj.group(25) is not None:
                    if float(searchObj.group(25)) > 0:
                        Costo = float(searchObj.group(25))-float(result[0])
                        Costo += float(result[4])
                if (Costo - NomInversion) < 0:
                    Errores.append(result[0])
        cur.executemany(sqlInserErrors,Errores)
        print('Success Update Camp')
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("PresupuestoCamp", "Success", "errosPresupuestoCamp.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("PresupuestoCamp", "{}", "errosPresupuestoCamp.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print(datetime.now())



if __name__ == '__main__':
    openConnection()
    PresupusetoCamp(conn)
    conn.close()
# -*- coding: UTF-8 -*-
import json
import requests
import sys
import re
import mysql.connector as mysql
from datetime import datetime, timedelta

# host= '3.95.117.169'
host = '3.95.117.169'
name = 'MediaPlatforms'
user = 'omgdev'
password = 'Sdev@2002!'
autocommit = 'True'


def openConnection():
    global conn
    try:
        conn = mysql.connect(host=host, database=name,
                             user=user, password=password, autocommit=autocommit)
    except:
        print("ERROR: NO SE PUEDO ESTABLECER CONEXION MYSQL.")
        sys.exit()


def CampAM(conn):
    global cur
    cur = conn.cursor()
    print(datetime.now())
    slqCampana = "select CampaingID, Campaingname    from CampaingsAM;"
    sqlInserErrors = "INSERT INTO ErrorsCampaings(Error,Comentario,Media,TipoErrorID,CampaingID,Impressions,StatusCampaing) VALUES (%s,%s,%s,%s,%s,%s,%s)"
    sqlReview = "select count(idErrorsCampaings) from ErrorsCampaings where CampaingID =  {}"
    try:
        cur.execute(slqCampana,)
        resultscon = cur.fetchall()
        Errores = []
        for result in resultscon:
            Nomenclatura = result[1]
            CampaingID = result[0]
            searchObj = re.search(
                r'(.*)(\([0-9]+\))', Nomenclatura, re.M | re.I)
            if not searchObj:
                sqlReview = sqlReview.format(CampaingID)
                cur.execute(sqlReview)
                rev = cur.fetchall()
                if rev[0][0] < 1:
                    Error = result[1].split('(')
                    Comentario = 'Error de nomenclatura verifica cada uno de sus elementos'
                    Camp = Error[0]
                    nuevo = [Camp, Comentario, 'AM',
                             '1', CampaingID, '0', 'ACTIVE']
                    Errores.append(nuevo)
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(sqlInserErrors, Errores)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("CampAM", "Success", "revision_errores_adsmovil.py","{}");'.format(
            dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("CampAM", "{}", "revision_errores_adsmovil.py","{}");'.format(
            e, dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print(datetime.now())


def ReviewCamp(conn):
    global cur
    cur = conn.cursor()
    print(datetime.now())
    slqCampana = "select e.idErrorsCampaings, am.Campaingname from CampaingsAM am inner join ErrorsCampaings e on e.CampaingID = am.CampaingID where e.Estado > 0;"
    slqUpdate = "UPDATE `ErrorsCampaings` SET `Estado` = '0' WHERE (`idErrorsCampaings` =  %s);"
    try:
        cur.execute(slqCampana,)
        resultscon = cur.fetchall()
        Errores = []
        for result in resultscon:
            Nomenclatura = result[1]
            errorID = result[0]
            searchObj = re.search(r'([0-9,.]+)_(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.+&]+)_([a-zA-Z0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&0-9]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&0-9]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&0-9]+)_([a-zA-Z-/.+]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ.+0-9]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(2019|19|20|2020)_([0-9,.]+)_(BA|AL|TR|TRRS|TRRRSS|IN|DES|RV|CO|MESAD|LE)_([0-9,.]+)_(CPM|CPMA|CPVI|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCO|CPCO)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([0-9,.-]+)?(_B-)?(_)?([0-9.,]+)?(_S-)?(_)?([0-9.,]+)?(\(([0-9.)])\))?(/[0-9].+)?', Nomenclatura, re.M | re.I)
            if searchObj:
                nuevo = [errorID]
                Errores.append(nuevo)
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(slqUpdate, Errores)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("CampAM", "Success", "revision_errores_adsmovil.py","{}");'.format(
            dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("CampAM", "{}", "revision_errores_adsmovil.py","{}");'.format(
            e, dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print(datetime.now())


if __name__ == '__main__':
    openConnection()
    CampAM(conn)
    ReviewCamp(conn)
    conn.close()

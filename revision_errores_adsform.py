# -*- coding: UTF-8 -*-
import json
import requests
import sys
import re
import mysql.connector as mysql
from datetime import datetime, timedelta
import configparser
conn = None

config = configparser.ConfigParser()
config.read('config.ini')

host= '3.95.117.169'
name = 'MediaPlatforms'
user = 'omgdev'
password = 'Sdev@2002!'
autocommit= 'True'
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
    print (datetime.now())
    hoy = datetime.now().strftime("%Y-%m-%d")
    sqlConjuntosFB = """
    select b.CampaingID, a.AccountsID,a.Account,b.Campaingname, b.Campaignspendinglimit,b.Campaigndailybudget,b.Campaignlifetimebudget,c.AdSetID,c.Adsetname,c.Adsetlifetimebudget,SUM(c.Adsetlifetimebudget) as tlotalconjungo,c.Adsetdailybudget,a.Media,b.Campaignstatus,b.Campaignstatus,c.Status
    from Accounts a
    INNER JOIN Campaings b on a.AccountsID=b.AccountsID
    INNER JOIN  Adsets c on b.CampaingID=c.CampaingID
    where a.Media='AF' and c.status in ('ACTIVE','enabled')  group by b.CampaingID  desc;
    
    """
    sqlInserErrors = "INSERT INTO ErrorsCampaings(Error,Comentario,Media,TipoErrorID,CampaingID,Impressions,StatusCampaing) VALUES (%s,%s,%s,%s,%s,%s,%s)"
    sqlSelectErrors = "SELECT COUNT(*) FROM ErrorsCampaings where CampaingID=%s and TipoErrorID=%s and Media=%s"
    try:
        cur.execute(sqlConjuntosFB,)
        resultscon = cur.fetchall()
        Errores = []
        for result in resultscon:
            Nomenclatura = result[1]
            CampaingID = result[0]
            searchObj = re.search(r'^(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.+&]+)_([a-zA-Z0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-Z-/.+]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ.+]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(19|2019)_([0-9,.]+)_(BA|AL|TR|TRRS|TRRRSS|IN|DES|RV|CO)_([0-9,.]+)_(CPM|CPMA|CPVi|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCO|CPCO)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([0-9,.-]+)?(_B-)?(_)?([0-9.]+)?(_S-)?(_)?([0-9.]+)?(\s)?(\(([0-9.)]+)\))?$', Nomenclatura, re.M | re.I)
            if not searchObj:
                
                cur.execute(sqlSelectErrors, (CampaingID, 1, 'AF'))
                rescampaing = cur.fetchone()
                if rescampaing[0] == 0:
                    Error = result[1].split('(')
                    Comentario = 'Error de nomenclatura verifica cada uno de sus elementos'
                    Camp = result[3]
                    nuevo=[Camp,Comentario,'AM','1',CampaingID,'0','ACTIVE']
                    Errores.append(nuevo)
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(sqlInserErrors,Errores)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("CampAM", "Success", "pushReviewAdsMovil.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
        print('Success Campaing AF')
    except Exception as e:
        print(e)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("CampAM", "{}", "pushReviewAdsMovil.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print(datetime.now())


def ReviewCamp(conn):
    global cur
    cur = conn.cursor()
    print (datetime.now())
    slqCampana = "select e.idErrorsCampaings, am.Campaingname from CampaingsAM am inner join ErrorsCampaings e on e.CampaingID = am.CampaingID where e.Estado > 0;"
    slqUpdate = "UPDATE `ErrorsCampaings` SET `Estado` = '0' WHERE (`idErrorsCampaings` =  %s);"
    try:
        cur.execute(slqCampana,)
        resultscon = cur.fetchall()
        Errores = []
        for result in resultscon:
            Nomenclatura = result[1]
            errorID = result[0]
            searchObj = re.search(r'^(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.+&]+)_([a-zA-Z0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-Z-/.+]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ.+]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(19|2019)_([0-9,.]+)_(BA|AL|TR|TRRS|TRRRSS|IN|DES|RV|CO)_([0-9,.]+)_(CPM|CPMA|CPVi|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCO|CPCO)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([0-9,.-]+)?(_B-)?(_)?([0-9.]+)?(_S-)?(_)?([0-9.]+)?(\s)?(\(([0-9.)]+)\))?$', Nomenclatura, re.M | re.I)
            if searchObj:
                nuevo=[errorID]
                Errores.append(nuevo)
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(slqUpdate,Errores)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("CampAM", "Success", "pushReviewAdsMovil.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("CampAM", "{}", "pushReviewAdsMovil.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print(datetime.now())



if __name__ == '__main__':
    openConnection()
    CampAM(conn)
    #ReviewCamp(conn)
    conn.close()
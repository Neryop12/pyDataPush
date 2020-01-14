# -*- coding: UTF-8 -*-
import json
import requests
import sys
import re
import mysql.connector as mysql
from datetime import datetime, timedelta
import time
import numpy as mp

conn = None


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


def reviewErroresNomen(conn):
    global cur
    cur = conn.cursor()
    Comentario = ''
    Estatus = ''
    hoy = datetime.now().strftime("%Y-%m-%d")
    try:
        print (datetime.now())
        sqlConjuntosFB = """
        select err.idErrorsCampaings,ca.Campaingname from ErrorsCampaings err
        inner join Campaings ca on ca.CampaingID = err.CampaingID
        where err.Estado = 1;
        """
        slqUpdate = "UPDATE `ErrorsCampaings` SET `Estado` = %s WHERE (idErrorsCampaings =  %s ) ;"

        cur.execute(sqlConjuntosFB,)
        resultscon = cur.fetchall()
        Errores = []
        for result in resultscon:
           searchObj = re.search(r'(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.+&]+)_([a-zA-Z0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&0-9]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&0-9]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&0-9]+)_([a-zA-Z-/.+]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ.+0-9]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(19|2019)_([0-9,.]+)_(BA|AL|TR|TRRS|TRRRSS|IN|DES|RV|CO|MESAD|LE)_([0-9,.]+)_(CPM|CPMA|CPVi|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCO|CPCO)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ+&0-9]+)_([0-9,.-]+)?(_B-)?(_)?([0-9,.]+)?(_S-)?(_)?([0-9,.]+)?(\(([0-9.)]+)\))?(/[0-9]+)?', result[1], re.M | re.I)
           if searchObj:
              res=(0,result[0])
              Errores.append(res)
        cur.executemany(slqUpdate, Errores)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("errors_fb_inv", "Success", "pushReviewErrors.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
        cur.close()
    #ANALISIS IMPRESIONES Y
        #print(m.groups())
    except Exception as e:
        print(e)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("errors_fb_inv", "{}", "pushReviewErrors.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print('Success Nomenclatura')
        print (datetime.now())

if __name__ == '__main__':
   openConnection()
   reviewErroresNomen(conn)
   conn.close()

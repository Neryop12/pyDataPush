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

def PresupusetoCamp(conn):
    cur=conn.cursor(buffered=True)
    Errores=[]
    fechahoy = datetime.now()
    dayhoy = fechahoy.strftime("%Y-%m-%d")
    #Query para insertar los datos, Media  -> OC
    sqlInserErrors = "INSERT INTO ErrorsCampaings(Error,Comentario,Media,TipoErrorID,CampaingID,Impressions,StatusCampaing) VALUES (%s,%s,%s,%s,%s,%s,%s) "
    sqlCamping = "select Distinct c.CampaingID , sum(d.cost), c.Campaingname, date_format(c.Enddate,'%Y-%m-%d'),ifnull((select m.cost from CampaingMetrics m where m.CampaingID = c.CampaingID group by m.id desc limit 1  ),0) costo,a.Media from Campaings c inner join dailycampaing d on d.CampaingID = c.CampaingID inner join Accounts a on a.AccountsID = c.AccountsID where c.EndDate > '{}'  group by c.CampaingID;".format(dayhoy)
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
                if (Costo - NomInversion) > 1:
                    Error = 'Error el presupuesto planificado es menor al presupuesto real '
                    Comentario = 'Error la campaña '+ result[0] +' excede el presupuesto real por: ' + str(round(Costo - NomInversion,1))
                    nuevo=[Error,Comentario,result[5],'15',result[0],'0','ACTIVE']
                    Errores.append(nuevo)
                else:
                    date_time_obj = datetime.strptime(result[3],'%Y-%m-%d') - datetime.now()
                    if date_time_obj.days >= 5:
                        porcentaje = Costo / NomInversion
                        if porcentaje >= 0.9:
                            Error = '!Advertencia! el presupuesto esta a punto de llegar su tope '
                            Comentario = 'Advertencia la campaña '+ result[0] +' alcanzara pronto su tope'
                            nuevo=[Error,Comentario,result[5],'13',result[1],'0','ACTIVE']
                            Errores.append(nuevo)
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(sqlInserErrors,Errores)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        print('Success Camp')
    except Exception as e:
        print(e)
    finally:
        print(datetime.now())



if __name__ == '__main__':
    openConnection()
    PresupusetoCamp(conn)
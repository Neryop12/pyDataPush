# -*- coding: UTF-8 -*-
import json
import requests
import sys
import re
import mysql.connector as mysql
from datetime import datetime, timedelta
import numpy as mp
conn = None

#Coneccion a la base de datos de mfcgt
def openConnection():
    global conn
    try:
        conn = mysql.connect(host='3.95.117.169', database='MediaPlatforms',
                             user='omgdev', password='Sdev@2002!', autocommit=True)
    except:
        print("ERROR: NO SE PUEDO ESTABLECER CONEXION MYSQL.")
        sys.exit()


def SaldosGO(conn):
    cur=conn.cursor(buffered=True)
    Errores=[]
    fechahoy = datetime.now()
    dayhoy = fechahoy.strftime("%Y-%m-%d")
    #Query para insertar los datos, Media  -> OC
    sqlInserErrors = "INSERT INTO ErrorsCampaings(Error,Comentario,Media,TipoErrorID,CampaingID,Impressions,StatusCampaing) VALUES (%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE Error=Values(Error),Comentario=Values(Comentario), TipoErrorID = Values(TipoErrorID) "
    sqlCamping = "select CampaingID,sum(cost) spend,Campaigndailybudget,Campaignlifetimebudget,date_format(Enddate,'%Y-%m-%d') FechaFin from dailycampaing where AdvertisingChannelType in ('Search','Display') group by CampaingID;"
    try:
        print(datetime.now())
        cur.execute(sqlCamping)
        cur.execute(sqlCamping)
        camp = cur.fetchall()
        for result in camp:
            date_time_obj = datetime.strptime(result[4],'%Y-%m-%d') - datetime.now()
            if date_time_obj.days > 1:
                if result[3] == 0:
                    Error = 'Error no existe presupuesto asignado '
                    Comentario = 'Error la campaña '+ result[0] +' no se le puede calcular su costo.'
                    nuevo=[Error,Comentario,'GO','2',result[0],'0','ACTIVE']
                    Errores.append(nuevo)
                else:
                    total = result[1] + date_time_obj.days*result[2]
                    if total > result[3]:
                        recomendado = (result[3] - result[1])/ date_time_obj.days
                        if recomendado > 0 :
                            Error = '!Advrtencia! Presupuesto diario se excede. '
                            Comentario = 'Error la campaña '+ result[0] +' debe de terner un costo diario de: ' + str(round(recomendado,1))
                            nuevo=[Error,Comentario,'GO','13',result[0],'0','ACTIVE']
                            Errores.append(nuevo)
                        else:
                            Error = 'Error el presupuesto planificado es menor al presupuesto real '
                            Comentario = 'Error la campaña '+ result[0] +' excede el presupuesto real del planificado'
                            nuevo=[Error,Comentario,'GO','2',result[0],'0','ACTIVE']
                            Errores.append(nuevo)
                    elif total < result[3]:
                        recomendado = (result[3] - result[1])/ date_time_obj.days
                        Error = '!Advrtencia! Presupuesto diario faltante. '
                        Comentario = 'Error la campaña '+ result[0] +' debe de terner un costo diario de: ' + str(round(recomendado,1))
                        nuevo=[Error,Comentario,'GO','14',result[0],'0','ACTIVE']
                        Errores.append(nuevo)
        cur.executemany(sqlInserErrors,Errores)
        print('Success GO')
    except Exception as e:
        print(e)
    finally:
        print(datetime.now())


if __name__ == '__main__':
    openConnection()
    SaldosGO(conn)
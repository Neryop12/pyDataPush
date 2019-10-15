# -*- coding: UTF-8 -*-
import json
import requests
import sys
import re
import mysql.connector as mysql
from datetime import datetime, timedelta
import numpy as mp
conn = None
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


def SaldosGO(conn):
    cur=conn.cursor(buffered=True)
    Errores=[]
    fechahoy = datetime.now()
    dayhoy = fechahoy.strftime("%Y-%m-%d")
    #Query para insertar los datos, Media  -> OC
    sqlInserErrors = "INSERT INTO ErrorsCampaings(Error,Comentario,Media,TipoErrorID,CampaingID,Impressions,StatusCampaing) VALUES (%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE Error=Values(Error),Comentario=Values(Comentario), TipoErrorID = Values(TipoErrorID) "
    sqlCamping = "select c.CampaingID,sum(dc.cost) spend,dc.Campaigndailybudget,dc.Campaignlifetimebudget,date_format(dc.Enddate,'%Y-%m-%d') FechaFin  from Dailycampaing dc  inner join Campaings c on c.CampaingID = dc.CampaingID where Placement in ('Search','Display') and c.Campaignstatus='enabled' group by CampaingID;"
    try:
        print(datetime.now())
        cur.execute(sqlCamping)
        cur.execute(sqlCamping)
        camp = cur.fetchall()
        for result in camp:
            if result[4]:
                date_time_obj = datetime.strptime(result[4],'%Y-%m-%d') - datetime.now()
                if date_time_obj.days > 1:
                    if result[3] == 0:
                        Error = 'Error no existe presupuesto asignado '
                        Comentario = 'Error la campaña '+ result[0] +' no se le puede calcular su costo.'
                        nuevo=[Error,Comentario,'GO','2',result[0],'0','ACTIVE']
                        Errores.append(nuevo)
                    else:
                        total = result[1] + (date_time_obj.days+1)*result[2]
                        if total > result[3]:
                            recomendado = (result[3] - result[1])/ (date_time_obj.days + 1)
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
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("SaldosGO", "success", "errorsSaldos.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
        print('Success GO')
    except Exception as e:
        print(e)
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("SaldosGo", "{}", "errorsSaldos.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print(datetime.now())


if __name__ == '__main__':
    openConnection()
    SaldosGO(conn)
    conn.close()
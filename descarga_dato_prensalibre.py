import json
import requests
import sys
import re
import mysql.connector as mysql
from datetime import datetime, timedelta
import schedule
import time
#3.95.117.169
conn = None
def openConnection():
    global conn
    try:
        conn = mysql.connect(host='localhost', database='MediaPlatforms',
                             user='root', password='1234', autocommit=True)
    except:
        print("ERROR: NO SE PUEDO ESTABLECER CONEXION MYSQL.")
        sys.exit()


def PushPrensalibre(conn):
    global cur
    fechaayer = datetime.now() - timedelta(days=1)
    #Formato de las fechas para aceptar en el GET
    dayayer = fechaayer.strftime("%Y-%m-%d")
    print (datetime.now())
    cur=conn.cursor(buffered=True)
    campanas=[]
    sqlInsertLocalMedia = sqlInsertLocalMedia = "INSERT INTO `MediaPlatforms`.`localmedia` (`Cliente`, `StartDate`, `EndDate`, `Nombre`, `Nomenclatura`, `Formato`, `Orden`, `Tipo`, `size`, `Impresiones`, `Clicks`, `ctr`,`Costo`,`MediaTypeID`) VALUES (%s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s,%s,%s);"
    try:
        url='https://leads.prensalibre.com/getData/20190801/20190831'
        Result2 = requests.get(
                            url,

                            headers={
                                'api-key': '$2y$10$0X3HbMYVvNAHNMBPSycl.OI2FMIM21wHAnKt7MDWRy/qhmE2TfDrC',
                                },
                        )
        Result2=Result2.json()
        for row in Result2:
            Start = datetime.strptime(row['FECHA_INICIO'], "%d/%m/%Y").strftime("%Y-%m-%d")
            End = datetime.strptime(row['FECHA_FIN'], "%d/%m/%Y").strftime("%Y-%m-%d")
            Costo = str( row['COSTO']).replace(',','')
            campana=[row['CLIENTE'],Start,End,row['NOMBRE'],row['NOMENCLATURA'],row['FORMATO'],row['ORDEN'],row['TIPO'],row['TAMANIO'],row['IMPRESIONES'],row['CLICKS'],row['CTR'],Costo,'1']    
            campanas.append(campana)
        cur.execute("SET FOREIGN_KEY_CHECKS=0")
        cur.executemany(sqlInsertLocalMedia,campanas)
        cur.execute("SET FOREIGN_KEY_CHECKS=1")
        print('Success MM Campanas')
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("pushAdsMovil", "Success", "pushDataAdsMovil.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
    finally:
        print(datetime.now())



if __name__ == '__main__':
    openConnection()
    PushPrensalibre(conn)
    conn.close()
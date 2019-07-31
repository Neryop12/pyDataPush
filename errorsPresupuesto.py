import json
import requests
import sys
import re
import mysql.connector as mysql
from datetime import datetime
import numpy as mp
conn = None

#Coneccion a la base de datos
def openConnection():
    global conn
    try:
        conn = mysql.connect(host='localhost',database='mfcgt',user='root',password='1234',autocommit=True)
    except:
        logger.error("ERROR: NO SE PUEDO ESTABLECER CONEXION MYSQL.")
        sys.exit()

def ComparacionErrores(conn):
    cur=conn.cursor(buffered=True)
    try:
        sqlSelectCompra = "select DISTINCT  com.id,  odc id_compra, idpresupuesto presupuesto, m.id	  from mfcgt.mfccompradiaria com inner join mfcgt.mfccampana cam on cam.id = com.idcampana inner join mfcgt.mfc m on m.id = cam.idmfc;"
        cur.execute(sqlSelectCompra)
        #Lo paso a numpy para que las busquedas sean más rapidas, dado a que son arrays.
        data = mp.array(cur.fetchall())
        r=requests.get("http://10.10.2.99:10000/pbi/api_gt/public/api/v1/ordenes_fl/2019-01-01/2019-01-31")
        r=r.json()
        ap=mp.asarray(r) 
        resutl=mp.where(r[0] == 'OMD')
        print(resutl[0])
    except Exception as e:
        print(e)
    finally:
        print(datetime.now())


if __name__ == '__main__':
    openConnection()
    ComparacionErrores(conn)
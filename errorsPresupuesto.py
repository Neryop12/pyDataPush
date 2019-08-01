import json
import requests
import sys
import re
import mysql.connector as mysql
from datetime import datetime
import numpy as mp
conn = None

#Coneccion a la base de datos de mfcgt
def openConnection():
    global conn
    try:
        conn = mysql.connect(host='localhost',database='mfcgt',user='root',password='1234',autocommit=True)
    except:
        logger.error("ERROR: NO SE PUEDO ESTABLECER CONEXION MYSQL.")
        sys.exit()

#Coneccion a la base de datos de mfcgt
def openConnectionMedia():
    global connMedia
    try:
        connMedia = mysql.connect(host='localhost',database='mediaplatforms',user='root',password='1234',autocommit=True)
    except:
        logger.error("ERROR: NO SE PUEDO ESTABLECER CONEXION MYSQL.")
        sys.exit()        

def ComparacionErrores(conn, connMedia):
    cur=conn.cursor(buffered=True)
    curInsert = connMedia.cursor(buffered=True)
    Errores=[]
    sqlInserErrors = "INSERT INTO mediaplatforms.ErrorsCampaings(Error,Comentario,Media,TipoErrorID,CampaingID,Impressions,StatusCampaing) VALUES (%s,%s,%s,%s,%s,%s,%s)"
    try:
        print(datetime.now())
        sqlSelectCompra = "select DISTINCT   odc id_compra, idpresupuesto presupuesto, m.id flow_id, com.id from mfcgt.mfccompradiaria com inner join mfcgt.mfccampana cam on cam.id = com.idcampana inner join mfcgt.mfc m on m.id = cam.idmfc where m.aprobado = 1 order by com.id;"
        cur.execute(sqlSelectCompra)
        #Lo paso a numpy para que las busquedas sean más rapidas, dado a que son arrays.
        data = mp.array(cur.fetchall())
        r=requests.get("http://10.10.2.99:10000/pbi/api_gt/public/api/v1/ordenes_fl/2019-01-01/2019-01-31")
        r=r.json()
        ap=mp.array(r) 
        #Buqueda de la base de datos al API
        for rowBase in data:
            existe=False
            codigo=False
            #Busqueda de presupuesto
            if not rowBase[1] or rowBase[1]=='0':
                Error = 'Codigo de Presupuesto esta vacio o es 0.'
                Comentario = 'El Codigo de presupuesto no esta asignado.'
                nuevo=[Error,Comentario,'OC','7',rowBase[2],'0','ACTIVE']
                Errores.append(nuevo)
            else:
                for rowApi in ap:
                    if(rowBase[1]==rowApi['codigo_presupuesto']):
                        if(str(rowBase[3])==rowApi['flow_id']):
                            existe=True
                        else:
                             Error = 'Flow id no encontrado.'
                             Comentario = 'Error el Flow id  Ingresado no es mismo '
                             nuevo=[Error,Comentario,'OC','7',rowBase[2],'0','ACTIVE']
                             Errores.append(nuevo)
                        break
                if not existe:
                    Error = 'Codigo de Presupuesto No.' + str(rowBase[1]) + ' no encontrado.'
                    Comentario = 'Error el codigo de Presupuesto Ingresado no se encuentra en '
                    nuevo=[Error,Comentario,'OC','7',rowBase[2],'0','ACTIVE']
                    Errores.append(nuevo)
            #Busqueda de orden de compra
            if not rowBase[0] or rowBase[0]=='0':
                Error = 'Codigo de Orden esta vacio o es 0.'
                Comentario = 'El Codigo de orden no esta asignado.'
                nuevo=[Error,Comentario,'OC','8',rowBase[2],'0','ACTIVE']
                Errores.append(nuevo)
            else:
                for rowApi in ap:
                    if(str(rowBase[0])==rowApi['numero_orden']):
                        if  rowBase[1]!=rowApi['codigo_presupuesto']:
                            Error = 'Numero de Orden ' + str(rowBase[1]) + '.'
                            Comentario = 'Error el numero de orden Ingresado  no esta asigando al mismo presupuesto'
                            nuevo=[Error,Comentario,'OC','8',rowBase[2],'0','ACTIVE']
                            Errores.append(nuevo)
                        if str(rowBase[3])!=rowApi['id_compra']:
                            Error = 'Numero de Orden ' + str(rowBase[1]) + '.'
                            Comentario = 'Error el numero de orden Ingresado  no esta asigando al mismo id de orden'
                            nuevo=[Error,Comentario,'OC','8',rowBase[2],'0','ACTIVE']
                            Errores.append(nuevo)
                        else:
                            codigo=True
                            break
                        break
                if not codigo:
                    Error = 'Numero de Orden ' + str(rowBase[1]) + ' no encontrado.'
                    Comentario = 'Error el numero de orden Ingresado no se encuentra en '
                    nuevo=[Error,Comentario,'OC','8',rowBase[2],'0','ACTIVE']
                    Errores.append(nuevo)
        #Busqueda del Api hacia la base de datos
        for rowApi in ap:
            existe=False
            codigo=False
            flow=False
            #Busqueda de presupuesto en base de datos
            if not rowApi['codigo_presupuesto'] or rowApi['codigo_presupuesto']=='0':
                Error = 'Codigo de Presupuesto esta vacio o es 0.'
                Comentario = 'El Codigo de presupuesto no esta asignado.'
                nuevo=[Error,Comentario,'OC','7',rowBase[2],'0','ACTIVE']
                Errores.append(nuevo)
            else:
                for rowBase in data:
                    if(rowApi['codigo_presupuesto']==rowBase[1]):
                        existe=True
                        break
                if not existe:
                    Error = 'Codigo de Presupuesto No.' + str(rowApi['flow_id']) + ' no encontrado.'
                    Comentario = 'Error el codigo de Presupuesto Ingresado no se encuentra en '
                    nuevo=[Error,Comentario,'OC','7',rowApi['flow_id'],'0','ACTIVE']
                    Errores.append(nuevo)
            #Busque de codigo en la base de datos
            if not rowApi['numero_orden'] or rowApi['numero_orden']=='0':
                Error = 'Codigo de Presupuesto esta vacio o es 0.'
                Comentario = 'El Codigo de presupuesto no esta asignado.'
                nuevo=[Error,Comentario,'OC','7',rowApi['flow_id'],'0','ACTIVE']
                Errores.append(nuevo)
            else:
                for rowBase in data:
                    if(rowApi['numero_orden']==str(rowBase[0])):
                        codigo=True
                        break
                if not codigo:
                        Error = 'Numero de Orden ' + str(rowBase[1]) + '.'
                        Comentario = 'Error el numero de orden Ingresado  no esta asigando al mismo presupuesto'
                        nuevo=[Error,Comentario,'OC','8',rowApi['flow_id'],'0','ACTIVE']
                        Errores.append(nuevo)
             #Busque de Flow en la base de datos
            if not rowApi['flow_id'] or rowApi['flow_id']=='0':
                Error = 'Flow Id esta vacio o es 0.'
                Comentario = 'El Codigo de presupuesto no esta asignado.'
                nuevo=[Error,Comentario,'OC','7',rowApi['flow_id'],'0','ACTIVE']
                Errores.append(nuevo)
            else:
                for rowBase in data:
                    if(rowApi['flow_id']==str(rowBase[3])):
                        flow=True
                        break
                if not flow:
                        Error = 'Numero de Orden ' + str(rowBase[1]) + '.'
                        Comentario = 'Error el numero de orden Ingresado  no esta asigando al mismo presupuesto'
                        nuevo=[Error,Comentario,'OC','8',rowApi['flow_id'],'0','ACTIVE']
                        Errores.append(nuevo)
                    
        curInsert.executemany(sqlInserErrors,Errores)
    except Exception as e:
        print(e)
    finally:
        print(datetime.now())


if __name__ == '__main__':
    openConnection()
    openConnectionMedia()
    ComparacionErrores(conn, connMedia)
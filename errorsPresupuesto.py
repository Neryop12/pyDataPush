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

        

def ComparacionErrores(conn):
    cur=conn.cursor(buffered=True)
    Errores=[]
    #Query para insertar los datos, Media  -> OC
    sqlInserErrors = "INSERT INTO mediaplatforms.ErrorsCampaings(Error,Comentario,Media,TipoErrorID,CampaingID,Impressions,StatusCampaing) VALUES (%s,%s,%s,%s,%s,%s,%s)"
    try:
        print(datetime.now())
        #Query para seleccionar los datos de (orden de compra, presupuesto, flowid y el idcompra) se realizo dos inner joins en con los pk
        #Se filtra para mostrar solo el estado aprobado del MFC (aprobado=1)
        sqlSelectCompra = "select DISTINCT   odc id_compra, idpresupuesto presupuesto, m.id flow_id, com.id from mfcgt.mfccompradiaria com inner join mfcgt.mfccampana cam on cam.id = com.idcampana inner join mfcgt.mfc m on m.id = cam.idmfc where m.aprobado = 1 AND com.idformatodigital > 0  order by com.id;"
        cur.execute(sqlSelectCompra)
        #Lo paso a numpy para que las busquedas sean más rapidas, dado a que son arrays.
        data = mp.array(cur.fetchall())
        r=requests.get("http://10.10.2.99:10000/pbi/api_gt/public/api/v1/ordenes_fl/2019-01-01/2019-01-31")
        #Primero se convierte el request a JSON
        r=r.json()
        #Posteriormente se convierte a un array numpy
        ap=mp.array(r) 
        #Buqueda de la base de datos al API
        for rowBase in data:
            #Se declaran dos variables Booleanas, para saber si se encuentra el dato dentro del API existe->Presupuseto codigo->orden de compra
            existe=False
            codigo=False
            #Busqueda de presupuesto
            #Primero reviza si el presupuesto en la base de datos no esta vacio, es nullo o es 0
            if not rowBase[1] or rowBase[1]=='0':
                Error = 'Codigo de Presupuesto esta vacio o es 0.'
                Comentario = 'El Codigo de presupuesto no esta asignado.'
                nuevo=[Error,Comentario,'OC','7',rowBase[2],'0','ACTIVE']
                Errores.append(nuevo)
            #De no serlo se recorre el array del API, para buscar alguna considencia en el mismo. 
            else:
                for rowApi in ap:
                    if(rowBase[1]==rowApi['codigo_presupuesto']):
                        #De existir un presupuesto, reviza si en esa row se encuntra el mismo Flow_id que en la base de datos
                        if(str(rowBase[3])==rowApi['flow_id']):
                            existe=True
                        else:
                             Error = 'Flow id no encontrado.'
                             Comentario = 'Error el Flow id  Ingresado no es mismo '
                             nuevo=[Error,Comentario,'OC','7',rowBase[2],'0','ACTIVE']
                             Errores.append(nuevo)
                        break
                #Si no existe se inserta un nuevo error
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
                    
        cur.executemany(sqlInserErrors,Errores)
    except Exception as e:
        print(e)
    finally:
        print(datetime.now())


if __name__ == '__main__':
    openConnection()    
    ComparacionErrores(conn)
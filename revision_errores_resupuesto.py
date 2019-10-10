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



def ComparacionErrores(conn):
    cur=conn.cursor(buffered=True)
    Errores=[]
    fechahoy = datetime.now()
    dayhoy = fechahoy.strftime("%Y-%m-%d")
    #Query para insertar los datos, Media  -> OC
    sqlInserErrors = "INSERT INTO MediaPlatforms.ErrorsCampaings(Error,Comentario,Media,TipoErrorID,CampaingID,Impressions,StatusCampaing,Estado) select distinct %s,%s,%s,%s,%s,%s,%s,%s from `MediaPlatforms`.`ErrorsCampaings` WHERE NOT exists (SELECT DISTINCT * FROM MediaPlatforms.ErrorsCampaings where TipoErrorID=%s and CampaingID=%s);"
    sqlCamping = "select CampaingID,Campaingname from MediaPlatforms.Campaings where EndDate > '{}' and (Campaignstatus='ACTIVE' or Campaignstatus='enabled') ;".format(str(dayhoy))
    try:
        print(datetime.now())
        #Query para seleccionar los datos de (orden de compra, presupuesto, flowid y el idcompra) se realizo dos inner joins en con los pk
        #Se filtra para mostrar solo el estado aprobado del MFC (aprobado=1)
        sqlSelectCompra = "select DISTINCT   odc id_compra, idpresupuesto presupuesto, m.id flow_id, com.id from mfcgt.mfccompradiaria com inner join mfcgt.mfccampana cam on cam.id = com.idcampana inner join mfcgt.mfc m on m.id = cam.idmfc where m.aprobado = 1 AND com.idformatodigital > 0  order by com.id;"
        cur.execute(sqlSelectCompra)
        #Lo paso a numpy para que las busquedas sean mas rapidas, dado a que son arrays.
        #Obtencion de la fecha acutal y la fecha de un dia antes
        fechaayer = datetime.now() - timedelta(days=30)
        #Formato de las fechas para aceptar en el GET
        dayayer = fechaayer.strftime("%Y-%m-%d")

        data = mp.array(cur.fetchall())
        cur.execute(sqlCamping)
        camp = cur.fetchall()
        #Obttencion del GET con los datos de PBI
        r=requests.get("http://10.10.2.99:10000/pbi/api_gt/public/api/v1/ordenes_fl/{}/{}".format(str(dayayer),str(dayhoy)))
        #Primero se convierte el request a JSON
        r=r.json()
        #Posteriormente se convierte a un array numpy
        ap=mp.array(r)
        #Buqueda de la base de datos al API
        for result in camp:
            #Variables bool para saber si se tubo una coencidencia en el ciclo for
            #Variable para ODC
            ODCb = False
            #Variable para el Presupuesto
            Presb = False
            #Variable para el numero de orden de compra
            Codb = False
            #Variable para el flow id
            Flowb = False
            #Nomenclatura del campaing name
            Nomenclatura = result[1]
            CampaingID = result[0]
            searchObj = re.search(r'^(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.+&]+)_([a-zA-Z0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-Z-/.+]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ.+]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(19|2019)_([0-9,.]+)_(BA|AL|TR|TRRS|IN|DES|RV|CO)_([0-9,.]+)_(CPM|CPMA|CPVi|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCo)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([0-9,.-]+)(_B-)?([0-9]+)?(_S-)?([0-9]+)?(\(([0-9.)]+)\))?', Nomenclatura, re.M | re.I)
            if searchObj:
                #Posicion 19 Numero de Orden << Como pueden ver más de una por camp. se agrrega a un arreglo
                NomODCAR = searchObj.group(19)
                #Se realiza un split - para obtener todos los nummeros de orden
                NomODC = NomODCAR.split('-')
                #For para recorrer los numeros de orden
                for Nom in NomODC:
                    ODCb = False
                    #Poscion 11 inversion de la camp.
                    NomInversion = float(searchObj.group(11))
                    #Si el numero de orden es menor a 1 quiere decir que no esta asignado
                    if int(Nom) < 1:
                        Error = 'Implementado sin orden de compra'
                        Comentario = 'El numero de orden de compra no esta asignado.'
                        nuevo=[Error,Comentario,'OC','7',CampaingID,'0','ACTIVE','1','7',CampaingID]
                        Errores.append(nuevo)
                    else:
                        #De lo contrario se recorre el API de MTS para encontrar el numero de orden
                        for rowApi in ap:
                            if int(Nom) ==  int(rowApi['numero_orden']):
                                #Reviza primero si el flow_id del API esta ingresado correctamente
                                if not rowApi['flow_id'] or int(rowApi['flow_id'])==0:
                                    Error = 'Error no existe Flow asignado'
                                    Comentario = 'El flow id  no esta asignado.'
                                    nuevo=[Error,Comentario,'OC','12',CampaingID,'0','ACTIVE','0','12',CampaingID]
                                    Errores.append(nuevo)
                                    ODCb = True
                                    break
                                else:
                                    #De encontralo se realiza un For para buscar entre MediaFlowChar
                                    for rowMFC in data:
                                        Presb = False
                                        Codb = False
                                        #Se pregunta si el Flow id existe en MFC
                                        if int(rowApi['flow_id']) == rowMFC[2]:
                                            #De encontrase se reviza si el codigo de presupuesto es el mismo que en MTS
                                            if str(rowApi['codigo_presupuesto']) == rowMFC[1]:
                                                Presb = True
                                            #De igual manera se pregunta para el numero de orden
                                            if int(rowApi['numero_orden']) == rowMFC[0]:
                                                Codb = True
                                            Flowb = True
                                            break
                                        #Insersiones aun no implementadas en la plataforma (Estado 0)
                                        if not Flowb:
                                            Error = 'Error flow ' + str(rowApi['flow_id']) + '.'
                                            Comentario = 'Error el flow no se encontro'
                                            nuevo=[Error,Comentario,'OC','12',CampaingID,'0','ACTIVE','0','12',CampaingID]
                                            Errores.append(nuevo)
                                        if not Presb:
                                            Error = 'Error Presupuesto ' + str(rowApi['codigo_presupuesto']) + '.'
                                            Comentario = 'Error el Presupuesto no se encontro'
                                            nuevo=[Error,Comentario,'OC','12',CampaingID,'0','ACTIVE','0','12',CampaingID]
                                            Errores.append(nuevo)
                                        if not Codb:
                                            Error = 'Error orden de compra ' + str(rowApi['numero_orden']) + '.'
                                            Comentario = 'Error la orden de compra no se encontro'
                                            nuevo=[Error,Comentario,'OC','12',CampaingID,'0','ACTIVE','0', '12',CampaingID]
                                            Errores.append(nuevo)
                                        if Presb and Codb:
                                            if NomInversion >= float(rowApi['valor_total']):
                                                Error = 'Error de inversion '
                                                Comentario = 'Error la inversion supera lo planificado'
                                                nuevo=[Error,Comentario,'OC','12',CampaingID,'0','ACTIVE','0','12',CampaingID]
                                                Errores.append(nuevo)
                                    break
                        if not ODCb:
                            Error = 'Numero de Orden ' + str(Nom) + '.'
                            Comentario = 'Error el numero de orden Ingresado  no esta asigando al mismo presupuesto'
                            nuevo=[Error,Comentario,'OC','8',CampaingID,'0','ACTIVE','1','8',CampaingID]
                            Errores.append(nuevo)
        #cur.executemany(sqlInserErrors,Errores)
        print('Success')
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = "INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ('ComparacionErrores', 'Success', 'errosPresupuesto.py','{}');".format(dayhoy)
        cur.execute(sqlBitacora)
    except Exception as e:
        print(e)
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("ComparacionErrores", "{}", "errosPresupuesto.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print(datetime.now())


def reviewComparacion(conn):
    cur=conn.cursor(buffered=True)
    Errores=[]
    fechahoy = datetime.now()
    dayhoy = fechahoy.strftime("%Y-%m-%d")
    #Query para insertar los datos, Media  -> OC
    sqlInserErrors = "UPDATE `MediaPlatforms`.`ErrorsCampaings` SET `Estado` = '0' WHERE CampaingID=%s);"
    sqlCamping = "select Distinct c.CampaingID , sum(d.cost), c.Campaingname, date_format(c.Enddate,'%Y-%m-%d'),ifnull((select m.cost from CampaingMetrics m where m.CampaingID = c.CampaingID group by m.id desc limit 1  ),0) costo,a.Media from Campaings c inner join dailycampaing d on d.CampaingID = c.CampaingID inner join Accounts a on a.AccountsID = c.AccountsID inner join ErrorsCampaings ec on ec.CampaingID = c.CampaingID where ec.TipoErrorID = 15 and ec.Estado =1  group by c.CampaingID;"
    try:
        pass
    except expression as identifier:
        pass
    finally:
        pass

if __name__ == '__main__':
    openConnection()
    ComparacionErrores(conn)
    conn.close()
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






def reviewErrores(conn):
    global cur
    cur = conn.cursor()
    Comentario = ''
    Estatus = ''
    hoy = datetime.now().strftime("%Y-%m-%d")
    try:
        print (datetime.now())
        sqlConjuntosFB = """
        select distinct  cd.multiplestiposa 'InicioMfc', cd.multiplestiposb 'FinalMfc',ob.nombre objetivo, ifnull(cd.costo,ifnull(cd.multiplescostosb,bonificacion))   CostoMfc,
		pl.abreviatura mediomf, date_format(ca.StartDate,'%m/%d/%Y') InicioAd,
        date_format(ca.EndDate,'%m/%d/%Y') FinAd,ca.Campaignobjective,ca.Campaignlifetimebudget CostoAd,a.Media,ca.CampaingID,ca.Campaingname, dp.codigo paismf, adm.Country piasad, er.idErrorsCampaings,er.TipoErrorID
        from ErrorsCampaings er
        inner join Campaings ca on ca.CampaingID = er.CampaingID
        inner join mfcgt.mfccompradiaria cd on ca.Campaingname = cd.multiplestiposg
        inner join mfcgt.mfccampana fca on fca.id = cd.idcampana
        inner join mfcgt.mfc mf on fca.idmfc = mf.id
        inner join mfcgt.dpais dp on dp.id = mf.paisimplementar
        inner join Accounts a on a.AccountsID = ca.AccountsID
        inner join Adsets ad on ca.CampaingID = ad.CampaingID
        inner join AdSetMetrics adm on adm.AdSetID = ad.AdSetID
        inner join mfcgt.dformatodigital d on d.id = cd.idformatodigital
        inner join mfcgt.danuncio a on a.id = d.idanuncio
        inner join mfcgt.dmetrica me on me.id = a.idmetrica
        inner join mfcgt.dobjetivo ob on ob.id = me.idobjetivo
        inner join mfcgt.dplataforma pl on pl.id = ob.idplataforma
        where  ca.Campaignstatus in ('ACTIVE','enabled') and ca.EndDate > '{}' and er.Estado >0 ;
        """.format(hoy)
        slqUpdate = "UPDATE `ErrorsCampaings` SET `Estado` = %s WHERE (idErrorsCampaings =  %s ) ;"

        cur.execute(sqlConjuntosFB,)
        resultscon = cur.fetchall()
        Errores = []
        for result in resultscon:
            if result[15] == 16:
                if result[0] == result[5]:
                    res=(0,result[14])
                    Errores.append(res)
            if result[15] == 17:
                if result[1] == result[6]:
                    res=(0,result[14])
                    Errores.append(res)
            if result[15] == 2:
                if result[3] == result[8]:
                    res=(0,result[14])
                    Errores.append(res)
            if result[15] == 18:
                if result[7] != '':
                    if result[7] == 'POST_ENGAGEMENT':
                        if result[2] == 'INTERACCION':
                            res=(0,result[14])
                            Errores.append(res)
                    elif result[7] == 'REACH':
                        if result[2] == 'ALCANCE':
                            res=(0,result[14])
                            Errores.append(res)
                    elif result[7] == 'BRAND_AWARENESS':
                        if result[2] == 'AWARENESS':
                            res=(0,result[14])
                            Errores.append(res)
                    elif result[7] == 'CONVERSIONS' or result[7] == 'LEAD_GENERATION':
                        if result[2] == 'CONVERSION':
                            res=(0,result[14])
                            Errores.append(res)
                    elif result[7] == 'POST_ENGAGEMENT' or result[7] == 'EVENT_RESPONSES':
                        if result[2] == 'INTERACCION':
                            res=(0,result[14])
                            Errores.append(res)
                    elif result[7] == 'VIDEO_VIEWS' :
                        if result[2] == 'REPRODUCCION':
                            res=(0,result[14])
                            Errores.append(res)
                    elif result[7] == 'LINK_CLICKS' :
                        if result[2] == 'TRAFICO':
                            res=(0,result[14])
                            Errores.append(res)
            if result[15] == 19:
                if result[4] == 'FB' or result[4] == 'FBIG' or result[4] == 'IG':
                    if result[9] == 'FB':
                        res=(0,result[14])
                        Errores.append(res)
                elif result[4] == 'GO' or result[4] == 'YT' or result[4] == 'GM':
                    if result[9] == 'GO':
                        res=(0,result[14])
                        Errores.append(res)
            if result[15] == 6:
                if result[12] == result[13]:
                        res=(0,result[14])
                        Errores.append(res)


        cur.executemany(slqUpdate, Errores)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("put_estado_mfc", "Success", "pushReviewErrors.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
        cur.close()
    #ANALISIS IMPRESIONES Y
        #print(m.groups())
    except Exception as e:
        print(e)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("put_estado_mfc", "{}", "pushReviewErrors.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print('Success Nomenclatura')
        print (datetime.now())



def reviewErroresNomen(conn):
    global cur
    cur = conn.cursor()
    Comentario = ''
    Estatus = ''
    hoy = datetime.now().strftime("%Y-%m-%d")
    try:
        print (datetime.now())
        sqlConjuntosFB = """
        select err.idErrorsCampaings from ErrorsCampaings err
        inner join Campaings ca on ca.CampaingID = err.CampaingID
        inner join mfcgt.mfccompradiaria cd on ca.Campaingname = cd.multiplestiposg
        where err.Estado = 1;
        """
        slqUpdate = "UPDATE `ErrorsCampaings` SET `Estado` = %s WHERE (idErrorsCampaings =  %s ) ;"

        cur.execute(sqlConjuntosFB,)
        resultscon = cur.fetchall()
        Errores = []
        for result in resultscon:
            res=(0,result[0])
            Errores.append(res)


        cur.executemany(slqUpdate, Errores)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("put_estado_mfc", "Success", "pushReviewErrors.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
        cur.close()
    #ANALISIS IMPRESIONES Y
        #print(m.groups())
    except Exception as e:
        print(e)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("put_estado_mfc", "{}", "pushReviewErrors.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print('Success Nomenclatura')
        print (datetime.now())

if __name__ == '__main__':
   openConnection()
   reviewErrores(conn)
   reviewErroresNomen(conn)
   conn.close()

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
# host= 'localhost'
name = 'MediaPlatforms'
user = 'omgdev'
password = 'Sdev@2002!'
autocommit= 'True'


# name = 'MediaPlatforms'
# user = 'omgdev'
# password = 'Sdev@2002!'
# autocommit= 'True'
def openConnection():
    global conn
    try:
        conn = mysql.connect(host=host, database=name,
                             user=user, password=password, autocommit=autocommit)
    except:
        print("ERROR: NO SE PUEDO ESTABLECER CONEXION MYSQL.")
        sys.exit()



def errors_fb_inv(conn):
    global cur
    cur = conn.cursor()
    Comentario = ''
    Estatus = ''
    hoy = datetime.now().strftime("%Y-%m-%d")
    try:
        print (datetime.now())
        sqlConjuntosFB = """
        select ca.Campaingname,a.Media,ca.CampaingID from Campaings ca
        left outer join mfcgt.mfccompradiaria cd on ca.Campaingname = cd.multiplestiposg
        inner join Accounts a on a.AccountsID = ca.AccountsID
        where  ca.Campaignstatus in ('ACTIVE','enabled') and ca.EndDate > '{}' and isnull(cd.multiplestiposg) = true and a.Media <>'AF';
        """.format(hoy)
        sqlSelectErrors = "SELECT COUNT(*) FROM ErrorsCampaings where CampaingID=%s and TipoErrorID=%s and Media=%s"
        sqlInserErrors = "INSERT INTO ErrorsCampaings(Error,Comentario,Media,TipoErrorID,CampaingID,Impressions,StatusCampaing) VALUES (%s,%s,%s,%s,%s,%s,%s)"
        cur.execute(sqlConjuntosFB,)
        resultscon = cur.fetchall()
        Errores = []
        for result in resultscon:
            Comentario = "Error la nomenclatura implementada no se encuentra en el sistema de MFC"
            cur.execute(sqlSelectErrors, (result[2], 1, result[1]))
            rescampaing = cur.fetchone()
            if rescampaing[0] < 1:
                nuevoerror = (result[0], Comentario,
                                result[1], 1, result[2], 0, 'ACTIVE')
                Errores.append(nuevoerror)
        cur.executemany(sqlInserErrors, Errores)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("errors_fb_inv", "Success", "revision_errores_mfc.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
        cur.close()
    #ANALISIS IMPRESIONES Y
        #print(m.groups())
    except Exception as e:
        print(e)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("errors_fb_inv", "{}", "revision_errores_mfc.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print('Success Nomenclatura')
        print (datetime.now())


def errors_am__inv(conn):
    global cur
    cur = conn.cursor()
    Comentario = ''
    Estatus = ''
    hoy = datetime.now().strftime("%Y-%m-%d")
    try:
        print (datetime.now())
        sqlConjuntosFB = """
        select SUBSTRING_INDEX(Campaingname,' ',1) Campaingname,ca.CampaingID from Campaings ca
        left outer join mfcgt.mfccompradiaria cd on SUBSTRING_INDEX(ca.Campaingname,' ',1) = cd.multiplestiposg
        where  isnull(cd.multiplestiposg) = true;
        """
        sqlSelectErrors = "SELECT COUNT(*) FROM ErrorsCampaings where CampaingID=%s and TipoErrorID=%s and Media=%s"
        sqlInserErrors = "INSERT INTO ErrorsCampaings(Error,Comentario,Media,TipoErrorID,CampaingID,Impressions,StatusCampaing) VALUES (%s,%s,%s,%s,%s,%s,%s)"
        cur.execute(sqlConjuntosFB,)
        resultscon = cur.fetchall()
        Errores = []
        for result in resultscon:
            Comentario = "Error la nomenclatura implementada no se encuentra en el sistema de MFC"
            cur.execute(sqlSelectErrors, (result[1], 1, 'AM'))
            rescampaing = cur.fetchone()
            if rescampaing[0] < 1:
                nuevoerror = (result[0], Comentario,
                                'AM', 1, result[1], 0, 'ACTIVE')
                Errores.append(nuevoerror)
        cur.executemany(sqlInserErrors, Errores)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("errors_am_inv", "Success", "revision_errores_mfc.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
        cur.close()
    #ANALISIS IMPRESIONES Y
        #print(m.groups())
    except Exception as e:
        print(e)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("errors_am_inv", "{}", "revision_errores_mfc.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print('Success Nomenclatura')
        print (datetime.now())



def errors_mm__inv(conn):
    global cur
    cur = conn.cursor()
    Comentario = ''
    Estatus = ''
    hoy = datetime.now().strftime("%Y-%m-%d")
    try:
        print (datetime.now())
        sqlConjuntosFB = """
        select SUBSTRING_INDEX(Campaingname,' ',1) Campaingname,ca.CampaingID from Campaings ca
        left outer join mfcgt.mfccompradiaria cd on SUBSTRING_INDEX(ca.Campaingname,' ',1) = cd.multiplestiposg
        inner join Accounts a on a.AccountsID = ca.AccountsID
        where  isnull(cd.multiplestiposg) = true and a.Media <> 'AF';
        """
        sqlSelectErrors = "SELECT COUNT(*) FROM ErrorsCampaings where CampaingID=%s and TipoErrorID=%s and Media=%s"
        sqlInserErrors = "INSERT INTO ErrorsCampaings(Error,Comentario,Media,TipoErrorID,CampaingID,Impressions,StatusCampaing) VALUES (%s,%s,%s,%s,%s,%s,%s)"
        cur.execute(sqlConjuntosFB,)
        resultscon = cur.fetchall()
        Errores = []
        for result in resultscon:
            Comentario = "Error la nomenclatura implementada no se encuentra en el sistema de MFC"
            cur.execute(sqlSelectErrors, (result[1], 1, 'MM'))
            rescampaing = cur.fetchone()
            if rescampaing[0] < 1:
                nuevoerror = (result[0], Comentario,
                                'MM', 1, result[1], 0, 'ACTIVE')
                Errores.append(nuevoerror)
        cur.executemany(sqlInserErrors, Errores)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("errors_mm_inv", "Success", "revision_errores_mfc.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
        cur.close()
    #ANALISIS IMPRESIONES Y
        #print(m.groups())
    except Exception as e:
        print(e)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("errors_mm_inv", "{}", "revision_errores_mfc.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print('Success Nomenclatura')
        print (datetime.now())


def errors_AF(conn):
    global cur
    cur = conn.cursor()
    Comentario = ''
    Estatus = ''
    hoy = datetime.now().strftime("%Y-%m-%d")
    try:
        print (datetime.now())
        sqlConjuntosFB = """
        SELECT SUBSTRING_INDEX(Campaingname,'_',1) Campaingname from Campaings ca
        left outer join mfcgt.mfccompradiaria cd on SUBSTRING_INDEX(ca.Campaingname,'_',1) = cd.id
        inner join Accounts a on a.AccountsID = ca.AccountsID
        where  ca.Campaignstatus in ('ACTIVE','enabled') and ca.EndDate > '{}' and isnull(cd.multiplestiposg) = true
        and a.Media ='AF';""".format(hoy)
        sqlSelectErrors = "SELECT COUNT(*) FROM ErrorsCampaings where CampaingID=%s and TipoErrorID=%s and Media=%s"
        sqlInserErrors = "INSERT INTO ErrorsCampaings(Error,Comentario,Media,TipoErrorID,CampaingID,Impressions,StatusCampaing) VALUES (%s,%s,%s,%s,%s,%s,%s)"
        cur.execute(sqlConjuntosFB,)
        resultscon = cur.fetchall()
        Errores = []
        for result in resultscon:
            Comentario = "Error la nomenclatura implementada no se encuentra en el sistema de MFC"
            cur.execute(sqlSelectErrors, (result[2], 1, result[1]))
            rescampaing = cur.fetchone()
            if rescampaing[0] < 1:
                nuevoerror = (result[0], Comentario,
                                result[1], 1, result[2], 0, 'ACTIVE')
                Errores.append(nuevoerror)
        cur.executemany(sqlInserErrors, Errores)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("errors_fb_inv", "Success", "revision_errores_mfc.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
        cur.close()
    #ANALISIS IMPRESIONES Y
        #print(m.groups())
    except Exception as e:
        print(e)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("errors_fb_inv", "{}", "revision_errores_mfc.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print('Success Nomenclatura')
        print (datetime.now())



def errors_plataforma(conn):
    global cur
    cur = conn.cursor()
    Comentario = ''
    Estatus = ''
    hoy = datetime.now().strftime("%Y-%m-%d")
    try:
        print (datetime.now())
        sqlConjuntosFB = """
        select distinct  cd.multiplestiposa 'InicioMfc', cd.multiplestiposb 'FinalMfc',ob.nombre objetivo, cd.costo CostoMfc,pl.abreviatura mediomf, date_format(ca.StartDate,'%m/%d/%Y') InicioAd,
        date_format(ca.EndDate,'%m/%d/%Y') FinAd,ca.Campaignobjective,ca.Campaignlifetimebudget CostoAd,a.Media,ca.CampaingID,ca.Campaingname,dp.codigo paismf, adm.Country piasad, ca.Campaingbuyingtype
        from Campaings ca
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
        where  ca.Campaignstatus in ('ACTIVE','enabled') and ca.EndDate > '2020-01-06' and a.Media <> 'AF' ;
        """
        sqlSelectErrors = "SELECT COUNT(*) FROM ErrorsCampaings where CampaingID=%s and TipoErrorID=%s and Media=%s"
        sqlInserErrors = "INSERT INTO ErrorsCampaings(Error,Comentario,Media,TipoErrorID,CampaingID,Impressions,StatusCampaing) VALUES (%s,%s,%s,%s,%s,%s,%s)"
        cur.execute(sqlConjuntosFB,)
        resultscon = cur.fetchall()
        Errores = []
        for result in resultscon:
            if result[0] != result[5]:
                err = 'Error Fecha de Inicio'
                Comentario = "Error la Fecha de Inicio no coincide en plataforma"
                cur.execute(sqlSelectErrors, (result[10], 16, result[9]))
                rescampaing = cur.fetchone()
                if rescampaing[0] < 1:
                    nuevoerror = (err, Comentario,
                                    result[9], 16, result[10], 0, 'ACTIVE')
                    Errores.append(nuevoerror)
            if result[1] != result[6]:
                err = 'Error Fecha de Fin'
                Comentario = "Error la Fecha de Fin no coincide en plataforma"
                cur.execute(sqlSelectErrors, (result[10], 17, result[9]))
                rescampaing = cur.fetchone()
                if rescampaing[0] < 1:
                    nuevoerror = (err, Comentario,
                                    result[9], 17, result[10], 0, 'ACTIVE')
                    Errores.append(nuevoerror)
            if result[7] == 'BRAND_AWARENESS' and result[14] == 'RESERVED':
                if result[3] != result[8]:
                    err = 'Error Presupuesto Presupueto en MFC:' + str(result[3]) + 'en Plataforma:' + str(result[8])
                    Comentario = "Error el presupueto no coincide en plataforma. "
                    cur.execute(sqlSelectErrors, (result[10], 2, result[9]))
                    rescampaing = cur.fetchone()
                    if rescampaing[0] < 1:
                        nuevoerror = (err, Comentario,
                                        result[9], 2, result[10], 0, 'ACTIVE')
                        Errores.append(nuevoerror)
            if result[7] != '':
                if result[7] == 'POST_ENGAGEMENT':
                    if result[2] != 'INTERACCION':
                        err = 'Error de Objetivo'
                        Comentario = "Error el Objetivo no coincide en plataforma"
                        cur.execute(sqlSelectErrors, (result[10], 18, result[9]))
                        rescampaing = cur.fetchone()
                        if rescampaing[0] < 1:
                            nuevoerror = (err, Comentario,
                                            result[9], 18, result[10], 0, 'ACTIVE')
                            Errores.append(nuevoerror)
                elif result[7] == 'REACH':
                    if result[2] != 'ALCANCE':
                        err = 'Error de Objetivo'
                        Comentario = "Error el Objetivo no coincide en plataforma"
                        cur.execute(sqlSelectErrors, (result[10], 18, result[9]))
                        rescampaing = cur.fetchone()
                        if rescampaing[0] < 1:
                            nuevoerror = (err, Comentario,
                                            result[9], 18, result[10], 0, 'ACTIVE')
                            Errores.append(nuevoerror)
                elif result[7] == 'BRAND_AWARENESS':
                    if result[2] != 'AWARENESS':
                        err = 'Error de Objetivo'
                        Comentario = "Error el Objetivo no coincide en plataforma"
                        cur.execute(sqlSelectErrors, (result[10], 18, result[9]))
                        rescampaing = cur.fetchone()
                        if rescampaing[0] < 1:
                            nuevoerror = (err, Comentario,
                                            result[9], 18, result[8], 0, 'ACTIVE')
                            Errores.append(nuevoerror)
                elif result[7] == 'CONVERSIONS' or result[7] == 'LEAD_GENERATION':
                    if result[2] != 'CONVERSION':
                        err = 'Error de Objetivo'
                        Comentario = "Error el Objetivo no coincide en plataforma"
                        cur.execute(sqlSelectErrors, (result[10], 18, result[9]))
                        rescampaing = cur.fetchone()
                        if rescampaing[0] < 1:
                            nuevoerror = (err, Comentario,
                                            result[9], 18, result[10], 0, 'ACTIVE')
                            Errores.append(nuevoerror)
                elif result[7] == 'POST_ENGAGEMENT' or result[7] == 'EVENT_RESPONSES':
                    if result[2] != 'INTERACCION':
                        err = 'Error de Objetivo'
                        Comentario = "Error el Objetivo no coincide en plataforma"
                        cur.execute(sqlSelectErrors, (result[10], 18, result[9]))
                        rescampaing = cur.fetchone()
                        if rescampaing[0] < 1:
                            nuevoerror = (err, Comentario,
                                            result[9], 18, result[10], 0, 'ACTIVE')
                            Errores.append(nuevoerror)
                elif result[7] == 'VIDEO_VIEWS' :
                    if result[2] != 'REPRODUCCION':
                        err = 'Error de Objetivo'
                        Comentario = "Error el Objetivo no coincide en plataforma"
                        cur.execute(sqlSelectErrors, (result[10], 18, result[9]))
                        rescampaing = cur.fetchone()
                        if rescampaing[0] < 1:
                            nuevoerror = (err, Comentario,
                                            result[9], 18, result[10], 0, 'ACTIVE')
                            Errores.append(nuevoerror)
                elif result[7] == 'LINK_CLICKS' :
                    if result[2] != 'TRAFICO':
                        err = 'Error de Objetivo'
                        Comentario = "Error el Objetivo no coincide en plataforma"
                        cur.execute(sqlSelectErrors, (result[10], 18, result[9]))
                        rescampaing = cur.fetchone()
                        if rescampaing[0] < 1:
                            nuevoerror = (err, Comentario,
                                            result[9], 18, result[10], 0, 'ACTIVE')
                            Errores.append(nuevoerror)
            if result[4] == 'FB' or result[4] == 'FBIG' or result[4] == 'IG':
                if result[9] != 'FB':
                    err = 'Error Medio'
                    Comentario = "Error el medio no coincide en plataforma"
                    cur.execute(sqlSelectErrors, (result[10], 19, result[9]))
                    rescampaing = cur.fetchone()
                    if rescampaing[0] < 1:
                        nuevoerror = (err, Comentario,
                                        result[9], 19, result[10], 0, 'ACTIVE')
                        Errores.append(nuevoerror)
            elif result[4] == 'GO' or result[4] == 'YT' or result[4] == 'GM':
                if result[9] != 'GO':
                    err = 'Error Medio'
                    Comentario = "Error el medio no coincide en plataforma"
                    cur.execute(sqlSelectErrors, (result[10], 19, result[9]))
                    rescampaing = cur.fetchone()
                    if rescampaing[0] < 1:
                        nuevoerror = (err, Comentario,
                                        result[9], 19, result[10], 0, 'ACTIVE')
                        Errores.append(nuevoerror)
            elif result[4] == 'CHAPINTV' or result[4] == 'PL' or result[4] == 'TEA' or result[4] == 'CANAL12':
                if result[9] != 'GO':
                    err = 'Error Medio'
                    Comentario = "Error el medio no coincide en plataforma"
                    cur.execute(sqlSelectErrors, (result[10], 19, result[9]))
                    rescampaing = cur.fetchone()
                    if rescampaing[0] < 1:
                        nuevoerror = (err, Comentario,
                                        result[9], 19, result[10], 0, 'ACTIVE')
                        Errores.append(nuevoerror)
            if result[13]:
                if result[12] != result[13]:
                    err = 'Error de Pais Impresiones en MFC: ' + result[12] + 'Plataforma: ' + result[13]
                    Comentario = "Error el pais a implentar no coincide en plataforma: "
                    cur.execute(sqlSelectErrors, (result[10], 6, result[9]))
                    rescampaing = cur.fetchone()
                    if rescampaing[0] < 1:
                        nuevoerror = (err, Comentario,
                                        result[9], 6, result[10], 0, 'ACTIVE')
                        Errores.append(nuevoerror)


        cur.executemany(sqlInserErrors, Errores)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("errors_fb_inv", "Success", "revision_errores_mfc.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
        cur.close()
    #ANALISIS IMPRESIONES Y
        #print(m.groups())
    except Exception as e:
        print(e)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("errors_fb_inv", "{}", "revision_errores_mfc.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print('Success Nomenclatura')
        print (datetime.now())

def errors_plataforma_AF(conn):
    global cur
    cur = conn.cursor()
    Comentario = ''
    Estatus = ''
    hoy = datetime.now().strftime("%Y-%m-%d")
    try:
        print (datetime.now())
        sqlConjuntosFB = """
        select distinct  cd.multiplestiposa 'InicioMfc', cd.multiplestiposb 'FinalMfc',ob.nombre objetivo, cd.costo CostoMfc,pl.abreviatura mediomf, date_format(ca.StartDate,'%m/%d/%Y') InicioAd,
        date_format(ca.EndDate,'%m/%d/%Y') FinAd,ca.Campaignobjective,ca.Campaignlifetimebudget CostoAd,a.Media,ca.CampaingID,ca.Campaingname,dp.codigo paismf, adm.Country piasad, ca.Campaingbuyingtype
        from Campaings ca
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
        where  ca.Campaignstatus in ('ACTIVE','enabled') and ca.EndDate > '2020-01-06' and a.Media = 'AF' ;
        """
        sqlSelectErrors = "SELECT COUNT(*) FROM ErrorsCampaings where CampaingID=%s and TipoErrorID=%s and Media=%s"
        sqlInserErrors = "INSERT INTO ErrorsCampaings(Error,Comentario,Media,TipoErrorID,CampaingID,Impressions,StatusCampaing) VALUES (%s,%s,%s,%s,%s,%s,%s)"
        cur.execute(sqlConjuntosFB,)
        resultscon = cur.fetchall()
        Errores = []
        for result in resultscon:
            if result[0] != result[5]:
                err = 'Error Fecha de Inicio'
                Comentario = "Error la Fecha de Inicio no coincide en plataforma"
                cur.execute(sqlSelectErrors, (result[10], 16, result[9]))
                rescampaing = cur.fetchone()
                if rescampaing[0] < 1:
                    nuevoerror = (err, Comentario,
                                    result[9], 16, result[10], 0, 'ACTIVE')
                    Errores.append(nuevoerror)
            if result[1] != result[6]:
                err = 'Error Fecha de Fin'
                Comentario = "Error la Fecha de Fin no coincide en plataforma"
                cur.execute(sqlSelectErrors, (result[10], 17, result[9]))
                rescampaing = cur.fetchone()
                if rescampaing[0] < 1:
                    nuevoerror = (err, Comentario,
                                    result[9], 17, result[10], 0, 'ACTIVE')
                    Errores.append(nuevoerror)
            if result[7] == 'BRAND_AWARENESS' and result[14] == 'RESERVED':
                if result[3] != result[8]:
                    err = 'Error Presupuesto Presupueto en MFC:' + str(result[3]) + 'en Plataforma:' + str(result[8])
                    Comentario = "Error el presupueto no coincide en plataforma. "
                    cur.execute(sqlSelectErrors, (result[10], 2, result[9]))
                    rescampaing = cur.fetchone()
                    if rescampaing[0] < 1:
                        nuevoerror = (err, Comentario,
                                        result[9], 2, result[10], 0, 'ACTIVE')
                        Errores.append(nuevoerror)
            if result[7] != '':
                if result[7] == 'POST_ENGAGEMENT':
                    if result[2] != 'INTERACCION':
                        err = 'Error de Objetivo'
                        Comentario = "Error el Objetivo no coincide en plataforma"
                        cur.execute(sqlSelectErrors, (result[10], 18, result[9]))
                        rescampaing = cur.fetchone()
                        if rescampaing[0] < 1:
                            nuevoerror = (err, Comentario,
                                            result[9], 18, result[10], 0, 'ACTIVE')
                            Errores.append(nuevoerror)
                elif result[7] == 'REACH':
                    if result[2] != 'ALCANCE':
                        err = 'Error de Objetivo'
                        Comentario = "Error el Objetivo no coincide en plataforma"
                        cur.execute(sqlSelectErrors, (result[10], 18, result[9]))
                        rescampaing = cur.fetchone()
                        if rescampaing[0] < 1:
                            nuevoerror = (err, Comentario,
                                            result[9], 18, result[10], 0, 'ACTIVE')
                            Errores.append(nuevoerror)
                elif result[7] == 'BRAND_AWARENESS':
                    if result[2] != 'AWARENESS':
                        err = 'Error de Objetivo'
                        Comentario = "Error el Objetivo no coincide en plataforma"
                        cur.execute(sqlSelectErrors, (result[10], 18, result[9]))
                        rescampaing = cur.fetchone()
                        if rescampaing[0] < 1:
                            nuevoerror = (err, Comentario,
                                            result[9], 18, result[8], 0, 'ACTIVE')
                            Errores.append(nuevoerror)
                elif result[7] == 'CONVERSIONS' or result[7] == 'LEAD_GENERATION':
                    if result[2] != 'CONVERSION':
                        err = 'Error de Objetivo'
                        Comentario = "Error el Objetivo no coincide en plataforma"
                        cur.execute(sqlSelectErrors, (result[10], 18, result[9]))
                        rescampaing = cur.fetchone()
                        if rescampaing[0] < 1:
                            nuevoerror = (err, Comentario,
                                            result[9], 18, result[10], 0, 'ACTIVE')
                            Errores.append(nuevoerror)
                elif result[7] == 'POST_ENGAGEMENT' or result[7] == 'EVENT_RESPONSES':
                    if result[2] != 'INTERACCION':
                        err = 'Error de Objetivo'
                        Comentario = "Error el Objetivo no coincide en plataforma"
                        cur.execute(sqlSelectErrors, (result[10], 18, result[9]))
                        rescampaing = cur.fetchone()
                        if rescampaing[0] < 1:
                            nuevoerror = (err, Comentario,
                                            result[9], 18, result[10], 0, 'ACTIVE')
                            Errores.append(nuevoerror)
                elif result[7] == 'VIDEO_VIEWS' :
                    if result[2] != 'REPRODUCCION':
                        err = 'Error de Objetivo'
                        Comentario = "Error el Objetivo no coincide en plataforma"
                        cur.execute(sqlSelectErrors, (result[10], 18, result[9]))
                        rescampaing = cur.fetchone()
                        if rescampaing[0] < 1:
                            nuevoerror = (err, Comentario,
                                            result[9], 18, result[10], 0, 'ACTIVE')
                            Errores.append(nuevoerror)
                elif result[7] == 'LINK_CLICKS' :
                    if result[2] != 'TRAFICO':
                        err = 'Error de Objetivo'
                        Comentario = "Error el Objetivo no coincide en plataforma"
                        cur.execute(sqlSelectErrors, (result[10], 18, result[9]))
                        rescampaing = cur.fetchone()
                        if rescampaing[0] < 1:
                            nuevoerror = (err, Comentario,
                                            result[9], 18, result[10], 0, 'ACTIVE')
                            Errores.append(nuevoerror)
            if result[4] == 'FB' or result[4] == 'FBIG' or result[4] == 'IG':
                if result[9] != 'FB':
                    err = 'Error Medio'
                    Comentario = "Error el medio no coincide en plataforma"
                    cur.execute(sqlSelectErrors, (result[10], 19, result[9]))
                    rescampaing = cur.fetchone()
                    if rescampaing[0] < 1:
                        nuevoerror = (err, Comentario,
                                        result[9], 19, result[10], 0, 'ACTIVE')
                        Errores.append(nuevoerror)
            elif result[4] == 'GO' or result[4] == 'YT' or result[4] == 'GM':
                if result[9] != 'GO':
                    err = 'Error Medio'
                    Comentario = "Error el medio no coincide en plataforma"
                    cur.execute(sqlSelectErrors, (result[10], 19, result[9]))
                    rescampaing = cur.fetchone()
                    if rescampaing[0] < 1:
                        nuevoerror = (err, Comentario,
                                        result[9], 19, result[10], 0, 'ACTIVE')
                        Errores.append(nuevoerror)
            elif result[4] == 'CHAPINTV' or result[4] == 'PL' or result[4] == 'TEA' or result[4] == 'CANAL12':
                if result[9] != 'GO':
                    err = 'Error Medio'
                    Comentario = "Error el medio no coincide en plataforma"
                    cur.execute(sqlSelectErrors, (result[10], 19, result[9]))
                    rescampaing = cur.fetchone()
                    if rescampaing[0] < 1:
                        nuevoerror = (err, Comentario,
                                        result[9], 19, result[10], 0, 'ACTIVE')
                        Errores.append(nuevoerror)
            if result[13]:
                if result[12] != result[13]:
                    err = 'Error de Pais Impresiones en MFC: ' + result[12] + 'Plataforma: ' + result[13]
                    Comentario = "Error el pais a implentar no coincide en plataforma: "
                    cur.execute(sqlSelectErrors, (result[10], 6, result[9]))
                    rescampaing = cur.fetchone()
                    if rescampaing[0] < 1:
                        nuevoerror = (err, Comentario,
                                        result[9], 6, result[10], 0, 'ACTIVE')
                        Errores.append(nuevoerror)


        cur.executemany(sqlInserErrors, Errores)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("errors_fb_inv", "Success", "revision_errores_mfc.py","{}");'.format(dayhoy)
        cur.execute(sqlBitacora)
        cur.close()
    #ANALISIS IMPRESIONES Y
        #print(m.groups())
    except Exception as e:
        print(e)
        fechahoy = datetime.now()
        dayhoy = fechahoy.strftime("%Y-%m-%d %H:%M:%S")
        sqlBitacora = 'INSERT INTO `MediaPlatforms`.`bitacora` (`Operacion`, `Resultado`, `Documento`, `CreateDate`) VALUES ("errors_fb_inv", "{}", "revision_errores_mfc.py","{}");'.format(e,dayhoy)
        cur.execute(sqlBitacora)
    finally:
        print('Success Nomenclatura')
        print (datetime.now())



if __name__ == '__main__':
   openConnection()
   errors_fb_inv(conn)
   errors_mm__inv(conn)
   errors_plataforma(conn)
   conn.close()

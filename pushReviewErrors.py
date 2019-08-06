# -*- coding: UTF-8 -*-
import json
import requests
import sys
import re
import mysql.connector as mysql
from datetime import datetime
import time


def openConnection():
    global conn
    try:
        conn = mysql.connect(host='3.95.117.169',database='MediaPlatforms',user='omgdev',password='Sdev@2002!',autocommit=True)
    except:
        print("ERROR: NO SE PUEDO ESTABLECER CONEXION MYSQL.")
        sys.exit()


def errors_fb_inv(conn):
    global cur
    cur = conn.cursor()
    Comentario = ''
    Estatus = ''
    try:
        sqlConjuntosFB = "select a.AccountsID,a.Account, b.CampaingID,b.Campaingname, b.Campaignspendinglimit,b.Campaigndailybudget,b.Campaignlifetimebudget,c.AdSetID,c.Adsetname,c.Adsetlifetimebudget,SUM(c.Adsetlifetimebudget) as tlotalconjungo,c.Adsetdailybudget,a.Media,b.Campaignstatus,b.Campaignstatus,c.Status from Accounts a INNER JOIN Campaings b on a.AccountsID=b.AccountsID INNER JOIN  Adsets c on b.CampaingID=c.CampaingID where a.Media='FB' group by b.CampaingID  desc "
        sqlInserErrors = "INSERT INTO ErrorsCampaings(Error,Comentario,Media,TipoErrorID,CampaingID,Impressions,StatusCampaing,CreateDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
        sqlSelectErrors = "SELECT COUNT(*) FROM ErrorsCampaings where CampaingID=%s and TipoErrorID=%s and Media=%s"
        cur.execute(sqlConjuntosFB,)
        resultscon = cur.fetchall()
        Errores = []
        for result in resultscon:
            StatusCampaing = result[13]
            StatusAdsets = result[14]
            if StatusCampaing != 'PAUSED':
                if StatusAdsets == 'PAUSED':
                    Estatus = StatusAdsets
                else:
                    Estatus = StatusAdsets
            else:
                Estatus = StatusCampaing

            Nomenclatura = result[3].encode('utf-8')
            CampaingID = result[2]

            searchObj = re.search(r'^(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.+&]+)_([a-zA-Z0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-Z-/.+]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ.+]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(19)_([0-9,.]+)_(BA|AL|TR|TRRS|IN|DES|RV|CO)_([0-9,.]+)_(CPM|CPMA|CPVi|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCo)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([0-9,.-]+)(_B-)?([0-9]+)?(_S-)?([0-9]+)?(\(([0-9.)]+)\))?', Nomenclatura, re.M | re.I)
            if searchObj:
                NomInversion = float(searchObj.group(11))
                if result[4] == 0:
                    if float(result[6]) > NomInversion:
                        Error = 'Planificado: ' + \
                            str(NomInversion) + '/ Plataforma: ' + \
                            str(float(result[6]))
                        Comentario = "Cuidado error de inversion verifica la plataforma"
                        cur.execute(sqlSelectErrors, (CampaingID, 2, 'FB'))
                        rescampaing = cur.fetchone()
                        if rescampaing[0] == 0:

                                nuevoerror = (Error, Comentario,
                                              'FB', 2, CampaingID, 0, Estatus,datetime.now())
                                Errores.append(nuevoerror)
                    if searchObj.group(25) > 0:
                        Acumulado = float(result[6])-float(searchObj.group(25))
                        if Acumulado > NomInversion:
                            Error = 'Planificado: ' + \
                                str(NomInversion) + \
                                '/ Plataforma: '+str(Acumulado)
                            Comentario = "Error de inversion no debe ser mayor a la planificada"
                            cur.execute(sqlSelectErrors, (CampaingID, 2, 'FB'))
                            rescampaing = cur.fetchone()
                            if rescampaing[0] == 0:
                                if CampaingID != '':
                                    nuevoerror = (
                                        Error, Comentario, 'FB', 2, CampaingID, 0, Estatus,datetime.now())
                                    Errores.append(nuevoerror)

                    elif float(result[10]) > NomInversion:
                        Error = 'Planificado: ' + \
                            str(NomInversion) + '/ Plataforma: ' + \
                            str(float(result[10]))
                        Comentario = "Cuidado error de inversion de conjuntos mayor que la planificada "
                        cur.execute(sqlSelectErrors, (CampaingID, 4, 'FB'))
                        rescampaing = cur.fetchone()
                        if rescampaing[0] == 0:
                            if CampaingID != '':
                                nuevoerror = (Error, Comentario,
                                              'FB', 4, CampaingID, 0, Estatus,datetime.now())
                                Errores.append(nuevoerror)

                    elif float(result[5]) > 0:
                        Error = 'Inversiion Diaria: '+str(float(result[5]))
                        Comentario = "Cuidado error de inversion diaria "
                        cur.execute(sqlSelectErrors, (CampaingID, 3, 'FB'))
                        rescampaing = cur.fetchone()
                        if rescampaing[0] == 0:
                            if CampaingID != '':
                                nuevoerror = (Error, Comentario,
                                              'FB', 3, CampaingID, 0, Estatus,datetime.now())
                                Errores.append(nuevoerror)

                    elif float(result[11]) > 0:
                        Error = 'Inversiion Diaria Conjunto: ' + \
                            str(float(result[11]))
                        Comentario = "Error de inversion diaria del conjunto de anuncios no debe ser mayor a la planificada"
                        cur.execute(sqlSelectErrors, (CampaingID, 5, 'FB'))
                        rescampaing = cur.fetchone()
                        if rescampaing[0] == 0:
                            if CampaingID != '':
                                nuevoerror = (Error, Comentario,
                                              'FB', 5, CampaingID, 0, Estatus,datetime.now())
                                Errores.append(nuevoerror)
            else:
                Comentario = "Error de nomenclatura verifica cada uno de sus elementos"
                cur.execute(sqlSelectErrors, (CampaingID, 1, 'FB'))
                rescampaing = cur.fetchone()
                if rescampaing[0] < 1:
                    if CampaingID != '':
                        nuevoerror = (Nomenclatura, Comentario,
                                      'FB', 1, CampaingID, 0, Estatus,datetime.now())
                        Errores.append(nuevoerror)

        cur.executemany(sqlInserErrors, Errores)
        cur.close()
    #ANALISIS IMPRESIONES Y
        #print(m.groups())
    except Exception as e:
        print(e)
    finally:
        print('Success Errores FB Inversion Comprobados')


def errors_fb_pais(conn):
    global cur
    cur = conn.cursor()
    print (datetime.now())
    Comentario = ''
    Estatus = ''
    a=0;
    try:
        sqlCampaingsFB = "select a.AccountsID,a.Account,a.Media,b.CampaingID,b.Campaingname,b.Campaignspendinglimit,b.Campaigndailybudget,b.Campaignlifetimebudget, c.AdSetID,c.Adsetname,c.Adsetlifetimebudget,c.Adsetdailybudget, d.AdID,d.Adname,d.country,d.CreateDate,e.Impressions,b.Campaignstatus,d.Adstatus from Accounts a INNER JOIN Campaings b on a.AccountsID=b.AccountsID INNER JOIN  Adsets c on b.CampaingID=c.CampaingID INNER JOIN Ads d on d.AdSetID=c.AdSetID INNER JOIN MetricsAds e on e.AdID=d.AdID where a.Media='FB' group by d.Adname, d.Country ORDER BY d.CreateDate desc"
        sqlInserErrors = "INSERT INTO ErrorsCampaings(Error,Comentario,Media,TipoErrorID,CampaingID,Impressions,StatusCampaing,CreateDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
        sqlSelectErrors = "SELECT COUNT(*) FROM ErrorsCampaings where CampaingID=%s and TipoErrorID=%s and Media=%s"
        Errores = []
        cur.execute(sqlCampaingsFB,)
        results = cur.fetchall()
        for result in results:
            Nomenclatura = result[4].encode('utf-8')
            CampaingIDS = result[3]
            Impressions = result[16]
            StatusCampaing = result[17]
            StatusAdsets = result[18]
            if StatusCampaing != 'PAUSED':
                if StatusAdsets == 'PAUSED':
                    Estatus = 'PAUSED'
                else:
                    Estatus = StatusAdsets
            else:
                Estatus = StatusCampaing
            #VALORES NOMENCLATURA
            searchObj = re.search(r'^(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.+&]+)_([a-zA-Z0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-Z-/.+]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ.+]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(19)_([0-9,.]+)_(BA|AL|TR|TRRS|IN|DES|RV|CO)_([0-9,.]+)_(CPM|CPMA|CPVi|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCo)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([0-9,.-]+)(_B-)?([0-9]+)?(_S-)?([0-9]+)?(\(([0-9.)]+)\))?', Nomenclatura, re.M | re.I)
            if searchObj:
                NomPais = searchObj.group(1)
                NomCliente = searchObj.group(2)
                NomProducto = searchObj.group(4)
                if NomCliente == 'CCPRADERA':
                        if NomProducto == 'HUE':
                            if result[14] == 'MX':
                                a += 1
                        if NomProducto == 'CHIQ':
                            if result[14] == 'GT':
                                a += 1
                            elif result[14] == 'HN':
                                a += 1
                        if a == 0:
                            Error = result[14]
                            TipoErrorID = 6
                            Comentario = "Error de paises se estan imprimiendo anuncios en otros paises"
                            cur.execute(sqlSelectErrors,
                                        (CampaingIDS, TipoErrorID, Media))
                            rescampaing = cur.fetchone()
                            if rescampaing[0] < 1:
                                if CampaingIDS != '':
                                    nuevoerror = (
                                        Error, Comentario, 'FB', TipoErrorID, CampaingIDS, Impressions, Estatus,datetime.now())
                                    Errores.append(nuevoerror)
                if NomCliente != 'CCR' and NomCliente != 'CCPRADERA':

                    if result[14] != NomPais:
                        if result[14] == 'PE' and NomPais != 'PR' or result[14] == 'DO' and NomPais != 'RD' or result[14] == 'PA' and NomPais != 'PN':
                            Error = result[14]
                            TipoErrorID = 6
                            Comentario = "Error de paises se estan imprimiendo anuncios en otros paises"
                            cur.execute(sqlSelectErrors,
                                        (CampaingIDS, TipoErrorID, 'FB'))
                            rescampaing = cur.fetchone()
                            if rescampaing[0] < 1:
                                if CampaingIDS != '':
                                    nuevoerror = (
                                        Error, Comentario, 'FB', TipoErrorID, CampaingIDS, Impressions, Estatus,datetime.now())
                                    Errores.append(nuevoerror)

        cur.executemany(sqlInserErrors, Errores)
    #ANALISIS IMPRESIONES Y
        #print(m.groups())
    except Exception as e:
        print(e)
    finally:
        print('Success Errores FB Pais Comprobados')


def errors_go(conn):
    global cur
    cur = conn.cursor()

    print (datetime.now())
    Comentario = ''

    try:
        sqlCampaingsGO = "select a.AccountsID,a.Account,a.Media,b.CampaingID,b.Campaingname,b.Campaignspendinglimit,b.Campaigndailybudget,b.Campaignlifetimebudget, c.AdSetID,c.Adsetname,c.Adsetlifetimebudget,c.Adsetdailybudget, d.AdID,d.Adname,d.CreateDate,b.Campaignstatus,b.Campaignstatus,c.Status from Accounts a INNER JOIN Campaings b on a.AccountsID=b.AccountsID INNER JOIN  Adsets c on b.CampaingID=c.CampaingID INNER JOIN Ads d on d.AdSetID=c.AdSetID  where a.Media='GO'    group by b.CampaingID ORDER BY d.CreateDate desc "
        sqlInserErrors = "INSERT INTO ErrorsCampaings(Error,Comentario,Media,TipoErrorID,CampaingID,Impressions,StatusCampaing,CreateDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
        sqlSelectErrors = "SELECT COUNT(*) FROM ErrorsCampaings where CampaingID=%s and TipoErrorID=%s and Media=%s"
        Errores = []

        Estatus = ''
        cur.execute(sqlCampaingsGO,)
        results = cur.fetchall()
        for result in results:
            Nomenclatura = result[4].encode('utf-8')
            Media = result[2]
            CampaingID = result[3]
            #VALORES NOMENCLATURA
            StatusCampaing = result[15]
            StatusAdsets = result[16]
            StatusAds = result[17]
            if StatusCampaing != 'enabled':
                if StatusAdsets != 'enabled':
                        if StatusAds != 'enabled':
                            Estatus = StatusAdsets
                        else:
                            Estatus = StatusAdsets
                else:
                    Estatus = StatusAdsets
            else:
                Estatus = StatusCampaing

            searchObj = re.search(r'^(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.+&]+)_([a-zA-Z0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-Z-/.+]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ.+]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(19)_([0-9,.]+)_(BA|AL|TR|TRRS|IN|DES|RV|CO)_([0-9,.]+)_(CPM|CPMA|CPVi|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCo)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([0-9,.-]+)(_B-)?([0-9]+)?(_S-)?([0-9]+)?(\(([0-9.)]+)\))?', Nomenclatura, re.M | re.I)
            if searchObj:
                a = 1
            else:
                Error = Nomenclatura
                TipoErrorID = 1
                Comentario = "Error de nomenclatura verifica cada uno de sus elementos"
                cur.execute(sqlSelectErrors, (CampaingID, TipoErrorID, Media))
                rescampaing = cur.fetchone()
                if rescampaing[0] < 1:
                    if CampaingID != '':
                        nuevoerror = (Error, Comentario, Media,
                                      TipoErrorID, CampaingID, 0, Estatus,datetime.now())
                        Errores.append(nuevoerror)

        cur.executemany(sqlInserErrors, Errores)

    #ANALISIS IMPRESIONES Y
        #print(m.groups())
    except Exception as e:
        print(e)
    finally:
        print('Success Errores GO Comprobados')
#FIN VISTA


def errors_tw(conn):
    global cur
    cur = conn.cursor()
    print (datetime.now())
    Comentario = ''
    Errores = []
    try:
        sqlCampaingsTW = "select a.AccountsID,a.Account,a.Media,b.CampaingID,b.Campaingname,b.Campaignspendinglimit,b.Campaigndailybudget,b.Campaignlifetimebudget,e.Impressions,b.Campaignstatus from Accounts a INNER JOIN Campaings b on a.AccountsID=b.AccountsID  INNER JOIN CampaingMetrics e on b.CampaingID = e.CampaingID where a.Media='TW'  group by b.CampaingID ORDER BY a.CreateDate desc LIMIT 50000000000"
        sqlInserErrors = "INSERT INTO ErrorsCampaings(Error,Comentario,Media,TipoErrorID,CampaingID,Impressions,StatusCampaing,CreateDate) VALUES (%s,%s,%s,%s,%s,%s,%s)"
        sqlSelectErrors = "SELECT COUNT(*) FROM ErrorsCampaings where CampaingID=%s and TipoErrorID=%s and Media=%s"
        cur.execute(sqlCampaingsTW,)
        results = cur.fetchall()
        for result in results:
            Nomenclatura = result[4].encode('utf-8')
            Media = result[2]
            CampaingID = result[3]
            #Impressions=result[16]
            #VALORES NOMENCLATURA
            if result[8] > 0:

                searchObj = re.search(r'^(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.+&]+)_([a-zA-Z0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-Z-/.+]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ.+]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(19)_([0-9,.]+)_(BA|AL|TR|TRRS|IN|DES|RV|CO)_([0-9,.]+)_(CPM|CPMA|CPVi|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCo)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([0-9,.-]+)(_B-)?([0-9]+)?(_S-)?([0-9]+)?(\(([0-9.)]+)\))?', Nomenclatura, re.M | re.I)
                if searchObj:
                    a = 1
                else:
                    Error = Nomenclatura
                    TipoErrorID = 1
                    cur.execute(sqlSelectErrors,
                                (CampaingID, TipoErrorID, Media))
                    rescampaing = cur.fetchone()
                    if rescampaing[0] < 1:
                        if CampaingID != '':
                            nuevoerror = (Error, Comentario, Media,
                                          TipoErrorID, CampaingID, 0,'ACTIVE',datetime.now())
                            Comentario = "Error de nomenclatura verifica cada uno de sus elementos"
                            Errores.append(nuevoerror)

        cur.executemany(sqlInserErrors, Errores)

    #ANALISIS IMPRESIONES Y
        #print(m.groups())
    except Exception as e:
        print(e)
    finally:
        print('Success Errores TW Comprobados')


def errors_mm_inv(conn):
    global cur
    cur = conn.cursor()
    print (datetime.now())
    Comentario = ''
    Estatus = ''
    try:
        sqlConjuntosFB = "select a.AccountsID,a.Account, b.CampaingID,b.Campaingname, b.Campaignspendinglimit,b.Campaigndailybudget,b.Campaignlifetimebudget,c.AdSetID,c.Adsetname,c.Adsetlifetimebudget,SUM(c.Adsetlifetimebudget) as tlotalconjungo,c.Adsetdailybudget,a.Media,b.Campaignstatus,b.Campaignstatus,c.Status from Accounts a INNER JOIN Campaings b on a.AccountsID=b.AccountsID INNER JOIN  Adsets c on b.CampaingID=c.CampaingID where a.Media='MM' group by b.CampaingID  desc "
        sqlInserErrors = "INSERT INTO ErrorsCampaings(Error,Comentario,Media,TipoErrorID,CampaingID,Impressions,StatusCampaing,CreateDate) VALUES (%s,%s,%s,%s,%s,%s,%s)"
        sqlSelectErrors = "SELECT COUNT(*) FROM ErrorsCampaings where CampaingID=%s and TipoErrorID=%s and Media=%s"
        cur.execute(sqlConjuntosFB,)
        resultscon = cur.fetchall()
        Errores = []
        for result in resultscon:
            Estatus = 'ACTIVE'
            Nomenclatura = result[3].encode('utf-8')
            CampaingID = result[2]
            searchObj = re.search(r'^(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.+&]+)_([a-zA-Z0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-Z-/.+]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ.+]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(19)_([0-9,.]+)_(BA|AL|TR|TRRS|IN|DES|RV|CO)_([0-9,.]+)_(CPM|CPMA|CPVi|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCo)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([0-9,.-]+)(_B-)?([0-9]+)?(_S-)?([0-9]+)?(\(([0-9.)]+)\))?', Nomenclatura, re.M | re.I)
            if searchObj:
                NomInversion = float(searchObj.group(11))
                if not result[4] or result[4]==0:
                    if float(result[6]) > NomInversion:
                        Error = 'Planificado: ' + \
                            str(NomInversion) + '/ Plataforma: ' + \
                            str(float(result[6]))
                        Comentario = "Cuidado error de inversion verifica la plataforma"
                        cur.execute(sqlSelectErrors, (CampaingID, 2, 'MM'))
                        rescampaing = cur.fetchone()
                        if rescampaing[0] == 0:
                            if CampaingID != '':
                                nuevoerror = (Error, Comentario,
                                              'MM', 2, CampaingID, 0, Estatus,datetime.now())
                                Errores.append(nuevoerror)
                    if searchObj.group(25) is not None:
                        if searchObj.group(25) > 0:
                            Acumulado = float(searchObj.group(25))-float(result[5])
                            if Acumulado > NomInversion:
                                Error = 'Planificado: ' + \
                                    str(NomInversion) + \
                                    '/ Plataforma: '+str(Acumulado)
                                Comentario = "Error de inversion no debe ser mayor a la planificada"
                                cur.execute(sqlSelectErrors, (CampaingID, 2, 'MM'))
                                rescampaing = cur.fetchone()
                                if rescampaing[0] == 0:
                                    if CampaingID != '':
                                        nuevoerror = (
                                            Error, Comentario, 'MM', 2, CampaingID, 0, Estatus,datetime.now())
                                        Errores.append(nuevoerror)
                    elif result[10] is not None:
                        if float(result[10]) > NomInversion:
                            Error = 'Planificado: ' + \
                                str(NomInversion) + '/ Plataforma: ' + \
                                str(float(result[10]))
                            Comentario = "Cuidado error de inversion de conjuntos mayor que la planificada "
                            cur.execute(sqlSelectErrors, (CampaingID, 4, 'MM'))
                            rescampaing = cur.fetchone()
                            if rescampaing[0] == 0:
                                if CampaingID != '':
                                    nuevoerror = (Error, Comentario,
                                                'MM', 4, CampaingID, 0, Estatus,datetime.now())
                                    Errores.append(nuevoerror)
                    elif result[10] is not None:
                        if float(result[5]) > 0:
                            Error = 'Inversiion Diaria: '+str(float(result[5]))
                            Comentario = "Cuidado error de inversion diaria "
                            cur.execute(sqlSelectErrors, (CampaingID, 3, 'MM'))
                            rescampaing = cur.fetchone()
                            if rescampaing[0] == 0:
                                if CampaingID != '':
                                    nuevoerror = (Error, Comentario,
                                                'MM', 3, CampaingID, 0, Estatus,datetime.now())
                                    Errores.append(nuevoerror)
                    elif result[10] is not None:
                        if float(result[11]) > 0:
                            Error = 'Inversiion Diaria Conjunto: ' + \
                                str(float(result[11]))
                            Comentario = "Error de inversion diaria del conjunto de anuncios no debe ser mayor a la planificada"
                            cur.execute(sqlSelectErrors, (CampaingID, 5, 'MM'))
                            rescampaing = cur.fetchone()
                            if rescampaing[0] == 0:
                                if CampaingID != '':
                                    nuevoerror = (Error, Comentario,
                                                'MM', 5, CampaingID, 0, Estatus, datetime.now())
                                    Errores.append(nuevoerror)
            else:
                Comentario = "Error de nomenclatura verifica cada uno de sus elementos"
                cur.execute(sqlSelectErrors, (CampaingID, 1, 'MM'))
                rescampaing = cur.fetchone()
                if rescampaing[0] < 1:
                    if CampaingID != '':
                        nuevoerror = (Nomenclatura, Comentario,
                                      'MM', 1, CampaingID, 0, Estatus,datetime.now())
                        Errores.append(nuevoerror)

        cur.executemany(sqlInserErrors, Errores)
    #ANALISIS IMPRESIONES Y
    except Exception as e:
        print(e)
    finally:
        print('Success Errores MM Inversion Comprobados')


def errors_af(conn):
    global cur
    cur = conn.cursor()
    print (datetime.now())
    Comentario = ''
    Estatus = ''
    try:
        sqlConjuntosFB = "select a.AccountsID, a.Account, b.CampaingID, b.Campaingname, b.Campaignspendinglimit, b.Campaigndailybudget, b.Campaignlifetimebudget, c.AdSetID,c.Adsetname, c.Adsetlifetimebudget, SUM(c.Adsetlifetimebudget) as tlotalconjungo, c.Adsetdailybudget, a.Media, b.Campaignstatus, b.Campaignstatus, d.ReferrerType, d.Media, c.Status from Accounts a INNER JOIN Campaings b on a.AccountsID=b.AccountsID INNER JOIN  Adsets c on b.CampaingID=c.CampaingID INNER JOIN  Ads d on c.AdSetID=d.AdSetID where a.Media='AF' group by b.CampaingID  desc  LIMIT 50000000"
        sqlInserErrors = "INSERT INTO ErrorsCampaings(Error,Comentario,Media,TipoErrorID,CampaingID,Impressions,StatusCampaing,CreateDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s)"
        sqlSelectErrors = "SELECT COUNT(*) FROM ErrorsCampaings where CampaingID=%s and TipoErrorID=%s and Media=%s"
        cur.execute(sqlConjuntosFB,)
        resultscon = cur.fetchall()
        Errores = []
        for result in resultscon:
            Estatus = 'ACTIVE'
            Nomenclatura = result[3].encode('utf-8')
            CampaingID = result[2]
            searchObj = re.search(r'^(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.+&]+)_([a-zA-Z0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-Z-/.+]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ.+]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(19)_([0-9,.]+)_(BA|AL|TR|TRRS|IN|DES|RV|CO)_([0-9,.]+)_(CPM|CPMA|CPVi|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCo)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([0-9,.-]+)(_B-)?([0-9]+)?(_S-)?([0-9]+)?(\(([0-9.)]+)\))?', Nomenclatura, re.M | re.I)
            if searchObj:
                NomInversion = float(searchObj.group(11))

                if result[4] == 0:
                    if float(result[6]) > NomInversion:
                        Error = 'Planificado: ' + \
                            str(NomInversion) + '/ Plataforma: ' + \
                            str(float(result[6]))
                        Comentario = "Cuidado error de inversion verifica la plataforma"
                        cur.execute(sqlSelectErrors, (CampaingID, 2, 'AF'))
                        rescampaing = cur.fetchone()
                        if rescampaing[0] == 0:
                            if CampaingID != '':
                                nuevoerror = (Error, Comentario,
                                              'MM', 2, CampaingID, 0, Estatus,datetime.now())
                                Errores.append(nuevoerror)

                    if searchObj.group(25) > 0:
                        Acumulado = float(searchObj.group(25))-float(result[5])
                        if Acumulado > NomInversion:
                            Error = 'Planificado: ' + \
                                str(NomInversion) + \
                                '/ Plataforma: '+str(Acumulado)
                            Comentario = "Error de inversion no debe ser mayor a la planificada"
                            cur.execute(sqlSelectErrors, (CampaingID, 2, 'AF'))
                            rescampaing = cur.fetchone()
                            if rescampaing[0] == 0:
                                if CampaingID != '':
                                    nuevoerror = (
                                        Error, Comentario, 'AF', 2, CampaingID, 0, Estatus,datetime.now())
                                    Errores.append(nuevoerror)

                    elif float(result[10]) > NomInversion:
                        Error = 'Planificado: ' + \
                            str(NomInversion) + '/ Plataforma: ' + \
                            str(float(result[10]))
                        Comentario = "Cuidado error de inversion de conjuntos mayor que la planificada "
                        cur.execute(sqlSelectErrors, (CampaingID, 4, 'AF'))
                        rescampaing = cur.fetchone()
                        if rescampaing[0] == 0:
                            if CampaingID != '':
                                nuevoerror = (Error, Comentario,
                                              'AF', 4, CampaingID, 0, Estatus,datetime.now())
                                Errores.append(nuevoerror)

                    elif float(result[5]) > 0:
                        Error = 'Inversiion Diaria: '+str(float(result[5]))
                        Comentario = "Cuidado error de inversion diaria "
                        cur.execute(sqlSelectErrors, (CampaingID, 3, 'AF'))
                        rescampaing = cur.fetchone()
                        if rescampaing[0] == 0:
                            if CampaingID != '':
                                nuevoerror = (Error, Comentario,
                                              'AF', 3, CampaingID, 0, Estatus,datetime.now())
                                Errores.append(nuevoerror)

                    elif float(result[11]) > 0:
                        Error = 'Inversiion Diaria Conjunto: ' + \
                            str(float(result[11]))
                        Comentario = "Error de inversion diaria del conjunto de anuncios no debe ser mayor a la planificada"
                        cur.execute(sqlSelectErrors, (CampaingID, 5, 'AF'))
                        rescampaing = cur.fetchone()
                        if rescampaing[0] == 0:
                            if CampaingID != '':
                                nuevoerror = (Error, Comentario,
                                              'AF', 5, CampaingID, 0, Estatus,datetime.now())
                                Errores.append(nuevoerror)
            else:
                Comentario = "Error de nomenclatura verifica cada uno de sus elementos"
                cur.execute(sqlSelectErrors, (CampaingID, 1, 'AF'))
                rescampaing = cur.fetchone()
                if rescampaing[0] < 1:
                    if CampaingID != '':
                        nuevoerror = (Nomenclatura, Comentario,
                                      'AF', 1, CampaingID, 0, Estatus,datetime.now())
                        Errores.append(nuevoerror)

        cur.executemany(sqlInserErrors, Errores)
    #ANALISIS IMPRESIONES Y
    except Exception as e:
        print(e)
    finally:
        print('Success Errores AF Inversion Comprobados')


def reviewerrorsNom(conn):
    global cur
    cur = conn.cursor()
    print (datetime.now())
    try:
        berror = "SELECT * FROM ErrorsCampaings"
        bcampaings = 'SELECT CampaingID,Campaingname,Campaignstatus  FROM Campaings where CampaingID=%s'
        btStatus = "select a.CampaingID,a.Campaingname,a.Campaignstatus,b.Status,c.Adstatus from Campaings  a INNER JOIN  Adsets b on a.CampaingID=b.CampaingID  INNER JOIN  Ads c on b.AdSetID=c.AdSetID where a.Campaignstatus='PAUSED' or a.Campaignstatus='enable' or b.Status='PAUSED' or b.Status='enable' or c.Adstatus='PAUSED' or c.Adstatus='enable'"
        bupdatestatus = "UPDATE ErrorsCampaings SET StatusCampaing=%s where CampaingID=%s"
        bupdate = "UPDATE ErrorsCampaings SET estado=0 where CampaingID=%s"
        cupdate = "UPDATE ErrorsCampaings SET error=%s where CampaingID=%s"
        cur.execute(btStatus,)
        rest = cur.fetchall()
        for res in rest:
            CampID = res[0]
            if res[2] != 'PAUSED' or res[2] != 'paused':
                if res[3] != 'PAUSED' or res[2] != 'paused':
                    if res[4] != 'PAUSED' or res[2] != 'paused':
                        Estatus = res[4]
                    else:
                        Estatus = res[4]
                else:
                    Estatus = res[3]
            else:
                Estatus = res[2]
            cur.execute(bupdatestatus, (Estatus, CampID))

        cur.execute(berror,)
        resultscon = cur.fetchall()
        #SELECIONAMOS TODOS LOS ERRORES ACTUALES
        for res in resultscon:
            ##SI EL ERROR ES TIPO NOMENCLATURA
            if res[3] > 0 and res[5] == 1:
                rs = res[6]
                cur.execute(bcampaings, (rs,))
                ncampanas = cur.fetchall()
                for res in ncampanas:
                    ID = res[0]
                    Nomenclatura = res[1].encode('utf-8')
                    searchObj = re.search(r'^(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.+&]+)_([a-zA-Z0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-Z-/.+]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ.+]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(19)_([0-9,.]+)_(BA|AL|TR|TRRS|IN|DES|RV|CO)_([0-9,.]+)_(CPM|CPMA|CPVi|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCo)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([0-9,.-]+)(_B-)?([0-9]+)?(_S-)?([0-9]+)?(\(([0-9.)]+)\))?', Nomenclatura, re.M | re.I)
                    if searchObj:
                        cur.execute(bupdate, (ID,))
                    else:
                            cur.execute(cupdate, (Nomenclatura, ID))
    except Exception as e:
        print(e)
    finally:
        print('Actualización Errores NomenclaturaOK')


def reviewerrorsInv(conn):
    global cur
    cur = conn.cursor()
    print (datetime.now())
    try:
        berror = "SELECT * FROM ErrorsCampaings"
        bcampaings = 'select a.AccountsID,a.Account, b.CampaingID,b.Campaingname, b.Campaignspendinglimit,b.Campaigndailybudget,b.Campaignlifetimebudget,c.AdSetID,c.Adsetname,c.Adsetlifetimebudget,SUM(c.Adsetlifetimebudget) as tlotalconjungo,c.Adsetdailybudget,a.Media,b.Campaignstatus,b.Campaignstatus,c.Status from Accounts a INNER JOIN Campaings b on a.AccountsID=b.AccountsID INNER JOIN  Adsets c on b.CampaingID=c.CampaingID where b.CampaingID=%s  group by b.CampaingID  desc  '
        bupdate = "UPDATE ErrorsCampaings SET estado=0 where CampaingID=%s and TipoErrorID=%s"
        bupdateCamp = "UPDATE ErrorsCampaings SET estado=0 where CampaingID=%s "
        cur.execute(berror,)
        resultscon = cur.fetchall()
        #SELECIONAMOS TODOS LOS ERRORES ACTUALES
        for res in resultscon:
            ##SI EL ERROR ES TIPO NOMENCLATURA
            if res[3] > 0 and res[5] > 1 and res[5] < 6:
                #IDCAMPAING
                rs = res[6]
                TipoError = res[5]
                cur.execute(bcampaings, (rs,))
                resultscon = cur.fetchall()
                for result in resultscon:
                    Nomenclatura = result[3].encode('utf-8')
                    ID = result[2]
                    searchObj = re.search(r'^(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.+&]+)_([a-zA-Z0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-Z-/.+]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ.+]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(19)_([0-9,.]+)_(BA|AL|TR|TRRS|IN|DES|RV|CO)_([0-9,.]+)_(CPM|CPMA|CPVi|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCo)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([0-9,.-]+)(_B-)?([0-9]+)?(_S-)?([0-9]+)?(\(([0-9.)]+)\))?', Nomenclatura, re.M | re.I)
                    if searchObj:
                        NomInversion = float(searchObj.group(11))
                        if float(result[4]) > 0:
                            cur.execute(bupdateCamp, (ID,))
                        else:
                            #ERROR ACUMULADO PERFORMACE
                            if searchObj.group(25) > 0 and TipoError == 2:
                                Acumulado = float(
                                    result[6])-float(searchObj.group(25))
                                if int(NomInversion+1) >= int(Acumulado):
                                    cur.execute(bupdate, (ID, 2))

                            #Verificacion Inversion Nomenclatura
                            if float(result[6]) <= NomInversion and TipoError == 2:
                                cur.execute(bupdate, (ID, 2))

                            if float(result[10]) <= NomInversion and TipoError == 4:
                                cur.execute(bupdate, (ID, 4))

                            if float(result[5]) == 0 and float(result[6]) <= NomInversion and TipoError == 3:
                                cur.execute(bupdate, (ID, 3))

                            if float(result[11]) == 0 and float(result[10]) <= NomInversion and TipoError == 5:
                                cur.execute(bupdate, (ID, 5))
    except Exception as e:
        print(e)
    finally:
        print('Actualización Errores Inversion OK')


def push_errors(conn):
    errors_fb_inv(conn)
    errors_fb_pais(conn)
    errors_mm_inv(conn)
    errors_af(conn)
    errors_tw(conn)
    errors_go(conn)


if __name__ == '__main__':
   openConnection()
   #errors_fb_inv(conn)
   #push_errors(conn)
   errors_fb_inv(conn)
   #reviewerrorsInv(conn)
   #reviewerrorsNom(conn)
   conn.close()

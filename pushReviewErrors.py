# -*- coding: UTF-8 -*-
import json
import requests
import sys
import re
import mysql.connector as mysql
from datetime import datetime
import schedule
import time
#3.95.117.169
def openConnection():
    global conn
    try:
        conn = mysql.connect(host='3.95.117.169',database='MediaPlatforms',user='omgdev',password='Sdev@2002!',autocommit=True)
        return conn
    except:
        print("ERROR: NO SE PUEDO ESTABLECER CONEXION MYSQL.")
        sys.exit()

def errors_fb_inv(conn):
    global cur
    cur=conn.cursor(buffered=True)
    startTime = datetime.now()
    print (datetime.now())
    Comentario=''
    TipoErrorID=0
    a=0
    Estatus=''
    try:
        sqlConjuntosFB="select a.AccountsID,a.Account, b.CampaingID,b.Campaingname, b.Campaignspendinglimit,b.Campaigndailybudget,b.Campaignlifetimebudget,c.AdSetID,c.Adsetname,c.Adsetlifetimebudget,SUM(c.Adsetlifetimebudget) as tlotalconjungo,c.Adsetdailybudget,a.Media,b.Campaignstatus,b.Campaignstatus,c.Status from Accounts a INNER JOIN Campaings b on a.AccountsID=b.AccountsID INNER JOIN  Adsets c on b.CampaingID=c.CampaingID where a.Media='FB' group by b.CampaingID  desc "
        sqlInserErrors = "INSERT INTO ErrorsCampaings(Error,Comentario,Media,TipoErrorID,CampaingID,Impressions,StatusCampaing) VALUES (%s,%s,%s,%s,%s,%s,%s)"
        sqlSelectErrors = "SELECT COUNT(*) FROM ErrorsCampaings where CampaingID=%s and TipoErrorID=%s and Media=%s"
        cur.execute(sqlConjuntosFB,)
        resultscon=cur.fetchall()
        Errores=[]
        Impressions=0
        for result in resultscon:
            StatusCampaing=result[13]
            StatusAdsets=result[14]
            if StatusCampaing!='PAUSED':
                if StatusAdsets=='PAUSED':
                    Estatus=StatusAdsets
                else:
                    Estatus=StatusAdsets
            else:
                Estatus=StatusCampaing

            Nomenclatura=result[3].encode('utf-8')
            CampaingID=result[2]
            Media=result[12]

            searchObj = re.search(r'^(GT|PN|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR|DO)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/&]+)_([a-zA-Z0-9-/&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/&]+)_([a-zA-Z-/]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(19)_([0-9,.]+)_(BA|AL|TR|TRRS|IN|DES|RV|CO)_([0-9,.]+)_(CPM|CPMA|CPVi|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCo)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ]+)_([0-9,.-]+)_(B-)?([0-9]+)_(S-)?([0-9]+).*', Nomenclatura, re.M|re.I)
            if searchObj:
                if result[7]=='23843423455430637':
                    print result[7]
                    print result[10]
                NomInversion= searchObj.group(11)

                if result[4]==0:
                    if result[6]>NomInversion:

                        Error=result[6]
                        Comentario="Cuidado error de inversion diaria verifica la plataforma"
                        rescampaing=cur.fetchone()
                        if rescampaing[0]==0:
                            if CampaingID!='':
                                nuevoerror=(Error,Comentario,'FB',2,CampaingID,0,Estatus)
                                Errores.append(nuevoerror)
                    if result[10]>NomInversion:
                        Error=result[10]
                        Comentario="Cuidado error de inversion diaria de conjunto de anuncios verifica la plataforma "
                        cur.execute(sqlSelectErrors,(CampaingID,4,'FB'))
                        rescampaing=cur.fetchone()
                        if rescampaing[0]==0:
                            if CampaingID!='':
                                nuevoerror=(Error,Comentario,'FB',4,CampaingID,0,Estatus)
                                Errores.append(nuevoerror)
                    if result[5]>0:
                        Error=result[5]
                        Comentario="Error de inversion no debe ser mayor a la planificada"
                        cur.execute(sqlSelectErrors,(CampaingID,3,'FB'))
                        rescampaing=cur.fetchone()
                        if rescampaing[0]==0:
                            if CampaingID!='':
                                nuevoerror=(Error,Comentario,'FB',3,CampaingID,0,Estatus)
                                Errores.append(nuevoerror)
                    if result[11]>0:
                        Error=result[11]
                        Comentario="Error de inversion de conjunto de anuncios no debe ser mayor a la planificada"
                        cur.execute(sqlSelectErrors,(CampaingID,5,'FB'))
                        rescampaing=cur.fetchone()
                        if rescampaing[0]==0:
                            if CampaingID!='':
                                nuevoerror=(Error,Comentario,'FB',5,CampaingID,0,Estatus)
                                Errores.append(nuevoerror)
            else:
                Comentario="Error de nomenclatura verifica cada uno de sus elementos"
                cur.execute(sqlSelectErrors,(CampaingID,1,'FB'))
                rescampaing=cur.fetchone()
                if rescampaing[0]<1:
                    if CampaingID!='':
                        nuevoerror=(Nomenclatura,Comentario,'FB',1,CampaingID,0,Estatus)
                        Errores.append(nuevoerror)

        cur.executemany(sqlInserErrors,Errores)


    #ANALISIS IMPRESIONES Y
        #print(m.groups())
    except Exception as e:
        print(e)
    finally:
        print('Success Errores FB Inversion Comprobados')

def errors_fb_pais(conn):
    global cur
    cur=conn.cursor(buffered=True)
    startTime = datetime.now()
    print (datetime.now())
    Comentario=''
    TipoErrorID=0
    Estatus=''
    a=0
    try:
        sqlCampaingsFB="select a.AccountsID,a.Account,a.Media,b.CampaingID,b.Campaingname,b.Campaignspendinglimit,b.Campaigndailybudget,b.Campaignlifetimebudget, c.AdSetID,c.Adsetname,c.Adsetlifetimebudget,c.Adsetdailybudget, d.AdID,d.Adname,d.country,d.CreateDate,e.Impressions,b.Campaignstatus,d.Adstatus from Accounts a INNER JOIN Campaings b on a.AccountsID=b.AccountsID INNER JOIN  Adsets c on b.CampaingID=c.CampaingID INNER JOIN Ads d on d.AdSetID=c.AdSetID INNER JOIN MetricsAds e on e.AdID=d.AdID where a.Media='FB' group by d.Adname, d.Country ORDER BY d.CreateDate desc"
        sqlInserErrors = "INSERT INTO ErrorsCampaings(Error,Comentario,Media,TipoErrorID,CampaingID,Impressions,StatusCampaing) VALUES (%s,%s,%s,%s,%s,%s,%s)"
        sqlSelectErrors = "SELECT COUNT(*) FROM ErrorsCampaings where CampaingID=%s and TipoErrorID=%s and Media=%s"

        Errores=[]
        Impressions=0
        cur.execute(sqlCampaingsFB,)
        results=cur.fetchall()
        for result in results:
            Nomenclatura=result[4].encode('utf-8')
            Media=result[2]
            CampaingIDS=result[3]
            Impressions=result[16]
            StatusCampaing=result[17]
            StatusAdsets=result[18]
            if StatusCampaing!='PAUSED':
                if StatusAdsets=='PAUSED':
                    Estatus='PAUSED'
                else:
                    Estatus=StatusAdsets
            else:
                Estatus=StatusCampaing
            #VALORES NOMENCLATURA
            searchObj = re.search( r'^(GT|PN|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR|DO)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/&]+)_([a-zA-Z0-9-/&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/&]+)_([a-zA-Z-/]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(19)_([0-9,.]+)_(BA|AL|TR|TRRS|IN|DES|RV|CO)_([0-9,.]+)_(CPM|CPMA|CPVi|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCo)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ]+)_([0-9,.-]+)_(B-)?([0-9]+)_(S-)?([0-9]+).*', Nomenclatura, re.M|re.I)
            if searchObj:
                NomPais= searchObj.group(1)
                NomCliente= searchObj.group(2)
                NomMarca= searchObj.group(3)
                NomProducto= searchObj.group(4)
                NomCampana= searchObj.group(5)
                NomVersion= searchObj.group(6)
                NomMedio= searchObj.group(7)
                NomFormato= searchObj.group(8)
                NomMes= searchObj.group(9)
                NomAno= searchObj.group(10)
                NomInversion= searchObj.group(11)
                NomObjetivo= searchObj.group(12)
                NomMetObjetivo= searchObj.group(13)
                NomTipoKpi= searchObj.group(14)
                NomCostoKpi= searchObj.group(15)
                NomFormato= searchObj.group(16)
                NomEstrategia= searchObj.group(17)
                NomTactica= searchObj.group(18)
                NomOrden= searchObj.group(19)
                NomBono= searchObj.group(20)
                NomSaldo= searchObj.group(21)
                if NomCliente=='CCPRADERA':
                        if NomProducto=='HUE':
                            if result[14]=='MX':
                                a+=1
                        if NomProducto=='CHIQ':
                            if result[14]=='GT':
                                a+=1
                            elif result[14]=='HN':
                                a+=1
                        if a==0:
                            Error=result[14]
                            TipoErrorID=6
                            Comentario="Error de paises se estan imprimiendo anuncios en otros paises"
                            cur.execute(sqlSelectErrors,(CampaingIDS,TipoErrorID,Media))
                            rescampaing=cur.fetchone()
                            if rescampaing[0]<1:
                                if CampaingIDS!='':
                                    nuevoerror=(Error,Comentario,Media,TipoErrorID,CampaingIDS,Impressions,Estatus)
                                    Errores.append(nuevoerror)
                if NomCliente!='CCR' and NomCliente!='CCPRADERA':

                    if result[14]!=NomPais:
                        if result[14]=='PE' and NomPais!=PR or result[14]=='DO' and NomPais!=RD or result[14]=='PA' and NomPais!='PN':
                            Error=result[14]
                            TipoErrorID=6
                            Comentario="Error de paises se estan imprimiendo anuncios en otros paises"
                            cur.execute(sqlSelectErrors,(CampaingIDS,TipoErrorID,Media))
                            rescampaing=cur.fetchone()
                            if rescampaing[0]<1:
                                if CampaingIDS!='':
                                    nuevoerror=(Error,Comentario,Media,TipoErrorID,CampaingIDS,Impressions,Estatus)
                                    Errores.append(nuevoerror)

        cur.executemany(sqlInserErrors,Errores)
    #ANALISIS IMPRESIONES Y
        #print(m.groups())
    except Exception as e:
        print(e)
    finally:
        print('Success Errores FB Pais Comprobados')

def errors_go(conn):
    global cur
    cur=conn.cursor(buffered=True)
    startTime = datetime.now()
    print (datetime.now())
    Comentario=''
    TipoErrorID=0
    a=0
    try:
        sqlCampaingsGO="select a.AccountsID,a.Account,a.Media,b.CampaingID,b.Campaingname,b.Campaignspendinglimit,b.Campaigndailybudget,b.Campaignlifetimebudget, c.AdSetID,c.Adsetname,c.Adsetlifetimebudget,c.Adsetdailybudget, d.AdID,d.Adname,d.CreateDate,b.Campaignstatus,b.Campaignstatus,c.Status from Accounts a INNER JOIN Campaings b on a.AccountsID=b.AccountsID  INNER JOIN  Adsets c on b.CampaingID=c.CampaingID INNER JOIN Ads d on d.AdSetID=c.AdSetID where a.Media='GO'  group by d.AdID ORDER BY d.CreateDate desc LIMIT 50000000000"
        sqlInserErrors = "INSERT INTO ErrorsCampaings(Error,Comentario,Media,TipoErrorID,CampaingID,Impressions) VALUES (%s,%s,%s,%s,%s,%s)"
        sqlSelectErrors = "SELECT COUNT(*) FROM ErrorsCampaings where CampaingID=%s and TipoErrorID=%s and Media=%s"

        Errores=[]
        Impressions=0
        Estatus=''
        cur.execute(sqlCampaingsGO,)
        results=cur.fetchall()
        for result in results:
            Nomenclatura=result[4].encode('utf-8')
            Media=result[2]
            CampaingID=result[3]
            #VALORES NOMENCLATURA
            StatusCampaing=result[15]
            StatusAdsets=result[16]
            if StatusCampaing!='enabled':
                if StatusAdsets!='enabled':
                    Estatus=StatusAdsets
                else:
                    Estatus=StatusAdsets
            else:
                Estatus=StatusCampaing


            searchObj = re.search(r'^(GT|PN|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR|DO)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/&]+)_([a-zA-Z0-9-/&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/&]+)_([a-zA-Z-/]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(19)_([0-9,.]+)_(BA|AL|TR|TRRS|IN|DES|RV|CO)_([0-9,.]+)_(CPM|CPMA|CPVi|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCo)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ]+)_([0-9,.-]+)_(B-)?([0-9]+)_(S-)?([0-9]+).*', Nomenclatura, re.M|re.I)
            if searchObj:
                a=1
            else:
                Error=Nomenclatura
                TipoErrorID=1
                Comentario="Error de nomenclatura verifica cada uno de sus elementos"
                cur.execute(sqlSelectErrors,(CampaingID,TipoErrorID,Media))
                rescampaing=cur.fetchone()
                if rescampaing[0]<1:
                    if CampaingIDS!='':
                        nuevoerror=(Error,Comentario,Media,TipoErrorID,CampaingID,0)
                        Errores.append(nuevoerror)

        cur.executemany(sqlInserErrors,Errores)


    #ANALISIS IMPRESIONES Y
        #print(m.groups())
    except Exception as e:
        print(e)
    finally:
        print('Success Errores GO Comprobados')
#FIN VISTA
def errors_tw(conn):
    global cur
    cur=conn.cursor(buffered=True)
    startTime = datetime.now()
    print (datetime.now())
    Comentario=''
    TipoErrorID=0
    a=0
    Errores=[]
    Impressions=0
    try:
        sqlCampaingsTW="select a.AccountsID,a.Account,a.Media,b.CampaingID,b.Campaingname,b.Campaignspendinglimit,b.Campaigndailybudget,b.Campaignlifetimebudget,e.Impressions,b.Campaignstatus from Accounts a INNER JOIN Campaings b on a.AccountsID=b.AccountsID  INNER JOIN CampaingMetrics e on b.CampaingID = e.CampaingID where a.Media='TW'  group by b.CampaingID ORDER BY a.CreateDate desc LIMIT 50000000000"
        sqlInserErrors = "INSERT INTO ErrorsCampaings(Error,Comentario,Media,TipoErrorID,CampaingID,Impressions) VALUES (%s,%s,%s,%s,%s,%s)"
        sqlSelectErrors = "SELECT COUNT(*) FROM ErrorsCampaings where CampaingID=%s and TipoErrorID=%s and Media=%s"
        cur.execute(sqlCampaingsTW,)
        results=cur.fetchall()
        for result in results:
            Nomenclatura=result[4].encode('utf-8')
            Media=result[2]
            CampaingID=result[3]
            #Impressions=result[16]
            #VALORES NOMENCLATURA
            if result[8]>0:

                searchObj = re.search(r'^(GT|PN|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR|DO)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/&]+)_([a-zA-Z0-9-/&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/&]+)_([a-zA-Z-/]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(19)_([0-9,.]+)_(BA|AL|TR|TRRS|IN|DES|RV|CO)_([0-9,.]+)_(CPM|CPMA|CPVi|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCo)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ]+)_([0-9,.-]+)_(B-)?([0-9]+)_(S-)?([0-9]+).*', Nomenclatura, re.M|re.I)
                if searchObj:
                    a=1
                else:
                    Error=Nomenclatura
                    TipoErrorID=1
                    cur.execute(sqlSelectErrors,(CampaingID,TipoErrorID,Media))
                    rescampaing=cur.fetchone()
                    if rescampaing[0]<1:
                        if CampaingIDS!='':
                            nuevoerror=(Error,Comentario,Media,TipoErrorID,CampaingID,0)
                            Comentario="Error de nomenclatura verifica cada uno de sus elementos"
                            Errores.append(nuevoerror)

        cur.executemany(sqlInserErrors,Errores)


    #ANALISIS IMPRESIONES Y
        #print(m.groups())
    except Exception as e:
        print(e)
    finally:
        print('Success Errores TW Comprobados')

def reviewerrors(conn):
    global cur
    cur=conn.cursor(buffered=True)
    startTime = datetime.now()
    print (datetime.now())
    try:
        berror="SELECT * FROM ErrorsCampaings"
        bcampaings='SELECT CampaingID,Campaingname,Campaignstatus  FROM Campaings where CampaingID=%s'
        btcampaings='SELECT CampaingID,Campaingname,Campaignstatus FROM Campaings'
        btStatus="select a.CampaingID,a.Campaingname,a.Campaignstatus,b.Status,c.Adstatus from Campaings  a INNER JOIN  Adsets b on a.CampaingID=b.CampaingID  INNER JOIN  Ads c on b.AdSetID=c.AdSetID where a.Campaignstatus='PAUSED' or a.Campaignstatus='enable' or b.Status='PAUSED' or b.Status='enable' or c.Adstatus='PAUSED' or c.Adstatus='enable'"
        bupdatestatus="UPDATE ErrorsCampaings SET StatusCampaing=%s where CampaingID=%s"
        bupdate="UPDATE ErrorsCampaings SET estado=0 where CampaingID=%s"
        cupdate="UPDATE ErrorsCampaings SET error=%s where CampaingID=%s"
        cur.execute(btStatus,)
        rest=cur.fetchall()
        for res in rest:
            CampID=res[0]
            if res[2]!='PAUSED' or res[2]!='enabled':
                if res[3]!='PAUSED' or res[2]!='enabled':
                    if res[4]!='PAUSED' or res[2]!='enabled':
                        Estatus=res[4]
                    else:
                        Estatus=res[4]
                else:
                    Estatus=res[3]
            else:
                Estatus=res[2]
            cur.execute(bupdatestatus,(Estatus,CampID))

        cur.execute(berror,)
        resultscon=cur.fetchall()
        #SELECIONAMOS TODOS LOS ERRORES ACTUALES
        for res in resultscon:
            ##SI EL ERROR ES TIPO NOMENCLATURA
            if res[3]>0 and res[5]==1:
                rs=res[6]
                cur.execute(bcampaings,(rs,))
                ncampanas=cur.fetchall()
                for res in ncampanas:
                    ID=res[0]
                    Nomenclatura=res[1].encode('utf-8')
                    searchObj = re.search(r'^(GT|PN|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR|DO)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/&]+)_([a-zA-Z0-9-/&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/&]+)_([a-zA-Z-/]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(19)_([0-9,.]+)_(BA|AL|TR|TRRS|IN|DES|RV|CO)_([0-9,.]+)_(CPM|CPMA|CPVi|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCo)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ]+)_([0-9,.-]+)_(B-)?([0-9]+)_(S-)?([0-9]+).*', Nomenclatura, re.M|re.I)
                    if searchObj:
                        cur.execute(bupdate,(ID,))
                    else:
                        cur.execute(cupdate,(Nomenclatura,ID))
    except Exception as e:
        print(e)
    finally:
        print('Actualización Errores OK')

def push_errors(conn):
    errors_fb_inv(conn)
    errors_fb_pais(conn)
    errors_tw(conn)
    errors_go(conn)

if __name__ == '__main__':
   openConnection()
   push_errors(conn)
   reviewerrors(conn)
   conn.close()
    #fb_ads()
   #reviewerrors()
   #reviewerrors()
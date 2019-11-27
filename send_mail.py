import smtplib, ssl
from string import Template
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import json
import requests
import sys
import re
import mysql.connector as mysql
from datetime import datetime, timedelta
import numpy as mp
#uomg@omg.com.gt OMG2018u
conn = None
def openConnection():
    global conn
    try:
        conn = mysql.connect(host='3.95.117.169', database='MediaPlatforms',
                             user='omgdev', password='Sdev@2002!', autocommit=True)
    except:
        print("ERROR: NO SE PUEDO ESTABLECER CONEXION MYSQL.")
        sys.exit()


def send(campanas,names):
    cur=conn.cursor(buffered=True)
    try:
        body = ''
        
        sender_email = "adops-noreply@omg.com.gt"  # Enter your address
        password = "OMGdev2019"
        names =  names[1:-1]
        sqlCamping = """
                    select distinct username, c.CampaingID from omgguate.usuario u 
                    inner join SysAdOps.RolsUsers ru on ru.UserID = u.idusuario
                    inner join SysAdOps.Rols r on r.RolID = ru.RolID
                    inner join mfcgt.mfcasignacion asg on asg.idusuario = u.idusuario
                    inner join accountxmarca am on am.marca = asg.idmarca
                    inner join Accounts a on a.AccountsID = am.account
                    inner join Campaings c on c.AccountsID = a.AccountsID 
                    where c.CampaingID in ({})
                    order by username;
                    """.format(names)
        cur.execute(sqlCamping,)
        resultscon = cur.fetchall()
        correo = resultscon[0][0]
        last = resultscon[len(resultscon)-1][0]
        for res in resultscon:
            if res[0] != correo or res[0] == last:
                html = """\
                    <html>
                   
                    <body>
                    
                        <p>Hola,<br>
                        Listado de Camapañas<br>
                        </p>
                        <table style="width:100%; border-collapse: collapse;" >
                        <tr>
                            <th  style="border:1px solid black;padding: 10px;" >Cuenta</th>
                            <th style="border:1px solid black;padding: 10px;">Marca</th>
                            <th style="border:1px solid black;padding: 10px;">Medio</th>
                            <th style="border:1px solid black;padding: 10px;">Nombre Camapaña</th>
                            <th style="border:1px solid black;padding: 10px;">Resultado Presupuesto</th>
                            <th style="border:1px solid black;padding: 10px;">Resutlado KPI</th>
                            <th style="border:1px solid black;padding: 10px;">Presupuseto%</th>
                            <th style="border:1px solid black;padding: 10px;">KPI%</th>
                            <th style="border:1px solid black;padding: 10px;">Presupuseto</th>
                            <th style="border:1px solid black;padding: 10px;">KPI</th>
                        </tr>
                        {}
                        </table> 
                    </body>
                    </html>
                    """.format(body)
                body = MIMEText(html,'html') # convert the body to a MIME compatible string
                receiver_email = correo
                msg = MIMEMultipart()
                msg['From'] = 'adops-noreply@omg.com.gt'
                msg['To'] = correo
                msg['Subject'] = 'Listado de Campañas ' + str(datetime.now())
                msg.attach(body)
                server = smtplib.SMTP('smtp.office365.com',587)
                server.ehlo()
                server.starttls()
                server.login(sender_email, password)
                #server.sendmail(sender_email, receiver_email, msg.as_string())
                body = ''
                correo = res[0]
            
            for cam in campanas:
                if cam[0] ==  res[1]:
                    body = body + '<tr style="border:1px solid black;padding: 10px;"> '
                    body = body + '<td>{}</td>'.format(cam[1])
                    body = body + '<td>{}</td>'.format(cam[2])
                    body = body + '<td>{}</td>'.format(cam[3])
                    body = body + '<td>{}</td>'.format(cam[4])
                    body = body + '<td>{}</td>'.format(cam[5])
                    body = body + '<td>{}</td>'.format(cam[6])
                    body = body + '<td>{}</td>'.format(cam[7])
                    body = body + '<td>{}</td>'.format(cam[8])
                    body = body + '<td>{}</td>'.format(cam[9])
                    body = body + '<td>{}</td>'.format(cam[10])
                    body = body + '</tr>'
                    break
        
        
    except Exception as e:
        print(e)
    else:
        print(datetime.now())
    
    

   


    

def CampaingsReview(conn):
    cur=conn.cursor(buffered=True)
    sqlCamping = """
                    select  dc.nombre as Account, dc.id idcliente,m.id idmarca ,c.CampaingID CampaingID,  a.Media Media,  c.Campaingname Campaingname, round(sum(distinct d.Cost),2) as 'InversionConsumida', date_format(c.StartDate, '%d/%m/%Y') StartDate , m.nombre as Marca,
                    date_format(c.EndDate,'%d/%m/%Y') EndDate , SUBSTRING_INDEX(SUBSTRING_INDEX(c.Campaingname, '_', 11),'_',-1) as 'PresupuestoPlan',SUBSTRING_INDEX (SUBSTRING_INDEX(c.Campaingname, '_', 13),'_',-1) KPIPlanificado, 
                    md.Nombre KPI,ifnull(sum(distinct d.result),0) 'KPIConsumido',c.Campaignstatus State,m.nombre Marca ,dc.nombre Cliente,date_format(now(),'%M') mes,  
                    '0' as 'TotalDias','0' as 'DiasEjecutados','0' as 'DiasPorservir', "0" as 'PresupuestoEsperado',"0" as 'PorcentajePresupuesto', 
                    "0" as 'PorcentajeEsperadoV',"0" as 'PorcentajeRealV',"0" as 'KPIEsperado',"0" as 'PorcentajeKPI', "0" as 'PorcentajeEsperadoK',"0" as 'PorcentajeRealK', "0" as 'EstadoKPI', "0" as 'EstadoPresupuesto'
                    from dailycampaing d
                    inner join Campaings c on c.CampaingID = d.CampaingID
                    inner join Accounts a on c.AccountsID = a.AccountsID
                    inner join accountxmarca am on am.account = a.AccountsID
                    inner join mfcgt.mfcasignacion asg on asg.idmarca = am.marca
                    inner join mfcgt.dmarca m on am.marca = m.id
                    inner join mfcgt.dcliente dc on dc.id = m.idcliente
                    inner join modelocompra md on md.abr = SUBSTRING_INDEX (SUBSTRING_INDEX(c.Campaingname, '_', 14),'_',-1) 
                    where c.Campaignstatus in ('ACTIVE','enabled')  and asg.idusuario = {} and c.EndDate > '{}'
                    group by d.CampaingID;    
                    """
    try:
        campanas=[]
        campananames = ''
        print(datetime.now())
        cur.execute(sqlCamping,)
        resultscon = cur.fetchall()
        for row in resultscon:
            Nomenclatura = row[3]
            searchObj = re.search(r'^(GT|CAM|RD|US|SV|HN|NI|CR|PA|RD|PN|CHI|HUE|PR)_([a-zA-ZáéíóúÁÉÍÓÚÑñ\s0-9-/.+&]+)_([a-zA-Z0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ0-9-/.+&]+)_([a-zA-Z-/.+]+)_([a-zA-ZáéíóúÁÉÍÓÚÑñ.+]+)_(ENE|FEB|MAR|ABR|MAY|JUN|JUL|AGO|SEP|OCT|NOV|DIC)_(19|2019)_([0-9,.]+)_(BA|AL|TR|TRRS|TRRRSS|IN|DES|RV|CO)_([0-9,.]+)_(CPM|CPMA|CPVi|CPC|CPI|CPD|CPV|CPCo|CPME|CPE|PF|RF|MC|CPCO|CPCO)_([0-9.,]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([a-zA-Z-/áéíóúÁÉÍÓÚÑñ.+]+)_([0-9,.-]+)?(_B-)?(_)?([0-9.]+)?(_S-)?(_)?([0-9.]+)?(\s)?(\(([0-9.)]+)\))?$', Nomenclatura, re.M | re.I)
            if searchObj:
                if row[5] != '0000-00-00' and row[6] != '0000-00-00':
                    Start = datetime.strptime(row[5], "%d/%m/%Y")
                    End = datetime.strptime(row[6], "%d/%m/%Y")
                    TotalDias = End - Start
                    DiasEjectuados = datetime.now() -  Start 
                    DiasPorservir = End - datetime.now() 
                    if TotalDias.days > 0:
                        porcentDay = DiasEjectuados.days / ((TotalDias.days) + 1 )
                    PresupuestoEsperado = round(float(row[7]) * porcentDay,2)
                    if float( row[7]) > 0:
                        PorcentajeEsperadoV = round(float( PresupuestoEsperado)/ float( row[7]),2)
                        PorcentajeRealV = round(float(row[4])/ float(row[7]),2)
                        PorcentajePresupuesto = PorcentajeRealV - 1
                    KPIEsperado = round(float(row[8]) * porcentDay,2)
                    if float( row[8]) > 0:
                        PorcentajeEsperadoK = round(float( KPIEsperado)/ float( row[8]),2)
                        PorcentajeRealK = round(float(row[10])/ float(row[8]),2)
                        PorcentajeKPI = PorcentajeRealK - 1
                    TotalDias = TotalDias.days
                    DiasEjectuados = DiasEjectuados.days
                    DiasPorservir = DiasPorservir.days + 1
                    EstadoPresupuesto = 0 
                    EstadoKPI = 0
                    if porcentDay <= 0.25:
                        if abs(int(PorcentajePresupuesto)) <= 0.15:
                            EstadoPresupuesto =  1
                        if abs(int(PorcentajeKPI)) <= 0.15:
                            EstadoKPI =  1
                    elif porcentDay > 0.25 and porcentDay <=0.50:
                        if abs(int(PorcentajePresupuesto)) <= 0.10:
                            EstadoPresupuesto =  1
                        if abs(int(PorcentajeKPI)) <= 0.10:
                            EstadoKPI =  1
                    elif porcentDay > 0.50 and porcentDay <=0.85:
                        if abs(int(PorcentajePresupuesto)) <= 0.05:
                            EstadoPresupuesto =  1
                        if abs(int(PorcentajeKPI)) <= 0.05:
                            EstadoKPI =  1
                    elif porcentDay > 0.85:
                        if abs(int(PorcentajePresupuesto)) <= 0.01:
                            EstadoPresupuesto =  1
                        if abs(int(PorcentajeKPI)) <= 0.01:
                            EstadoKPI =  1
                    if EstadoPresupuesto == 0 and EstadoKPI == 1:
                        campana=(row[1],row[0],row[12],row[2],row[3],'Estado Presupuesto Malo','Estado KPI Bueno',str(round(float(PorcentajePresupuesto))),str(round(float(PorcentajeKPI),2)),str(round(float(row[4]),2)),str(round(float(row[8]),2)))
                        campananames= campananames + ',' + row[1]
                    elif EstadoPresupuesto == 1 and EstadoKPI == 0: 
                        campana=(row[1],row[0],row[12],row[2],row[3],'Estado Presupuesto Bueno','Estado KPI Malo',str(round(float(PorcentajePresupuesto))),str(round(float(PorcentajeKPI),2)),str(round(float(row[4]),2)),str(round(float(row[8]),2)))
                        campananames= campananames + ',' + row[1]
                    elif EstadoPresupuesto == 0 and EstadoKPI == 0: 
                        campana=(row[1],row[0],row[12],row[2],row[3],'Estado Presupuesto Malo','Estado KPI Malo',str(round(float(PorcentajePresupuesto))),str(round(float(PorcentajeKPI),2)),str(round(float(row[4]),2)),str(round(float(row[8]),2)))
                        campananames= campananames + ',' + row[1]
                    campanas.append(campana)
        if campanas:
            
            send(campanas,campananames)
    except Exception as e:
        print(e)
    finally:
        print(datetime.now())
    

if __name__ == '__main__':
    openConnection()
    CampaingsReview(conn)
    conn.close()
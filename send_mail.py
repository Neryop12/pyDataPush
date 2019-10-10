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
#uomg@omg.com.gt OMG2018u
conn = None
def openConnection():
    global conn
    try:
        conn = mysql.connect(host='54.91.190.25', database='MediaPlatforms',
                             user='omgdev', password='Sdev@2002!', autocommit=True)
    except:
        print("ERROR: NO SE PUEDO ESTABLECER CONEXION MYSQL.")
        sys.exit()


def send(campanas):
    port = 587  # For SSL
    smtp_server = "'smtp.office365.com'"
    sender_email = "uomg@omg.com.gt"  # Enter your address
    receiver_email = "pancho_op12@hotmail.com"  # Enter receiver address
    password = "OMG2018u"
    msg = MIMEMultipart()
    msg['From'] = 'uomg@omg.com.gt'
    msg['To'] = 'pancho_op12@hotmail.com'
    msg['Subject'] = 'preba1'
    body = ''
    for cam in campanas:
        body = body + '<tr>'
        body = body + '<td>{}</td>'.format(cam[0])
        body = body + '<td>{}</td>'.format(cam[1])
        body = body + '<td>{}</td>'.format(cam[2])
        body = body + '<td>{}</td>'.format(cam[3])
        body = body + '<td>{}</td>'.format(cam[4])
        body = body + '<td>{}</td>'.format(cam[5])
        body = body + '<td>{}</td>'.format(cam[6])
        body = body + '<td>{}</td>'.format(cam[7])
        body = body + '</tr>'

    html = """\
    <html>
    <body>
        <p>Hola,<br>
        Listado de Camapañas<br>
        </p>
         <table style="width:100%">
        <tr>
            <th>Marca</th>
            <th>Medio</th>
            <th>CampaingName</th>
            <th>Resultado Presupuesto</th>
            <th>Resultado KPI</th>
            <th>Presupuesto%</th>
            <th>KPI%</th>
        </tr>
        {}
        </table> 
    </body>
    </html>
    """.format(body)


    body = MIMEText(html,'html') # convert the body to a MIME compatible string
    msg.attach(body)
    server = smtplib.SMTP('smtp.office365.com',587)
    server.ehlo()
    server.starttls()
    server.login(sender_email, password)
    server.sendmail(sender_email, receiver_email, msg.as_string())

def CampaingsReview(conn):
    cur=conn.cursor(buffered=True)
    sqlCamping = """
                    select  a.Account as Account ,c.CampaingID CampaingID,  a.Media Media,  c.Campaingname Campaingname,sum(d.Cost) as 'InversionConsumida', date_format(c.StartDate, '%d/%m/%Y') StartDate , 
                    date_format(c.EndDate,'%d/%m/%Y') EndDate , SUBSTRING_INDEX(SUBSTRING_INDEX(c.Campaingname, '_', 11),'_',-1) as 'PresupuestoPlan',SUBSTRING_INDEX (SUBSTRING_INDEX(c.Campaingname, '_', 13),'_',-1) KPIPlanificado, 
                    md.Nombre KPI,ifnull(sum(d.result),0) 'KPIConsumido',c.Campaignstatus State,m.nombre Marca ,dc.nombre Cliente,date_format(now(),'%M') mes,  
                    '0' as 'TotalDias','0' as 'DiasEjecutados','0' as 'DiasPorservir', "0" as 'PresupuestoEsperado',"0" as 'PorcentajePresupuesto', 
                    "0" as 'PorcentajeEsperadoV',"0" as 'PorcentajeRealV',"0" as 'KPIEsperado',"0" as 'PorcentajeKPI', "0" as 'PorcentajeEsperadoK',"0" as 'PorcentajeRealK', "0" as 'EstadoKPI', "0" as 'EstadoPresupuesto'
                    from Dailycampaing d
                    inner join Campaings c on c.CampaingID = d.CampaingID
                    inner join Accounts a on c.AccountsID = a.AccountsID
                    inner join accountxmarca am on am.account = a.AccountsID
                    inner join mfcgt.mfcasignacion asg on asg.idmarca = am.marca
                    inner join mfcgt.dmarca m on am.marca = m.id
                    inner join mfcgt.dcliente dc on dc.id = m.idcliente
                    inner join modelocompra md on md.abr = SUBSTRING_INDEX (SUBSTRING_INDEX(c.Campaingname, '_', 14),'_',-1) 
                    where c.Campaignstatus in ('ACTIVE','enabled')  
                    group by d.CampaingID;    
                    """
    try:
        campanas=[]
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
                        campana=(row[0],row[12],row[1],row[2],row[3],'Estado Presupuesto Malo','Estado KPI Bueno',PorcentajePresupuesto,PorcentajeKPI,row[4],row[8])
                    elif EstadoPresupuesto == 1 and EstadoKPI == 0: 
                        campana=(row[0],row[12],row[1],row[2],row[3],'Estado Presupuesto Bueno','Estado KPI Malo',PorcentajePresupuesto,PorcentajeKPI,row[4],row[8])
                    elif EstadoPresupuesto == 0 and EstadoKPI == 0: 
                        campana=(row[0],row[12],row[1],row[2],row[3],'Estado Presupuesto Malo','Estado KPI Malo',PorcentajePresupuesto,PorcentajeKPI,row[4],row[10])
                    campanas.append(campana)
        if campanas:
            send(campanas)
    except Exception as e:
        print(e)
    finally:
        print(datetime.now())
    

if __name__ == '__main__':
    openConnection()
    CampaingsReview(conn)
    conn.close()
import pandas as pd
import numpy as np
import dbconnect as sql
import config.db as db
from datetime import datetime, timedelta
import mysql.connector as mysql

def lectura_Datos(conn, conn_report):
    now = datetime.now()
    CreateDate = now.strftime("%Y-%m-%d %H:%M:%S")
    campanas = []
    metricas_campanas = []
    creative_ads = []
    response = ''
    data_claro = pd.read_excel('datos.xlsx')
    df = pd.DataFrame(data_claro)
    df = df.fillna(0)
    creative_id = 0
    diferent_name = ''
    Result  = 0
    for index, row in df.iterrows():
        name = str(row['Nombre de la campaña'])
        inversion = float(row['Inversion'])
        
            
        metrica = [ 0, float(row['Inversion']), int(row['Frecuencia']),int(row['Alcance']), int(row['Interacciones']), 
            int(row['Impresiones']), int(row['Clicks']), int(row['Views']), int(row['Views']), int(row['Views']), int(row['Conversiones']),  int(row['Resultados']),
            's', int(row['ID']), CreateDate, float(row['KPI']), int(row['Descargas']),4,1
        ]
        
        metricas_campanas.append(metrica)
            #creative_ads.append(creative_ad)
    
    sql.connect.insertMetricasCampanas(metricas_campanas,'Claro_Datos',conn_report)
    
        

        


if __name__ == '__main__':
    conn = mysql.connect(host='3.95.117.169', database='MediaPlatformsReports',
                             user='root', password='AnnalectDB2019', autocommit=True)
    
    lectura_Datos(conn, conn)
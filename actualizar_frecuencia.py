import pandas as pd
import numpy as np
import dbconnect as sql
import config.db as db
from datetime import datetime, timedelta
import mysql.connector as mysql

def openConnection():
    try:
        conn = mysql.connect(host='3.95.117.169', database='MediaPlatformsReports',
                             user='root', password='AnnalectDB2019', autocommit=True)
        return conn
    except Exception as e:
        print("ERROR: NO SE PUEDO ESTABLECER CONEXION MYSQL.")

def lectura_Datos(conn):
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
        id = int(row['ID'])
        cost = float(row['Importe gastado (USD)'])
        metrica = [int(row['Conversiones']),int(row['Conversiones']), id]
        
        metricas_campanas.append(metrica)
        
    
    sql.connect.ActualizarConversiones(metricas_campanas,conn)
    #sql.connect.insertCreadtiveAdsReport(creative_ads, conn_report)
        

        


if __name__ == '__main__':
    conn = openConnection()
    lectura_Datos(conn)
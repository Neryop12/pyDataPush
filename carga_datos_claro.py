import pandas as pd
import numpy as np
import dbconnect as sql
import config.db as db
from datetime import datetime, timedelta


def lectura_Datos(conn, conn_report):
    now = datetime.now()
    CreateDate = now.strftime("%Y-%m-%d %H:%M:%S")
    campanas = []
    metricas_campanas = []
    creative_ads = []
    response = ''
    data_claro = pd.read_excel('MAYO.xlsx')
    df = pd.DataFrame(data_claro)
    df = df.fillna(0)
    creative_id = 0
    diferent_name = ''
    for index, row in df.iterrows():
        name = str(row['Nombre de la Campaña'])
        if name != diferent_name:
            creative_id = 0
            diferent_name = name 
            response =  sql.connect.ObtenerIDMFC(diferent_name, conn)
            if response != None:
                nomeclaruta = str(response[0]) + '_' + diferent_name
                campana = [str(response[0]), nomeclaruta, int(response[0]), CreateDate]
                campanas.append(campana)
            else:
                print(name)
                continue
        if response != None:
            creative_id += 1
            metrica = [ str(response[0]), float(row['Inversión Consumida']), int(row['Frecuencia']),int(row['Alcance']), int(row['Interacciones']), 
                int(row['Impresiones']), int(row['Clicks']), int(row['Views']), int(row['VideoWatch']), int(row['Visualización de fotos']), int(row['Views']), int(row['Resultados']),
                str(row['Objetivo']), str(response[0]), CreateDate, int(row['KPI'])
            ]
            creative_ad = [ str(response[0]) + '_' + str(creative_id), str(row['Nombre del Anuncio']), str(row['URL']), str(response[0]) , CreateDate]
            metricas_campanas.append(metrica)
            creative_ads.append(creative_ad)
    sql.connect.insertCampanasReport(campanas,conn_report)
    sql.connect.insertMetricasCampanas(metricas_campanas,'Claro_Datos',conn_report)
    sql.connect.insertCreadtiveAdsReport(creative_ads, conn_report)
        

        


if __name__ == '__main__':
    conn = sql.connect.open(db.DB['host'], db.DB['user'], db.DB['password'],
                            db.DB['dbname'], db.DB['port'], db.DB['autocommit'])
    conn_report = sql.connect.open(db.DBR['host'], db.DB['user'], db.DB['password'],
                            db.DBR['dbname'], db.DB['port'], db.DB['autocommit'])
    lectura_Datos(conn, conn_report)
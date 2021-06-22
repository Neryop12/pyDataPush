import config.db as db
import dbconnect as sql
import datos_fbgotw as medios
import datos_mediamath as mm
import datos_adform as adf
import datos_mediosextras as mediosextras
import datos_adsmovil as am
import psycopg2
if __name__ == '__main__':

    # Iniciamos la conexion
    conn = conn = psycopg2.connect(
            host='visor-rds.cebdvwvqle9k.us-west-1.rds.amazonaws.com',
            database='MediaPlatformsReports',
            user='postgres',
            password='Postgres21'
        )
    # Facebook
    try:
        dfcampanas = medios.Spreadsheet(
            db.FB['key'], db.FB['media'], db.FB['campanas'])
        dfadsets = medios.Spreadsheet(
            db.FB['key'], db.FB['media'], db.FB['adsets'])
        dfads = medios.Spreadsheet(db.FB['key'], db.FB['media'], db.FB['ads'])

        #medios.metricas_campanas(dfcampanas, db.FB['media'], conn)
        #medios.metricas_adsets(dfadsets, db.FB['media'], conn)

        dfdiarios = medios.Spreadsheet(db.DAY['key'], 'FB', db.DAY['FB'])
        #medios.actualizarestado(dfdiarios, 'FB', conn)
        medios.diario_campanas(dfdiarios, 'FB', conn)

       

    except Exception as e:
        print(e)
    # Google
    
    try:
        dfcampanas = medios.Spreadsheet(
            db.GO['key'], db.GO['media'], db.GO['campanas'])
        dfadsets = medios.Spreadsheet(
            db.GO['key'], db.GO['media'], db.GO['adsets'])
        dfads = medios.Spreadsheet(db.GO['key'], db.GO['media'], db.GO['ads'])

        medios.metricas_campanas(dfcampanas, db.GO['media'], conn)
        medios.metricas_adsets(dfadsets, db.GO['media'], conn)
        medios.metricas_ads(dfads, db.GO['media'], conn)

        dfdiarios = medios.Spreadsheet(db.DAY['key'], 'GO', db.DAY['GO'])
        #medios.actualizarestado(dfdiarios, 'GO', conn)
        medios.diario_campanas(dfdiarios, 'GO', conn)

    except Exception as e:
        print(e)

    # Twitter
    try:
        dfcampanas = medios.Spreadsheet(
            db.TW['key'], db.TW['media'], db.TW['campanas'])
        dfadsets = medios.Spreadsheet(
            db.TW['key'], db.TW['media'], db.TW['adsets'])
        dfads = medios.Spreadsheet(db.TW['key'], db.TW['media'], db.TW['ads'])

        #medios.metricas_campanas(dfcampanas, db.TW['media'], conn)
        #medios.metricas_adsets(dfadsets, db.TW['media'], conn)
        #medios.metricas_ads(dfads, db.TW['media'], conn)

        dfdiarios = medios.Spreadsheet(db.DAY['key'], 'TW', db.DAY['TW'])
        #medios.actualizarestado(dfdiarios, 'TW', conn)
        medios.diario_campanas(dfdiarios, 'TW', conn)

    except Exception as e:
        print(e)
     # MediaMath
    
    try:
        mm.GetToken()
        mm.GetSession()
        mm.CuentasCampanas(conn)
        mm.Adsets(conn)
        mm.Ads(conn)
    except Exception as e:
        print(e)
    

    # Cerramos la conexion
    sql.connect.close(conn)

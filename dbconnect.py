import config.db as db
import mysql.connector as mysql
from mysql.connector import Error
from datetime import date
import sys


class connect(object):

    def open(HOST, USER, PASS, DB, AUTOCOMMIT):
        try:
            conn = mysql.connect(host=HOST, database=DB,
                                 user=USER, password=PASS, autocommit=AUTOCOMMIT)
            return conn
        except Exception as e:
            print('Error on line {}'.format(
                sys.exc_info()[-1].tb_lineno), type(e).__name__, e)

    def close(conn):
        if (conn.is_connected()):
            conn.close()
            print("MySQL connection is closed")

    def insertCuentas(cuentas, media, conn):
        cur = conn.cursor()
        query = """INSERT INTO  Accounts (AccountsID, Account,Media,CreateDate) values(%s,%s,%s,%s) ON DUPLICATE KEY UPDATE Account=VALUES(Account)"""
        try:
            cur.execute("SET FOREIGN_KEY_CHECKS=0")
            cur.executemany(query, cuentas)
            cur.execute("SET FOREIGN_KEY_CHECKS=1")
            print('Cuentas almacenadas ' + media)
        except Exception as e:
            print('Error on line {}'.format(
                sys.exc_info()[-1].tb_lineno), type(e).__name__, e)

    def insertCampanas(campanas, media, conn):
        cur = conn.cursor()
        query = """INSERT INTO Campaings(
                CampaingID,Campaingname,Campaignspendinglimit,
                Campaigndailybudget,Campaignlifetimebudget,Campaignobjective,
                Campaignstatus,AccountsID,StartDate,
                EndDate,Campaingbuyingtype,Campaignbudgetremaining,
                Percentofbudgetused,Cost,CampaingIDMFC,CreateDate)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON DUPLICATE KEY UPDATE
                Campaingname=VALUES(Campaingname),
                Campaignspendinglimit=VALUES(Campaignspendinglimit),
                Campaigndailybudget=VALUES(Campaigndailybudget),
                Campaignlifetimebudget=VALUES(Campaignlifetimebudget),
                Campaignobjective=VALUES(Campaignobjective),
                Campaignstatus=VALUES(Campaignstatus),
                StartDate=VALUES(StartDate),
                EndDate=VALUES(EndDate),
                Campaingbuyingtype=VALUES(Campaingbuyingtype),
                Campaignbudgetremaining=VALUES(Campaignbudgetremaining),
                Percentofbudgetused=VALUES(Percentofbudgetused),
                Cost=VALUES(Cost),
                CampaingIDMFC=VALUES(CampaingIDMFC) """
        try:
            cur.execute("SET FOREIGN_KEY_CHECKS=0")
            cur.executemany(query, campanas)
            cur.execute("SET FOREIGN_KEY_CHECKS=1")
            print('Campanas almacenadas ' + media)
        except Exception as e:
            print('Error on line {}'.format(
                sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
    # Metodo para almacenar Metricas de campanas

    def insertMetricasCampanas(metricas, media, conn):
        cur = conn.cursor()
        query = """INSERT INTO CampaingMetrics
        (CampaingID,Cost,Frequency,
        Reach,Postengagements,Impressions
        ,Clicks,Landingpageviews,
        Videowachesat75,ThruPlay,Conversions,Result,Objetive,CampaignIDMFC,CreateDate, KPICost, AppInstalls, Week,CloseData)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        try:
            cur.execute("SET FOREIGN_KEY_CHECKS=0")
            cur.execute("set global max_allowed_packet=67108864")
            cur.executemany(query, metricas)
            cur.execute("SET FOREIGN_KEY_CHECKS=1")
            print('Metricas Camp almacenadas ' + media)
        except Exception as e:
            print('Error on line {}'.format(
                sys.exc_info()[-1].tb_lineno), type(e).__name__, e)
                
    def insertMetricasCampanasTemp(metricas, media, conn):
        cur = conn.cursor()
        query = """INSERT INTO CampaingMetricsTemp
        (CampaingID,Cost,Frequency,
        Reach,Postengagements,Impressions
        ,Clicks,Landingpageviews,
        Videowachesat75,ThruPlay,Conversions,Result,Objetive,CampaignIDMFC,CreateDate, KPICost, AppInstalls, Week,CloseData) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        try:
            cur.execute("SET FOREIGN_KEY_CHECKS=0")
            cur.execute("set global max_allowed_packet=67108864")
            cur.executemany(query, metricas)
            cur.execute("SET FOREIGN_KEY_CHECKS=1")
            print('Metricas Camp almacenadas ' + media)
        except Exception as e:
            print('Error on line {}'.format(
                sys.exc_info()[-1].tb_lineno), type(e).__name__, e)

    def insertAdsets(adsets, media, conn):
        cur = conn.cursor()
        query = """INSERT INTO Adsets(AdSetID,Adsetname,Adsetlifetimebudget,Adsetdailybudget,Adsettargeting,Adsetend,Adsetstart,CampaingID,Status,CreateDate,Referer,Media)
                               VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                               ON DUPLICATE KEY UPDATE
                                AdSetName=VALUES(AdSetName),Adsetlifetimebudget=VALUES(Adsetlifetimebudget),Adsetdailybudget=VALUES(Adsetdailybudget),
                                Status=VALUES(Status), Adsetend=VALUES(Adsetend), Adsetstart=VALUES(Adsetstart), Media=VALUES(Media)"""
        try:
            cur.execute("SET FOREIGN_KEY_CHECKS=0")
            cur.execute("set global max_allowed_packet=67108864")
            cur.executemany(query, adsets)
            cur.execute("SET FOREIGN_KEY_CHECKS=1")
            print('Adsets almacenadas ' + media)
        except Exception as e:
            print('Error on line {}'.format(
                sys.exc_info()[-1].tb_lineno), type(e).__name__, e)

    def insertAds(ads, media, conn):
        cur = conn.cursor()
        query = """INSERT INTO Ads(AdID,Adname,Country,Adstatus,AdSetID,CreateDate,Referer,Media) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON DUPLICATE KEY UPDATE Adname=VALUES(Adname),Adstatus=VALUES(Adstatus), Media=VALUES(Media);"""
        try:
            cur.execute("SET FOREIGN_KEY_CHECKS=0")

            cur.executemany(query, ads)
            cur.execute("SET FOREIGN_KEY_CHECKS=1")
            print('Ads almacenadas ' + media)
        except Exception as e:
            print('Error on line {}'.format(
                sys.exc_info()[-1].tb_lineno), type(e).__name__, e)

    def insertMetricasAdSet(metricas, media, conn):
        cur = conn.cursor()
        query = """INSERT INTO AdSetMetrics
        (AdSetID,Cost,Frequency,
        Reach,Postengagements,Impressions
        ,Clicks,Landingpageviews,
        Videowachesat75,ThruPlay,Conversions,Country,CreateDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        try:
            cur.execute("SET FOREIGN_KEY_CHECKS=0")
            cur.execute("set global max_allowed_packet=67108864")
            cur.executemany(query, metricas)
            cur.execute("SET FOREIGN_KEY_CHECKS=1")
            print('Metricas Adsets almacenadas ' + media)
        except Exception as e:
            print('Error on line {}'.format(
                sys.exc_info()[-1].tb_lineno), type(e).__name__, e)

    def insertMetricasAdSet_daily(metricas, media, conn):
        cur = conn.cursor()
        query = """INSERT INTO AdSetMetrics_daily
        (id,AdSetID,Cost,Frequency,
        Reach,Postengagements,Impressions
        ,Clicks,Landingpageviews,
        Videowachesat75,ThruPlay,Conversions,Country,CreateDate,CampaignIDMFC,Result) 
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON duplicate key update
        Cost=VALUES(Cost),Frequency=VALUES(Frequency),Reach=VALUES(Reach),Postengagements=VALUES(Postengagements)
        ,Impressions=VALUES(Impressions),Clicks=VALUES(Clicks),Landingpageviews=VALUES(Landingpageviews)
        ,Videowachesat75=VALUES(Videowachesat75),Result=VALUES(Result),
        UpdateDate=VALUES(CreateDate)
        """
        try:
            cur.execute("SET FOREIGN_KEY_CHECKS=0")
            cur.execute("set global max_allowed_packet=67108864")
            cur.executemany(query, metricas)
            cur.execute("SET FOREIGN_KEY_CHECKS=1")
            print('Metricas Adsets almacenadas ' + media)
        except Exception as e:
            print('Error on line {}'.format(
                sys.exc_info()[-1].tb_lineno), type(e).__name__, e)

    def insertMetricasAd(metricas, media, conn):
        cur = conn.cursor()
        query = """INSERT INTO MetricsAds
        (AdID,Cost,Frequency,
        Reach,Postengagements,Impressions
        ,Clicks,Landingpageviews,
        Videowachesat75,ThruPlay,Conversions,Country,CreateDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        try:
            cur.execute("SET FOREIGN_KEY_CHECKS=0")

            cur.executemany(query, metricas)
            cur.execute("SET FOREIGN_KEY_CHECKS=1")
            print('Metricas Ads almacenadas ' + media)
        except Exception as e:
            print(e)

    def insertCreativesAd(creatives, media, conn):
        cur = conn.cursor()
        query = """INSERT INTO CreativeAds
        (AdcreativeID, Creativename, Linktopromotedpost, AdcreativethumbnailURL, AdcreativeimageURL, ExternaldestinationURL, Adcreativeobjecttype, PromotedpostID, Promotedpostname, PromotedpostInstagramID, Promotedpostmessage, Promotedpostcaption, PromotedpostdestinationURL, PromotedpostimageURL, LinktopromotedInstagrampost, AdID,Adname,Country,CreateDate) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON DUPLICATE KEY UPDATE
        AdcreativeID=VALUES(AdcreativeID),
        Creativename=VALUES(Creativename);"""
        try:
            cur.execute("SET FOREIGN_KEY_CHECKS=0")
            cur.execute("set global max_allowed_packet=67108864")
            cur.executemany(query, creatives)
            cur.execute("SET FOREIGN_KEY_CHECKS=1")
            print('creatives almacenadas ' + media)
        except Exception as e:
            print('Error on line {}'.format(
                sys.exc_info()[-1].tb_lineno), type(e).__name__, e)

    def insertDiarioCampanas(metricas, media, conn):
        cur = conn.cursor()

        query = """INSERT INTO CampaingMetrics_daily
        (id,CampaingID,Cost,Frequency,
        Reach,Postengagements,Impressions
        ,Clicks,Landingpageviews,
        Videowachesat75,ThruPlay,Conversions,
        Result,Objetive,CampaignIDMFC,CreateDate, KPICost, AppInstalls, Week,CloseData)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON duplicate key update
        Cost=VALUES(Cost),Frequency=VALUES(Frequency),Reach=VALUES(Reach),Postengagements=VALUES(Postengagements)
        ,Impressions=VALUES(Impressions),Clicks=VALUES(Clicks),Landingpageviews=VALUES(Landingpageviews)
        ,Videowachesat75=VALUES(Videowachesat75),Result=VALUES(Result),Objetive=VALUES(Objetive),
        KPICost=VALUES(KPICost),AppInstalls=VALUES(AppInstalls),UpdateDate=VALUES(CreateDate)
        """

        try:
            cur.execute("SET FOREIGN_KEY_CHECKS=0;")
            cur.executemany(query, metricas)
            cur.execute("SET FOREIGN_KEY_CHECKS=1;")
            print('Camps Diario ' + media)
        except Exception as e:
            print(e)

    def insertExtraMetrics(metricas, media, conn):
        cur = conn.cursor()
        query = """INSERT INTO CampaingsExtraMetrics
            (CampaingID,Estimatedadrecalllift,CreateDate) VALUES (%s,%s,%s)"""
        try:
            cur.execute("SET FOREIGN_KEY_CHECKS=0")
            cur.execute("set global max_allowed_packet=67108864")
            cur.executemany(query, metricas)
            cur.execute("SET FOREIGN_KEY_CHECKS=1")
            print('Reporting Metricas extras ' + media)
        except Exception as e:
            print('Error on line {}'.format(
                sys.exc_info()[-1].tb_lineno), type(e).__name__, e)

    def insertHistoric(metricas, media, conn):
        cur = conn.cursor()
        query = """INSERT IGNORE INTO HistoricCampaings(
            CampaingID,Campaingname,Campaigndailybudget,
            Campaignlifetimebudget,Percentofbudgetused,
            StartDate,EndDate,Result,Objetive,CampaignIDMFC,
            Cost,Frequency,
            Reach,Postengagements,Impressions,
            Clicks,Landingpageviews,
            VideoWatches75,ThruPlay,Conversions,CreateDate,KPICost) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        try:
            cur.execute("SET FOREIGN_KEY_CHECKS=0")
            cur.execute("set global max_allowed_packet=67108864")
            cur.executemany(query, metricas)
            cur.execute("SET FOREIGN_KEY_CHECKS=1")
            print('Historics ' + media)
        except Exception as e:
            print('Error on line {}'.format(
                sys.exc_info()[-1].tb_lineno), type(e).__name__, e)

    def CreativeAdf(creatives, media, conn):
        cur = conn.cursor()
        query = """INSERT INTO CreativeAds(AdID, Adname, Size,
                            AdType, Weight, Format,
                            AdMessage, CreativeType) VALUES
                            (%s,%s,%s,
                            %s,%s,%s,
                            %s,%s)
                              ON DUPLICATE KEY UPDATE adname=VALUES(adname),size=VALUES(size),adtype=VALUES(adtype),weight=VALUES(weight),format=VALUES(format),admessage=VALUES(admessage),creativetype=VALUES(creativetype)"""
        try:
            cur.execute("SET FOREIGN_KEY_CHECKS=0")
            cur.executemany(query, creatives)
            cur.execute("SET FOREIGN_KEY_CHECKS=1")
            print('Creatives ' + media)
        except Exception as e:
            print('Error on line {}'.format(
                sys.exc_info()[-1].tb_lineno), type(e).__name__, e)

    def ActualizarEstado(campanas, media, conn):
        cur = conn.cursor()
        query = """Update Campaings set Campaignstatus=%s where CampaingID=%s"""
        try:
            cur.execute("SET FOREIGN_KEY_CHECKS=0")
            cur.execute("set global max_allowed_packet=67108864")
            cur.executemany(query, campanas)
            cur.execute("SET FOREIGN_KEY_CHECKS=1")
            print('Estado actualizado' + media)
        except Exception as e:
            print('Error on line {}'.format(
                sys.exc_info()[-1].tb_lineno), type(e).__name__, e)

    def ActionsCamapanas(campanas, media, conn):
        cur = conn.cursor()
        query = """INSERT INTO CampaignsActions(Campaingname, CampaingIDMFC, ActionType,
                            ActionTargetID, CampaingID, Actions,ActionTypeID,PeopleAction ,CreateDate) VALUES
                            (%s,%s,%s,
                            %s,%s,%s,%s,%s,%s)"""
        try:
            cur.execute("SET FOREIGN_KEY_CHECKS=0")
            cur.execute("TRUNCATE TABLE CampaignsActions;")
            cur.executemany(query, campanas)
            cur.execute("SET FOREIGN_KEY_CHECKS=1")
            print('Actions ' + media)
            cur.close()
        except Exception as e:
            print(e)

    def ObtenerIDMFC(Id, conn):
        try:
            cur = conn.cursor(buffered=True)
            query = """ SELECT ID FROM mfcgt.mfccompradiaria 
                        WHERE multiplestiposg like '%{}%';
                    """.format(Id)
            cur.execute(query)
            return cur.fetchone()
        except Exception as e:
            print(e)
            return None

    def insertCampanasReport(campanas, conn):
        cur = conn.cursor()
        query = """INSERT INTO Campaings(
                CampaingID,Campaingname,CampaingIDMFC,CreateDate)
                VALUES (%s,%s,%s,%s)
                 ON DUPLICATE KEY UPDATE Campaingname=VALUES(Campaingname), CreateDate=VALUES(CreateDate)
                """
        try:
            cur.execute("SET FOREIGN_KEY_CHECKS=0")
            cur.executemany(query, campanas)
            cur.execute("SET FOREIGN_KEY_CHECKS=1")
            print('Campanas almacenadas ')
        except Exception as e:
            print(e)

    def insertCreadtiveAdsReport(creativeads, conn):
        cur = conn.cursor()
        query = """INSERT INTO CreativeAds(
                AdcreativeID,Creativename,Linktopromotedpost,AdID,CreateDate)
                VALUES (%s,%s,%s,%s,%s)
                 ON DUPLICATE KEY UPDATE Creativename=VALUES(Creativename), CreateDate=VALUES(CreateDate), Linktopromotedpost=VALUES(Linktopromotedpost)
                """
        try:
            cur.execute("SET FOREIGN_KEY_CHECKS=0")
            cur.executemany(query, creativeads)
            cur.execute("SET FOREIGN_KEY_CHECKS=1")
            print('Creative Ads Almacenadas ')
        except Exception as e:
            print(e)

    def ActualizarConversiones(metricas, conn):
        cur = conn.cursor()
        query = """Update CampaingMetrics set Result=%s,Conversions=%s where id>0 and Week = 46 and CampaignIDMFC=%s"""
        try:
            cur.execute("SET FOREIGN_KEY_CHECKS=0")
            cur.execute("set global max_allowed_packet=67108864")
            cur.executemany(query, metricas)
            cur.execute("SET FOREIGN_KEY_CHECKS=1")
            print('Estado actualizado')
        except Exception as e:
            print('Error on line {}'.format(
                sys.exc_info()[-1].tb_lineno), type(e).__name__, e)

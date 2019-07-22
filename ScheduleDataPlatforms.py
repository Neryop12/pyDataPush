import pushDataMedia as DM
import threading
import schedule
import time
from datetime import datetime
from datetime import datetime
import logger

def saveData(conn):
   DM.push_camps(conn)
   time.sleep(10)
   DM.push_adsets(conn)
   time.sleep(10)
   DM.push_ads(conn)

def errores(conn):
   DM.push_errors(conn)

def revisionerrores(conn):
    DM.reviewerrors(conn)

#https://github.com/dbader/schedule
DM.openConnection()
schedule.every(15).minutes.do(saveData(conn))
schedule.every(20).minutes.do(push_errors(conn))
schedule.every(25).minutes.do(revisionerrores(conn))
conn.close()
#schedule.every(110).minutes.do(FBAdsSets)
#schedule.every(120).minutes.do(GOCampaings)
#schedule.every(130).minutes.do(GOAdsSets)
#schedule.every(140).minutes.do(GOAds)

#schedule.every().hour.do(job)
#schedule.every().day.at("10:30").do(job)
#schedule.every(5).to(10).minutes.do(job)
#schedule.every().monday.do(job)
#schedule.every().wednesday.at("15:26").do(job)
#schedule.every().minute.at(":17").do(job)

while True:

   schedule.run_pending(conn)

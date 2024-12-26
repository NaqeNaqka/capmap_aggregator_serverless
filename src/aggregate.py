import os
import json
import threading
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

from GetJAO import getJao
from GetSEECAO import getSEECAO
from supaConnect import uploadToSupa, checkRemoteFileDate

from logging_config import setup_logging
logger = setup_logging()

def main(start_date = datetime, end_date = datetime):
    if not start_date:
        # December 1, 2019, 23:00:00
        start_date = datetime(2019, 12, 1, 23, 0, 0)
    if not end_date:
         # January 1, 2025, 23:59:59
        end_date = datetime(2025, 1, 1, 23, 59, 59)


    logger.info("Aggregator running...")
    collectionWasAccessedB4Today = False
    auctionsFileName = "auctions.json"

    try:
        lastModifiedDate_local = checkRemoteFileDate()
        distanceFromAccessTime = datetime.timestamp(datetime.today()) - datetime.timestamp(lastModifiedDate_local)
        collectionWasAccessedB4Today = int(distanceFromAccessTime) >= 86400 #if 24 hrs (in seconds) or more have passed
        
        logger.info(f"Was collection accessed before today? {collectionWasAccessedB4Today}")
        logger.info(f"Last Access Date (Tirana TZ): {lastModifiedDate_local}")
        logger.info(f"Time elapsed: ~{int(distanceFromAccessTime / 3600)}hrs")
        
        CONTINUE_AGGREGATION = collectionWasAccessedB4Today
            
    except FileNotFoundError:
        logger.info("An auction collection file is not present.")    
        CONTINUE_AGGREGATION = True 

    if CONTINUE_AGGREGATION:
        logger.info("Continuing with aggregation...")

        url = os.environ.get("SUPABASE_URL")
        key = os.environ.get("SUPABASE_KEY")

        all_data = []
        listLock = threading.Lock()
                
        def getDataFrom(source, start_date, end_date, horizon):
            if source == "JAO":
                collector = getJao
            else:
                collector = getSEECAO
                
            data = collector(start_date, end_date, horizon)
            with listLock:
                all_data.append(data)
            
        with ThreadPoolExecutor(max_workers=10) as executor:
            # Caution: setting the horizon to Yearly will collect auctions based ONLY on the dates' years (JAO)
            executor.submit(getDataFrom, "JAO", start_date, end_date, "Monthly")
            executor.submit(getDataFrom, "JAO", start_date, end_date, "Yearly")
            executor.submit(getDataFrom, "SEECAO", start_date, end_date, "Monthly")
            executor.submit(getDataFrom, "SEECAO", start_date, end_date, "Yearly")

        if not all_data:
            logger.info("\nNo Data collected.")
            
        else:
            with open(auctionsFileName, 'w') as file:
                    file.write(json.dumps(all_data))
            logger.info(f"Data successfully exported to {auctionsFileName}")
            uploadToSupa() 

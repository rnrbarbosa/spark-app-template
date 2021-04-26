import logging
import logging.config
import sys

class Store:
    logging.config.fileConfig("../conf/logging.conf")

    def __init__(self, spark):
        self.spark = spark

    def store(self,df):
        logger = logging.getLogger("Store")
        logger.info("START - Storing Data")

        try:
            # df.coalesce(1)\
            #     .write\
            #     .option("header", "true")\
            #     .csv("data/out.csv")
            logger.info("END - Stored Data Succefully")
        except Exception as exp:
            logger.error("An error occurred while persisting data>"+ str(exp))
            raise Exception("Problem persisting the Data")
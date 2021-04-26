import logging
import logging.config

class Store:
    logging.config.fileConfig("../conf/logging.conf")

    def __init__(self, spark):
        self.spark = spark

    def store(self,df):
        logger = logging.getLogger("Store")
        logger.info("Store Dataframe output")
        # df.coalesce(1)\
        #     .write\
        #     .option("header", "true")\
        #     .csv("data/out.csv")
        logger.info("ENd - Storing Data")

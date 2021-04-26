import logging


class Store():
    logging.config.fileConfig("conf/logging.conf")

    def __init__(self, spark):
        self.spark = spark

    def store(self,df):
        logging.info("Start - Storing Data")
        # df.coalesce(1)\
        #     .write\
        #     .option("header", "true")\
        #     .csv("data/out.csv")
        logging.info("ENd - Storing Data")

import logging


class Transform():
    logging.config.fileConfig("conf/logging.conf")

    def __init__(self, spark):
        self.spark = spark

    def transform(self,df):
        logging.info("Start - Transform Data")
        df = df.na.drop()
        df.show()
        logging.info("End - Processing Data")
        return df

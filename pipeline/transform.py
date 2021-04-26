import logging
import logging.config


class Transform:
    logging.config.fileConfig("../conf/logging.conf")

    def __init__(self, spark):
        self.spark = spark

    def transform(self,df):
        logger = logging.getLogger("Transform")
        logger.info("Processing Data...")
        df = df.na.drop()
        df.show()
        logger.info("End - Processing Data")
        return df

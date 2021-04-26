import logging
import logging.config
import sys

class Transform:
    logging.config.fileConfig("conf/logging.conf")

    def __init__(self, spark):
        self.spark = spark

    def transform(self,df):
        logger = logging.getLogger("Transform")
        logger.info("Processing Data...")
        try:
            df = df.na.drop()
            df.show()
            logger.info("End - Processing Data")

        except Exception as exp:
            logger.error("An error occurred while persisting data>" + str(exp))
            sys.exit(1)

        return df
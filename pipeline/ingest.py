import logging
import logging.config
import sys

class Ingest:
    logging.config.fileConfig("../conf/logging.conf")

    def __init__(self, spark):
        self.spark = spark

    def ingest(self):
        try:
            logger = logging.getLogger("Ingest")
            logger.info("Ingesting Data from CSV to Dataframe")
            df = self.spark.read.csv("data/data.csv", inferSchema=True, header=True)
            logger.info("Dataframe Created")

            df.show()
            df.describe().show()
            return df
        except Exception as exp:
            logger.error("An error occurred while ingesting data>"+ str(exp))
            sys.exit(1)

from pipeline.ingest import Ingest
from pipeline.store import Store
from pipeline.transform import Transform
from pyspark.sql import SparkSession
import logging
import logging.config


class Pipeline:
    def __init__(self):
        self.spark = SparkSession\
            .builder\
            .appName("Data Pipeline")\
            .enableHiveSupport()\
            .getOrCreate()

    def pipeline(self):
        print("Running Pipeline....")
        ingest_step = Ingest(self.spark)
        df = ingest_step.ingest()
        transform_step = Transform(self.spark)
        df = transform_step.transform(df)
        store_step = Store(self.spark)
        store_step.store(df)



if __name__ == '__main__':

    logging.config.fileConfig("conf/logging.conf")
    logging.info("Starting APPLICATION....")
    logging.debug("debug Logging")
    logging.info("Info Logging")
    logging.warning("Warning Logging")
    logging.error("Error Logging")

    pipeline = Pipeline()
    pipeline.pipeline()

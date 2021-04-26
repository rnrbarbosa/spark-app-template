from pipeline.ingest import Ingest
from pipeline.store import Store
from pipeline.transform import Transform
from pyspark.sql import SparkSession
import logging
import logging.config


class Pipeline:
    logging.config.fileConfig("conf/logging.conf")

    def __init__(self):
        self.spark = SparkSession\
            .builder\
            .appName("Data Pipeline")\
            .enableHiveSupport()\
            .getOrCreate()

    def pipeline(self):
        logging.info("Starting APPLICATION....")
        logging.info("Running Pipeline....")

        ingest_step = Ingest(self.spark)
        df = ingest_step.ingest()
        transform_step = Transform(self.spark)
        df = transform_step.transform(df)
        store_step = Store(self.spark)
        store_step.store(df)



if __name__ == '__main__':

    pipeline = Pipeline()
    pipeline.pipeline()

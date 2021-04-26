from pipeline.ingest import Ingest
from pipeline.store import Store
from pipeline.transform import Transform
from pyspark.sql import SparkSession
import logging
import logging.config
import sys

from pipeline import ingest, transform, store
from pyspark.sql import SparkSession


class Pipeline:
    logging.config.fileConfig("conf/logging.conf")

    def __init__(self):
        self.spark = SparkSession\
            .builder\
            .appName("Data Pipeline")\
            .enableHiveSupport()\
            .getOrCreate()

    def pipeline(self):
        # try
            logging.info("Starting APPLICATION....")
            logging.info("Running Pipeline....")
            #########################################
            # INGESTION STEP
            #########################################
            ingest_step = ingest.Ingest(self.spark)
            df = ingest_step.ingest()
            #########################################
            # TRANSFORM / PROCESSING STEP
            #########################################
            transform_step = transform.Transform(self.spark)
            df = transform_step.transform(df)
            #########################################
            # STORE /SERVING STEP
            #########################################
            store_step = store.Store(self.spark)
            store_step.store(df)
        # except Exception as exp:
        #     logging.error("Error")
        # sys.exit(1)
            return



if __name__ == '__main__':

    pipeline = Pipeline()
    pipeline.pipeline()

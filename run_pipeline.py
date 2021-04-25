from pipeline.ingest import Ingest
from pipeline.store import Store
from pipeline.transform import Transform
from pyspark.sql import SparkSession


class Pipeline:
    def pipeline(self):
        print("Running Pipeline....")
        ingest_step = Ingest(self.spark)
        df = ingest_step.ingest()
        transform_step = Transform(self.spark)
        df = transform_step.transform(df)
        store_step = Store(self.spark)
        store_step.store(df)
        return

    def create_spark_session(self):
        self.spark = SparkSession\
            .builder\
            .appName("Data Pipeline")\
            .enableHiveSupport()\
            .getOrCreate()
        return

if __name__ == '__main__':
    pipeline = Pipeline()
    pipeline.create_spark_session()
    pipeline.pipeline()
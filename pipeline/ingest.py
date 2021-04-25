class Ingest():
    def __init__(self, spark):
        self.spark = spark

    def ingest(self):
        print("Ingesting Data")
        df = self.spark.read.csv("data/data.csv", inferSchema=True, header=True)
        df.show()
        df.describe().show()
        return df

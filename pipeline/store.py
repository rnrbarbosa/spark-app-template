class Store():

    def __init__(self, spark):
        self.spark = spark

    def store(self,df):
        print("Storing Data")
        df.show(1)
        df.toPandas().to_csv("data/transformed_data.csv")
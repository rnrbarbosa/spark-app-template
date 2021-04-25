
class Transform():

    def __init__(self, spark):
        self.spark = spark

    def transform(self,df):
        print("Transform Data")
        df = df.na.drop()
        df.show()
        return df

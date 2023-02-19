import logging
import logging.config
import psycopg2
import pandas
import pandas.io.sql as sqlio

class Ingest:
    logging.config.fileConfig("resource/configs/logging.conf")
    def __init__(self, spark):
        self.spark = spark


    def read_from_csv(self):
        logger = logging.getLogger("Ingest")
        logger.info("Ingesting Data")

        df = self.spark.read.format("csv").option("header","true").load("./retailstore.csv")
        return df

    # Read data from HiveQL
    def read_from_hive(self):
        logger = logging.getLogger("Ingest")
        logger.info("Ingesting Data")

        df = self.spark.sql("select * from yamamoto_db.yamamoto_table")
        df.show()
        df.describe().show()
        return df

    # Read data from Postgres Database
    def read_from_postgres_psycopg2(self):
        # Setting Logging
        logger = logging.getLogger("Ingest")
        logger.info("Ingesting Data")

        connection = psycopg2.connect(user='postgres', password='1011997', host='localhost', database='yamamoto')
        cursor = connection.cursor()
        sql_query = "select * from yamamoto_schema.yamamoto_table"
        pdDF = sqlio.read_sql_query(sql_query, connection)
        sparkDf = self.spark.createDataFrame(pdDF)
        sparkDf.show()
        return sparkDf

    # .option("dbtable","yamamoto_schema.yamamoto_table") \
    def read_from_postgres_using_jdbc(self):
        df =self.spark.read.format("jdbc")\
            .option("url","jdbc:postgresql://localhost:5432/yamamoto")\
            .option("dbtable", "yamamoto_schema.course_table1") \
            .option("user", "postgres") \
            .option("password", "1011997")\
            .load()
        df.show()

        return df
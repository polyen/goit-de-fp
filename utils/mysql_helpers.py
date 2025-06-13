from dotenv import load_dotenv
import os

load_dotenv()

jdbc_url = os.getenv("JDBC_URL")
jdbc_user = os.getenv('JDBC_USER')
jdbc_password = os.getenv('JDBC_PASSWORD')


def read_sql_table(spark, table_name):
    return spark.read.format('jdbc').options(
        url=jdbc_url,
        driver='com.mysql.cj.jdbc.Driver',
        dbtable=table_name,
        user=jdbc_user,
        password=jdbc_password) \
        .load()


def write_sql_table(df, table_name):
    df.write.format('jdbc').options(
        url=jdbc_url,
        driver='com.mysql.cj.jdbc.Driver',
        dbtable=table_name,
        user=jdbc_user,
        password=jdbc_password) \
        .mode('append') \
        .save()

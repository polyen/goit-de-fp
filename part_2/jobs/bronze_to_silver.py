from constants import BRONZE_DIR, BIO_FILE, SILVER_DIR, RESULTS_FILE
from get_spark import get_spark_session
import re
from pyspark.sql.functions import udf, expr
from pyspark.sql.types import StringType


def clean_text(text):
    return re.sub(r'[^a-zA-Z0-9,.\\"\']', '', str(text))


clean_text_udf = udf(clean_text, StringType())

bio_df_text_columns = ["name", "sex", "born", "country", "country_noc", "description", "special_notes"]
results_df_text_columns = ["edition", "country_noc", "sport", "event", "pos", "medal"]


def process_df(df, text_columns):
    for column in text_columns:
        df = df.withColumn(column, clean_text_udf(df[column]))
    return df.dropDuplicates()


def bronze_to_silver():
    spark = get_spark_session()

    bio_df = spark.read.parquet(f'{BRONZE_DIR}/{BIO_FILE}.parquet')
    bio_df = process_df(bio_df, bio_df_text_columns)

    for col_name in ['height', 'weight']:
        bio_df = bio_df.filter(
            expr(f"try_cast({col_name} as double) is not null")
        )

    print("Bio DataFrame Schema:")
    bio_df.show(10)

    bio_df.write.mode('overwrite').parquet(f'{SILVER_DIR}/{BIO_FILE}.parquet')

    results_df = spark.read.parquet(f'{BRONZE_DIR}/{RESULTS_FILE}.parquet')
    results_df = process_df(results_df, results_df_text_columns)

    print("Results DataFrame Schema:")
    results_df.show(10)

    results_df.write.mode('overwrite').parquet(f'{SILVER_DIR}/{RESULTS_FILE}.parquet')


if __name__ == "__main__":
    bronze_to_silver()
    print("Data processed from bronze to silver layer.")

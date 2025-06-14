from constants import BIO_FILE, RESULTS_FILE, BRONZE_DIR
from download_data import download_data
from get_spark import get_spark_session


def lending_to_bronze():
    download_data(BIO_FILE)
    download_data(RESULTS_FILE)

    spark = get_spark_session()
    bio_df = spark.read.csv(f"{BIO_FILE}.csv", header=True, inferSchema=True)
    print('Bio DataFrame Schema:')
    bio_df.show(10)

    results_df = spark.read.csv(f"{RESULTS_FILE}.csv", header=True, inferSchema=True)
    print('Results DataFrame Schema:')
    results_df.show(10)

    bio_df.write.mode('overwrite').parquet(f'{BRONZE_DIR}/{BIO_FILE}.parquet')
    results_df.write.mode('overwrite').parquet(f'{BRONZE_DIR}/{RESULTS_FILE}.parquet')


if __name__ == "__main__":
    lending_to_bronze()
    print("Data downloaded to bronze layer.")

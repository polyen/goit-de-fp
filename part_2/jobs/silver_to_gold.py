from pyspark.sql.types import StringType

from constants import SILVER_DIR, BIO_FILE, RESULTS_FILE, GOLD_DIR
from get_spark import get_spark_session
from pyspark.sql import functions as F


def silver_to_gold():
    spark = get_spark_session()

    # зчитувати дві таблиці: silver/athlete_bio та silver/athlete_event_results,
    bio_df = spark.read.parquet(f'{SILVER_DIR}/{BIO_FILE}.parquet')
    bio_df.withColumn("athlete_id", F.col("athlete_id").cast(StringType()))

    results_df = spark.read.parquet(f'{SILVER_DIR}/{RESULTS_FILE}.parquet')
    results_df.withColumn("athlete_id", F.col("athlete_id").cast(StringType()))

    #  join за колонкою athlete_id
    gold_df = results_df.join(bio_df, results_df.athlete_id == bio_df.athlete_id, 'inner') \
        .select(results_df.athlete_id.alias('athlete_id'),
                results_df.country_noc.alias('country_noc'),
                results_df.sport.alias('sport'),
                results_df.medal.alias('medal'),
                bio_df.sex.alias('sex'),
                bio_df.height.alias('height'),
                bio_df.weight.alias('weight'),
                )

    # для кожної комбінації цих 4 стовпчиків — sport, medal, sex, country_noc — знаходити середні значення weight і height,
    # додати колонку timestamp з часовою міткою виконання програми,
    gold_df = gold_df.groupBy('sport', 'medal', 'sex', 'country_noc') \
        .agg(
        F.round(F.avg("height"), 2).alias("avg_height"),
        F.round(F.avg("weight"), 2).alias("avg_weight"),
        F.current_timestamp().alias("timestamp")
    )

    print("Joined DataFrame Schema:")
    gold_df.show(10)

    # записувати дані в gold/avg_stats.
    gold_df.write.mode('overwrite').parquet(f'{GOLD_DIR}/avg_stats.parquet')

    print("Data processed from silver to gold layer.")


if __name__ == "__main__":
    silver_to_gold()

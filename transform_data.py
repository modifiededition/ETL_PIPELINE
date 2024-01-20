import argparse
from pyspark.sql import SparkSession
import pyspark.sql.functions as F


required_columns = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "tip_amount",
    "total_amount",
]

zone_req_cols = ["LocationID", "Zone"]


def fn_to_get_season(x):
    """
    This function is designed to determine and return the season of the month.
    """
    if x in [12, 1, 2]:
        return "Winter"

    elif x in [3, 4, 5]:
        return "Spring"

    elif x in [6, 7, 8]:
        return "Autumn"
    else:
        return "Summer"


def get_payment_type_values(x):
    """
    This function is intended to establish a mapping of payment types within the dataset to their corresponding values.
    """
    if x == 1:
        return "Credit card"

    elif x == 2:
        return "Cash"

    else:
        return "Others"


def fn_to_get_day(x):
    """
    This function is designed to provide the day of the week as output.
    """
    if x == 1:
        return "Sunday"
    elif x == 2:
        return "Monday"
    elif x == 3:
        return "Tuesday"
    elif x == 4:
        return "Wednesday"
    elif x == 5:
        return "Thursday"
    elif x == 6:
        return "Friday"
    elif x == 7:
        return "Saturday"


def start_pre_processing():
    """
    This function orchestrates the complete data processing pipeline, encompassing the following key stages:
    - Data Retrieval: It involves fetching raw data from the Data Lake in Google Cloud Storage (GCS).
    - Transformations: The function applies data transformations across a distributed environment,
    leveraging Apache Spark managed by a DataProc cluster.
    - Data Storage: Finally, the processed results are stored in the data warehouse, specifically in BigQuery.
    """

    df = spark.read.parquet(input_path)

    df = cleaning(df)

    zone_df = fetch_zone_data()

    df = fetching_pickup_and_dropoff_location_names(df, zone_df)

    df = engineering_data(df)

    # write dataframe to the Big Query
    df.write.format("bigquery").option("table", output_path).mode("append").save()


def cleaning(df):
    """
    This function is designed to execute data cleansing operations on the raw dataset.
    The cleansing process involves the following criteria for filtering out undesirable data:

    - Null Passenger Count: Rows with null values in the passenger count field will be excluded.

    - Negative Trip Distance: Rows with a trip distance less than equal to zero will be omitted.

    - Negative Total Amount: Rows where the total amount is less than equal to zero will be removed.
    """

    subset_df = df.select(required_columns)

    # Filter DataFrame to include rows where the passenger_count is not null
    not_null_data_df = subset_df.filter(~(F.col("passenger_count").isNull()))

    # Filter DataFrame to include rows where the trip_distance is greater than 0
    trip_distance_gt_than_zero_df = not_null_data_df.filter(
        not_null_data_df.trip_distance > 0
    )

    # Filter DataFrame to include rows where the total_amount is greater than 0

    trip_distance_and_fare_gt_than_zero_df = trip_distance_gt_than_zero_df.filter(
        trip_distance_gt_than_zero_df.total_amount > 0
    )

    return trip_distance_and_fare_gt_than_zero_df


def fetch_zone_data():
    """
    This function is designed to retrieve the zone lookup dataset from Google Cloud Storage (GCS).
    """
    zone_df = spark.read.csv(f"gs://{bucket_name}/code/zone_lookup.csv", header=True)
    subset_zone_df = zone_df.select(zone_req_cols)

    return subset_zone_df


def fetching_pickup_and_dropoff_location_names(df, subset_zone_df):
    """
    This function is specifically developed to merge the pickup and drop-off location IDs of the taxi rides dataset
    with the corresponding zone names.
    """

    merged_df_pickup_location = df.join(
        subset_zone_df, df["PULocationID"] == subset_zone_df["LocationID"], how="left"
    )

    merged_df_pickup_location = merged_df_pickup_location.drop(
        *["PULocationID", "LocationID"]
    )

    merged_df_pickup_location = merged_df_pickup_location.withColumnRenamed(
        "Zone", "pickup_location"
    )

    merged_df_dropoff_location = merged_df_pickup_location.join(
        subset_zone_df,
        merged_df_pickup_location["DOLocationID"] == subset_zone_df["LocationID"],
        how="left",
    )

    merged_df_dropoff_location = merged_df_dropoff_location.drop(
        *["DOLocationID", "LocationID"]
    )

    merged_df_dropoff_location = merged_df_dropoff_location.withColumnRenamed(
        "Zone", "dropoff_location"
    )

    return merged_df_dropoff_location


def engineering_data(df):
    """
    This function is employed to execute essential transformations, encompassing the mapping of payment type values,
    as well as the extraction of year, month, day of the month, day of the week, and hour from the pickup time.
    """

    map_pay_type_val = F.udf(get_payment_type_values)

    merged_df_dropoff_location = df.withColumn(
        "payment_type", map_pay_type_val("payment_type")
    )

    merged_df_dropoff_location = (
        merged_df_dropoff_location.withColumn("year", F.year("tpep_pickup_datetime"))
        .withColumn("month", F.month("tpep_pickup_datetime"))
        .withColumn("day_of_month", F.dayofmonth("tpep_pickup_datetime"))
        .withColumn("day", F.dayofweek("tpep_pickup_datetime"))
        .withColumn("hour", F.hour("tpep_pickup_datetime"))
    )

    map_to_get_season = F.udf(fn_to_get_season)

    merged_df_dropoff_location = merged_df_dropoff_location.withColumn(
        "season", map_to_get_season("month")
    )

    map_to_get_day_val = F.udf(fn_to_get_day)

    merged_df_dropoff_location = merged_df_dropoff_location.withColumn(
        "day", map_to_get_day_val("day")
    )

    return merged_df_dropoff_location


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Get I/O Argeument")
    parser.add_argument("--input_path", help="Input data path")
    parser.add_argument("--output_path", help="Result Output Path")
    parser.add_argument("--bucket_name", help="bucket name")

    args = parser.parse_args()

    input_path = args.input_path
    output_path = args.output_path
    bucket_name = args.bucket_name

    # configuring Spark
    spark = SparkSession.builder.appName("spark-jobs-dataproc").getOrCreate()

    spark.conf.set("temporaryGcsBucket", bucket_name)

    start_pre_processing()

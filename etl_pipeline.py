import os
import requests
from typing import Union
import subprocess

from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

from config import bucket_name, cluster_name, region, jars_file, big_query_schema_name


@task(name="Extracting", retries=2)
def fetch(year, month):
    """
    This function is designed to fetch the yellow taxi dataset in Parque format from the web.
    In the event of a failure, the funtion is configured to retry.
    """

    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{year}-{month}.parquet"

    # make directory on disk(if not created)
    directory_path = f"taxi_rides_data/yellow/{year}"

    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        print(f"Directory '{directory_path}' created successfully.")

    # Send an HTTP GET request to the URL
    response = requests.get(url)
    file_name = f"{directory_path}/yellow_{year}_{month}.parquet"

    if response.status_code == 200:
        with open(file_name, "wb") as f:
            f.write(response.content)
    else:
        raise Exception(f"{file_name} not fetched. Retrying!")

    return file_name


@task(name="Loading", description="Writing raw data file to the GCS(data lake)")
def write_gcs(path: str):
    """
    This function facilitates the uploading of a Parquet file to Google Cloud Storage (GCS).
    """

    gcs_block = GcsBucket.load("my-gcs-block")
    gcs_block.upload_from_path(from_path=path, to_path=path)
    return path


@task(name="Transforming")
def transform(command):
    """
    This function orchestrates the execution of Spark jobs to process raw data within a distributed environment,
    managed by the Dataproc cluster. The processing is distributed across the specified number of workers, as configured
    during the infrastructure setup using Terraform.
    """
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    print(result.stderr)


def handle_year_and_month_val(years, months):
    if isinstance(years, str):
        years = [years]

    if isinstance(months, str):
        months = [months]

    return years, months


@flow(name="nyc_yellow_etl_pipeline")
def parent_etl_pipeline(
    years: Union[list[str], str] = "2020",
    months: Union[list[str], str] = ["01", "02"],
):
    """
    This function is responsible for executing the entire Extract, Transform, Load (ETL) pipeline, consisting of the following sequential steps:

    1. Data Retrieval: Fetching the raw dataset of yellow taxi rides from the web.
    2. Data Storage: Storing the raw data in the Data Lake (Google Cloud Storage - GCS).
    3. Transformation with Spark: Retrieving the data from the Data Lake and applying transformations using Spark jobs in a Dataproc cluster.
    The number of workers for parallel processing is configurable during infrastructure setup using Terraform.
    4. Loading to BigQuery: Loading the transformed dataset into BigQuery.
    5. Dashboard Preparation: Utilizing the cleaned and processed dataset from BigQuery to create the necessary data for our dashboard.

    The orchestration and monitoring of the entire process are handled by the Prefect.

    To initiate the pipeline, configure your Google Cloud Storage (GCS) bucket name and Dataproc cluster name in the `config.py` file.

    Activate your virtual environment and execute the following commands to run the pipeline:

    1. Start the Prefect server for monitoring your flow run:
    -  prefect server start

    2. Run the entire pipeline with the following command, specifying the desired years and months for fetching yellow taxi ride data:
    -  prefect deployment build etl_pipeline:parent_etl_pipeline -n yellow_taxi_rides_flow \
          --params='{"years":["2020","2021"], "months": ["01","02","03"]}' -a

    In this example, the specified years are 2020 and 2021, with corresponding months being 1, 2, and 3.
    The pipeline will retrieve yellow taxi ride data for January, February, and March of both 2020 and 2021.

    Visit the Prefect server UI (default: localhost:4200) to monitor the flow run.

    Execute the flow run by starting an agent from the work pool using the command:
    - prefect agent start -p "your_agent_name"

    The agent name, along with the command to start an agent will appear on your terminal after executing the second command.
    In the Prefect UI's deployment section, you will find the `nyc_yellow_etl_pipeline'.
    Start the deployment by clicking the "Run" button on the screen.

    Note:
    - Additionally, you have the option to schedule the execution of your pipeline directly from the Prefect UI. Navigate to the deployment section,
      select your created deployment,and configure the desired schedule, specifying the interval for repetition.
    - The agent name, as well as the command to start the agent, will appear on your terminal after executing the second command.
    """

    # submit transform_data script to the dataproc
    command = f"gsutil cp transform_data.py gs://{bucket_name}/code/transform_data.py"
    subprocess.run(command, shell=True, capture_output=True, text=True)

    # check if the zone lookup file exists or not
    try:
        command = f"gsutil stat gs://{bucket_name}/code/zone_lookup.csv"
        subprocess.run(command, shell=True, check=True, capture_output=True, text=True)

    except subprocess.CalledProcessError as e:
        response = requests.get(
            "https://d37ci6vzurychx.cloudfront.net/misc/taxi+_zone_lookup.csv"
        )
        if response.status_code == 200:
            with open("./zone_lookup.csv", "wb") as f:
                f.write(response.content)
        command = f"gsutil cp zone_lookup.csv gs://{bucket_name}/code/zone_lookup.csv"
        subprocess.run(command, shell=True, capture_output=True, text=True)

    years, months = handle_year_and_month_val(years, months)

    for year in years:
        for month in months:
            # fetching data from the web

            path = fetch(year, month)

            # writing data to the GCS
            path = write_gcs(path)

            # transforming raw data and loading the result at big query

            command = f"gcloud dataproc jobs submit pyspark\
                  --cluster={cluster_name}\
                  --region={region}\
                  --jars={jars_file}\
                  gs://{bucket_name}/code/transform_data.py \
                  -- \
                  --input_path=gs://{bucket_name}/{path} \
                  --output_path={big_query_schema_name}.yellow_taxi_rides_processed_data\
                  --bucket_name={bucket_name}"

            transform(command)


if __name__ == "__main__":
    years = ["2020", "2021"]
    months = ["01", "02", "03"]

    parent_etl_pipeline(years=years, months=months)

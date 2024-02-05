import pyarrow as pa
import pyarrow.parquet as pq
import os


if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


################
# these variables are porposely cleared for security
# todo: add a secrets file and read from there.
################

project_id = "xxxxx"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = f"{project_id}-xxxxx.json"
bucket_name_suffix = "xx"

bucket_name = f"mage_zoomcamp{bucket_name_suffix}"
table_name = "nyc_green_taxi_data"
root_path = f"{bucket_name}/{table_name}"


@data_exporter
def export_data(data, *args, **kwargs):

    table = pa.Table.from_pandas(data)

    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path = root_path,
        partition_cols = ['lpep_pickup_date'],
        filesystem = gcs
    )





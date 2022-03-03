import os
from datetime import datetime
from pyspark.sql import SparkSession
from utils import *

if __name__ == '__main__':
    start_at = datetime.now()
    time_fmt = start_at.timestamp()
    env = os.getenv('ENV')
    print(f"The processing is starting at {start_at} on {env}")
    spark = init(SparkSession, f"SparkPOC_{time_fmt}", env)

    path_url = "hdfs://localhost:9000"
    path_in = f"{path_url}/data/raws/lab.json"
    path_out = f"{path_url}/data/output/{time_fmt}/csv"

    raw_df = read_json(spark, path_in, debug=1)
    parse_df = parse_json(raw_df)
    write_data(parse_df, path_out)

    spark.stop()

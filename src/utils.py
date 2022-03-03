from pyspark.sql.functions import *
from pyspark.sql.types import *


def init(SparkSession, name, env):
    if env == 'local':
        spark = SparkSession.builder \
            .config("spark.sql.debug.maxToStringFields", "1000") \
            .appName(name) \
            .getOrCreate()
        return spark
    else:
        spark = SparkSession.builder \
            .config("spark.sql.debug.maxToStringFields", "1000") \
            .appName(name) \
            .getOrCreate()
        return spark


def read_json(spark, path, multi_line=False, debug=0):
    df = spark.read.json(path=path, multiLine=multi_line)
    if debug:
        df.show(3)
        df.printSchema()

    return df


def write_data(df, path):
    df.write \
        .mode('overwrite') \
        .option('header', 'true') \
        .csv(path)


def read_nested_json(df):
    column_list = []

    for column_name in df.schema.names:
        # print("Outside isinstance loop: " + column_name)
        if isinstance(df.schema[column_name].dataType, ArrayType):
            # print("Inside isinstance loop of ArrayType: " + column_name)
            df = df.withColumn(column_name, explode_outer(column_name).alias(column_name))
            column_list.append(column_name)

        elif isinstance(df.schema[column_name].dataType, StructType):
            # print("Inside isinstance loop of StructType: " + column_name)
            for field in df.schema[column_name].dataType.fields:
                column_list.append(col(column_name + "." + field.name).alias(column_name + "_" + field.name))
        else:
            column_list.append(column_name)

    df_out = df.select(column_list)
    return df_out


def parse_json(df):
    read_nested_json_flag = True

    while read_nested_json_flag:
        # print("Reading Nested JSON File ... ")
        df = read_nested_json(df)
        # df.show(100, False)
        read_nested_json_flag = False

        for column_name in df.schema.names:
            if isinstance(df.schema[column_name].dataType, ArrayType):
                read_nested_json_flag = True
            elif isinstance(df.schema[column_name].dataType, StructType):
                read_nested_json_flag = True

    return df

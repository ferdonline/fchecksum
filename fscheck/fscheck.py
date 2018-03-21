from __future__ import absolute_import
from pyspark.sql import types as T
import sparkmanager as sm
import os

_MB = 1024**2
_DEBUG=True

spark_config = {
    "spark.shuffle.compress": False,
    "spark.checkpoint.compress": True,
    "spark.sql.autoBroadcastJoinThreshold": 0,
    # "spark.sql.catalogImplementation": "hive",
    "spark.sql.files.maxPartitionBytes": 128 * _MB
}


SCHEMA = T.StructType([
    T.StructField("checksum", T.StringType(), False),
    T.StructField("filename", T.StringType(), True),
])


def run(file1, file2, write_out=True, spark_opts=""):
    sm.create("fscheck", spark_config, spark_opts)
    df1 = sm.spark.read.schema(SCHEMA).csv(file1, sep=" ")
    df2 = sm.spark.read.schema(SCHEMA).csv(file2, sep=" ")

    if _DEBUG:
        df1=df1.cache()
        df2 = df2.cache()
        df1.show()
        df2.show()

    ### Checks
    # 1 Only left and right
    only_left = (df1
        .join(df2, "filename", how="left_anti")
        .select(df1.filename)
        .where(df1.filename.isNotNull())
    )
    only_right = (df2
        .join(df1, "filename", how="left_anti")
        .select(df2.filename)
        .where(df2.filename.isNotNull())
    )

    # 2 Different checksum
    different_checksum = (
        df1
            .join(df2, "filename")
            .where(df1.checksum != df2.checksum)
            .select(df1.filename)
    )

    # 3 Missing field
    problematic_left = (df1
        .where(df1.filename.isNull())
        .select(df1.checksum).alias("filename")
    ).union(df1
        .where(df1.checksum.isNull())
        .select(df1.filename)
    )
    problematic_right = (df2
        .where(df2.filename.isNull())
        .select(df2.checksum).alias("filename")
    ).union(df2
        .where(df2.checksum.isNull())
        .select(df2.filename)
    )

    all_dfs = {
        "only_left": only_left,
        "only_right": only_right,
        "different_checksum": different_checksum,
        "problematic_left": problematic_left,
        "problematic_right": problematic_right
    }

    if write_out:
        if write_out is True:
            write_out="fscheck_output"
        os.path.exists(write_out) or os.makedirs(write_out)

        for name, df in all_dfs.items():
            out_filepath = os.path.join(write_out, name + ".csv")
            df.write.csv(out_filepath, sep="", mode="overwrite")

    return all_dfs

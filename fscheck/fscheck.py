from __future__ import absolute_import
from pyspark.sql import types as T
import sparkmanager as sm
import os

_MB = 1024**2
_DEBUG=True

spark_config = {
    "spark.shuffle.compress": False,
    "spark.sql.autoBroadcastJoinThreshold": 0,
    "spark.sql.files.maxPartitionBytes": 128 * _MB
}


SCHEMA = T.StructType([
    T.StructField("checksum", T.StringType(), False),
    T.StructField("filename", T.StringType(), True),
])


def run(file1, file2, output=True, spark_options=None):
    sm.create("fscheck", spark_config, spark_options)
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

    if output:
        if output is True:
            output="fscheck_output"
        os.path.exists(output) or os.makedirs(output)

        for name, df in all_dfs.items():
            out_filepath = os.path.join(output, name + ".csv")
            print(" - Creating " + out_filepath)
            df.write.csv(out_filepath, mode="overwrite")

    print("Complete")

    return all_dfs

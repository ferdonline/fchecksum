from __future__ import absolute_import
from pyspark.sql import types as T
from pyspark.sql import functions as F
from collections import OrderedDict
import os

import sparkmanager as sm
from pyspark.storagelevel import StorageLevel


_MB = 1024**2
_DEFAULTS = dict(
    delimiter=",",
    verbosity=1,
)

spark_config = {
    "spark.shuffle.compress": False,
    "spark.sql.autoBroadcastJoinThreshold": 0,
    "spark.sql.files.maxPartitionBytes": 128 * _MB
}



SCHEMA = T.StructType([
    T.StructField("checksum", T.StringType(), False),
    T.StructField("filename", T.StringType(), True),
])


def run(file1, file2, output=True, spark_options=None, **opts):
    # type: (str, str, object, object, **object) -> dict

    # ====== Init Spark and dataframes ======
    sm.create("fscheck", spark_config, spark_options)
    
    options = _DEFAULTS.copy()
    options.update(opts)
    df1 = sm.spark.read.schema(SCHEMA).csv(file1, sep=options["delimiter"])
    df2 = sm.spark.read.schema(SCHEMA).csv(file2, sep=options["delimiter"])
        
    # ======  Optimization ======
    n_partitions = df1.rdd.getNumPartitions()
    shuffle_partitions = ((n_partitions-1)/50 +1) * 50
    if options["verbosity"]:
        print("Processing {} partitions (shuffle counts: {})".format(n_partitions, shuffle_partitions))
    sm.conf.set("spark.sql.shuffle.partitions", shuffle_partitions)

    df1=df1.repartition("filename").persist(StorageLevel.MEMORY_AND_DISK)
    df2=df2.repartition("filename").persist(StorageLevel.MEMORY_AND_DISK)

    # ======  Checks ======
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
        .where("filename is NULL OR checksum is NULL")
        .select(F.when(df1.filename.isNull(), df1.checksum).otherwise(df1.filename).alias("entry"))
    )
    problematic_right = (df2
        .where("filename is NULL OR checksum is NULL")
        .select(F.when(df2.filename.isNull(), df2.checksum).otherwise(df2.filename).alias("entry"))
    )
    
    # ====== Results gathering ======

    all_dfs = OrderedDict([
        ("only_left", only_left),
        ("only_right", only_right),
        ("different_checksum", different_checksum),
        ("problematic_left", problematic_left),
        ("problematic_right", problematic_right)
    ])

    if output:
        if output is True:
            output="fscheck_output"
        os.path.exists(output) or os.makedirs(output)

        for name, df in all_dfs.items():
            out_filepath = os.path.join(output, name + ".csv")
            if options["verbosity"]:
                print(" - Creating " + out_filepath)
            df.write.csv(out_filepath, mode="overwrite")

    return all_dfs

# import sys
# import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("appName").getOrCreate()
# import pandas as pd
# import pyspark.sql.functions as F
# from pyspark.sql import DataFrame

# from functools import reduce
# from operator import add

# from datetime import datetime, timedelta

# from databricks.feature_store import FeatureStoreClient, feature_table

# fs = FeatureStoreClient()

# feat_table = fs.get_table(name="robkisk.demographic_features")
# fs.get_feature_table(name="robkisk.demographic_features")
# fs.read_table(name="robkisk.demographic_features")

# for t in spark.catalog.listTables("hive_metastore.robkisk"):
#     print(t.name)

# for t in spark.catalog.listTables("robkisk.default"):
#     print(t.name)

spark.table("hive_metastore.robkisk.passenger_demographic_base").show(3, False)
spark.table("hive_metastore.robkisk.passenger_ticket_base").show(3, False)
spark.table("hive_metastore.robkisk.passenger_ticket_features").show(3, False)
spark.table("hive_metastore.robkisk.passenger_demographic_features").show(3, False)
spark.table("hive_metastore.robkisk.passenger_labels").show(3, False)

# spark.sql("drop table hive_metastore.robkisk.demographic_features")
# spark.sql("drop table hive_metastore.robkisk.passenger_demographic_features")
# spark.sql("drop table hive_metastore.robkisk.passenger_labels")
# spark.sql("drop table hive_metastore.robkisk.passenger_ticket_features")
# spark.sql("drop table robkisk.default.sklearn_housing")
# spark.sql("drop table hive_metastore.robkisk.passenger_labels")
# spark.sql("drop table hive_metastore.robkisk.passenger_ticket_feautures")

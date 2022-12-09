import sys
import os

import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql import DataFrame

from functools import reduce
from operator import add

from datetime import datetime, timedelta

from databricks.feature_store import FeatureStoreClient, feature_table

fs = FeatureStoreClient()

# feat_table = fs.get_table(name="robkisk.demographic_features")
# fs.get_feature_table(name="robkisk.demographic_features")
# fs.read_table(name="robkisk.demographic_features")

for t in spark.catalog.listTables("hive_metastore.robkisk"):
    print(t.name)

# spark.table("robkisk.demographic_features").show(10, False)

# spark.sql("drop schema if exists robkisk cascade")

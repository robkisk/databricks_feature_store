import pyspark.sql.functions as func
from databricks.feature_store import FeatureStoreClient, feature_table
from pyspark.sql.functions import col

fs = FeatureStoreClient()


def compute_passenger_demographic_features(df):

    # Extract prefix from name, such as Mr. Mrs., etc.
    return (
        df.withColumn("NamePrefix", func.regexp_extract(col("Name"), "([A-Za-z]+)\.", 1))
        # Extract a secondary name in the Name column if one exists
        .withColumn(
            "NameSecondary_extract",
            func.regexp_extract(col("Name"), "\(([A-Za-z ]+)\)", 1),
        )
        # Create a feature indicating if a secondary name is present in the Name column
        .selectExpr(
            "*",
            "case when length(NameSecondary_extract) > 0 then NameSecondary_extract else NULL end as NameSecondary",
        )
        .drop("NameSecondary_extract")
        .selectExpr(
            "PassengerId",
            "Name",
            "Sex",
            "case when Age = 'NaN' then NULL else Age end as Age",
            "SibSp",
            "NamePrefix",
            "NameSecondary",
            "case when NameSecondary is not NULL then '1' else '0' end as NameMultiple",
        )
    )


df = spark.table("robkisk.passenger_demographic_features")
passenger_demographic_features = compute_passenger_demographic_features(df)

# passenger_demographic_features.show(10, False)
# display(passenger_demographic_features)

feature_table_name = "robkisk.demographic_features"

# If the feature table has already been created, no need to recreate
try:
    fs.get_table(feature_table_name)
    print("Feature table entry already exists")
    pass

except Exception:
    fs.create_table(
        name=feature_table_name,
        primary_keys="PassengerId",
        schema=passenger_demographic_features.schema,
        description="Demographic-related features for Titanic passengers",
    )

fs.write_table(name=feature_table_name, df=passenger_demographic_features, mode="merge")

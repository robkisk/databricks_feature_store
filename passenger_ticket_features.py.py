import pyspark.sql.functions as func
from databricks.feature_store import FeatureStoreClient, feature_table
from pyspark.sql.functions import col

fs = FeatureStoreClient()


def compute_passenger_ticket_features(df):
    # Extract characters of ticket if they exist
    return (
        df.withColumn("TicketChars_extract", func.regexp_extract(col("Ticket"), "([A-Za-z]+)", 1))
        .selectExpr(
            "*",
            "case when length(TicketChars_extract) > 0 then upper(TicketChars_extract) else NULL end as TicketChars",
        )
        .drop("TicketChars_extract")
        # Extract the Cabin character
        .withColumn("CabinChar", func.split(col("Cabin"), "")[0])
        # Indicate if multiple Cabins are present
        .withColumn("CabinMulti_extract", func.size(func.split(col("Cabin"), " ")))
        .selectExpr(
            "*", "case when CabinMulti_extract < 0 then '0' else cast(CabinMulti_extract as string) end as CabinMulti"
        )
        .drop("CabinMulti_extract")
        # Round the Fare column
        .withColumn("FareRounded", func.round(col("Fare"), 0))
        .drop("Ticket", "Cabin")
    )


df = spark.table("robkisk.passenger_ticket_feautures")
passenger_ticket_features = compute_passenger_ticket_features(df)

# display(passenger_ticket_features)
# passenger_ticket_features.show(10, False)

feature_table_name = "robkisk.ticket_features"

# If the feature table has already been created, no need to recreate
try:
    fs.get_table(feature_table_name)
    print("Feature table entry already exists")
    pass

except Exception:
    fs.create_table(
        name=feature_table_name,
        primary_keys="PassengerId",
        schema=passenger_ticket_features.schema,
        description="Ticket-related features for Titanic passengers",
    )

fs.write_table(name=feature_table_name, df=passenger_ticket_features, mode="merge")

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructType

spark = SparkSession.builder.getOrCreate()


def create_tables():
    def create_schema(col_types):
        struct = StructType()
        for col_name, type in col_types:
            struct.add(col_name, type)
        return struct

    def create_pd_dataframe(csv_file_path, schema):
        df = pd.read_csv(csv_file_path)
        return spark.createDataFrame(df, schema=schema)

    def write_to_delta(spark_df, delta_table_name):
        spark.sql("create schema if not exists robkisk")
        spark_df.write.mode("overwrite").format("delta").saveAsTable(delta_table_name)
        out = f"""The following tables were created:
              - {delta_tables['ticket']}
              - {delta_tables['demographic']}
              - {delta_tables['labels']}
          """

        print(out)

    # Create Spark DataFrame schemas
    passenger_ticket_types = [
        ("PassengerId", StringType()),
        ("Ticket", StringType()),
        ("Fare", DoubleType()),
        ("Cabin", StringType()),
        ("Embarked", StringType()),
        ("Pclass", StringType()),
        ("Parch", StringType()),
    ]

    passenger_demographic_types = [
        ("PassengerId", StringType()),
        ("Name", StringType()),
        ("Sex", StringType()),
        ("Age", DoubleType()),
        ("SibSp", StringType()),
    ]

    passenger_label_types = [("PassengerId", StringType()), ("Survived", IntegerType())]

    passenger_ticket_schema = create_schema(passenger_ticket_types)
    passenger_dempgraphic_schema = create_schema(passenger_demographic_types)
    passenger_label_schema = create_schema(passenger_label_types)

    passenger_ticket_features = create_pd_dataframe(
        "data/passenger_ticket.csv", passenger_ticket_schema
    )
    passenger_demographic_features = create_pd_dataframe(
        "data/passenger_demographic.csv", passenger_dempgraphic_schema
    )
    passenger_labels = create_pd_dataframe(
        "data/passenger_labels.csv", passenger_label_schema
    )

    delta_tables = {
        "ticket": "robkisk.passenger_ticket_feautures",
        "demographic": "robkisk.passenger_demographic_features",
        "labels": "robkisk.passenger_labels",
    }

    write_to_delta(passenger_ticket_features, delta_tables["ticket"])
    write_to_delta(passenger_demographic_features, delta_tables["demographic"])
    write_to_delta(passenger_labels, delta_tables["labels"])

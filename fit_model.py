from databricks.feature_store import FeatureLookup
from databricks.feature_store import FeatureStoreClient
import mlflow
from mlflow.tracking import MlflowClient

import xgboost as xgb
from sklearn.ensemble import RandomForestClassifier
from sklearn.compose import ColumnTransformer
from sklearn.pipeline import FeatureUnion
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, FunctionTransformer
from sklearn.impute import SimpleImputer
from sklearn.pipeline import make_pipeline, make_union
from sklearn.metrics import classification_report, precision_recall_fscore_support
from sklearn.model_selection import train_test_split
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("appName").getOrCreate()

fs = FeatureStoreClient()

experiment_location = "/Users/robby.kiskanyan@databricks.com/feature_store_experiment/"  # workspace location


def get_or_create_experiment(experiment_location: str) -> None:

    if not mlflow.get_experiment_by_name(experiment_location):
        print("Experiment does not exist. Creating experiment")

        mlflow.create_experiment(experiment_location)

    mlflow.set_experiment(experiment_location)


# experiment_location = "/Shared/feature_store_experiment"
get_or_create_experiment(experiment_location)

mlflow.set_experiment(experiment_location)
feature_lookups = [
    FeatureLookup(
        table_name="hive_metastore.robkisk.passenger_ticket_features",
        feature_names=["CabinChar", "CabinMulti", "Embarked", "FareRounded", "Parch", "Pclass"],
        lookup_key="PassengerId",
    ),
    FeatureLookup(
        table_name="hive_metastore.robkisk.passenger_demographic_features",
        feature_names=["Age", "NameMultiple", "NamePrefix", "Sex", "SibSp"],
        lookup_key="PassengerId",
    ),
]

# Select passenger records of interest
passengers_and_target = spark.table("hive_metastore.robkisk.passenger_labels")

# Attach features to passengers
training_set = fs.create_training_set(
    df=passengers_and_target, feature_lookups=feature_lookups, label="Survived", exclude_columns=["PassengerId"]
)

# Create training datast
training_df = training_set.load_df()

data = training_df.toPandas()

label = "Survived"
features = [col for col in data.columns if col not in [label, "PassengerId"]]

X_train, X_test, y_train, y_test = train_test_split(
    data[features], data[label], test_size=0.25, random_state=123, shuffle=True
)

# Categorize columns by data type
categorical_vars = ["NamePrefix", "Sex", "CabinChar", "CabinMulti", "Embarked", "Parch", "Pclass", "SibSp"]
numeric_vars = ["Age", "FareRounded"]
binary_vars = ["NameMultiple"]

# Create the a pre-processing and modleing pipeline
binary_transform = make_pipeline(SimpleImputer(strategy="constant", fill_value="missing"))

numeric_transform = make_pipeline(SimpleImputer(strategy="most_frequent"))

categorical_transform = make_pipeline(
    SimpleImputer(missing_values=None, strategy="constant", fill_value="missing"),
    OneHotEncoder(handle_unknown="ignore"),
)

transformer = ColumnTransformer(
    [
        ("categorial_vars", categorical_transform, categorical_vars),
        ("numeric_vars", numeric_transform, numeric_vars),
        ("binary_vars", binary_transform, binary_vars),
    ],
    remainder="drop",
)

model = xgb.XGBClassifier(n_estimators=50, use_label_encoder=False)

classification_pipeline = Pipeline([("preprocess", transformer), ("classifier", model)])

with mlflow.start_run() as run:

    run_id = run.info.run_id
    # mlflow.xgboost.autolog()

    # Fit model
    classification_pipeline.fit(X_train, y_train)

    train_pred = classification_pipeline.predict(X_train)
    test_pred = classification_pipeline.predict(X_test)

    # Calculate validation statistics
    precision_train, recall_train, f1_train, _ = precision_recall_fscore_support(
        y_train, train_pred, average="weighted"
    )
    precision_test, recall_test, f1_test, _ = precision_recall_fscore_support(y_test, test_pred, average="weighted")

    decimals = 2
    validation_statistics = {
        "precision_training": round(precision_train, decimals),
        "precision_testing": round(precision_test, decimals),
        "recall_training": round(recall_train, decimals),
        "recall_testing": round(recall_test, decimals),
        "f1_training": round(f1_train, decimals),
        "f1_testing": round(f1_test, decimals),
    }

    # Log the validation statistics
    mlflow.log_metrics(validation_statistics)

    # Fit final model
    final_model = classification_pipeline.fit(data[features], data[label])

    # Log the model and training data metadata
    fs.log_model(final_model, artifact_path="model", flavor=mlflow.sklearn, training_set=training_set)

client = MlflowClient()

# Create a Model Registry entry for the model if one does not exist
model_registry_name = "feature_store_models_robkisk"
try:
    client.get_registered_model(model_registry_name)
    print(" Registered model already exists")
except:
    client.create_registered_model(model_registry_name)

# Get model run id and artifact path
model_info = client.get_run(run_id).to_dictionary()
artifact_uri = model_info["info"]["artifact_uri"]

registered_model = client.create_model_version(name=model_registry_name, source=artifact_uri + "/model", run_id=run_id)

promote_to_prod = client.transition_model_version_stage(
    name=model_registry_name, version=int(registered_model.version), stage="Production", archive_existing_versions=True
)

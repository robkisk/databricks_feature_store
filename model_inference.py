import mlflow.spark
from mlflow.tracking import MlflowClient
from databricks.feature_store import FeatureStoreClient

client = MlflowClient()
fs = FeatureStoreClient()

new_passenger_records = spark.table("robkisk.default.passenger_labels").select("PassengerId").limit(20)


def get_run_id(model_name, stage="Production"):
    """Get production model id from Model Registry"""
    prod_run = [run for run in client.search_model_versions(f"name='{model_name}'") if run.current_stage == stage][0]
    return prod_run.run_id


# Replace the first parameter with your model's name
run_id = get_run_id("feature_store_models_robkisk", stage="Production")
run_id


model_uri = f"runs:/{run_id}/model"

with_predictions = fs.score_batch(model_uri, new_passenger_records)

with_predictions.show(20, False)

#import libraries
import json
import sagemaker
from sagemaker.feature_store.feature_group import FeatureGroup
from sagemaker.session import Session
import boto3
from sagemaker import get_execution_role
import time
from time import gmtime, strftime, sleep
import pandas as pd


region = "xxxxxx"

bucket ="xxxxxxxxxx"

s3_client = boto3.client("s3", region_name=region)
region = boto3.Session().region_name
boto_session = boto3.Session(region_name=region)
sagemaker_client = boto_session.client(service_name="sagemaker", region_name=region)
featurestore_runtime = boto_session.client(
    service_name="sagemaker-featurestore-runtime", region_name=region
)

feature_store_session = Session(
    boto_session=boto_session,
    sagemaker_client=sagemaker_client,
    sagemaker_featurestore_runtime_client=featurestore_runtime,
)

# You can modify the following to use a bucket of your choosing
default_s3_bucket_name = feature_store_session.default_bucket()
prefix = "sagemaker-featurestore-demo"

# You can modify the following to use a role of your choosing. See the documentation for how to create this.
role = get_execution_role()
print(role)


source_data = s3_client.get_object(Bucket=bucket , Key="csv_moussa - Copy.csv")

fitness_feature_group_name = "fitness-feature-group-" + strftime("%d-%H-%M-%S", gmtime())

#define feature group
fitness_feature_group = FeatureGroup(
    name=fitness_feature_group_name, sagemaker_session=feature_store_session
)

# record identifier and event time feature names
record_identifier_feature_name = "ID"
event_time_feature_name = "Date"

def wait_for_feature_group_creation_complete(feature_group):
    status = feature_group.describe().get("FeatureGroupStatus")
    while status == "Creating":
        print("Waiting for Feature Group Creation")
        time.sleep(5)
        status = feature_group.describe().get("FeatureGroupStatus")
    if status != "Created":
        raise RuntimeError(f"Failed to create feature group {feature_group.name}")
    print(f"FeatureGroup {feature_group.name} successfully created.")

def handler(event, context):
    # read source data

    raw_data = pd.read_csv(source_data['Body'] ,encoding='utf-8')

    data = raw_data[["Date et heure de l'envoi",'Prénom', 'Nom de famille' , 'ID']].copy()

    data = data.dropna(subset=["Date et heure de l'envoi"])
    data.rename(columns={'Prénom': 'Prenom'}, inplace=True)
    data.rename(columns={'Nom de famille': 'Nom_de_famille'}, inplace=True)
    data.rename(columns={"Date et heure de l'envoi": 'Date'}, inplace=True)

    # define feature group
    fitness_feature_group.load_feature_definitions(data_frame=data)

    # create feature group
    fitness_feature_group.create(
        s3_uri=f"s3://{default_s3_bucket_name}/{prefix}",
        record_identifier_name=record_identifier_feature_name,
        event_time_feature_name=event_time_feature_name,
        role_arn="xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
        enable_online_store=True,
    )

    wait_for_feature_group_creation_complete(feature_group=fitness_feature_group)

    fitness_feature_group.ingest(data_frame=data, max_workers=1, wait=True)


    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

import weaviate
import weaviate.classes.config as wc
from weaviate.classes.init import AdditionalConfig, Timeout
from weaviate.util import generate_uuid5
import requests
from datetime import datetime, timezone
import json as json
from tqdm import tqdm
import os
import zipfile
from pathlib import Path
import base64
import ijson
import sys

if len(sys.argv) > 1:
    LOCAL_JSON_PATH = sys.argv[1]
else:
    print("Error: No file path provided.")
    print(f"Usage: python {sys.argv[0]} <path_to_json_file>")
    sys.exit(1)
if not os.path.isfile(LOCAL_JSON_PATH):
    print(f"Error: The file '{LOCAL_JSON_PATH}' does not exist.")
    sys.exit(1)
if not LOCAL_JSON_PATH.endswith('.json'):
    print(f"Error: The file '{LOCAL_JSON_PATH}' does not have a .json extension.")
    sys.exit(1)

# Don't need OPENAI_APIKEY when connecting to local LM Studio
headers = {
    # "X-OpenAI-Api-Key": os.getenv("OPENAI_APIKEY")
    "X-OpenAI-Api-Key": "NO_KEY_NEEDED_FOR_LM_STUDIO"
}  # Replace with your OpenAI API key

client = None
REVIEW_COLLECTION_NAME = "review2"

try:
    client = weaviate.connect_to_local(
        headers=headers,     
        additional_config=AdditionalConfig(
            timeout=Timeout(init=30, query=60, insert=120)  # Values in seconds
        )
    )
    assert client.is_live()
    metainfo = client.get_meta()
    #print(json.dumps(metainfo, indent=2))  # Print the meta information in a readable format
    #if client.collections.exists(REVIEW_COLLECTION_NAME):
        #print(f"Dropping existing {REVIEW_COLLECTION_NAME} collection")
        #client.collections.delete(REVIEW_COLLECTION_NAME)
    
    if not client.collections.exists(REVIEW_COLLECTION_NAME):
        print(f"Before Create {REVIEW_COLLECTION_NAME} collection")
        client.collections.create(
            name = REVIEW_COLLECTION_NAME,
            properties = [
                wc.Property(name="overall", data_type=wc.DataType.NUMBER),
                wc.Property(name="verified", data_type=wc.DataType.BOOL, skip_vectorization=True),
                wc.Property(name="reviewerID", data_type=wc.DataType.TEXT, skip_vectorization=True),
                wc.Property(name="asin", data_type=wc.DataType.TEXT, skip_vectorization=True),
                #wc.Property(name="style", data_type=wc.DataType.OBJECT, skip_vectorization=True),
                wc.Property(name="reviewerName", data_type=wc.DataType.TEXT, skip_vectorization=True),
                wc.Property(name="reviewText", data_type=wc.DataType.TEXT),
                wc.Property(name="summary", data_type=wc.DataType.TEXT),
                wc.Property(name="unixReviewTime", data_type=wc.DataType.NUMBER, skip_vectorization=True),
                wc.Property(name="unixReviewDate", data_type=wc.DataType.DATE, skip_vectorization=True),
                wc.Property(name="reviewTime", data_type=wc.DataType.TEXT, skip_vectorization=True),
                wc.Property(name="image", data_type=wc.DataType.TEXT_ARRAY, skip_vectorization=True),
            ],
            # references=[
            #     wc.ReferenceProperty(
            #         name="asin",
            #         target_collection="product"
            #     )
            # ],
            vectorizer_config = wc.Configure.Vectorizer.text2vec_openai(
                base_url="http://host.docker.internal:1234"
            ),
            generative_config = wc.Configure.Generative.openai(
                base_url="http://host.docker.internal:1234"
            )
        )
        print("After Create review collection")

    counter = 0
    interval = 100  # print progress every this many records; should be bigger than the batch_size

    # Get the collection
    reviews = client.collections.get(REVIEW_COLLECTION_NAME)

    # Enter context manager
    with reviews.batch.dynamic() as batch:
        print("opening ", LOCAL_JSON_PATH)
        with open(LOCAL_JSON_PATH, "rb") as f:
            print("about to call parse")
            objects = ijson.items(f, '', multiple_values=True)
            print("after calling parse")
            for obj in objects:
                #print("category: ", obj["category"])
                review_obj = {}
                if "overall" in obj:
                    review_obj["overall"] = float(obj["overall"])
                if "verified" in obj:
                    review_obj["verified"] = obj["verified"]
                if "reviewerID" in obj:
                    review_obj["reviewerID"] = obj["reviewerID"]
                if "asin" in obj:
                    review_obj["asin"] = obj["asin"]
                if "reviewerName" in obj:
                    review_obj["reviewerName"] = obj["reviewerName"]
                if "reviewText" in obj:
                    review_obj["reviewText"] = obj["reviewText"]
                if "summary" in obj:
                    review_obj["summary"] = obj["summary"]
                if "unixReviewTime" in obj:
                    review_obj["unixReviewTime"] = obj["unixReviewTime"]
                    review_obj["unixReviewDate"] = datetime.fromtimestamp(obj["unixReviewTime"], timezone.utc).strftime("%Y-%m-%dT%H:%M:%S+00:00")
                if "reviewTime" in obj:
                    review_obj["reviewTime"] = obj["reviewTime"]
                if "image" in obj:
                    review_obj["image"] = obj["image"]
                else:
                    review_obj["image"] = []
                #print(json.dumps(review_obj, indent=2, default=str))
                # Add object to batch queue
                batch.add_object(
                    properties=review_obj,
                    #uuid=generate_uuid5(obj["asin"] + obj["reviewerID"] + obj["reviewTime"])
                )

                # Calculate and display progress
                counter += 1
                if counter % interval == 0:
                    print(f"{LOCAL_JSON_PATH}: Imported {counter} reviews...")

    print(f"{LOCAL_JSON_PATH}: Before flushing batch")
    batch.flush()
    print(f"{LOCAL_JSON_PATH}After flushing batch")
    # Check for failed objects
    old_failed_obj_count = len(reviews.batch.failed_objects)
    new_failed_obj_count = 0
    while True:
        if len(reviews.batch.failed_objects) == 0:
            print(f"{LOCAL_JSON_PATH}:All reviews imported successfully")
            break
        print(f"{LOCAL_JSON_PATH}:Retrying {len(reviews.batch.failed_objects)} failed objects...")
        counter = 0

        # save the failed_objects array because calling batch.dynamic() after will clear it out
        current_failed_object_count = len(reviews.batch.failed_objects)
        failed_objects = reviews.batch.failed_objects
        with reviews.batch.dynamic() as batch:
            print(f"{LOCAL_JSON_PATH}:Inside retry loop are {len(failed_objects)} failed objects...")

            for failed in failed_objects:
                try:
                    if new_failed_obj_count == old_failed_obj_count:
                        print(f"{LOCAL_JSON_PATH}: Debugging stuck object: {failed}")
                    batch.add_object(
                        properties=failed.object_.properties,
                        uuid=failed.object_.uuid
                    )
                except Exception as e:
                    print(f"{LOCAL_JSON_PATH}:Exception while retrying: {e}")
                    print(f"{LOCAL_JSON_PATH}:Failed Object: {failed}")
                    break

                counter += 1
                if counter % interval == 0:
                    print(f"{LOCAL_JSON_PATH}:  Retried {counter} reviews...")

            # Execute the batch to retry failed objects
            batch.flush() 
        old_failed_object_count = current_failed_object_count
        new_failed_object_count = len(reviews.batch.failed_objects)

except Exception as e:
    print(f"Exception: {e}")
finally:  # This will always be executed, even if an exception is raised
    client.close()  # Close the connection & release resources

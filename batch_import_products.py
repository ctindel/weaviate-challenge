import weaviate
import weaviate.classes.config as wc
from weaviate.util import generate_uuid5
from weaviate.classes.init import AdditionalConfig, Timeout
import requests
from datetime import datetime, timezone
import json
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
#    if client.collections.exists("product"):
#        print("Dropping existing product collection")
#        client.collections.delete("product")
    
    if not client.collections.exists("product"):
        print("Before Create product collection")
        client.collections.create(
            name = "product",
            properties = [
                wc.Property(name="category", data_type=wc.DataType.TEXT_ARRAY, index_filterable=True, index_searchable=True),
                wc.Property(name="tech1", data_type=wc.DataType.TEXT, skip_vectorization=True, index_filterable=False, index_searchable=False),
                wc.Property(name="tech2", data_type=wc.DataType.TEXT, skip_vectorization=True, index_filterable=False, index_searchable=False),
                wc.Property(name="description", data_type=wc.DataType.TEXT_ARRAY, index_filterable=True, index_searchable=True),
                wc.Property(name="fit", data_type=wc.DataType.TEXT, skip_vectorization=True, index_filterable=False, index_searchable=False),
                wc.Property(name="title", data_type=wc.DataType.TEXT, index_filterable=True, index_searchable=True),
                wc.Property(name="also_buy", data_type=wc.DataType.TEXT_ARRAY, skip_vectorization=True, index_filterable=False, index_searchable=False),
                wc.Property(name="image", data_type=wc.DataType.TEXT_ARRAY, skip_vectorization=True, index_filterable=False, index_searchable=False),
                wc.Property(name="brand", data_type=wc.DataType.TEXT, index_filterable=True, index_searchable=True),
                wc.Property(name="feature", data_type=wc.DataType.TEXT_ARRAY, skip_vectorization=True, index_filterable=False, index_searchable=False),
                wc.Property(name="rank", data_type=wc.DataType.TEXT_ARRAY, skip_vectorization=True, index_filterable=False, index_searchable=False),
                wc.Property(name="also_view", data_type=wc.DataType.TEXT_ARRAY, skip_vectorization=True, index_filterable=False, index_searchable=False),
                #wc.Property(name="details", data_type=wc.DataType.OBJECT),
                wc.Property(name="main_cat", data_type=wc.DataType.TEXT, index_filterable=True, index_searchable=True),
                #wc.Property(name="similar_item", data_type=wc.DataType.TEXT, skip_vectorization=True, index_filterable=False, index_searchable=False),
                wc.Property(name="date", data_type=wc.DataType.TEXT, skip_vectorization=True, index_filterable=True, index_searchable=True),
                wc.Property(name="price", data_type=wc.DataType.TEXT, skip_vectorization=True, index_filterable=True, index_searchable=True),
                wc.Property(name="asin", data_type=wc.DataType.TEXT, index_filterable=True, index_searchable=True),
            ],
            vectorizer_config = wc.Configure.Vectorizer.text2vec_openai(
                base_url="http://host.docker.internal:1234"
            ),
            generative_config = wc.Configure.Generative.openai(
                base_url="http://host.docker.internal:1234"
            )
        )
        print("After Create product collection")

    counter = 0
    interval = 100  # print progress every this many records; should be bigger than the batch_size

    # Get the collection
    products = client.collections.get("product")

    # Enter context manager
    with products.batch.dynamic() as batch:
        print("opening ", LOCAL_JSON_PATH)
        with open(LOCAL_JSON_PATH, "rb") as f:
            #print("about to call parse")
            objects = ijson.items(f, '', multiple_values=True)
            #print("after calling parse")
            for obj in objects:
                #print("category: ", obj["category"])
                product_obj = {}
#                if "category" in obj:
#                    product_obj["category"] = obj["category"]
#                if "tech1" in obj:
#                    product_obj["tech1"] = obj["tech1"]
#                if "tech2" in obj:
#                    product_obj["tech2"] = obj["tech2"]
#                if "description" in obj:
#                    product_obj["description"] = obj["description"]
#                if "fit" in obj:
#                    product_obj["fit"] = obj["fit"]
#                if "title" in obj:
#                    product_obj["title"] = obj["title"]
#                if "also_buy" in obj:
#                    product_obj["also_buy"] = obj["also_buy"]
#                if "image" in obj:
#                    product_obj["image"] = obj["image"]
#                if "brand" in obj:
#                    product_obj["brand"] = obj["brand"]
#                if "feature" in obj:
#                    product_obj["feature"] = obj["feature"]
#                if "rank" in obj:
#                    product_obj["rank"] = obj["rank"]
#                if "also_view" in obj:
#                    product_obj["also_view"] = obj["also_view"]
#                if "main_cat" in obj:
#                    product_obj["main_cat"] = obj["main_cat"]
#                if "date" in obj:
#                    product_obj["date"] = obj["date"]
#                if "price" in obj:
#                    product_obj["price"] = obj["price"]
#                #if "details" in obj:
#                    #product_obj["details"] = obj["details"]
#                #if "similar_item" in obj:
#                    #product_obj["similar_item"] = obj["similar_item"]
                product_obj = {
                    "category" : obj["category"],
                    "tech1" : obj["tech1"],
                    "tech2" : obj["tech2"],
                    "description" : obj["description"],
                    "fit" : obj["fit"],
                    "title" : obj["title"],
                    "also_buy" : obj["also_buy"],
                    "image" : obj["image"],
                    "brand" : obj["brand"],
                    "feature" : obj["feature"],
                    "rank" : obj["rank"],
                    "also_view" : obj["also_view"],
                    "main_cat" : obj["main_cat"],
                    "date" : obj["date"],
                    "price" : obj["price"],
                    "asin" : obj["asin"],
                    #"details" : obj["details"],
                    #"similar_item" : obj["similar_item"],
                }
                if not isinstance(obj["category"], list):
                    product_obj["category"] = [obj["category"]]
                if not isinstance(obj["description"], list):
                    product_obj["description"] = [obj["description"]]
                if not isinstance(obj["also_buy"], list):
                    product_obj["also_buy"] = [obj["also_buy"]]
                if not isinstance(obj["image"], list):
                    product_obj["image"] = [obj["image"]]
                if not isinstance(obj["feature"], list):
                    product_obj["feature"] = [obj["feature"]]
                if not isinstance(obj["also_view"], list):
                    product_obj["also_view"] = [obj["also_view"]]

                # Sometimes rank is a string and sometimes it is an array
                if isinstance(obj["rank"], str):
                    product_obj["rank"] = [obj["rank"]]
                elif isinstance(obj["rank"], list):
                    product_obj["rank"] = obj["rank"]
                else:
                    product_obj["rank"] = []
                #print(json.dumps(product_obj, indent=2))
                # Add object to batch queue
                batch.add_object(
                    properties=product_obj,
                    #uuid=generate_uuid5(obj["asin"])
                    # references=reference_obj  # You can add references here
                )

                # Calculate and display progress
                counter += 1
                if counter % interval == 0:
                    print(f"{LOCAL_JSON_PATH}: Imported {counter} products...")
    print(f"{LOCAL_JSON_PATH}: Before flushing batch")
    batch.flush()
    print(f"{LOCAL_JSON_PATH}: After flushing batch")
    # Check for failed objects
    old_failed_obj_count = len(products.batch.failed_objects)
    new_failed_obj_count = 0
    while True:
        if len(products.batch.failed_objects) == 0:
            print(f"{LOCAL_JSON_PATH}: All products imported successfully")
            break
        print(f"{LOCAL_JSON_PATH}: Retrying {len(products.batch.failed_objects)} failed objects...")
        counter = 0

        # save the failed_objects array because calling batch.dynamic() after will clear it out
        current_failed_object_count = len(products.batch.failed_objects)
        failed_objects = products.batch.failed_objects
        with products.batch.dynamic() as batch:
            print(f"{LOCAL_JSON_PATH}: Inside retry loop are {len(failed_objects)} failed objects...")

            for failed in failed_objects:
                try:
                    if new_failed_obj_count == old_failed_obj_count:
                        print(f"{LOCAL_JSON_PATH}: Debugging stuck object: {failed}")
                    batch.add_object(
                        properties=failed.object_.properties,
                        uuid=failed.object_.uuid
                    )
                except Exception as e:
                    print(f"{LOCAL_JSON_PATH}: Exception while retrying: {e}")
                    print(f"{LOCAL_JSON_PATH}: Failed Object: {failed}")
                    break

                counter += 1
                if counter % interval == 0:
                    print(f"{LOCAL_JSON_PATH}: Retried {counter} products...")

            # Execute the batch to retry failed objects
            batch.flush()
        old_failed_object_count = current_failed_object_count
        new_failed_object_count = len(products.batch.failed_objects)

    # Check for failed objects
    # if len(products.batch.failed_objects) > 0:
    #     print(f"Failed to import {len(products.batch.failed_objects)} objects")
    #     for failed in products.batch.failed_objects:
    #         #print(f"e.g. Failed to import object with error: {failed.message}")
    #         print("Failed to import object with error: %s", json.dumps(failed, indent=2, default=str))   

except Exception as e:
    print(f"Exception: {e}")
finally:  # This will always be executed, even if an exception is raised
    client.close()  # Close the connection & release resources

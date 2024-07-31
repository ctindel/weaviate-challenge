import weaviate
import weaviate.classes.config as wc
import weaviate.classes.query as wq
from weaviate.util import generate_uuid5
import requests
from datetime import datetime, timezone
import json
from tqdm import tqdm
import os
import zipfile
from pathlib import Path
import base64

headers = {
    "X-OpenAI-Api-Key": "NOT_NEEDED_FOR_LM_STUDIO"
}  # Replace with your OpenAI API key

def url_to_base64(url):
    image_response = requests.get(url)
    content = image_response.content
    return base64.b64encode(content).decode("utf-8")

def print_product(response_object):
    print("Product Title: " + response_object.properties["title"])
    print("  Artist: " + response_object.properties["brand"])
    print("  ASIN: " + response_object.properties["asin"])
    print("  Categories: ")
    for c in response_object.properties["category"]:
        print("    " + c)
    print("  Price: " + response_object.properties["price"])
    print("  Description: ")
    for d in response_object.properties["description"]:
        print("    " + d)
    #print(f"\tDistance to query: {response_object.metadata.distance:.3f}")

def print_reviews(response_objects):
    count = 1

    for o in response_objects:
        print(f"  Review {count}:")
        print(f"    Review Score: " + str(o.properties["overall"]))
        print(f"    Review Date: " + o.properties["unixReviewDate"].strftime("%Y-%m-%d"))
        print(f"    Reviewer Name: " + o.properties["reviewerName"])
        print(f"    Review Text: " + o.properties["reviewText"].replace("\n", "").replace("\r", ""))

def image_query(src_img_url):
    query_b64 = url_to_base64(src_img_url)

    response = products.query.near_image(
        near_image=query_b64,
        limit=5,
        return_metadata=wq.MetadataQuery(distance=True),
        return_properties=["title", "category", "price", "description"]  # To include the poster property in the response (`blob` properties are not returned by default)
    )

    # Inspect the response
    for o in response.objects:
        print(
            "  ", o.properties["title"], o.properties["category"], o.properties["price"], o.properties["description"]
        ) 
        print(
            f"\tDistance to query: {o.metadata.distance:.3f}"
        )  # Print the distance of the object from the query

def text_query(search_string):
    response = products.query.near_text(
        query=search_string,
        limit=5,
        return_metadata=wq.MetadataQuery(distance=True),
    )

    seen_asin = []
    for o in response.objects:
        if o.properties["asin"] in seen_asin:
            continue
        seen_asin.append(o.properties["asin"])
        print_product(o)

def text_query_with_reviews(search_string):
    response = products.query.near_text(
        query=search_string,
        limit=5,
        return_metadata=wq.MetadataQuery(distance=True),
    )

    seen_asin = []
    for o in response.objects:
        if o.properties["asin"] in seen_asin:
            continue
        seen_asin.append(o.properties["asin"])
        print_product(o)
        review_response = reviews.query.bm25(
            query=o.properties["asin"],
            filters=wq.Filter.by_property("asin").equal(o.properties["asin"]),
            limit=5
        )
        print_reviews(review_response.objects)
        print("\n")

def keyword_query(keyword):
    response = products.query.bm25(
        query=keyword, limit=5, return_metadata=wq.MetadataQuery(score=True)
    )

    seen_asin = []
    for o in response.objects:
        if o.properties["asin"] in seen_asin:
            continue
        seen_asin.append(o.properties["asin"])

def hybrid_query(search_string):
    response = products.query.hybrid(
        query=search_string, limit=5, return_metadata=wq.MetadataQuery(score=True)
    )

    # Inspect the response
    for o in response.objects:
        print_product(o)

def text_query_with_filter(search_string):
    response = products.query.near_text(
        query=search_string,
        limit=5,
        return_metadata=wq.MetadataQuery(distance=True),
        filters=wq.Filter.by_property("release_date").greater_than(datetime.fromisoformat('2020-01-01T00:00:00+00:00')),
        return_properties=["title",  "asin","category", "price", "description"],
    )

    # Inspect the response
    for o in response.objects:
        print_product(o)

def text_query_with_filter_translation(search_string, language):
    response = movies.generate.near_text(
        query=search_string,
        limit=5,
        return_metadata=wq.MetadataQuery(distance=True),
        filters=wq.Filter.by_property("release_date").greater_than(datetime.fromisoformat('2020-01-01T00:00:00+00:00')),
        return_properties=["title", "asin", "category", "price", "description"],
        single_prompt="Translate this into " + language + ": {title}"
    )

    # Inspect the response
    for o in response.objects:
        print(
            "  ", o.properties["title"], "(" + o.generated + ")", o.properties["release_date"].year, o.properties["tmdb_id"]
        )  # Print the title and release year (note the release date is a datetime object)
        print(
            f"\tDistance to query: {o.metadata.distance:.3f}"
        )  # Print the distance of the object from the query

def text_query_with_filter_common_grouping(search_string):
    response = movies.generate.near_text(
        query=search_string,
        limit=5,
        return_metadata=wq.MetadataQuery(distance=True),
        filters=wq.Filter.by_property("release_date").greater_than(datetime.fromisoformat('2020-01-01T00:00:00+00:00')),
        return_properties=["title", "asin", "category", "price", "description"],
        grouped_task="What do these movies have in common?",
        grouped_properties=["title", "overview"]
    )
    # Inspect the response
    for o in response.objects:
        print(
            "  ", o.properties["title"], o.properties["release_date"].year, o.properties["tmdb_id"]
        )  # Print the title and release year (note the release date is a datetime object)
        print(
            f"\tDistance to query: {o.metadata.distance:.3f}"
        )  # Print the distance of the object from the query
    print("What these movies have in common according to the LLM:", response.generated)

try:
    client = weaviate.connect_to_local(headers=headers)
    assert client.is_live()
    metainfo = client.get_meta()
    # Get the collection
    products = client.collections.get("product")
    reviews = client.collections.get("review")

    # Perform query
    # print("\nRunning an image query based on the ISS")
    # image_query("https://github.com/weaviate-tutorials/edu-datasets/blob/main/img/International_Space_Station_after_undocking_of_STS-132.jpg?raw=true")

    #print("\nRunning a text query for 'charlie parker'")
    #text_query("charlie parker")
    text_query("background music for dinner parties")
    #text_query("classical trumpet players")
    #text_query("ethereal and eerie music")

    # print("\nRunning a keyword query for 'history'")
    #keyword_query("0001393774")
    #keyword_query("Keith Green")
    #keyword_query("Trans-Siberian Orchestra")
    #keyword_query("bebop")
    #keyword_query("classical trumpet players")

    # print("\nRunning a hybrid query for 'history'")
    # hybrid_query("history")

    # print("\nRunning a text query with filter for 'dystopian future'")
    # text_query_with_filter("dystopian future")

    # print("\nRunning a text query with filter for 'dystopian future' and translating the title to French")
    # text_query_with_filter_translation("dystopian future", "french")

    #print("\nRunning a text query with filter for 'dystopian future' and grouping the results")
    #text_query_with_filter_common_grouping("dystopian future")
except Exception as e:
    print(e)
finally:  # This will always be executed, even if an exception is raised
    client.close()  # Close the connection & release resources

import weaviate
import weaviate.classes as wvc
import weaviate.classes.config as wc
import os
import json

headers = {
    #"X-OpenAI-Api-Key": os.getenv("OPENAI_APIKEY")
}  # Replace with your OpenAI API key

try:

    client = weaviate.connect_to_local(headers=headers)
    assert client.is_live()
    collections = client.collections.list_all()
    # Print all collection names
    for name in collections:
        print(name)
        collection = client.collections.get(name)
        aggregation = collection.aggregate.over_all(total_count=True)
        print(aggregation.total_count)

except Exception as e:
    print(e)
finally:  # This will always be executed, even if an exception is raised
    client.close()  # Close the connection & release resources

import json
from google.cloud import pubsub_v1
from google.cloud import storage

# Replace placeholders with your own GCP configuration before running.
PROJECT_ID = "YOUR_GCP_PROJECT_ID"
BUCKET_NAME = "YOUR_GCS_BUCKET_NAME"
TOPIC_ID = "YOUR_PUBSUB_TOPIC"
PROCESSED_FILE = "YOUR_ALREADY_PROCESSED_FILE.json"

publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

storage_client = storage.Client()

published_count = 0
files_processed = 0

for blob in storage_client.list_blobs(BUCKET_NAME):
    if not blob.name.endswith(".json"):
        continue

    if blob.name == PROCESSED_FILE:
        print(f"Skipping already processed file: {blob.name}")
        continue

    files_processed += 1
    print(f"Processing: {blob.name}")

    bundle_text = blob.download_as_text()
    bundle = json.loads(bundle_text)
    entries = bundle.get("entry", [])

    for entry in entries:
        resource = entry.get("resource")

        if not resource:
            continue

        message = json.dumps(resource)
        data = message.encode("utf-8")

        publisher.publish(topic_path, data)
        published_count += 1

print()
print("--------------------------------")
print(f"Files processed: {files_processed}")
print(f"FHIR resources published: {published_count}")
print("--------------------------------")

publisher.stop()

import json

from google.cloud import pubsub_v1
from google.cloud import storage


PROJECT_ID = "project-c18eeebc-1b19-4616-80b"
BUCKET_NAME = "clinical-data-pipeline-fhir-2026"
TOPIC_ID = "clinical-data-events"

# This file was already processed during our first test.
PROCESSED_FILE = (
    "Alexandra16_Mosciski958_37549f60-b5a3-69cd-dea6-5a71c4bc23cf.json"
)


# ---------- Pub/Sub ----------
publisher = pubsub_v1.PublisherClient()

topic_path = publisher.topic_path(
    PROJECT_ID,
    TOPIC_ID
)


# ---------- Cloud Storage ----------
storage_client = storage.Client()


published_count = 0
files_processed = 0


# Get all objects from the bucket
for blob in storage_client.list_blobs(BUCKET_NAME):

    # We only want Synthea JSON files
    if not blob.name.endswith(".json"):
        continue

    # Do NOT process the patient we already processed
    if blob.name == PROCESSED_FILE:
        print(f"Skipping already processed file: {blob.name}")
        continue

    files_processed += 1

    print(f"Processing: {blob.name}")

    # Read the FHIR Bundle from Cloud Storage
    bundle_text = blob.download_as_text()

    # JSON text → Python dictionary
    bundle = json.loads(bundle_text)

    # Get resources inside the Bundle
    entries = bundle.get("entry", [])

    for entry in entries:

        resource = entry.get("resource")

        if not resource:
            continue

        # Python dictionary → JSON string
        message = json.dumps(resource)

        # JSON string → bytes
        data = message.encode("utf-8")

        # Publish resource to Pub/Sub topic
        publisher.publish(
            topic_path,
            data
        )

        published_count += 1


print()
print("--------------------------------")
print(f"Files processed: {files_processed}")
print(f"FHIR resources published: {published_count}")
print("--------------------------------")

# Make sure all asynchronous publishes are completed
publisher.stop()
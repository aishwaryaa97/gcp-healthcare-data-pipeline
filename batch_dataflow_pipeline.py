import json
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.io import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions

# Replace placeholders with your own GCP configuration before running.
PROJECT_ID = "YOUR_GCP_PROJECT_ID"
BUCKET_NAME = "YOUR_GCS_BUCKET_NAME"

INPUT_PATTERN = f"gs://{BUCKET_NAME}/*.json"

BIGQUERY_TABLE = (
    "YOUR_GCP_PROJECT_ID:YOUR_BIGQUERY_DATASET.YOUR_BIGQUERY_TABLE"
)

PROCESSED_FILE = "YOUR_ALREADY_PROCESSED_FILE.json"

BQ_SCHEMA = {
    "fields": [
        {"name": "resource_type", "type": "STRING", "mode": "NULLABLE"},
        {"name": "resource_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "patient_id", "type": "STRING", "mode": "NULLABLE"},
        {"name": "event_timestamp", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "raw_fhir_json", "type": "JSON", "mode": "NULLABLE"},
    ]
}


def process_fhir_file(readable_file):
    """Read one complete FHIR Bundle and convert its resources to BigQuery rows."""

    file_name = readable_file.metadata.path.split("/")[-1]

    if file_name == PROCESSED_FILE:
        return []

    try:
        bundle_text = readable_file.read_utf8()
        bundle = json.loads(bundle_text)
    except (json.JSONDecodeError, UnicodeDecodeError):
        return []

    rows = []

    for entry in bundle.get("entry", []):
        resource = entry.get("resource")

        if not resource:
            continue

        resource_type = resource.get("resourceType")
        resource_id = resource.get("id")

        if not resource_type or not resource_id:
            continue

        if resource_type == "Patient":
            patient_id = resource_id
        else:
            patient_id = None
            subject = resource.get("subject", {})
            patient = resource.get("patient", {})
            reference = subject.get("reference") or patient.get("reference")

            if reference and "/" in reference:
                patient_id = reference.split("/")[-1]

        rows.append(
            {
                "resource_type": resource_type,
                "resource_id": resource_id,
                "patient_id": patient_id,
                "event_timestamp": datetime.now(timezone.utc),
                "raw_fhir_json": resource,
            }
        )

    return rows


def run():
    pipeline_options = PipelineOptions(
        save_main_session=True,
    )

    with beam.Pipeline(options=pipeline_options) as pipeline:
        (
            pipeline
            | "Match FHIR files"
            >> fileio.MatchFiles(INPUT_PATTERN)
            | "Read complete FHIR files"
            >> fileio.ReadMatches()
            | "Process FHIR Bundles"
            >> beam.FlatMap(process_fhir_file)
            | "Write to BigQuery"
            >> WriteToBigQuery(
                table=BIGQUERY_TABLE,
                schema=BQ_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_NEVER,
                type_overrides={"JSON": dict},
            )
        )


if __name__ == "__main__":
    run()

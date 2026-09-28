import json
from datetime import datetime, timezone

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.io import WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions


PROJECT_ID = "project-c18eeebc-1b19-4616-80b"

BUCKET_NAME = "clinical-data-pipeline-fhir-2026"

INPUT_PATTERN = (
    f"gs://{BUCKET_NAME}/*.json"
)

BIGQUERY_TABLE = (
    "project-c18eeebc-1b19-4616-80b:clinical_data.raw_fhir_events"
)

# This patient was already processed during our first streaming test.
PROCESSED_FILE = (
    "Alexandra16_Mosciski958_37549f60-b5a3-69cd-dea6-5a71c4bc23cf.json"
)


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
    """
    Read one complete FHIR Bundle from Cloud Storage
    and convert its resources into BigQuery rows.
    """

    file_name = readable_file.metadata.path.split("/")[-1]

    # Skip the patient already loaded during our first test.
    if file_name == PROCESSED_FILE:
        return []

    try:
        # Read the complete JSON file.
        bundle_text = readable_file.read_utf8()

        # JSON text -> Python dictionary.
        bundle = json.loads(bundle_text)

    except (json.JSONDecodeError, UnicodeDecodeError):
        return []

    rows = []

    # A FHIR Bundle contains resources inside "entry".
    for entry in bundle.get("entry", []):

        resource = entry.get("resource")

        if not resource:
            continue

        resource_type = resource.get("resourceType")
        resource_id = resource.get("id")

        if not resource_type or not resource_id:
            continue

        # Determine the patient associated with the resource.
        if resource_type == "Patient":
            patient_id = resource_id

        else:
            patient_id = None

            subject = resource.get("subject", {})
            patient = resource.get("patient", {})

            reference = (
                subject.get("reference")
                or patient.get("reference")
            )

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

            # Find all Synthea JSON files in Cloud Storage.
            | "Match FHIR files"
            >> fileio.MatchFiles(INPUT_PATTERN)

            # Read each complete file instead of reading line-by-line.
            | "Read complete FHIR files"
            >> fileio.ReadMatches()

            # Convert each FHIR Bundle into BigQuery rows.
            | "Process FHIR Bundles"
            >> beam.FlatMap(process_fhir_file)

            # Append the resources to the existing raw table.
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
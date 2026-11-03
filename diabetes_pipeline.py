
"""
Patient Diabetes Risk Data Pipeline (Apache Beam).

Reads patient CSV files, validates every row, sends invalid rows
to a quarantine output, and preserves valid patient records.

Each patient remains an individual record.

Run locally:
    python patient_pipeline.py --runner DirectRunner \
        --input "sample/raw/*.csv" \
        --output out/patients
"""

import argparse
import csv
import io
import json
import logging

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.metrics import Metrics
from apache_beam.options.pipeline_options import PipelineOptions


# ---------------------------------------------------------
# 1. DATA REQUIREMENTS
# ---------------------------------------------------------

REQUIRED = ("patient_id", "bmi")

BAD = "bad"


# ---------------------------------------------------------
# 2. READ CSV FILES
# ---------------------------------------------------------

class ReadCsvFile(beam.DoFn):

    def __init__(self):
        self.files = Metrics.counter("patients", "files_read")
        self.empty = Metrics.counter("patients", "empty_files")
        self.rows = Metrics.counter("patients", "rows_read")

    def process(self, readable_file):

        name = readable_file.metadata.path.rsplit("/", 1)[-1]

        self.files.inc()

        count = 0

        with readable_file.open() as raw:

            text = io.TextIOWrapper(
                raw,
                encoding="utf-8-sig",
                newline=""
            )

            for row in csv.DictReader(text):

                count += 1
                row["_source"] = name

                yield row

        self.rows.inc(count)

        if count == 0:
            self.empty.inc()


# ---------------------------------------------------------
# 3. DATA VALIDATION
# ---------------------------------------------------------

class RowError(Exception):
    """Raised when a patient record fails validation."""


class ValidateRow(beam.DoFn):

    def __init__(self):

        self.good = Metrics.counter("patients", "good_rows")
        self.bad = Metrics.counter("patients", "bad_rows")

    def process(self, row):

        try:

            # Check mandatory columns
            missing = [
                c for c in REQUIRED
                if row.get(c) in (None, "")
            ]

            if missing:
                raise RowError(
                    "missing:" + ",".join(missing)
                )

            # Validate patient ID
            patient_id = int(row["patient_id"])

            if patient_id < 0:
                raise RowError("invalid_patient_id")

            # Validate BMI
            bmi = float(row["bmi"])

            if bmi <= 0:
                raise RowError("invalid_bmi")

            # Keep all original columns
            row["patient_id"] = patient_id
            row["bmi"] = bmi

        except (RowError, ValueError, TypeError) as err:

            self.bad.inc()

            yield beam.pvalue.TaggedOutput(
                BAD,
                {
                    "reason": str(err),
                    "source": row.get("_source"),
                    "raw": json.dumps(row)[:500]
                }
            )

            return

        self.good.inc()

        # Each patient remains an individual record
        yield row


# ---------------------------------------------------------
# 4. BUILD PIPELINE
# ---------------------------------------------------------

def build(p, args):

    checked = (

        p
        | "MatchFiles" >> fileio.MatchFiles(args.input)

        | "ReadMatches" >> fileio.ReadMatches()

        | "ReadCSV" >> beam.ParDo(ReadCsvFile())

        | "ValidatePatients" >>
            beam.ParDo(ValidateRow()).with_outputs(
                BAD,
                main="good"
            )
    )

    return checked.good, checked[BAD]


# ---------------------------------------------------------
# 5. WRITE RESULTS
# ---------------------------------------------------------

def write_results(good, bad, args):

    # Valid patient records
    (
        good
        | "PatientJSON" >> beam.Map(json.dumps)
        | "WritePatients" >>
            beam.io.WriteToText(
                args.output,
                file_name_suffix=".jsonl"
            )
    )

    # Invalid patient records
    (
        bad
        | "BadJSON" >> beam.Map(json.dumps)
        | "WriteBadPatients" >>
            beam.io.WriteToText(
                args.output + "_bad",
                file_name_suffix=".jsonl"
            )
    )


# ---------------------------------------------------------
# 6. ARGUMENTS
# ---------------------------------------------------------

def parse_args(argv):

    parser = argparse.ArgumentParser(
        description=__doc__
    )

    parser.add_argument(
        "--input",
        required=True,
        help="Patient CSV file or glob"
    )

    parser.add_argument(
        "--output",
        required=True,
        help="Output file prefix"
    )

    args, beam_args = parser.parse_known_args(argv)

    return args, beam_args


# ---------------------------------------------------------
# 7. METRICS
# ---------------------------------------------------------

def print_counters(result):

    query = (
        beam.metrics.MetricsFilter()
        .with_namespace("patients")
    )

    counters = result.metrics().query(query)["counters"]

    print("\nPatient Pipeline Counters:")

    for c in sorted(
        counters,
        key=lambda c: c.key.metric.name
    ):

        value = (
            c.committed
            if c.committed is not None
            else c.attempted
        )

        print(f"  {c.key.metric.name:<16}{value}")


# ---------------------------------------------------------
# 8. RUN PIPELINE
# ---------------------------------------------------------

def run(argv=None):

    args, beam_args = parse_args(argv)

    options = PipelineOptions(
        beam_args,
        save_main_session=True
    )

    logging.getLogger().setLevel(logging.WARNING)

    pipeline = beam.Pipeline(options=options)

    good, bad = build(pipeline, args)

    write_results(good, bad, args)

    result = pipeline.run()

    result.wait_until_finish()

    if options.get_all_options().get("runner") in (
        None,
        "DirectRunner"
    ):

        print_counters(result)


if __name__ == "__main__":
    run()

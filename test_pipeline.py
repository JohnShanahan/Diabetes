
"""
Unit tests for the Patient Pipeline.

Run with:
    python -m pytest -q test_pipeline.py
"""

import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that, equal_to

import diabetes_pipeline as dp


# ---------------------------------------------------------
# 1. SAMPLE PATIENT RECORD
# ---------------------------------------------------------

GOOD = {
    "patient_id": "101",
    "age": "45",
    "gender": "Female",
    "glucose": "120",
    "bmi": "28.5",
    "_source": "diabetes.csv"
}


# ---------------------------------------------------------
# 2. TEST PATIENT ID VALIDATION
# ---------------------------------------------------------

def test_validate_sends_good_and_bad_rows_to_the_right_place():

    rows = [
        GOOD,
        dict(GOOD, patient_id=""),
        dict(GOOD, patient_id="abc"),
        dict(GOOD, patient_id="-1"),
        dict(GOOD, patient_id=None)
    ]

    reasons = [
        "missing:patient_id",
        "invalid_patient_id",
        "invalid_patient_id",
        "missing:patient_id"
    ]

    with TestPipeline() as p:

        out = (
            p
            | beam.Create(rows)
            | beam.ParDo(dp.ValidateRow()).with_outputs(
                dp.BAD,
                main="good"
            )
        )

        # One valid patient
        count = (
            out.good
            | beam.combiners.Count.Globally()
        )

        assert_that(
            count,
            equal_to([1]),
            label="one valid patient"
        )

        # Four invalid patient records
        why = (
            out[dp.BAD]
            | beam.Map(lambda row: row["reason"])
        )

        assert_that(
            why,
            equal_to(reasons),
            label="four invalid patients"
        )


# ---------------------------------------------------------
# 3. TEST PATIENT DATA PRESERVATION
# ---------------------------------------------------------

def test_valid_patient_preserves_all_columns():

    with TestPipeline() as p:

        out = (
            p
            | beam.Create([GOOD])
            | beam.ParDo(dp.ValidateRow())
        )

        assert_that(
            out,
            equal_to([{
                "patient_id": 101,
                "age": "45",
                "gender": "Female",
                "glucose": "120",
                "bmi": "28.5",
                "_source": "diabetes.csv"
            }]),
            label="preserve patient columns"
        )


# ---------------------------------------------------------
# 4. TEST PATIENT ID CONVERSION
# ---------------------------------------------------------

def test_patient_id_is_integer():

    with TestPipeline() as p:

        out = (
            p
            | beam.Create([GOOD])
            | beam.ParDo(dp.ValidateRow())
            | beam.Map(lambda row: row["patient_id"])
        )

        assert_that(
            out,
            equal_to([101]),
            label="patient ID is integer"
        )

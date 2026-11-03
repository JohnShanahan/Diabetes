
"""
Create a tiny SYNTHETIC Diabetes Patient dataset for testing
the patient pipeline without any cloud account.

Creates two CSV schema versions, a header-only file,
and deliberately invalid patient IDs.

All patient data is fictional.
"""

import csv
import os
import random

OUT = "sample"
RAW = OUT + "/raw"

# Synthetic patient CSV schemas
OLD = [
    "patient_id",
    "age",
    "gender",
    "bmi",
    "glucose",
    "blood_pressure",
    "smoking_history",
    "diabetes_risk"
]

NEW = [
    "patient_id",
    "age",
    "gender",
    "bmi",
    "glucose",
    "HbA1c",
    "smoking_history",
    "diabetes_risk",
    "family_history"
]


# ---------------------------------------------------------
# 1. GENERATE SYNTHETIC PATIENT RECORDS
# ---------------------------------------------------------

def rows_for(rng, style):

    out = []

    for patient_id in range(1001, 1021):

        age = rng.randint(20, 80)
        gender = rng.choice(["Male", "Female"])

        bmi = round(rng.uniform(18.0, 40.0), 1)
        glucose = rng.randint(70, 200)

        blood_pressure = rng.randint(60, 120)
        hba1c = round(rng.uniform(4.0, 9.0), 1)

        smoking_history = rng.choice([
            "never",
            "former",
            "current",
            "No Info"
        ])

        diabetes_risk = rng.choice([
            "Low",
            "Medium",
            "High"
        ])

        row = {
            "patient_id": patient_id,
            "age": age,
            "gender": gender,
            "bmi": bmi,
            "glucose": glucose,
            "blood_pressure": blood_pressure,
            "HbA1c": hba1c,
            "smoking_history": smoking_history,
            "diabetes_risk": diabetes_risk,
            "family_history": rng.choice(["Yes", "No"])
        }

        out.append(row)

    return out


# ---------------------------------------------------------
# 2. WRITE CSV FILES
# ---------------------------------------------------------

def write(path, columns, rows):

    with open(
        path,
        "w",
        newline="",
        encoding="utf-8"
    ) as f:

        w = csv.DictWriter(
            f,
            fieldnames=columns,
            extrasaction="ignore"
        )

        w.writeheader()
        w.writerows(rows)


# ---------------------------------------------------------
# 3. CREATE SAMPLE DATASET
# ---------------------------------------------------------

def main():

    os.makedirs(RAW, exist_ok=True)

    rng = random.Random(42)

    old = rows_for(rng, "old")
    new = rows_for(rng, "new")

    # Deliberately invalid patient IDs
    old += [
        dict(old[0], patient_id=""),
        dict(old[1], patient_id="abc"),
        dict(old[2], patient_id=-1),
        dict(old[3], patient_id=None)
    ]

    # Write two different CSV schema versions

    write(
        f"{RAW}/diabetes_patients_old.csv",
        OLD,
        old
    )

    write(
        f"{RAW}/diabetes_patients_new.csv",
        NEW,
        new
    )

    # Create header-only CSV file

    write(
        f"{RAW}/diabetes_patients_empty.csv",
        NEW,
        []
    )

    print(
        "Created synthetic patient dataset:",
        len(old), "rows in old schema,",
        len(new), "rows in new schema,",
        "1 header-only file."
    )

    print("Files saved under:", RAW)


if __name__ == "__main__":
    main()

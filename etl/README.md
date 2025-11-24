# Data Quality Report — ETL Cleaning Summary

```bash
aws s3 cp etl/20_join_areas.py s3://ug1-travel-safe-bucket/code/etl/20_join_areas.py
```

```bash
aws emr-serverless start-job-run `
>>   --region ca-central-1 `
>>   --application-id 00g0msfb7tko833d `
>>   --execution-role-arn arn:aws:iam::358461553153:role/TravelSafe-EMR-ExecRole `
>>   --job-driver file://configs/jobdriver_join_accidents.json `
>>   --configuration-overrides file://configs/config_overrides.json
```

```output
{
    "applicationId": "...",
    "jobRunId": "...",
    "arn": "arn:aws:emr-serverless:ca-central-1:358461553153:/applications/00g0msfb7tko833d/jobruns/00g0nlfe0katrg3f"
}
```

same thing for crimes

## 1. Datasets Processed

- Crimes: Données Québec — actes criminels (CSV)

- Accidents: Données Québec — collisions routières (GeoJSON)

## 2. Cleaning Objectives

- Standardize column naming using a consistent safe schema.

- Clean geographic coordinates and filter to Montréal region.

- Remove rows with invalid or missing date/coordinate info.

- Deduplicate rows based on key identifying fields.

- Produce clean Parquet partitioned by year and month for efficient querying.

## 3. Key Transformations
### Crimes Dataset
#### Columns kept

- `categorie`

- `date`-> converted to `event_date`

- `quart`

- `pdq`

- `latitude`, `longitude`

#### Columns removed

- `x`, `y` (redundant coordinate system)

#### Transformations

- Lower-cased and sanitized all column names.

- Converted `date` -> `event_date` (Spark DateType).

- Trimmed whitespace on all string columns.

- Latitude/longitude converted to DoubleType.

- Filtered coordinates to:

    - `latitude`: _45.0_ to _46.1_

    - `longitude`: _–74.2_ to _–73.2_

- Removed rows where `event_date` could not be parsed.

- Deduplication based on:

```
event_date, pdq, lat, lon, categorie
```

#### Output columns

`categorie`, `quart`, `pdq`, `event_date`, `lat`, `lon`, `year`, `month`

### Accidents Dataset
#### Columns kept

- `gravite`

- `cd_muncp`

- `loc_long` -> `lon`

- `loc_lat` -> `lat`

- `DT_ACCDN` -> `event_date`

- `mrc`

#### Columns removed

- Very detailed per-vehicle, weather, surface, configuration metadata not required for risk scoring.

- Intermediate geometry wrapper from GeoJSON.

#### Transformations

- Enforced UTF-8 safe handling for accented characters.

- Extracted properties from GeoJSON (features[].properties).

- Converted `DT_ACCDN` -> `event_date` (Spark DateType).

- Filtered to Montréal MRC using regex:

```
Montréal (66)
```

- Filtered coordinates using the same bounding box as crimes.

- Deduplicated based on:

```
event_date, cd_muncp, lat, lon, gravite
```

#### Output columns

`gravite`, `cd_muncp`, `event_date`, `lat`, `lon`, `mrc`, `year`, `month`

## 4. Validation Rules

- **Latitude requirements**: between _45.0_ and _46.1_

- **Longitude requirements**: between _–74.2_ and _–73.2_

- `event_date` must be non-null

- Montréal-only filtering enforced for accidents

- Metrics logged automatically to S3 under:

```
s3://ug1-travel-safe-bucket/curated/<dataset>/_metrics/
```

## 5. Output Structure

- Data is written to S3 as Parquet with explicit partitioning:

```
s3://ug1-travel-safe-bucket/curated/<dataset>/year=YYYY/month=MM/part-*.parquet
```

- Metrics written as JSON:

```
s3://ug1-travel-safe-bucket/curated/<dataset>/_metrics/*.json
```

## 6. Data Quality Observations

- Accident data contains non-ASCII characters (`é`, `è`, `→`, smart quotes) → requires UTF-8 handling.

- Some accident rows contain missing or malformed coordinates.

- Crime data occasionally contains out-of-region points.

- PDQ values sometimes contain inconsistent spacing or formatting.

- Raw accidents file contains many optional fields that are often NULL.

## 7. Recommendations

- Add extended profiling: null count per column, distinct count, min/max values.

- Compute borough + H3 hex index in ETL to simplify scoring later.

- Improve error handling when decoding accident GeoJSON with mixed encodings.

- Add a column-level data dictionary documenting meaning and transformations.
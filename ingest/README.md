# Ingest — CKAN Dataset Downloader and Uploader

This directory contains the script used to collect public datasets from **Données Québec** and **Données Montréal** through their CKAN APIs and upload them to an **AWS S3** bucket in a structured and versioned manner.

---

## 1. Setup Before Running

### a. AWS Requirements
- Create an S3 bucket (e.g., `ug1-travel-safe-bucket`).
- Enable **versioning** on the bucket.
- Create an **IAM user** with **programmatic access** (for local CLI use).  
  Example: `travel-safe-ingest`.
- Attach the policy in `iam_policy_TravelSafeS3Access.json` to allow read/write access to the bucket.

- Configure AWS credentials on the local machine:
```bash
aws configure
# Set AWS Access Key ID, Secret Access Key, and region (ca-central-1)
```

### b. Local Python Environment
Install required packages in the project environment:
```bash
pip install -r ingest/requirements.txt
```

---

## 2. Running the Ingestion Script

The ingestion script retrieves open data from a CKAN API (such as Données Montréal or Données Québec), writes the result locally, and uploads it to the specified S3 path.

### Example Command — Montréal Crime Dataset
```bash
python ingest/fetch_ckan_resource.py   --api-base https://donnees.montreal.ca/api/3/action   --resource-id 0f6d2b4a-f2cd-4e54-8a0f-25a823cfcc2f   --bucket ug1-travel-safe-bucket   --prefix raw/crime   --dataset-name actes_criminels   --region ca-central-1
```

### Example Command — Québec Accident Dataset
```bash
python ingest/fetch_ckan_resource.py   --api-base https://www.donneesquebec.ca/recherche/api/3/action   --resource-id b3b7f567-5473-4cb0-b1b3-534fddbbf8e2   --bucket ug1-travel-safe-bucket   --prefix raw/accidents   --dataset-name accidents_routiers   --region ca-central-1
```

---

## 3. What Happens During Execution

1. The script queries the CKAN `datastore_search` endpoint for the provided resource ID.
2. Data is fetched in **paginated chunks** and saved locally as `.ndjson` in the `ingest/out/` folder.
3. If the CKAN datastore is inactive, the script automatically downloads the original CSV/GeoJSON file.
4. Once data is saved locally, it is uploaded to the specified S3 bucket path:
   ```
   s3://<bucket-name>/raw/<dataset>/<YYYY-MM>/<filename>.ndjson
   ```
5. Console logs show information about records fetched, upload confirmation, and any warnings.

---

## 4. Expected Outcome

After a successful run:

### a. Local Output
The `ingest/out/` folder contains the raw `.ndjson` or `.json` file downloaded from the CKAN API.
Example:
```
ingest/out/
├── actes_criminels-2025-10-28.ndjson
└── accidents_routiers-2025-10-28.ndjson
```

### b. S3 Output
The same dataset is uploaded to the S3 bucket following this structure:
```
s3://ug1-travel-safe-bucket/raw/crime/2025-10/actes_criminels-2025-10-28.ndjson
s3://ug1-travel-safe-bucket/raw/accidents/2025-10/accidents_routiers-2025-10-28.ndjson
```

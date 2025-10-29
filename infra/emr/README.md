## Configuration Summary
- **S3 bucket:** `ug1-travel-safe-bucket`
  - Folders: `/raw`, `/curated`, `/served`, `/logs`
- **Execution role:** `TravelSafe-EMR-ExecRole`
  - Permissions: S3 Get/Put/List, CloudWatchLogs Create/Put
  - Trusted entity: `emr-serverless.amazonaws.com`
- **User:** `travel-safe-ingest`
  - Attached policy: `TravelSafeEmrSubmitter`
  - Allows `emr-serverless:StartJobRun` and `iam:PassRole` on the execution role
- **EMR Serverless application:** `spark-etl-app` (ID `00g0msfb7tko833d`)
  - Type = Spark 7.10.0 (x86_64)
  - Region = `ca-central-1`
  - Log URI = `s3://ug1-travel-safe-bucket/logs/`

## Validation
Executed a test job using the built-in Spark example:

```bash
aws emr-serverless start-job-run \
  --region ca-central-1 \
  --application-id 00g0msfb7tko833d \
  --execution-role-arn arn:aws:iam::358461553153:role/TravelSafe-EMR-ExecRole \
  --job-driver file://configs/jobdriver_test_pi.json \
  --configuration-overrides file://configs/config_overrides.json

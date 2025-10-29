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


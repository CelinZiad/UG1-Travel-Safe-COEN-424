In order to create a dynamodb in aws, run the following commands in your terminal:

```bash
aws dynamodb create-table --cli-input-json file://dynamodb/create_table.json --region ca-central-1
```

To load data into the dynamodb table, run the following command in your terminal:

```bash
python dynamodb/load_all_periods.py `
>>   --bucket ug1-travel-safe-bucket `
>>   --prefix-root served/areas_scores/ `
>>   --table TravelSafeScores `
>>   --region ca-central-1
```
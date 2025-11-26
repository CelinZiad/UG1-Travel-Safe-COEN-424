import argparse
import json
from decimal import Decimal

import boto3


def parse_args():
    parser = argparse.ArgumentParser(
        description="Load TravelSafe scores from S3 to DynamoDB"
    )
    parser.add_argument(
        "--bucket",
        required=True,
        help="S3 bucket name, e.g. ug1-travel-safe-bucket",
    )
    parser.add_argument(
        "--period",
        required=True,
        help="Period in yyyymm format, e.g. 201201 or 202510",
    )
    parser.add_argument(
        "--prefix-root",
        default="served/areas_scores",
        help="Root prefix under the bucket (default: served/areas_scores)",
    )
    parser.add_argument(
        "--table",
        default="TravelSafeScores",
        help="DynamoDB table name (default: TravelSafeScores)",
    )
    parser.add_argument(
        "--region",
        default="ca-central-1",
        help="AWS region (default: ca-central-1)",
    )
    return parser.parse_args()


def to_dynamodb_value(value):
    """
    Convert Python values to DynamoDB-friendly ones.
    - keep ints and bools as-is
    - convert floats to Decimal
    - leave strings as-is
    """
    if isinstance(value, (int, bool)) or value is None:
        return value
    if isinstance(value, float):
        return Decimal(str(value))
    return value


def record_to_item(record):
    """
    Map one JSON record from S3 to a DynamoDB item.
    Be tolerant to older/partial schemas.
    We assume only records with PK/SK reach this function.
    """
    pk = record["PK"]
    sk = record["SK"]

    # areaId: prefer explicit, otherwise fall back to PK
    area_id = record.get("areaId", pk)

    # Derive a reasonable areaName if missing:
    # e.g. BOROUGH#Saint-Léonard -> Saint-Léonard
    default_area_name = area_id.split("#", 1)[-1] if "#" in area_id else area_id
    area_name = record.get("areaName", default_area_name)

    borough_code = record.get("borough_code", area_name)

    quart = record.get("quart")  # can be None if not present
    period = record.get("period", "unknown")

    colour = record.get("colour", "UNKNOWN")
    is_latest = bool(record.get("isLatest", False))

    item = {
        # Keys
        "pk": pk,
        "sk": sk,

        # Core attributes
        "areaId": area_id,
        "areaName": area_name,
        "borough_code": borough_code,
        "quart": quart,
        "period": period,
        "colour": colour,
        "isLatest": is_latest,
    }

    # Metrics (may be missing on some rows; use get + conversion)
    if "safetyScore" in record:
        item["safetyScore"] = to_dynamodb_value(record["safetyScore"])
    if "risk_crime" in record:
        item["risk_crime"] = to_dynamodb_value(record["risk_crime"])
    if "risk_acc" in record:
        item["risk_acc"] = to_dynamodb_value(record["risk_acc"])
    if "risk_total" in record:
        item["risk_total"] = to_dynamodb_value(record["risk_total"])
    if "numIncidentsCrime" in record:
        item["numIncidentsCrime"] = record["numIncidentsCrime"]
    if "numIncidentsAccidents" in record:
        item["numIncidentsAccidents"] = record["numIncidentsAccidents"]

    # GSI sort key: marks latest vs historical + embeds time context
    # Example: "isLatest#202510#soir" or "hist#201201#nuit"
    prefix = "isLatest" if is_latest else "hist"
    quart_part = quart if quart is not None else "all"
    item["gsi1sk"] = f"{prefix}#{period}#{quart_part}"

    return item


def load_period(bucket, prefix_root, period, table_name, region):
    s3 = boto3.client("s3", region_name=region)
    dynamodb = boto3.resource("dynamodb", region_name=region)
    table = dynamodb.Table(table_name)

    prefix = f"{prefix_root.rstrip('/')}/{period}/"

    print(
        f"Loading period {period} from "
        f"s3://{bucket}/{prefix} into DynamoDB table {table_name}"
    )

    kwargs = {"Bucket": bucket, "Prefix": prefix}
    while True:
        response = s3.list_objects_v2(**kwargs)

        for obj in response.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".json"):
                continue

            print(f"  Processing file: s3://{bucket}/{key}")
            body = s3.get_object(Bucket=bucket, Key=key)["Body"]

            for line in body.iter_lines():
                if not line:
                    continue

                record = json.loads(line)

                # Skip any non-score / metadata rows that don't have PK/SK
                if "PK" not in record or "SK" not in record:
                    print(
                        f"  Skipping record without PK/SK in {key}: {record}"
                    )
                    continue

                item = record_to_item(record)
                table.put_item(Item=item)  # upsert

        if response.get("IsTruncated"):
            kwargs["ContinuationToken"] = response["NextContinuationToken"]
        else:
            break

    print(f"Done loading period {period}")


def main():
    args = parse_args()
    load_period(
        bucket=args.bucket,
        prefix_root=args.prefix_root,
        period=args.period,
        table_name=args.table,
        region=args.region,
    )

main()

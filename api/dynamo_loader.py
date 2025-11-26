import argparse, json, os
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError


def create_tables(dynamodb):
    """Create DynamoDB tables if they don't exist."""

    # AreaScores table: stores safety scores per area and period
    try:
        dynamodb.create_table(
            TableName="TravelSafe_AreaScores",
            KeySchema=[
                {"AttributeName": "area_id", "KeyType": "HASH"},
                {"AttributeName": "period", "KeyType": "RANGE"}
            ],
            AttributeDefinitions=[
                {"AttributeName": "area_id", "AttributeType": "S"},
                {"AttributeName": "period", "AttributeType": "S"}
            ],
            BillingMode="PAY_PER_REQUEST"
        )
        print("[INFO] Created TravelSafe_AreaScores table")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceInUseException":
            print("[INFO] TravelSafe_AreaScores table already exists")
        else:
            raise

    # Areas table: stores area metadata
    try:
        dynamodb.create_table(
            TableName="TravelSafe_Areas",
            KeySchema=[
                {"AttributeName": "area_id", "KeyType": "HASH"}
            ],
            AttributeDefinitions=[
                {"AttributeName": "area_id", "AttributeType": "S"}
            ],
            BillingMode="PAY_PER_REQUEST"
        )
        print("[INFO] Created TravelSafe_Areas table")
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceInUseException":
            print("[INFO] TravelSafe_Areas table already exists")
        else:
            raise


def convert_floats(obj):
    """Convert floats to Decimal for DynamoDB compatibility."""
    if isinstance(obj, float):
        return Decimal(str(obj))
    elif isinstance(obj, dict):
        return {k: convert_floats(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_floats(i) for i in obj]
    return obj


def load_scores_from_s3(s3_client, bucket, prefix, dynamodb_table):
    """Load score JSON files from S3 into DynamoDB."""
    paginator = s3_client.get_paginator("list_objects_v2")

    areas_seen = set()
    count = 0

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".json"):
                continue

            response = s3_client.get_object(Bucket=bucket, Key=key)
            content = response["Body"].read().decode("utf-8")

            for line in content.strip().split("\n"):
                if not line:
                    continue
                record = json.loads(line)
                record = convert_floats(record)

                area_id = record.get("area_id")
                period = record.get("period")

                if not area_id or not period:
                    continue

                # Write to AreaScores table
                dynamodb_table.put_item(Item={
                    "area_id": area_id,
                    "period": period,
                    "borough_name": record.get("borough_name", ""),
                    "safety_score": record.get("safety_score", Decimal("0")),
                    "color": record.get("color", "yellow"),
                    "crime_count": record.get("crime_count", 0),
                    "accident_count": record.get("accident_count", 0)
                })
                count += 1

                # Track unique areas
                if area_id not in areas_seen:
                    areas_seen.add(area_id)

    print(f"[INFO] Loaded {count} score records")
    return areas_seen


def load_areas_metadata(dynamodb_table, areas_seen, s3_client, bucket, boroughs_key):
    """Load area metadata from boroughs GeoJSON."""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=boroughs_key)
        content = response["Body"].read().decode("utf-8")
        geojson = json.loads(content)

        for feature in geojson.get("features", []):
            props = feature.get("properties", {})
            name = props.get("name") or props.get("NOM") or props.get("nom")
            code = props.get("code") or props.get("ID") or props.get("id") or name

            if not code:
                continue

            geom = feature.get("geometry", {})

            dynamodb_table.put_item(Item={
                "area_id": str(code),
                "name": name or str(code),
                "geometry_type": geom.get("type", ""),
                "source": "montreal_boroughs"
            })

        print(f"[INFO] Loaded area metadata from {boroughs_key}")
    except ClientError as e:
        print(f"[WARN] Could not load boroughs file: {e}")

    # Add any areas from scores that weren't in boroughs file
    for area_id in areas_seen:
        try:
            dynamodb_table.put_item(
                Item={"area_id": area_id, "name": area_id, "source": "scores"},
                ConditionExpression="attribute_not_exists(area_id)"
            )
        except ClientError:
            pass


def main():
    ap = argparse.ArgumentParser("Load safety scores into DynamoDB")
    ap.add_argument("--bucket", default="ug1-travel-safe-bucket")
    ap.add_argument("--scores-prefix", default="served/scores/json/")
    ap.add_argument("--boroughs-key", default="ref/montreal_boroughs.geojson")
    ap.add_argument("--region", default="ca-central-1")
    ap.add_argument("--create-tables", action="store_true")
    args = ap.parse_args()

    session = boto3.Session(region_name=args.region)
    dynamodb = session.resource("dynamodb")
    s3 = session.client("s3")

    if args.create_tables:
        create_tables(session.client("dynamodb"))
        # Wait for tables to be active
        print("[INFO] Waiting for tables to be ready...")
        waiter = session.client("dynamodb").get_waiter("table_exists")
        waiter.wait(TableName="TravelSafe_AreaScores")
        waiter.wait(TableName="TravelSafe_Areas")

    scores_table = dynamodb.Table("TravelSafe_AreaScores")
    areas_table = dynamodb.Table("TravelSafe_Areas")

    print(f"[INFO] Loading scores from s3://{args.bucket}/{args.scores_prefix}")
    areas_seen = load_scores_from_s3(s3, args.bucket, args.scores_prefix, scores_table)

    print(f"[INFO] Loading area metadata")
    load_areas_metadata(areas_table, areas_seen, s3, args.bucket, args.boroughs_key)

    print("[OK] DynamoDB load complete")


if __name__ == "__main__":
    main()

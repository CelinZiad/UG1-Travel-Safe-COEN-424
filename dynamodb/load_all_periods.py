import argparse
import subprocess

import boto3


def parse_args():
    parser = argparse.ArgumentParser(
        description="Load all periods from S3 served/areas_scores into DynamoDB"
    )
    parser.add_argument(
        "--bucket",
        required=True,
        help="S3 bucket name, e.g. ug1-travel-safe-bucket",
    )
    parser.add_argument(
        "--prefix-root",
        default="served/areas_scores/",
        help="Root prefix under bucket (default: served/areas_scores/)",
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


def main():
    args = parse_args()

    s3 = boto3.client("s3", region_name=args.region)

    prefix_root = args.prefix_root
    if not prefix_root.endswith("/"):
        prefix_root += "/"

    print(f"Scanning s3://{args.bucket}/{prefix_root} for periods...")

    resp = s3.list_objects_v2(
        Bucket=args.bucket,
        Prefix=prefix_root,
        Delimiter="/",
    )

    if "CommonPrefixes" not in resp:
        print("No prefixes found. Empty served/areas_scores?")
        return

    periods = []
    for p in resp["CommonPrefixes"]:
        prefix = p["Prefix"]  # e.g. "served/areas_scores/201201/"
        period = prefix.split("/")[-2]  # extract "201201"
        periods.append(period)

    periods = sorted(periods)
    print(f"Found {len(periods)} periods:\n{periods}")

    # Loop and load each period
    for period in periods:
        print("\n========================")
        print(f"Loading period {period}")
        print("========================")

        # We assume we run this from repo root, so paths are relative to that.
        subprocess.run(
            [
                "python",
                "dynamodb/load_scores_to_dynamodb.py",
                "--bucket",
                args.bucket,
                "--period",
                period,
                "--prefix-root",
                prefix_root,
                "--table",
                args.table,
                "--region",
                args.region,
            ],
            check=True,
        )

    print("\nAll periods loaded!")

main()

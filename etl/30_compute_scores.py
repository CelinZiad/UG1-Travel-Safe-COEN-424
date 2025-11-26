import argparse, json
from pyspark.sql import SparkSession, functions as F


def compute_safety_score(crime_count, accident_count, weights):
    """
    Compute safety score (0-100) based on incident counts.
    Higher score = safer area.
    Uses inverse normalization: fewer incidents = higher score.
    """
    crime_weight = weights.get("crime", 0.6)
    accident_weight = weights.get("accident", 0.4)

    # Normalize counts using log scale to handle outliers
    crime_factor = 1 / (1 + crime_count * 0.1)
    accident_factor = 1 / (1 + accident_count * 0.05)

    raw_score = (crime_factor * crime_weight + accident_factor * accident_weight) * 100
    return min(100, max(0, raw_score))


def main():
    ap = argparse.ArgumentParser("Compute safety scores per area and time period")
    ap.add_argument("--crimes", required=True, help="s3://.../curated/crime_areas/")
    ap.add_argument("--accidents", required=True, help="s3://.../curated/accidents_areas/")
    ap.add_argument("--out", required=True, help="s3://.../served/scores/")
    ap.add_argument("--crime-weight", type=float, default=0.6)
    ap.add_argument("--accident-weight", type=float, default=0.4)
    args = ap.parse_args()

    spark = (SparkSession.builder
             .appName("compute-safety-scores")
             .getOrCreate())

    weights = {"crime": args.crime_weight, "accident": args.accident_weight}
    score_udf = F.udf(lambda c, a: compute_safety_score(c, a, weights))

    crimes_df = spark.read.parquet(args.crimes)
    accidents_df = spark.read.parquet(args.accidents)

    # Aggregate crimes by borough and period
    crime_agg = (crimes_df
                 .groupBy("borough_code", "borough_name", "year", "month")
                 .agg(F.count("*").alias("crime_count")))

    # Aggregate accidents by borough and period
    accident_agg = (accidents_df
                    .groupBy("borough_code", "borough_name", "year", "month")
                    .agg(F.count("*").alias("accident_count")))

    # Join crime and accident aggregates
    scores_df = (crime_agg
                 .join(accident_agg,
                       on=["borough_code", "borough_name", "year", "month"],
                       how="outer")
                 .fillna(0, subset=["crime_count", "accident_count"]))

    # Compute safety score
    scores_df = scores_df.withColumn(
        "safety_score",
        score_udf(F.col("crime_count"), F.col("accident_count")).cast("double")
    )

    # Add color classification
    scores_df = scores_df.withColumn(
        "color",
        F.when(F.col("safety_score") >= 70, "green")
         .when(F.col("safety_score") >= 40, "yellow")
         .otherwise("red")
    )

    # Add period column for easier querying
    scores_df = scores_df.withColumn(
        "period",
        F.concat(F.col("year"), F.lit("-"), F.col("month"))
    )

    # Add area_id for API compatibility
    scores_df = scores_df.withColumn(
        "area_id",
        F.coalesce(F.col("borough_code"), F.lit("unknown"))
    )

    out_path = args.out.rstrip("/") + "/"

    # Write as JSON for API consumption
    (scores_df
     .coalesce(1)
     .write
     .mode("overwrite")
     .json(out_path + "json/"))

    # Write as Parquet for analytics
    (scores_df
     .repartition(1, "year", "month")
     .write
     .mode("overwrite")
     .partitionBy("year", "month")
     .parquet(out_path + "parquet/"))

    # Generate latest scores summary (most recent period per area)
    from pyspark.sql.window import Window

    w = Window.partitionBy("area_id").orderBy(F.desc("year"), F.desc("month"))
    latest_df = (scores_df
                 .withColumn("rn", F.row_number().over(w))
                 .where(F.col("rn") == 1)
                 .drop("rn"))

    (latest_df
     .coalesce(1)
     .write
     .mode("overwrite")
     .json(out_path + "latest/"))

    # Write metrics
    total_areas = scores_df.select("area_id").distinct().count()
    total_periods = scores_df.select("period").distinct().count()

    (spark.createDataFrame([{
        "total_areas": total_areas,
        "total_periods": total_periods,
        "avg_score": scores_df.agg(F.avg("safety_score")).collect()[0][0]
    }]).write.mode("overwrite").json(out_path + "_metrics/"))

    print(f"[OK] scores computed -> {out_path}")


main()

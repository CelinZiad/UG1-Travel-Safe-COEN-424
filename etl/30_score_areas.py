import argparse
from pyspark.sql import SparkSession, functions as F

# ---------- Weight helpers ----------

def build_time_weight_col(age_col):
    """
    Piecewise time-decay weight based on age in months.
    """
    return (
        F.when((age_col >= 0) & (age_col <= 3), 1.0)
         .when((age_col > 3) & (age_col <= 6), 0.7)
         .when((age_col > 6) & (age_col <= 9), 0.4)
         .when((age_col > 9) & (age_col <= 12), 0.2)
         .otherwise(0.0)
    )

def build_category_weight_col(col):
    """
    Crime severity weights based on French category values.
    """
    return (
        F.when(col == "Infractions entraînant la mort", 5.0)
         .when(col == "Vols qualifiés", 3.0)
         .when(col == "Introduction", 2.0)
         .when(col == "Vol de véhicule à moteur", 2.0)
         .when(col == "Vol dans / sur véhicule à moteur", 1.5)
         .when(col == "Méfais", 1.0)
         .otherwise(0.0)
    )

def build_severity_weight_col(col):
    """
    Accident severity weights based on French gravité values.
    """
    return (
        F.when(col == "Mortel", 5.0)
         .when(col == "Grave", 4.0)
         .when(col == "Léger", 2.0)
         .when(col == "Dommages matériels seulement", 1.0)
         .when(col == "Dommages matériels inférieurs au seuil de rapportage", 0.5)
         .otherwise(0.0)
    )

# ---------- Main scoring job ----------

def main():
    ap = argparse.ArgumentParser("Batch safety scoring job (crime + accidents)")
    ap.add_argument("--crime-areas", required=True, help="s3://.../curated/crime_areas/")
    ap.add_argument("--accident-areas", required=True, help="s3://.../curated/accidents_areas/")
    ap.add_argument("--out", required=True, help="s3://.../served/areas_scores/")
    ap.add_argument("--period", required=True, help="Target period in yyyymm, e.g. 202510")
    ap.add_argument("--window-months", type=int, default=12)
    ap.add_argument("--area-col", default="borough_code")
    ap.add_argument("--area-name-col", default="borough_name")
    ap.add_argument("--area-prefix", default="BOROUGH#", help="Prefix for PK areaId, e.g. BOROUGH# or HEX#")
    args = ap.parse_args()

    spark = (
        SparkSession.builder
        .appName("safety-scoring")
        .getOrCreate()
    )

    target_year = int(args.period[:4])
    target_month = int(args.period[4:6])

    # Helper: age in months (relative to target period)
    def age_expr(year_col, month_col):
        y = F.col(year_col).cast("int")
        m = F.col(month_col).cast("int")
        return (target_year - y) * 12 + (target_month - m)

    # ---------- Crimes ----------
    crime = spark.read.parquet(args.crime_areas)
    if args.area_col not in crime.columns:
        raise ValueError(f"area-col '{args.area_col}' not found in crime dataset; have {crime.columns}")

    # Normalize crimes to also have a "quart" column like accidents
    if "time_of_day" not in crime.columns:
        raise ValueError("crime dataset is missing 'time_of_day' column")
    crime = crime.withColumn("quart", F.col("time_of_day"))

    # Grouping grain for scoring: area + quart (jour, soir, nuit)
    group_cols = [F.col(args.area_col), F.col("quart")]

    crime = crime.withColumn("age_m", age_expr("year", "month"))
    crime = crime.where((F.col("age_m") >= 0) & (F.col("age_m") < args.window_months))

    crime = crime.withColumn("base_weight_crime", build_category_weight_col(F.col("category")))
    crime = crime.withColumn("time_weight", build_time_weight_col(F.col("age_m")))
    crime = crime.withColumn(
        "event_weight_crime",
        F.col("base_weight_crime") * F.col("time_weight")
    )

    crime_agg = (
        crime.groupBy(*group_cols)
            .agg(
                F.sum("event_weight_crime").alias("risk_crime"),
                F.count("*").alias("numIncidentsCrime")
            )
    )

    # ---------- Accidents ----------
    acc = spark.read.parquet(args.accident_areas)
    if args.area_col not in acc.columns:
        raise ValueError(f"area-col '{args.area_col}' not found in accidents dataset; have {acc.columns}")

    if "quart" not in acc.columns:
        raise ValueError("accidents dataset is missing 'quart' column")

    acc = acc.withColumn("age_m", age_expr("year", "month"))
    acc = acc.where((F.col("age_m") >= 0) & (F.col("age_m") < args.window_months))

    acc = acc.withColumn("base_weight_acc", build_severity_weight_col(F.col("severity")))
    acc = acc.withColumn("time_weight", build_time_weight_col(F.col("age_m")))
    acc = acc.withColumn(
        "event_weight_acc",
        F.col("base_weight_acc") * F.col("time_weight")
    )

    acc_agg = (
        acc.groupBy(*group_cols)
           .agg(
               F.sum("event_weight_acc").alias("risk_acc"),
               F.count("*").alias("numIncidentsAccidents")
           )
    )

    # ---------- Combine crime + accidents ----------
    scored = (
    crime_agg.join(acc_agg, on=[args.area_col, "quart"], how="outer")
             .na.fill(
                 {
                     "risk_crime": 0.0,
                     "numIncidentsCrime": 0,
                     "risk_acc": 0.0,
                     "numIncidentsAccidents": 0,
                 }
             )
    )


    scored = scored.withColumn(
        "risk_total",
        F.col("risk_crime") + F.col("risk_acc")
    )

    # ---------- Min-max normalization ----------
    stats = scored.agg(
        F.min("risk_total").alias("min"),
        F.max("risk_total").alias("max")
    ).collect()[0]

    rmin = stats["min"]
    rmax = stats["max"]

    if rmin is None or rmax is None:
        raise ValueError("No rows to score; check input data and period/window.")

    eps = 1e-6
    denom = (rmax - rmin) if (rmax - rmin) != 0 else eps

    scored = scored.withColumn(
        "norm_risk",
        (F.col("risk_total") - F.lit(rmin)) / F.lit(denom)
    )

    scored = scored.withColumn(
        "safetyScore",
        F.when(F.lit(rmax) == F.lit(rmin), F.lit(50.0))
         .otherwise(100.0 * (1.0 - F.col("norm_risk")))
    )

    # ---------- Colour label ----------
    scored = scored.withColumn(
        "colour",
        F.when(F.col("safetyScore") < 40, F.lit("RED"))
         .when(F.col("safetyScore") < 70, F.lit("YELLOW"))
         .otherwise(F.lit("GREEN"))
    )

    # ---------- Area identity and period ----------
    scored = scored.withColumn(
        "areaId",
        F.concat(F.lit(args.area_prefix), F.col(args.area_col).cast("string"))
    )

    # Attach areaName if available in crime dataset
    if args.area_name_col in crime.columns:
        names = crime.select(args.area_col, args.area_name_col).dropDuplicates([args.area_col])
        scored = (
            scored.join(names, on=args.area_col, how="left")
                  .withColumnRenamed(args.area_name_col, "areaName")
        )
    else:
        scored = scored.withColumn("areaName", F.lit(None).cast("string"))

    scored = (
        scored.withColumn("period", F.lit(args.period))
              .withColumn("PK", F.col("areaId"))
              .withColumn(
                    "SK",
                F.concat(
                    F.lit("PERIOD#"), F.lit(args.period),
                    F.lit("#QUART#"), F.col("quart")
                )
            )
            .withColumn("isLatest", F.lit(True))
    )

    # ---------- Select + write JSON ----------
    out_cols = [
        "PK", "SK", "isLatest",
        "areaId", "areaName", args.area_col,
        "quart",
        "period",
        "risk_crime", "risk_acc", "risk_total",
        "numIncidentsCrime", "numIncidentsAccidents",
        "safetyScore", "colour",
    ]

    out_df = scored.select(*out_cols)

    out_path = args.out.rstrip("/") + f"/{args.period}/"
    (
        out_df.coalesce(1)
              .write.mode("overwrite")
              .json(out_path)
    )

    print(f"[OK] wrote safety scores to {out_path}")

    spark.stop()

main()

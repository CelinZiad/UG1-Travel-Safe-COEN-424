import argparse
from pyspark.sql import SparkSession, functions as F

# ---------- Weight helpers ----------

def build_time_weight_col(age_col):
    """
    Piecewise time-decay weight based on age in months.

    Buckets (inclusive):
      0-3   -> 1.0
      4-6   -> 0.6
      7-9   -> 0.4
      10-12 -> 0.2
    Anything outside 0-12 months gets weight 0.
    """
    return (
        F.when((age_col >= 0) & (age_col <= 3), 1.0)
         .when((age_col > 3) & (age_col <= 6), 0.6)
         .when((age_col > 6) & (age_col <= 9), 0.4)
         .when((age_col > 9) & (age_col <= 12), 0.2)
         .otherwise(0.0)
    )


def build_category_weight_col(col):
    """
    Crime severity weights based on French category values
    produced by the cleaning job.
    """
    return (
        F.when(col == "Infractions entraînant la mort", 5.0)
         .when(col == "Vols qualifiés", 3.0)
         .when(col == "Introduction", 2.0)
         .when(col == "Vol de véhicule à moteur", 2.0)
         .when(col == "Vol dans / sur véhicule à moteur", 1.5)
         .when(col == "Méfait", 1.0)
         .otherwise(0.0)
    )


def build_severity_weight_col(col):
    """
    Accident severity weights based on French GRAVITE values.
    """
    return (
        F.when(col == "Mortel", 5.0)
         .when(col == "Grave", 4.0)
         .when(col == "Léger", 2.0)
         .when(col == "Dommages matériels seulement", 1.0)
         .when(col == "Dommages matériels inférieurs au seuil de rapportage", 0.5)
         .otherwise(0.0)
    )


def expand_periods(from_period: str, to_period: str):
    """
    Expand yyyymm range (inclusive) into a list of yyyymm strings.

    Example: from_period=202405, to_period=202407
             -> ["202405", "202406", "202407"]
    """
    y1 = int(from_period[:4])
    m1 = int(from_period[4:6])
    y2 = int(to_period[:4])
    m2 = int(to_period[4:6])

    periods = []
    y, m = y1, m1
    while (y < y2) or (y == y2 and m <= m2):
        periods.append(f"{y}{m:02d}")
        m += 1
        if m > 12:
            m = 1
            y += 1
    return periods


# ---------- Main scoring job ----------

def main():
    ap = argparse.ArgumentParser("Batch safety scoring job (crime + accidents)")
    ap.add_argument("--crime-areas", required=True, help="s3://.../curated/crime_areas/")
    ap.add_argument("--accident-areas", required=True, help="s3://.../curated/accidents_areas/")
    ap.add_argument("--out", required=True, help="s3://.../served/areas_scores/")

    # Either a single period OR a range
    ap.add_argument(
        "--period",
        required=False,
        help="Target period in yyyymm, e.g. 202510. "
             "If omitted, you must provide --from-period and --to-period."
    )
    ap.add_argument(
        "--from-period",
        required=False,
        help="Start period (inclusive) in yyyymm, e.g. 201101"
    )
    ap.add_argument(
        "--to-period",
        required=False,
        help="End period (inclusive) in yyyymm, e.g. 202510"
    )

    ap.add_argument("--window-months", type=int, default=12,
                    help="Lookback window in months for events (default 12)")
    ap.add_argument("--area-col", default="borough_code")
    ap.add_argument("--area-name-col", default="borough_name")
    ap.add_argument("--area-prefix", default="BOROUGH#",
                    help="Prefix for PK areaId, e.g. BOROUGH# or HEX#")
    args = ap.parse_args()

    # Determine list of periods to score
    if args.period:
        periods = [args.period]
    else:
        if not (args.from_period and args.to_period):
            raise ValueError("You must provide either --period OR both --from-period and --to-period.")
        periods = expand_periods(args.from_period, args.to_period)

    latest_period = periods[-1]

    spark = (
        SparkSession.builder
        .appName("safety-scoring")
        .getOrCreate()
    )

    # ---------- Read base datasets ONCE ----------

    crime = spark.read.parquet(args.crime_areas)
    if args.area_col not in crime.columns:
        raise ValueError(f"area-col '{args.area_col}' not found in crime dataset; have {crime.columns}")
    if "time_of_day" not in crime.columns:
        raise ValueError("crime dataset is missing 'time_of_day' column")
    if "year" not in crime.columns or "month" not in crime.columns:
        raise ValueError("crime dataset is missing 'year' or 'month' columns")

    # Normalise crime quart
    crime = crime.withColumn("quart", F.col("time_of_day"))
    group_cols = [F.col(args.area_col), F.col("quart")]

    # Base crime weights (no time weighting yet)
    crime = crime.withColumn("base_weight_crime", build_category_weight_col(F.col("category")))

    # Names lookup (areaName) if present
    if args.area_name_col in crime.columns:
        names = crime.select(args.area_col, args.area_name_col).dropDuplicates([args.area_col])
    else:
        names = None

    acc = spark.read.parquet(args.accident_areas)
    if args.area_col not in acc.columns:
        raise ValueError(f"area-col '{args.area_col}' not found in accidents dataset; have {acc.columns}")
    if "quart" not in acc.columns:
        raise ValueError("accidents dataset is missing 'quart' column")
    if "year" not in acc.columns or "month" not in acc.columns:
        raise ValueError("accidents dataset is missing 'year' or 'month' columns")

    # Base accident weights
    acc = acc.withColumn("base_weight_acc", build_severity_weight_col(F.col("severity")))

    # ---------- Loop over all requested periods ----------

    for period in periods:
        target_year = int(period[:4])
        target_month = int(period[4:6])

        # Helper expression: age in months relative to this period
        def age_expr(year_col, month_col):
            y = F.col(year_col).cast("int")
            m = F.col(month_col).cast("int")
            return (target_year - y) * 12 + (target_month - m)

        # ----- Crimes for this period -----
        crime_p = crime.withColumn("age_m", age_expr("year", "month"))
        crime_p = crime_p.where((F.col("age_m") >= 0) & (F.col("age_m") < args.window_months))
        crime_p = crime_p.withColumn("time_weight", build_time_weight_col(F.col("age_m")))
        crime_p = crime_p.withColumn(
            "event_weight_crime",
            F.col("base_weight_crime") * F.col("time_weight")
        )

        crime_agg = (
            crime_p.groupBy(*group_cols)
                   .agg(
                       F.sum("event_weight_crime").alias("risk_crime"),
                       F.count("*").alias("numIncidentsCrime")
                   )
        )

        # ----- Accidents for this period -----
        acc_p = acc.withColumn("age_m", age_expr("year", "month"))
        acc_p = acc_p.where((F.col("age_m") >= 0) & (F.col("age_m") < args.window_months))
        acc_p = acc_p.withColumn("time_weight", build_time_weight_col(F.col("age_m")))
        acc_p = acc_p.withColumn(
            "event_weight_acc",
            F.col("base_weight_acc") * F.col("time_weight")
        )

        acc_agg = (
            acc_p.groupBy(*group_cols)
                 .agg(
                     F.sum("event_weight_acc").alias("risk_acc"),
                     F.count("*").alias("numIncidentsAccidents")
                 )
        )

        # ----- Combine crime + accidents -----
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

        # If absolutely no rows for this period, skip it
        if scored.rdd.isEmpty():
            print(f"[WARN] No events to score for period {period}; skipping.")
            continue

        scored = scored.withColumn(
            "risk_total",
            F.col("risk_crime") + F.col("risk_acc")
        )

        # ----- Min-max normalization per period -----
        stats = scored.agg(
            F.min("risk_total").alias("min"),
            F.max("risk_total").alias("max")
        ).collect()[0]

        rmin = stats["min"]
        rmax = stats["max"]

        if rmin is None or rmax is None:
            print(f"[WARN] No risk_total values for period {period}; skipping.")
            continue

        # Degenerate case: all risk_total identical
        if abs(float(rmax) - float(rmin)) < 1e-9:
            scored = scored.withColumn("safetyScore", F.lit(50.0))
        else:
            denom = float(rmax) - float(rmin)
            scored = scored.withColumn(
                "safetyScore",
                100.0 * (1.0 - (F.col("risk_total") - F.lit(float(rmin))) / F.lit(denom))
            )

        # ----- Colour label -----
        scored = scored.withColumn(
            "colour",
            F.when(F.col("safetyScore") < 40, F.lit("RED"))
             .when(F.col("safetyScore") < 70, F.lit("YELLOW"))
             .otherwise(F.lit("GREEN"))
        )

        # ----- Area identity + period + keys -----
        scored = scored.withColumn(
            "areaId",
            F.concat(F.lit(args.area_prefix), F.col(args.area_col).cast("string"))
        )

        if names is not None:
            scored = (
                scored.join(names, on=args.area_col, how="left")
                      .withColumnRenamed(args.area_name_col, "areaName")
            )
        else:
            scored = scored.withColumn("areaName", F.lit(None).cast("string"))

        scored = (
            scored.withColumn("period", F.lit(period))
                  .withColumn("PK", F.col("areaId"))
                  .withColumn(
                      "SK",
                      F.concat(
                          F.lit("PERIOD#"), F.lit(period),
                          F.lit("#QUART#"), F.col("quart")
                      )
                  )
                  .withColumn("isLatest", F.lit(period == latest_period))
        )

        # ----- Select + write JSON for this period -----
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

        out_path = args.out.rstrip("/") + f"/{period}/"
        (
            out_df.coalesce(1)
                  .write.mode("overwrite")
                  .json(out_path)
        )
        print(f"[OK] wrote safety scores for period {period} to {out_path}")

    spark.stop()

main()

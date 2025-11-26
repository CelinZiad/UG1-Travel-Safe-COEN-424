import argparse, datetime as dt, re
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

def sanitize(name: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", name.strip().lower())

def infer_format(path: str) -> str:
    p = path.lower()
    if p.endswith(".ndjson"):
        # Newline-delimited JSON (one JSON object per line)
        return "ndjson"
    if p.endswith(".geojson") or p.endswith(".json"):
        # Regular JSON / GeoJSON
        return "json"
    return "csv"

def read_raw(spark, path, fmt, sep):
    if fmt == "csv":
        return (spark.read
                .option("header", True)
                .option("inferSchema", True)
                .option("multiLine", True)
                .option("escape", "\"")
                .option("sep", sep)
                .csv(path))

    # JSON / NDJSON: always read JSON first
    if fmt == "ndjson":
        # NDJSON: one JSON object per line → no multiLine
        df = spark.read.json(path)
    else:
        # Regular JSON / GeoJSON: allow pretty-printed multi-line JSON
        df = (spark.read
              .option("multiLine", True)
              .json(path))

    # At this point df columns can be:
    # - ["type", "features", ...] for a FeatureCollection
    # - ["type", "geometry", "properties", ...] for one Feature per row
    # - already flat (normal JSON)

    lower = [c.lower() for c in df.columns]

    if "features" in lower:
        # FeatureCollection: { "type": "FeatureCollection", "features": [...] }
        df = df.select(F.explode("features").alias("f")).select("f.properties.*")
    elif "properties" in lower:
        # One Feature per row: { "type": "Feature", "geometry": ..., "properties": { ... } }
        df = df.select("properties.*")

    return df

def normalize_columns(df):
    for c in df.columns:
        df = df.withColumnRenamed(c, sanitize(c))
    return df

def profile_crime(df, min_lat, max_lat, min_lon, max_lon):
    """
    Keep: categorie, date -> event_date, quart, pdq, lat/lon
    Drop: x, y (implicitly by selecting only the fields we keep)
    """
    needed = {"categorie","date","quart","pdq","longitude","latitude"}
    miss = [c for c in needed if c not in df.columns]
    if miss:
        raise ValueError(f"crime: missing columns {miss}; have {df.columns}")

    df = (df
          .withColumn("event_date", F.to_date(F.col("date")))
          .withColumn("lat", F.col("latitude").cast(DoubleType()))
          .withColumn("lon", F.col("longitude").cast(DoubleType())))

    for c, t in df.dtypes:
        if t == "string":
            df = df.withColumn(c, F.trim(F.col(c)))

    df = (df
          .where(F.col("lat").isNotNull() & F.col("lon").isNotNull())
          .where((F.col("lat") >= min_lat) & (F.col("lat") <= max_lat))
          .where((F.col("lon") >= min_lon) & (F.col("lon") <= max_lon))
          .where(F.col("event_date").isNotNull()))

    # Keep French values but rename columns to English
    df = df.select("categorie", "quart", "pdq", "event_date", "lat", "lon")

    df = (df
          .withColumnRenamed("categorie", "category")
          .withColumnRenamed("quart", "time_of_day")
          .withColumnRenamed("pdq", "police_district"))

    df = df.dropDuplicates(["event_date", "police_district", "lat", "lon", "category"])
    return df

def profile_accidents(df, min_lat, max_lat, min_lon, max_lon):
    """
    Keep: gravite, cd_muncp, loc_long/loc_lat -> lon/lat, dt_accdn -> event_date
    Only rows where MRC matches Montréal (66)
    """
    needed = {"gravite","cd_muncp","loc_long","loc_lat","dt_accdn","mrc", "heure_accdn"}
    miss = [c for c in needed if c not in df.columns]
    if miss:
        raise ValueError(f"accidents: missing columns {miss}; have {df.columns}")

    df = (df
          .withColumn("event_date", F.to_date(F.col("dt_accdn")))
          .withColumn("lat", F.col("loc_lat").cast(DoubleType()))
          .withColumn("lon", F.col("loc_long").cast(DoubleType())))

    df = (df
          .where(F.col("lat").isNotNull() & F.col("lon").isNotNull())
          .where((F.col("lat") >= min_lat) & (F.col("lat") <= max_lat))
          .where((F.col("lon") >= min_lon) & (F.col("lon") <= max_lon))
          .where(F.col("event_date").isNotNull()))

    df = df.where(F.col("mrc").rlike(r"Montr[eé]al\s*\(66"))
    
    # Keep French values but rename columns to English
    df = df.select("gravite", "cd_muncp", "event_date", "lat", "lon", "mrc", "heure_accdn")

    # Extract start hour as integer
    start_part = F.split(F.col("heure_accdn"), "-")[0]  # "15:00:00" from "15:00:00-15:59:00"
    start_hour = F.substring(start_part, 1, 2).cast("int")  # 15

    df = df.withColumn(
        "quart",
        F.when(
            start_hour.isNull(),  # e.g. "Non précisé"
            F.lit("nuit")
        ).when(
            (start_hour >= 6) & (start_hour < 12),
            F.lit("jour")
        ).when(
            (start_hour >= 12) & (start_hour < 18),
            F.lit("soir")
        ).otherwise(
            F.lit("nuit")
        )
    )

    df = (df
          .withColumnRenamed("gravite", "severity")
          .withColumnRenamed("cd_muncp", "municipality_code")
          .withColumnRenamed("mrc", "mrc_name"))

    df = df.dropDuplicates(["event_date", "municipality_code", "lat", "lon", "severity"])

    return df


def main():
    ap = argparse.ArgumentParser("RAW -> clean Parquet (crime/accidents)")
    ap.add_argument("--raw", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--dataset", required=True, choices=["crime","accidents"])
    ap.add_argument("--format", choices=["csv","ndjson","json"], default=None)
    ap.add_argument("--sep", default=",")
    ap.add_argument("--min-lat", type=float, default=45.0)
    ap.add_argument("--max-lat", type=float, default=46.1)
    ap.add_argument("--min-lon", type=float, default=-74.2)
    ap.add_argument("--max-lon", type=float, default=-73.2)
    args = ap.parse_args()

    spark = SparkSession.builder.appName(f"clean-{args.dataset}").getOrCreate()
    fmt = args.format or infer_format(args.raw)

    df = read_raw(spark, args.raw, fmt, args.sep)
    df = normalize_columns(df)

    if args.dataset == "crime":
        df = profile_crime(df, args.min_lat, args.max_lat, args.min_lon, args.max_lon)
    else:
        df = profile_accidents(df, args.min_lat, args.max_lat, args.min_lon, args.max_lon)

    df = (df
          .withColumn("year",  F.date_format("event_date", "yyyy"))
          .withColumn("month", F.date_format("event_date", "MM")))

    year_month_counts = (
        df.groupBy("year", "month")
          .count()
          .orderBy("year", "month")
    )

    out = args.out.rstrip("/") + "/"

    (year_month_counts
        .write
        .mode("overwrite")
        .json(out + "_metrics/year_month_counts/"))

    (df.repartition(1, "year", "month")
       .write.mode("overwrite")
       .partitionBy("year", "month")
       .parquet(out))

    spark.createDataFrame([{
        "dataset": args.dataset,
        "row_count": df.count()
    }]).write.mode("overwrite").json(out + "_metrics/summary/")

    print(f"[OK] {args.dataset} -> {out}")

main()

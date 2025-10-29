import argparse, datetime as dt, re
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType

def sanitize(name: str) -> str:
    return re.sub(r"[^a-z0-9_]+", "_", name.strip().lower())

def infer_format(path: str) -> str:
    p = path.lower()
    if any(ext in p for ext in [".ndjson", ".json", ".geojson"]):
        return "ndjson"
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
    df = (spark.read
            .option("multiLine", True)
            .json(path))
    lower = [c.lower() for c in df.columns]
    if "features" in lower:
        df = df.select(F.explode("features").alias("f")).select("f.properties.*")
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

    df = df.select("categorie","quart","pdq","event_date","lat","lon")
    df = df.dropDuplicates(["event_date","pdq","lat","lon","categorie"])
    return df

def profile_accidents(df, min_lat, max_lat, min_lon, max_lon):
    """
    Keep: gravite, cd_muncp, loc_long/loc_lat -> lon/lat, dt_accdn -> event_date
    Only rows where MRC matches Montréal (66)
    """
    needed = {"gravite","cd_muncp","loc_long","loc_lat","dt_accdn","mrc"}
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
    df = df.select("gravite","cd_muncp","event_date","lat","lon","mrc")
    df = df.dropDuplicates(["event_date","cd_muncp","lat","lon","gravite"])

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
          .withColumn("year",  F.date_format("event_date","yyyy"))
          .withColumn("month", F.date_format("event_date","MM")))

    out = args.out.rstrip("/") + "/"
    (df.repartition(1, "year", "month")
       .write.mode("overwrite")
       .partitionBy("year","month")
       .parquet(out))

    spark.createDataFrame([{
        "dataset": args.dataset,
        "row_count": df.count()
    }]).write.mode("append").json(out + "_metrics/")

    print(f"[OK] {args.dataset} -> {out}")

main()

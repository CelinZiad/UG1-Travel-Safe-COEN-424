import json, argparse
from pyspark.sql import SparkSession, functions as F, types as T

# ---------- Geometry helpers ----------
def point_in_polygon(lon, lat, poly_or_multi):
    """
    Ray-casting point-in-polygon. poly_or_multi = list of polygons,
    each polygon = [outer_ring, hole1, ...], each ring = [(x,y), ...]
    """
    if lon is None or lat is None or poly_or_multi is None:
        return False

    def in_ring(x, y, ring):
        inside = False
        n = len(ring)
        for i in range(n):
            x1, y1 = ring[i]
            x2, y2 = ring[(i + 1) % n]
            if ((y1 > y) != (y2 > y)) and x < (x2 - x1) * (y - y1) / ((y2 - y1) or 1e-15) + x1:
                inside = not inside
        return inside

    def in_polygon(x, y, polygon):
        if not polygon:
            return False
        if not in_ring(x, y, polygon[0]):
            return False
        for k in range(1, len(polygon)):
            if in_ring(x, y, polygon[k]):
                return False
        return True

    for poly in poly_or_multi:
        if in_polygon(lon, lat, poly):
            return True
    return False

# ---------- Broadcast holder (set in main) ----------
BORO_BCAST = None  # will be spark.sparkContext.broadcast([...])

# ---------- UDFs ----------
@F.udf(T.StructType([
    T.StructField("borough_code", T.StringType(), True),
    T.StructField("borough_name", T.StringType(), True),
]))
def udf_assign_borough(lon, lat):
    global BORO_BCAST
    if BORO_BCAST is None or lon is None or lat is None:
        return None
    for b in BORO_BCAST.value:
        if point_in_polygon(lon, lat, b["geom"]):
            return {
                "borough_code": str(b["borough_code"]) if b["borough_code"] is not None else None,
                "borough_name": str(b["borough_name"]) if b["borough_name"] is not None else None
            }
    return None

# ---------- Load polygons ----------
def load_boroughs_list(spark, path):
    """
    Read a GeoJSON file (WGS84) and normalize to a list of:
      {"borough_code": str, "borough_name": str, "geom": list_of_polygons}
    where each polygon is [ring0, ring1, ...] and each ring is [(x, y), ...].
    """
    lines = spark.read.text(path).collect()
    text = "\n".join(r.value for r in lines)
    data = json.loads(text)

    features = data.get("features", [])
    items = []
    for f in features:
        props = f.get("properties", {}) or {}
        name = props.get("name") or props.get("NOM") or props.get("nom") or props.get("borough")
        code = props.get("code") or props.get("ID") or props.get("id") or name
        geom = f.get("geometry") or {}
        gtype = geom.get("type")
        coords = geom.get("coordinates")

        if not gtype or coords is None:
            continue

        def norm_polygon(poly):
            return [[(float(x), float(y)) for (x, y) in ring] for ring in poly]

        if gtype == "Polygon":
            multi = [norm_polygon(coords)]
        elif gtype == "MultiPolygon":
            multi = [norm_polygon(p) for p in coords]
        else:
            continue

        items.append({"borough_code": code, "borough_name": name, "geom": multi})

    if not items:
        raise ValueError("No polygon features loaded from boroughs file; check CRS (must be WGS84) and path.")
    return items

# ---------- Main ----------
def main():
    ap = argparse.ArgumentParser("Join curated data to Montreal areas (boroughs/hex)")
    ap.add_argument("--in", dest="inp", required=True, help="s3://.../curated/{dataset}/")
    ap.add_argument("--boroughs", required=True, help="s3://.../ref/montreal_boroughs.geojson (WGS84)")
    ap.add_argument("--out", required=True, help="s3://.../curated/{dataset}_areas")
    ap.add_argument("--with-hex", action="store_true", help="also add grid bin column (no external deps)")
    ap.add_argument("--bin-size-deg", type=float, default=0.0025, help="~275 m at Montreal latitude")
    args = ap.parse_args()

    spark = (SparkSession.builder
             .appName("join-areas")
             .getOrCreate())

    df = (spark.read.parquet(args.inp)
          .withColumn("lat", F.col("lat").cast("double"))
          .withColumn("lon", F.col("lon").cast("double"))
          .where(F.col("lat").isNotNull() & F.col("lon").isNotNull()))

    items = load_boroughs_list(spark, args.boroughs)
    global BORO_BCAST
    BORO_BCAST = spark.sparkContext.broadcast(items)

    assigned = (df.withColumn("borough", udf_assign_borough(F.col("lon"), F.col("lat")))
                  .withColumn("borough_code", F.col("borough.borough_code"))
                  .withColumn("borough_name", F.col("borough.borough_name"))
                  .drop("borough"))

    if args.with_hex:
        cell = F.lit(args.bin_size_deg)
        assigned = (assigned
                    .withColumn("bin_x", F.floor((F.col("lon") + F.lit(180.0)) / cell))
                    .withColumn("bin_y", F.floor((F.col("lat") + F.lit( 90.0)) / cell))
                    .withColumn("bin_id", F.concat_ws("_", F.lit("bin"), F.col("bin_x"), F.col("bin_y"))))

    assigned = assigned.repartition(1, "year", "month").persist()

    out = args.out.rstrip("/") + "/"
    (assigned.write
        .mode("overwrite")
        .partitionBy("year", "month")
        .parquet(out))

    rows_joined = assigned.count()
    (spark.createDataFrame([{"source": args.inp, "rows_joined": rows_joined}])
          .write.mode("overwrite")
          .json(out + "_metrics/"))

    print(f"[OK] joined -> {out}")

main()

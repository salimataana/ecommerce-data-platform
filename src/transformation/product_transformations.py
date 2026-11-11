from pyspark.sql import functions as F


def flatten_products(df):

    df_flat = (
        df.withColumn("dimensions_width", F.col("dimensions.width"))
        .withColumn("dimensions_height", F.col("dimensions.height"))
        .withColumn("dimensions_depth", F.col("dimensions.depth"))
        .withColumn("meta_barcode", F.col("meta.barcode"))
        .withColumn("meta_created_at", F.col("meta.createdAt"))
        .withColumn("meta_updated_at", F.col("meta.updatedAt"))
        .withColumn("meta_qr_code", F.col("meta.qrCode"))
        .drop("dimensions", "meta")
    )

    return df_flat


def convert_dates(df):
    return df.withColumn(
        "meta_created_at", F.to_timestamp(F.col("meta_created_at"))
    ).withColumn("meta_updated_at", F.to_timestamp(F.col("meta_updated_at")))


def normalize_products(df):
    return (
        df.withColumn("title", F.trim(F.col("title")))
        .withColumn("category", F.lower(F.trim(F.col("category"))))
        .withColumn("brand", F.trim(F.col("brand")))
        .withColumn("sku", F.trim(F.col("sku")))
    )

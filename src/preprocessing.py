from pyspark.sql.functions import col, current_timestamp
from delta.tables import DeltaTable

def run_preprocessing(raw_data_table, preprocessed_table, spark):

    if DeltaTable.isDeltaTable(spark, raw_data_table):
        df = spark.read.format("delta").load(raw_data_table)
    else:
        df = spark.table(raw_data_table)

    df = df.withColumn("preprocessing_timestamp", current_timestamp())

    df_count = df.count()
    print(f"✅ Preprocessed {df_count:,} records from {raw_data_table}")

    duplicates_removed = df_count - df.dropDuplicates().count()
    invalid_amount = df.filter(col("Amount") < 0).count()
    invalid_class = df.filter(~col("Class").isin([0, 1])).count()

    preprocessed_data = (
        df
        .filter(col("Amount") >= 0)
        .filter(col("Class").isin([0, 1]))
        .filter(col('Amount').isNotNull())
        .filter(col('Class').isNotNull())
        .dropDuplicates()
    )
    final_counts = preprocessed_data.count()
    total_removed = df_count - final_counts

    mode = "append" if DeltaTable.isDeltaTable(spark, preprocessed_table) else "overwrite"
    preprocessed_data.write.format("delta").mode(mode).save(preprocessed_table)

    print(f"✅ Saved {final_counts:,} preprocessed records to {preprocessed_table}")
    print(f"Data Quality Report:")
    print(f"   - Removed {duplicates_removed:,} duplicate records")
    print(f"   - Removed {invalid_amount:,} records with invalid Amount")
    print(f"   - Removed {invalid_class:,} records with invalid Class")
    print(f"   - Total records removed: {total_removed:,}")

    return preprocessed_data
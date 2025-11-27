from pyspark.sql.functions import current_timestamp
from delta.tables import DeltaTable

def run_ingestion(raw_data_path, raw_data_table, spark):
    
    if raw_data_path.endswith(".csv"):
        df = spark.read.option("header", "true").option("inferSchema", "true").csv(raw_data_path)
    else:
        df = spark.table(raw_data_table)

    df = df.withColumn("ingestion_timestamp", current_timestamp())
    
    mode = "append" if DeltaTable.isDeltaTable(spark, raw_data_table) else "overwrite"
    
    df.write.format("delta").mode(mode).save(raw_data_table)

    df_count = df.count()
    
    print(f"✅ Ingested {df_count:,} records into {raw_data_table}")
    
    return df
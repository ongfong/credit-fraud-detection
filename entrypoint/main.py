from src import ingestion, preprocessing, spark_config
import sys

def main():
    
    spark = None

    #path
    config_path = "./config/prototype_config.json"
    raw_data_path = "data/01-raw/creditcard_raw.csv"
    raw_data_table = "data/01-raw/creditcard_raw"
    preprocessed_table = "data/02-preprocessed/creditcard_preprocessed"
    feature_table = "data/03-feature/creditcard_feature"

    try:
        spark = spark_config.get_local_spark()
        print("Spark application is running...")

        # Ingestion
        ingestion.run_ingestion(raw_data_path, raw_data_table, spark)
        print("Data ingestion completed.")
        # Preprocessing
        preprocessing.run_preprocessing(raw_data_table, preprocessed_table, spark)
        # Feature Engineering
    except Exception as e:
        print(f"An error occurred: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if spark is not None:
            try:
                spark.stop()
                print("Spark session stopped.")
            except Exception as e:
                print(f"Error stopping Spark session: {e}")

if __name__ == "__main__":
    main()
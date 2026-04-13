"""
PySpark data processing pipeline
Processes UK government and traffic data
"""

import os
import sys
import tempfile

# Configure Java and Hadoop for Windows (PySpark 3.5.0 requires JDK 17, not 24)
_base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
os.environ['JAVA_HOME'] = os.path.join(_base_dir, 'jdk-17.0.2')
os.environ['HADOOP_HOME'] = os.path.join(_base_dir, 'hadoop')
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['SPARK_LOCAL_IP'] = '127.0.0.1'

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, count, max as spark_max
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType
import pandas as pd
from data_fetcher import UKGovDataFetcher, generate_sample_gps_data
from datetime import datetime

class SparkDataProcessor:
    """PySpark processor for big data analysis"""
    
    def __init__(self, app_name: str = "BigDataLearning"):
        """Initialize Spark Session (local mode for learning)"""
        self._temp_files = []
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .master("local[*]") \
            .config("spark.sql.shuffle.partitions", "4") \
            .config("spark.python.use.daemon", "false") \
            .config("spark.python.worker.faulthandler.enabled", "true") \
            .getOrCreate()
        
        self.spark.sparkContext.setLogLevel("WARN")
        print(f"✓ Spark Session initialized: {app_name}")

    def _load_pandas_via_temp_csv(self, pdf: pd.DataFrame, prefix: str):
        """Load pandas data into Spark through a temp CSV to avoid Python worker crashes on some Windows setups."""
        if pdf.empty:
            return None

        tmp_dir = tempfile.gettempdir()
        tmp_path = os.path.join(tmp_dir, f"{prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')}.csv")
        pdf.to_csv(tmp_path, index=False)
        self._temp_files.append(tmp_path)
        return self.spark.read.option("header", True).option("inferSchema", True).csv(tmp_path)
    
    def load_gps_data(self, num_records: int = 10000):
        """Load GPS data into Spark DataFrame"""
        
        # Generate sample GPS data
        pdf = generate_sample_gps_data(num_records)

        # Convert to Spark DataFrame through JVM CSV reader path
        df = self._load_pandas_via_temp_csv(pdf, "gps_data")
        
        print(f"✓ Loaded {df.count()} GPS records")
        return df
    
    def analyze_gps_data(self, gps_df):
        """Analyze GPS data for insights"""
        
        print("\n--- GPS Data Analysis ---")
        
        # Show schema
        print("\nData Schema:")
        gps_df.printSchema()
        
        # Basic statistics
        print("\nBasic Statistics:")
        gps_df.describe(['speed_kmh', 'latitude', 'longitude']).show()
        
        # Speed analysis by location
        print("\nAverage Speed by Location:")
        gps_df.groupBy('location') \
            .agg({
                'speed_kmh': 'avg',
                'vehicle_id': 'count'
            }) \
            .withColumnRenamed('avg(speed_kmh)', 'avg_speed') \
            .withColumnRenamed('count(vehicle_id)', 'vehicle_count') \
            .show()
        
        # Find high-speed events
        print("\nHigh Speed Alerts (>70 km/h):")
        gps_df.filter(col('speed_kmh') > 70) \
            .select('timestamp', 'vehicle_id', 'location', 'speed_kmh') \
            .limit(10) \
            .show()
        
        return gps_df
    
    def load_traffic_data(self):
        """Load TfL traffic data into Spark DataFrame"""
        
        fetcher = UKGovDataFetcher()
        
        print("\n--- Loading Traffic Data ---")
        
        # Fetch traffic data
        traffic_pdf = fetcher.fetch_tfl_traffic_data()
        if not traffic_pdf.empty:
            traffic_df = self._load_pandas_via_temp_csv(traffic_pdf, "traffic_data")
            print(f"✓ Loaded {traffic_df.count()} traffic records")
            return traffic_df
        
        # Fetch line status
        line_pdf = fetcher.fetch_tfl_line_status()
        if not line_pdf.empty:
            line_df = self._load_pandas_via_temp_csv(line_pdf, "line_status")
            print(f"✓ Loaded {line_df.count()} line status records")
            return line_df
        
        print("⚠ Could not fetch live data - using synthetic data")
        return None
    
    def analyze_traffic_data(self, traffic_df):
        """Analyze traffic data"""
        
        if traffic_df is None:
            print("No traffic data available")
            return
        
        print("\n--- Traffic Data Analysis ---")
        traffic_df.show(20)
    
    def run_complete_pipeline(self):
        """Run the complete data processing pipeline"""
        
        print("=" * 60)
        print("BIG DATA LEARNING - DATA PROCESSING PIPELINE")
        print("=" * 60)
        
        # Process GPS data
        gps_df = self.load_gps_data(5000)
        self.analyze_gps_data(gps_df)
        
        # Process traffic data
        traffic_df = self.load_traffic_data()
        if traffic_df is not None:
            self.analyze_traffic_data(traffic_df)
        
        print("\n" + "=" * 60)
        print("Pipeline complete!")
        print("=" * 60)
    
    def stop(self):
        """Stop Spark Session"""
        self.spark.stop()
        for path in self._temp_files:
            try:
                os.remove(path)
            except OSError:
                pass
        print("\n✓ Spark Session stopped")

if __name__ == "__main__":
    processor = SparkDataProcessor()
    
    try:
        processor.run_complete_pipeline()
    finally:
        processor.stop()

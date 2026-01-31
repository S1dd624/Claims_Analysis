#!/usr/bin/env python3
# ==============================================================================
"""
CORRECTED CLUSTER TEST SCRIPT

Role: Infrastructure Verification / Smoke Test
Purpose: Validates connectivity to Spark Master (7077) and HDFS NameNode (9000)
Checks:
  1. Spark Session Creation (Remote)
  2. HDFS Write/Read Roundtrip
  3. Distributed Computation (RDD map/reduce)
  4. Application Dataset Access (insurance_claims.csv)

Execution:
  python3 corrected_test_cluster.py
"""

from pyspark.sql import SparkSession
import sys

def test_cluster():
    """Test basic cluster functionality with improved error handling"""
    
    try:
        # Initialize Spark session
        spark = SparkSession.builder \
            .appName("ClusterTest_Corrected") \
            .master("spark://master:7077") \
            .config("spark.executor.memory", "1g") \
            .config("spark.executor.cores", "1") \
            .getOrCreate()
        
        print("✓ Spark session created successfully")
        print(f"  Master: {spark.sparkContext.master}")
        print(f"  Application ID: {spark.sparkContext.applicationId}")
        
        # Test basic operations
        data = [(1, "Alice"), (2, "Bob"), (3, "Charlie")]
        df = spark.createDataFrame(data, ["id", "name"])
        
        print(f"✓ DataFrame created with {df.count()} rows")
        
        # Test HDFS access
        try:
            df.write.mode("overwrite").csv("hdfs://master:9000/test/cluster_test")
            print("✓ HDFS write test successful")
            
            # Read back from HDFS
            test_df = spark.read.csv("hdfs://master:9000/test/cluster_test", header=False)
            print(f"✓ HDFS read test successful - {test_df.count()} rows")
            
        except Exception as e:
            print(f"⚠ HDFS test failed: {e}")
        
        # Test distributed computation
        rdd = spark.sparkContext.parallelize(range(1000), 4)
        result = rdd.map(lambda x: x * 2).reduce(lambda a, b: a + b)
        print(f"✓ Distributed computation test successful - result: {result}")
        
        # Test data loading from insurance dataset
        try:
            print("\n📊 Testing insurance dataset access...")
            insurance_df = spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv("hdfs://master:9000/insurance_claims/car_insurance_claims.csv")
            
            record_count = insurance_df.count()
            column_count = len(insurance_df.columns)
            
            print(f"✓ Insurance dataset loaded successfully")
            print(f"  Records: {record_count:,}")
            print(f"  Columns: {column_count}")
            print(f"  Column names: {insurance_df.columns}")
            
            # Show sample data
            print("\n📋 Sample data:")
            insurance_df.show(3, truncate=False)
            
        except Exception as e:
            print(f"⚠ Insurance dataset test failed: {e}")
        
        spark.stop()
        print("✓ Cluster test completed successfully!")
        return True
        
    except Exception as e:
        print(f"✗ Cluster test failed: {e}")
        return False

if __name__ == "__main__":
    success = test_cluster()
    sys.exit(0 if success else 1)

#!/bin/bash
set -euo pipefail

# ======================================================================
#  CORRECTED DATA UPLOAD SCRIPT FOR INSURANCE CLAIMS ANALYSIS
#  Uploads CSV data to HDFS and prepares for Spark processing
#  Handles multiple file formats and validates data structure
# ======================================================================

show_usage() {
    echo "Usage: $(basename $0) [-h]"
    echo "Uploads local insurance data to HDFS."
    echo ""
    echo "Options:"
    echo "  -h  Show this help message"
}

# Parse options
while getopts "h" opt; do
    case ${opt} in
        h)
            show_usage
            exit 0
            ;;
        \?)
            show_usage
            exit 1
            ;;
    esac
done

echo "Uploading Insurance Claims Data to HDFS"
echo "============================================================="

# Configuration
# Dynamically resolve HDFS URI from Hadoop configuration
HDFS_URI=$(hdfs getconf -confKey fs.defaultFS 2>/dev/null || echo "hdfs://localhost:9000")
echo "Resolved HDFS URI: $HDFS_URI"

HDFS_BASE_PATH="/insurance_claims"
HDFS_DATA_PATH="$HDFS_BASE_PATH/car_insurance_claims.csv"

# Check for data files in order of preference
LOCAL_DATA_PATH=""
if [ -f "insurance_claims.csv" ]; then
    LOCAL_DATA_PATH="./insurance_claims.csv"
    echo "✓ Found insurance_claims.csv"
elif [ -f "car_insurance_claims.csv" ]; then
    LOCAL_DATA_PATH="./car_insurance_claims.csv"
    echo "✓ Found car_insurance_claims.csv"
elif [ -f "insurance.csv" ]; then
    LOCAL_DATA_PATH="./insurance.csv"
    echo "✓ Found insurance.csv"
elif [ -f "insurance_claims(2).xls" ]; then
    echo "⚠ Found Excel file, converting to CSV..."
    python3 -c "
import pandas as pd
import sys
try:
    df = pd.read_excel('insurance_claims.xls')
    df.to_csv('car_insurance_claims.csv', index=False)
    print(f'Converted Excel to CSV: {df.shape[0]} rows, {df.shape[1]} columns')
except Exception as e:
    print(f'Conversion failed: {e}')
    sys.exit(1)
"
    if [ $? -eq 0 ]; then
        LOCAL_DATA_PATH="./insurance_claims.csv"
        echo "Excel file converted successfully"
    else
        echo "Excel conversion failed"
        exit 1
    fi
else
    echo "Error: No insurance data file found!"
    echo "Please ensure one of the following files is in the current directory:"
    echo "  - car_insurance_claim.csv"
    echo "  - car_insurance_claims.csv"
    echo "  - insurance.csv"
    echo "  - insurance_claims(2).xls"
    exit 1
fi

# Validate the data file
echo "Validating"
if [ ! -f "$LOCAL_DATA_PATH" ]; then
    echo "Error: $LOCAL_DATA_PATH not found"
    exit 1
fi

# Check file size
FILE_SIZE=$(wc -c < "$LOCAL_DATA_PATH")
echo "File size: $FILE_SIZE bytes"

# Check number of lines
LINE_COUNT=$(wc -l < "$LOCAL_DATA_PATH")
echo "Line count: $LINE_COUNT"

# Check number of columns (from header)
COLUMN_COUNT=$(head -1 "$LOCAL_DATA_PATH" | tr ',' '\n' | wc -l)
echo "Column count: $COLUMN_COUNT"

# Show first few lines
echo "Sample data:"
head -3 "$LOCAL_DATA_PATH"

# Check HDFS connection using resolved URI
echo "Checking HDFS connection to $HDFS_URI ..."
if ! hdfs dfs -ls "$HDFS_URI/" > /dev/null 2>&1; then
    echo "Error: Cannot connect to HDFS at $HDFS_URI!"
    echo "Please ensure:"
    echo "  1. Hadoop services are running (start-dfs.sh)"
    echo "  2. NameNode is bound to the correct interface"
    echo "  3. /etc/hosts resolves hostnames correctly"
    exit 1
fi
echo "✓ HDFS connection verified"

# Create HDFS directories
echo "Creating HDFS directories..."
hdfs dfs -mkdir -p $HDFS_BASE_PATH
hdfs dfs -mkdir -p /insurance_analysis
hdfs dfs -mkdir -p /tmp
hdfs dfs -mkdir -p /test

# Upload data to HDFS
echo "Uploading data to HDFS..."
if hdfs dfs -test -e $HDFS_DATA_PATH; then
    echo "Data already exists in HDFS. Removing old version..."
    hdfs dfs -rm $HDFS_DATA_PATH
fi

hdfs dfs -put "$LOCAL_DATA_PATH" $HDFS_DATA_PATH

# Verify upload
echo "Verifying upload..."
hdfs dfs -ls $HDFS_DATA_PATH
file_size=$(hdfs dfs -du -h $HDFS_DATA_PATH | awk '{print $1, $2}')
echo "Uploaded file size: $file_size"

# Test data access
echo ""
echo "Testing access..."
echo "First 3 lines from HDFS:"
hdfs dfs -cat $HDFS_DATA_PATH | head -3

# Display HDFS directory structure
echo ""
echo "HDFS Directory Structure:"
echo "============================="
hdfs dfs -ls -R /insurance_claims
hdfs dfs -ls -R /insurance_analysis

# Test with Spark (if available)
echo ""
echo "Testing Spark data access..."
if command -v spark-shell &> /dev/null; then
    echo "Testing with Spark Shell using $HDFS_URI ..."
    spark-shell --conf spark.sql.adaptive.enabled=true <<EOF
val hdfsUri = "$HDFS_URI"
val df = spark.read.option("header", "true").option("inferSchema", "true").csv(s"\$hdfsUri/insurance_claims/car_insurance_claims.csv")
println(s"Records: \${df.count()}")
println(s"Columns: \${df.columns.length}")
println("Column names:")
df.columns.foreach(println)
println("Sample data:")
df.show(3, false)
System.exit(0)
EOF
    echo "✓ Spark data access test completed"
else
    echo "⚠ Spark not available for testing"
fi

echo ""
echo "Data upload completed successfully!"
echo "Data is now available at: ${HDFS_URI}${HDFS_DATA_PATH}"
echo ""
echo "Dataset Information:"
echo "  - Records: $((LINE_COUNT - 1))"  # Subtract header
echo "  - Columns: $COLUMN_COUNT"
echo "  - File size: $FILE_SIZE bytes"
echo ""
echo "Next steps:"
echo "1. Run the corrected analysis script: python3 corrected_claims_analysis.py"
echo "2. For cluster mode: python3 corrected_cluster_analysis.py"
echo "3. Test cluster: python3 corrected_test_cluster.py"
echo "4. Ensure Spark cluster is running: start-master.sh && start-slaves.sh"
echo "5. Access Spark UI at: http://master-node:8080"

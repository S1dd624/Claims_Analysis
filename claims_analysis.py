#!/usr/bin/env python3
"""
ENHANCED INSURANCE CLAIMS BIG DATA ANALYSIS (CLIENT MODE)

Role: Single-Node Analysis / Development & Debugging
Architecture: Client Mode (Driver runs locally, interacts with Spark)
Target: Can run against local files OR HDFS (autodetects)

Execution:
  python3 corrected_claims_analysis.py

Dependencies:
  - PySpark (Local Master)
  - Matplotlib (TkAgg backend for Linux display)
  - HDFS accessible at hdfs://localhost:9000 (optional but preferred)
"""

import os
import sys
from datetime import datetime
import matplotlib
matplotlib.use('TkAgg')  # Interactive backend for inline display
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np

# Set style for professional visualizations
sns.set_style("whitegrid")
plt.rcParams['figure.figsize'] = (14, 8)
plt.rcParams['font.size'] = 11

class InsuranceClaimsAnalyzer:
    """Main analyzer class for insurance claims data"""
    
    def __init__(self):
        self.spark = None
        self.df_raw = None
        self.df_cleaned = None
        self.data_path = None
        self.start_time = datetime.now()
        
    def initialize_spark(self):
        """Initialize Spark session with optimized configuration"""
        print("=" * 80)
        print("INSURANCE CLAIMS ANALYSIS")
        print("=" * 80)
        print(f"Start Time: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        
        try:
            from pyspark.sql import SparkSession
            from pyspark import SparkConf
            
            conf = SparkConf()
            conf.set("spark.driver.memory", "2g")
            conf.set("spark.executor.memory", "2g")
            conf.set("spark.sql.adaptive.enabled", "true")
            conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
            
            self.spark = SparkSession.builder \
                .appName("EnhancedInsuranceAnalysis") \
                .master("local[*]") \
                .config(conf=conf) \
                .getOrCreate()
            
            self.spark.sparkContext.setLogLevel("ERROR")
            
            print(f"Spark Session initialized")
            print(f"  Version: {self.spark.version}")
            print(f"  Cores: {self.spark.sparkContext.defaultParallelism}")
            
            return True
            
        except Exception as e:
            print(f"Spark initialization failed: {e}")
            return False
    
    def locate_dataset(self):
        """Locate insurance dataset in HDFS or local filesystem"""
        print("\n" + "=" * 80)
        print("STEP 1: LOCATING DATASET")
        print("=" * 80)
        
        possible_paths = [
    "hdfs://acer:9000/insurance_claims/car_insurance_claims.csv",
    "./insurance_claims.csv",
    "./car_insurance_claims.csv",
]

        
        for path in possible_paths:
            try:
                test_df = self.spark.read \
                    .option("header", "true") \
                    .csv(path).limit(1)
                
                if test_df.count() > 0:
                    self.data_path = path
                    print(f"Found dataset: {path}\n")
                    return True
            except:
                continue
        
        print("Dataset not found in any expected location")
        return False
    
    def load_data(self):
        """Load and validate dataset"""
        print("=" * 80)
        print("STEP 2: LOADING DATA")
        print("=" * 80)
        
        try:
            self.df_raw = self.spark.read \
                .option("header", "true") \
                .option("inferSchema", "true") \
                .csv(self.data_path)
            
            count = self.df_raw.count()
            cols = len(self.df_raw.columns)
            
            print(f"Loaded {count:,} records with {cols} columns")
            print(f"  Columns: {', '.join(self.df_raw.columns[:8])}...\n")
            
            return count > 0
            
        except Exception as e:
            print(f"Loading failed: {e}")
            return False
    
    def explore_data(self):
        """Explore dataset structure and quality"""
        print("=" * 80)
        print("STEP 3: DATA EXPLORATION")
        print("=" * 80)
        
        from pyspark.sql.functions import col, count, when
        
        # Schema overview
        print("\nSchema Overview:")
        schema_info = []
        for field in self.df_raw.schema.fields[:15]:
            schema_info.append(f"  {field.name}: {field.dataType}")
        print("\n".join(schema_info))
        
        # Key demographics analysis
        print("\n" + "=" * 80)
        print("ANALYZING CUSTOMER DEMOGRAPHICS")
        print("=" * 80)
        
        # Age distribution
        if 'age' in [c.lower() for c in self.df_raw.columns]:
            age_col = [c for c in self.df_raw.columns if c.lower() == 'age'][0]
            age_data = self.df_raw.select(age_col).toPandas()
            
            print(f"\nAge Statistics:")
            print(f"  Mean Age: {age_data[age_col].mean():.1f} years")
            print(f"  Median Age: {age_data[age_col].median():.1f} years")
            print(f"  Age Range: {age_data[age_col].min():.0f} - {age_data[age_col].max():.0f} years")
            
            # Age distribution histogram
            fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
            
            ax1.hist(age_data[age_col].dropna(), bins=30, color='steelblue', edgecolor='black')
            ax1.set_xlabel('Age')
            ax1.set_ylabel('Number of Customers')
            ax1.set_title('Customer Age Distribution')
            ax1.grid(True, alpha=0.3)
            
            # Age groups pie chart
            age_groups = pd.cut(age_data[age_col], bins=[0, 25, 35, 45, 55, 100], 
                               labels=['<25', '25-35', '35-45', '45-55', '55+'])
            age_group_counts = age_groups.value_counts()
            
            colors = ['#ff9999', '#66b3ff', '#99ff99', '#ffcc99', '#ff99cc']
            ax2.pie(age_group_counts.values, labels=age_group_counts.index, autopct='%1.1f%%',
                   colors=colors, startangle=90)
            ax2.set_title('Customer Age Groups')
            
            plt.tight_layout()
            plt.show(block=False)
            plt.pause(2)
        
        # Gender distribution
        gender_cols = [c for c in self.df_raw.columns if 'gender' in c.lower() or 'sex' in c.lower()]
        if gender_cols:
            gender_col = gender_cols[0]
            gender_dist = self.df_raw.groupBy(gender_col).count().toPandas()
            
            print(f"\n{gender_col} Distribution:")
            for _, row in gender_dist.iterrows():
                print(f"  {row[gender_col]}: {row['count']:,} ({row['count']/self.df_raw.count()*100:.1f}%)")
            
            # Gender pie chart
            plt.figure(figsize=(8, 8))
            colors = ['#3498db', '#e74c3c', '#95a5a6']
            plt.pie(gender_dist['count'], labels=gender_dist[gender_col], 
                   autopct='%1.1f%%', colors=colors[:len(gender_dist)], startangle=90)
            plt.title(f'{gender_col} Distribution in Insurance Claims')
            plt.show(block=False)
            plt.pause(2)
        
    def clean_data(self):
        """Clean and prepare data for analysis"""
        print("\n" + "=" * 80)
        print("STEP 4: DATA CLEANING")
        print("=" * 80)
        
        from pyspark.sql.functions import col
        
        # Identify critical columns
        critical_cols = [c for c in self.df_raw.columns if any(
            keyword in c.lower() for keyword in 
            ['fraud', 'claim', 'amount', 'state', 'type']
        )]
        
        initial = self.df_raw.count()
        self.df_cleaned = self.df_raw
        
        # Remove rows with nulls in critical columns
        for col_name in critical_cols[:5]:
            try:
                self.df_cleaned = self.df_cleaned.filter(col(col_name).isNotNull())
            except:
                continue
        
        final = self.df_cleaned.count()
        removed = initial - final
        
        print(f"Cleaned dataset")
        print(f"  Before: {initial:,} records")
        print(f"  After: {final:,} records")
        print(f"  Removed: {removed:,} ({removed/initial*100:.1f}%)\n")
        
        self.df_cleaned.createOrReplaceTempView("insurance_data")
    
    def statistical_analysis(self):
        """Perform comprehensive statistical analysis"""
        print("=" * 80)
        print("STEP 5: STATISTICAL ANALYSIS")
        print("=" * 80)
        
        from pyspark.sql.functions import avg, min, max, stddev, count, col
        
        # Numeric analysis
        print("\nNumeric Column Statistics:")
        numeric_cols = [f.name for f in self.df_cleaned.schema.fields 
                       if str(f.dataType) in ['IntegerType()', 'DoubleType()', 'LongType()']][:8]
        
        if numeric_cols:
            # Statistical summary with scatter plot
            stats_data = []
            for c in numeric_cols:
                stats = self.df_cleaned.select(
                    avg(c).alias('mean'),
                    stddev(c).alias('std'),
                    min(c).alias('min'),
                    max(c).alias('max')
                ).collect()[0]
                
                if stats['mean'] is not None:
                    stats_data.append({
                        'Column': c,
                        'Mean': stats['mean'],
                        'Std': stats['std'],
                        'Min': stats['min'],
                        'Max': stats['max']
                    })
                    
                    print(f"\n  {c}:")
                    print(f"    Mean: {stats['mean']:.2f}")
                    print(f"    Std:  {stats['std']:.2f}" if stats['std'] else "    Std:  N/A")
                    print(f"    Range: [{stats['min']:.2f}, {stats['max']:.2f}]")
            
            # Create scatter plot matrix for key numeric variables
            if len(numeric_cols) >= 2:
                print(f"\nGenerating correlation scatter plots...")
                
                sample_data = self.df_cleaned.select(numeric_cols[:4]).sample(False, 0.1).toPandas()
                
                fig, axes = plt.subplots(2, 2, figsize=(14, 12))
                fig.suptitle('Insurance Claims: Numeric Variables Scatter Analysis', fontsize=16)
                
                for idx, col_pair in enumerate([(0,1), (0,2), (1,2), (2,3)]):
                    if idx >= 4 or len(numeric_cols) <= max(col_pair):
                        break
                    row, col_idx = idx // 2, idx % 2
                    x_col = numeric_cols[col_pair[0]]
                    y_col = numeric_cols[col_pair[1]] if col_pair[1] < len(numeric_cols) else numeric_cols[0]
                    
                    axes[row, col_idx].scatter(sample_data[x_col], sample_data[y_col], 
                                               alpha=0.5, c='coral', edgecolors='black')
                    axes[row, col_idx].set_xlabel(x_col)
                    axes[row, col_idx].set_ylabel(y_col)
                    axes[row, col_idx].grid(True, alpha=0.3)
                
                plt.tight_layout()
                plt.show(block=False)
                plt.pause(3)
        
        # Categorical analysis with visualizations
        print("\n\nCategorical Distributions:")
        categorical_cols = [f.name for f in self.df_cleaned.schema.fields 
                           if str(f.dataType) == 'StringType()'][:4]
        
        for cat_col in categorical_cols:
            print(f"\n  {cat_col} Distribution:")
            dist_df = self.df_cleaned.groupBy(cat_col).count() \
                .orderBy(col("count").desc()).limit(10)
            
            dist_pd = dist_df.toPandas()
            print(dist_pd.to_string(index=False))
            
            # Bar chart for categorical distribution
            plt.figure(figsize=(12, 6))
            plt.barh(dist_pd[cat_col], dist_pd['count'], color='skyblue', edgecolor='navy')
            plt.xlabel('Count')
            plt.ylabel(cat_col)
            plt.title(f'{cat_col} Distribution (Top 10 Categories)')
            plt.grid(True, alpha=0.3, axis='x')
            plt.tight_layout()
            plt.show(block=False)
            plt.pause(2)
        
        # Advanced aggregations
        self._advanced_aggregations()
        
        # Fraud analysis if fraud column exists
        self._fraud_analysis()
    
    def _fraud_analysis(self):
        """Analyze fraud patterns - Only show fraud distribution pie chart"""
        fraud_cols = [c for c in self.df_cleaned.columns if 'fraud' in c.lower()]
        
        if fraud_cols:
            fraud_col = fraud_cols[0]
            print("\n" + "=" * 80)
            print(f"FRAUD ANALYSIS - {fraud_col}")
            print("=" * 80)
            
            fraud_dist = self.df_cleaned.groupBy(fraud_col).count().toPandas()
            total = fraud_dist['count'].sum()
            
            print(f"\nFraud Distribution:")
            for _, row in fraud_dist.iterrows():
                pct = (row['count'] / total) * 100
                print(f"  {row[fraud_col]}: {row['count']:,} ({pct:.1f}%)")
            
            # Fraud pie chart - ONLY THIS FIGURE WILL APPEAR
            plt.figure(figsize=(10, 8))
            colors = ['#2ecc71', '#e74c3c', '#95a5a6']
            explode = [0.1 if 'Y' in str(val) or 'yes' in str(val).lower() else 0 
                      for val in fraud_dist[fraud_col]]
            
            plt.pie(fraud_dist['count'], labels=fraud_dist[fraud_col], 
                   autopct='%1.1f%%', colors=colors[:len(fraud_dist)], 
                   explode=explode, startangle=90, shadow=True)
            plt.title(f'Fraud Reported Distribution\n(Total Claims: {total:,})', fontsize=14)
            plt.show(block=False)
            plt.pause(3)
            
            # REMOVED: Car type fraud analysis plot to reduce figure count
    
    def _advanced_aggregations(self):
        """Perform advanced SQL aggregations"""
        print("\n\nAdvanced Insights:")
        
        # Find amount and group columns
        amount_cols = [c for c in self.df_cleaned.columns 
                      if 'amount' in c.lower() or 'claim' in c.lower()]
        group_cols = [c for c in self.df_cleaned.columns 
                     if 'state' in c.lower() or 'type' in c.lower()]
        
      # if amount_cols and group_cols:
         #   query = f"""
          #  SELECT 
           #     {group_cols[0]} as category,
          #      COUNT(*) as count,
          #      ROUND(AVG({amount_cols[0]}), 2) as avg_amount,
           #     ROUND(SUM({amount_cols[0]}), 2) as total_amount
          #  FROM insurance_data
          #  WHERE {group_cols[0]} IS NOT NULL
            #GROUP BY {group_cols[0]}
           # ORDER BY count DESC
            
           
            
           # result = self.spark.sql(query).toPandas()
           # print(f"\n  Top 10 {group_cols[0]} by Claims Count:")
           # print(result.to_string(index=False))
            
            # Dual axis plot
           # fig, ax1 = plt.subplots(figsize=(14, 6))
            
          #  x_pos = np.arange(len(result['category']))
          #  ax1.bar(x_pos, result['count'], color='teal', alpha=0.7, label='Count')
            #ax1.set_xlabel(group_cols[0])
            #ax1.set_ylabel('Number of Claims', color='teal')
           # ax1.tick_params(axis='y', labelcolor='teal')
           # ax1.set_xticks(x_pos)
           # ax1.set_xticklabels(result['category'], rotation=45, ha='right')
            
          #  if 'avg_amount' in result.columns and result['avg_amount'].notna().any():
              #  ax2 = ax1.twinx()
             #   ax2.plot(x_pos, result['avg_amount'], color='orange', marker='o', 
              #          linewidth=2, markersize=8, label='Avg Amount')
             #   ax2.set_ylabel('Average Claim Amount ($)', color='orange')
              #  ax2.tick_params(axis='y', labelcolor='orange')
            
           # plt.title(f'Claims Analysis by {group_cols[0]}')
           # plt.tight_layout()
          #  plt.show(block=False)
           # plt.pause(3)
    
    
    def save_results(self):
        """Save analysis results"""
        print("\n" + "=" * 80)
        print("STEP 6: SAVING RESULTS")
        print("=" * 80)
        
        try:
            # Save to HDFS
            self.df_cleaned.describe().write \
                .mode("overwrite") \
                .option("header", "true") \
                .csv("hdfs://localhost:9000/insurance_analysis/results/summary_stats")
            
            print("Results saved to HDFS: /insurance_analysis/results/")
        except:
            print("HDFS save skipped (using local output)")
        
        print("All visualizations displayed inline during analysis")
    
    def generate_summary(self):
        """Generate final summary report"""
        print("\n" + "=" * 80)
        print("ANALYSIS COMPLETE - SUMMARY REPORT")
        print("=" * 80)
        
        duration = (datetime.now() - self.start_time).total_seconds()
        
        print(f"\nDataset: {self.data_path}")
        print(f"Columns Analyzed: {len(self.df_cleaned.columns)}")
        print(f"Execution Time: {duration:.2f} seconds")
        
  
        
        print(f"\nEnd Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 80)
        print("\nANALYSIS COMPLETE")
        print("=" * 80)
        
        print("\nCharts displayed: Close all matplotlib windows to continue...")
        plt.show()  # Keep all plots open until user closes
    
    def run(self):
        """Execute complete analysis pipeline"""
        try:
            if not self.initialize_spark():
                return False
            
            if not self.locate_dataset():
                return False
            
            if not self.load_data():
                return False
            
            self.explore_data()
            self.clean_data()
            self.statistical_analysis()
            self.save_results()
            self.generate_summary()
            
            return True
            
        except Exception as e:
            print(f"\nFatal error: {e}")
            import traceback
            traceback.print_exc()
            return False
        
        finally:
            if self.spark:
                self.spark.stop()
                print("\nSpark session stopped cleanly")

def main():
    """Main entry point"""
    analyzer = InsuranceClaimsAnalyzer()
    success = analyzer.run()
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nAnalysis interrupted by user")
        sys.exit(1)
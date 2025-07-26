from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, count, col, when
import os
import pandas as pd

def main():
    # Initialize Spark session with error handling
    spark = SparkSession.builder \
        .appName("Banking Transactions Analysis") \
        .config("spark.master", "local[*]") \
        .getOrCreate()
    
    try:
        # Define paths
        script_dir = os.path.dirname(os.path.abspath(__file__))
        input_path = os.path.join(script_dir, "transactions.csv")
        
        print(f"Looking for input file at: {input_path}")
        
        # Check if file exists
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"transactions.csv not found at {input_path}")
        
        # Step 1: Read data
        print("Reading transaction data...")
        df = spark.read.csv(input_path, header=True, inferSchema=True)
        
        # Validate data exists
        record_count = df.count()
        if record_count == 0:
            raise ValueError("No data found in the input file")
        
        print(f"Total records loaded: {record_count}")
        df.cache()
        
        # Step 2: Average transaction per user
        print("Calculating average transaction per user...")
        avg_txn = df.filter(col("transaction_amount").isNotNull() & 
                           col("user_id").isNotNull()) \
                   .groupBy("user_id") \
                   .agg(avg("transaction_amount").alias("avg_transaction"))
        
        # Step 3: Failure rate per merchant
        print("Calculating failure rate per merchant...")
        total = df.filter(col("merchant_id").isNotNull()) \
                 .groupBy("merchant_id") \
                 .agg(count("*").alias("total_txns"))
        
        failed = df.filter((col("status") == "FAILED") & 
                          col("merchant_id").isNotNull()) \
                  .groupBy("merchant_id") \
                  .agg(count("*").alias("failed_txns"))
        
        failure_rate = total.join(failed, "merchant_id", "left") \
                           .fillna(0, subset=["failed_txns"]) \
                           .withColumn("failure_rate", 
                                     when(col("total_txns") > 0, 
                                          col("failed_txns") / col("total_txns"))
                                     .otherwise(0.0))
        
        # Step 4: Top 5 merchants by number of transactions
        print("Finding top 5 merchants by transaction count...")
        top_merchants = df.filter(col("merchant_id").isNotNull()) \
                         .groupBy("merchant_id") \
                         .agg(count("*").alias("txn_count")) \
                         .orderBy(col("txn_count").desc()) \
                         .limit(5)
        
        # Display results
        print("\n" + "="*60)
        print("BANKING TRANSACTIONS ANALYSIS RESULTS")
        print("="*60)
        
        print("\n📊 TOP 5 MERCHANTS BY TRANSACTION COUNT:")
        print("-" * 50)
        top_merchants.show(truncate=False)
        
        print("\n💰 TOP 10 USERS BY AVERAGE TRANSACTION AMOUNT:")
        print("-" * 50)
        avg_txn.orderBy(col("avg_transaction").desc()).limit(10).show(truncate=False)
        
        print("\n⚠️  TOP 10 MERCHANTS BY FAILURE RATE:")
        print("-" * 50)
        failure_rate.orderBy(col("failure_rate").desc()).limit(10).show(truncate=False)
        
        # Save results using pandas (bypasses Spark file writing issues)
        print("\n💾 Saving results to CSV files...")
        try:
            # Create results directory
            results_dir = os.path.join(script_dir, "analysis_results")
            os.makedirs(results_dir, exist_ok=True)
            
            # Convert Spark DataFrames to pandas and save as CSV
            print("Converting and saving average transactions...")
            avg_txn_pd = avg_txn.toPandas()
            avg_txn_pd.to_csv(os.path.join(results_dir, "avg_transaction_per_user.csv"), index=False)
            
            print("Converting and saving failure rates...")
            failure_rate_pd = failure_rate.toPandas()
            failure_rate_pd.to_csv(os.path.join(results_dir, "failure_rate_per_merchant.csv"), index=False)
            
            print("Converting and saving top merchants...")
            top_merchants_pd = top_merchants.toPandas()
            top_merchants_pd.to_csv(os.path.join(results_dir, "top_5_merchants.csv"), index=False)
            
            print(f"\n✅ SUCCESS! All results saved to: {results_dir}")
            print("\nGenerated files:")
            print("📁 avg_transaction_per_user.csv")
            print("📁 failure_rate_per_merchant.csv") 
            print("📁 top_5_merchants.csv")
            
            # Show summary statistics
            print(f"\n📈 SUMMARY STATISTICS:")
            print(f"Total users analyzed: {avg_txn.count()}")
            print(f"Total merchants analyzed: {failure_rate.count()}")
            print(f"Highest failure rate: {failure_rate.agg({'failure_rate': 'max'}).collect()[0][0]:.2%}")
            print(f"Average failure rate: {failure_rate.agg({'failure_rate': 'avg'}).collect()[0][0]:.2%}")
            
        except Exception as save_error:
            print(f"❌ Could not save CSV files: {save_error}")
            print("But the analysis completed successfully - results are displayed above.")
        
    except Exception as e:
        print(f"❌ Error occurred during analysis: {str(e)}")
        raise
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
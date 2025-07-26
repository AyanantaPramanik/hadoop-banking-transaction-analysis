import csv
import random
import os
from datetime import datetime, timedelta

def generate_sample_transactions(num_records=10000):
    """Generate sample transaction data for testing"""
    
    # Get script directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    output_file = os.path.join(script_dir, "transactions.csv")
    
    print(f"Generating {num_records} sample transactions...")
    
    # Sample data
    statuses = ["SUCCESS", "FAILED", "PENDING"]
    merchant_ids = [f"MERCHANT_{i:03d}" for i in range(1, 51)]  # 50 merchants
    
    with open(output_file, 'w', newline='', encoding='utf-8') as file:
        writer = csv.writer(file)
        
        # Write header
        writer.writerow(["user_id", "merchant_id", "transaction_amount", "status", "transaction_date"])
        
        # Generate sample data
        for i in range(num_records):
            user_id = f"USER_{random.randint(1, 1000):04d}"
            merchant_id = random.choice(merchant_ids)
            transaction_amount = round(random.uniform(10.0, 1000.0), 2)
            status = random.choices(statuses, weights=[80, 15, 5])[0]  # 80% success, 15% failed, 5% pending
            
            # Random date in last 30 days
            base_date = datetime.now() - timedelta(days=30)
            transaction_date = base_date + timedelta(days=random.randint(0, 30))
            transaction_date_str = transaction_date.strftime("%Y-%m-%d")
            
            writer.writerow([user_id, merchant_id, transaction_amount, status, transaction_date_str])
    
    print(f"Sample data generated successfully at: {output_file}")
    print("File structure:")
    print("- user_id: USER_0001 to USER_1000")
    print("- merchant_id: MERCHANT_001 to MERCHANT_050") 
    print("- transaction_amount: Random amounts between $10-$1000")
    print("- status: SUCCESS (80%), FAILED (15%), PENDING (5%)")
    print("- transaction_date: Random dates in last 30 days")

if __name__ == "__main__":
    generate_sample_transactions()
import uuid
import random
import csv
import json
import os
import sys
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()

# Step 1: Setup Constants
TRANSACTION_TYPES = ['POS', 'ATM', 'Online', 'UPI', 'NEFT', 'RTGS']
STATUSES = ['SUCCESS', 'FAILED', 'DECLINED']
CITIES = ['Mumbai', 'Delhi', 'Kolkata', 'Bangalore', 'Hyderabad', 'Chennai']

# Generate a pool of unique customer IDs to avoid duplicates
CUSTOMER_POOL_SIZE = 10000
CUSTOMER_IDS = [f"C{10000 + i:05d}" for i in range(CUSTOMER_POOL_SIZE)]

class TransactionGenerator:
    def __init__(self):
        self.used_transaction_ids = set()
    
    def generate_unique_transaction_id(self):
        """Generate a unique transaction ID to avoid duplicates"""
        while True:
            transaction_id = str(uuid.uuid4())
            if transaction_id not in self.used_transaction_ids:
                self.used_transaction_ids.add(transaction_id)
                return transaction_id
    
    def generate_transaction(self):
        """Generate a single realistic transaction"""
        # Generate realistic merchant/beneficiary names
        merchant_name = fake.company() if random.choice(['merchant', 'person']) == 'merchant' else fake.name()
        
        transaction = {
            "transaction_id": self.generate_unique_transaction_id(),
            "customer_id": random.choice(CUSTOMER_IDS),
            "customer_name": fake.name(),
            "timestamp": (datetime.now() - timedelta(minutes=random.randint(0, 525600))).isoformat(),
            "transaction_type": random.choice(TRANSACTION_TYPES),
            "amount": round(random.uniform(50.0, 100000.0), 2),
            "location": random.choice(CITIES),
            "merchant_name": merchant_name,
            "account_number": f"ACC{random.randint(100000000, 999999999)}",
            "status": random.choices(STATUSES, weights=[0.85, 0.10, 0.05])[0],
            "currency": "INR",
            "description": fake.sentence(nb_words=4)
        }
        return transaction
    
    def generate_transactions(self, n):
        """Generate n transactions with progress indicator"""
        transactions = []
        print(f"Generating {n} transactions...")
        
        for i in range(n):
            transactions.append(self.generate_transaction())
            
            # Progress indicator for large datasets
            if n > 1000 and (i + 1) % (n // 10) == 0:
                progress = ((i + 1) / n) * 100
                print(f"Progress: {progress:.0f}% ({i + 1}/{n} transactions)")
        
        return transactions

def validate_input():
    """Validate user input for number of transactions"""
    while True:
        try:
            user_input = input("How many transactions to generate? (e.g. 1000): ").strip()
            
            if not user_input:
                print("❌ Please enter a number.")
                continue
                
            n = int(user_input)
            
            if n <= 0:
                print("❌ Please enter a positive number.")
                continue
            elif n > 1000000:
                confirm = input(f"⚠️  Generating {n} transactions may take time. Continue? (y/n): ").strip().lower()
                if confirm not in ['y', 'yes']:
                    continue
            
            return n
            
        except ValueError:
            print("❌ Invalid input. Please enter a valid number.")
        except KeyboardInterrupt:
            print("\n❌ Operation cancelled by user.")
            sys.exit(0)

def ensure_directory_exists(filepath):
    """Create directory if it doesn't exist"""
    directory = os.path.dirname(filepath)
    if directory and not os.path.exists(directory):
        try:
            os.makedirs(directory, exist_ok=True)
            print(f"✅ Created directory: {directory}")
        except OSError as e:
            raise OSError(f"Failed to create directory {directory}: {e}")

def save_to_csv(data, filename):
    """Save transaction data to CSV with error handling"""
    try:
        ensure_directory_exists(filename)
        
        with open(filename, mode='w', newline='', encoding='utf-8') as f:
            if not data:
                raise ValueError("No data to save")
            
            writer = csv.DictWriter(f, fieldnames=data[0].keys())
            writer.writeheader()
            writer.writerows(data)
        
        print(f"✅ Successfully saved CSV to: {filename}")
        
    except (IOError, OSError) as e:
        print(f"❌ Error saving CSV file: {e}")
        raise
    except Exception as e:
        print(f"❌ Unexpected error while saving CSV: {e}")
        raise

def save_to_json(data, filename):
    """Save transaction data to JSON with error handling"""
    try:
        ensure_directory_exists(filename)
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
        
        print(f"✅ Successfully saved JSON to: {filename}")
        
    except (IOError, OSError) as e:
        print(f"❌ Error saving JSON file: {e}")
        raise
    except Exception as e:
        print(f"❌ Unexpected error while saving JSON: {e}")
        raise

def get_file_size(filepath):
    """Get human-readable file size"""
    try:
        size_bytes = os.path.getsize(filepath)
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024:
                return f"{size_bytes:.1f} {unit}"
            size_bytes /= 1024
        return f"{size_bytes:.1f} TB"
    except OSError:
        return "Unknown"

def main():
    """Main driver function with comprehensive error handling"""
    try:
        print("🏦 Financial Transaction Data Generator")
        print("=" * 40)
        
        # Get and validate user input
        n = validate_input()
        
        # Initialize generator
        generator = TransactionGenerator()
        
        # Generate transactions
        start_time = datetime.now()
        transactions = generator.generate_transactions(n)
        generation_time = datetime.now() - start_time
        
        if not transactions:
            print("❌ No transactions were generated.")
            return
        
        print(f"\n⏱️  Generation completed in {generation_time.total_seconds():.2f} seconds")
        
        # Define output paths
        output_dir = "../sample_output"
        csv_filename = f"{output_dir}/transactions.csv"
        json_filename = f"{output_dir}/transactions.json"
        
        # Save files
        print("\n📁 Saving files...")
        save_to_csv(transactions, csv_filename)
        save_to_json(transactions, json_filename)
        
        # Display summary
        print(f"\n📊 Summary:")
        print(f"   • Generated: {len(transactions)} transactions")
        print(f"   • Unique customers: {len(set(t['customer_id'] for t in transactions))}")
        print(f"   • CSV file size: {get_file_size(csv_filename)}")
        print(f"   • JSON file size: {get_file_size(json_filename)}")
        print(f"   • Success rate: {sum(1 for t in transactions if t['status'] == 'SUCCESS') / len(transactions) * 100:.1f}%")
        
        print(f"\n🎉 Successfully generated {n} transactions in CSV and JSON format!")
        
    except KeyboardInterrupt:
        print("\n❌ Operation cancelled by user.")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ An error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
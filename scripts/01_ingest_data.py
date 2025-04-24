"""
This script demonstrates data ingestion into the raw ingestion zone of the data lake.
It uploads data to MinIO's raw-ingestion-zone bucket in its native format.

Raw Ingestion Zone: Where data is stored in its original format without modifications.
"""
import pandas as pd
import os
from utils import upload_dataframe_to_minio, get_minio_client

def create_sample_transaction_data():
    """Create a sample customer transactions dataset."""
    data = {
        'transaction_id': range(1, 1001),
        'customer_id': [f'CUST{i:04d}' for i in range(1, 101)] * 10,
        'transaction_date': pd.date_range(start='2024-01-01', periods=1000).tolist(),
        'product_id': [f'PROD{i:03d}' for i in range(1, 51)] * 20,
        'amount': [round(10 + 90 * i/1000, 2) for i in range(1, 1001)],
        'payment_method': ['credit_card', 'debit_card', 'cash', 'digital_wallet'] * 250
    }
    return pd.DataFrame(data)

def create_sample_customer_data():
    """Create a sample customer dataset."""
    data = {
        'customer_id': [f'CUST{i:04d}' for i in range(1, 101)],
        'first_name': ['Customer' + str(i) for i in range(1, 101)],
        'last_name': ['Lastname' + str(i) for i in range(1, 101)],
        'email': [f'customer{i}@example.com' for i in range(1, 101)],
        'signup_date': pd.date_range(start='2023-01-01', periods=100).tolist(),
        'country': ['USA', 'Canada', 'UK', 'Germany', 'France'] * 20
    }
    return pd.DataFrame(data)

def create_sample_product_data():
    """Create a sample product dataset."""
    data = {
        'product_id': [f'PROD{i:03d}' for i in range(1, 51)],
        'product_name': ['Product ' + str(i) for i in range(1, 51)],
        'category': ['Electronics', 'Clothing', 'Home', 'Books', 'Food'] * 10,
        'price': [round(5 + 95 * i/50, 2) for i in range(1, 51)],
        'in_stock': [True, True, True, False] * 12 + [True, True]  # Fixed to exactly 50 elements
    }
    return pd.DataFrame(data)

def main():
    # Create sample data
    print("Creating sample datasets...")
    transactions_df = create_sample_transaction_data()
    customers_df = create_sample_customer_data()
    products_df = create_sample_product_data()

    # Save sample data to local CSV files
    data_dir = '/data/raw-ingestion-zone'
    os.makedirs(data_dir, exist_ok=True)

    # Save transactions data
    transactions_path = f'{data_dir}/transactions.csv'
    transactions_df.to_csv(transactions_path, index=False)
    print(f"Sample transactions data saved to {transactions_path}")

    # Save customers data
    customers_path = f'{data_dir}/customers.csv'
    customers_df.to_csv(customers_path, index=False)
    print(f"Sample customers data saved to {customers_path}")

    # Save products data
    products_path = f'{data_dir}/products.csv'
    products_df.to_csv(products_path, index=False)
    print(f"Sample products data saved to {products_path}")

    # Upload data to MinIO raw-ingestion-zone (in raw format - CSV)
    print("\nUploading data to raw-ingestion-zone...")

    # Upload transactions with metadata
    transactions_metadata = {
        'description': 'Raw transaction data from source system',
        'source_system': 'Point of Sale System',
        'data_owner': 'Sales Department',
        'update_frequency': 'Daily',
        'data_classification': 'Internal'
    }
    upload_dataframe_to_minio(
        transactions_df,
        'raw-ingestion-zone',
        'sales/transactions.csv',
        metadata=transactions_metadata
    )

    # Upload customers with metadata
    customers_metadata = {
        'description': 'Raw customer data from CRM',
        'source_system': 'CRM System',
        'data_owner': 'Marketing Department',
        'update_frequency': 'Weekly',
        'data_classification': 'Confidential'
    }
    upload_dataframe_to_minio(
        customers_df,
        'raw-ingestion-zone',
        'crm/customers.csv',
        metadata=customers_metadata
    )

    # Upload products with metadata
    products_metadata = {
        'description': 'Raw product catalog data',
        'source_system': 'Inventory Management System',
        'data_owner': 'Product Management',
        'update_frequency': 'Weekly',
        'data_classification': 'Internal'
    }
    upload_dataframe_to_minio(
        products_df,
        'raw-ingestion-zone',
        'inventory/products.csv',
        metadata=products_metadata
    )

    # Verify that the files were uploaded
    client = get_minio_client()
    print("\nVerifying uploaded files in raw-ingestion-zone:")

    for prefix in ['sales/', 'crm/', 'inventory/']:
        objects = list(client.list_objects('raw-ingestion-zone', prefix=prefix))
        if objects:
            print(f"Files in {prefix}: {[obj.object_name for obj in objects]}")
        else:
            print(f"No objects found in {prefix}")

    print("\nData ingestion into raw-ingestion-zone complete!")
    print("Note: The data in this zone is stored in its original format without modifications.")

if __name__ == "__main__":
    main()
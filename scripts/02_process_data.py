"""
This script demonstrates data transformation and enrichment in the process zone of the data lake.
It reads data from the raw-ingestion-zone, performs transformations, and writes to the process-zone.

Process Zone: Where data is prepared and transformed according to business needs.
"""
from utils import (
    download_dataframe_from_minio,
    upload_dataframe_to_minio,
    log_data_transformation,
    validate_data_quality
)
import pandas as pd
import numpy as np

def standardize_transaction_data(df):
    """Standardize and clean transaction data."""
    # Make a copy to avoid modifying the original
    processed_df = df.copy()

    # Convert date string to datetime if needed
    if not pd.api.types.is_datetime64_any_dtype(processed_df['transaction_date']):
        processed_df['transaction_date'] = pd.to_datetime(processed_df['transaction_date'])

    # Extract date components for analysis
    processed_df['year'] = processed_df['transaction_date'].dt.year
    processed_df['month'] = processed_df['transaction_date'].dt.month
    processed_df['day'] = processed_df['transaction_date'].dt.day
    processed_df['day_of_week'] = processed_df['transaction_date'].dt.dayofweek

    # Standardize payment methods
    payment_method_mapping = {
        'credit_card': 'Credit Card',
        'credit card': 'Credit Card',
        'creditcard': 'Credit Card',
        'debit_card': 'Debit Card',
        'debit card': 'Debit Card',
        'debitcard': 'Debit Card',
        'cash': 'Cash',
        'digital_wallet': 'Digital Wallet',
        'digital wallet': 'Digital Wallet',
        'digitalwallet': 'Digital Wallet'
    }
    processed_df['payment_method'] = processed_df['payment_method'].map(payment_method_mapping)

    # Categorize transactions by amount
    def categorize_amount(amount):
        if amount < 20:
            return 'Low'
        elif amount < 50:
            return 'Medium'
        else:
            return 'High'

    processed_df['amount_category'] = processed_df['amount'].apply(categorize_amount)

    return processed_df

def enrich_customer_data(df):
    """Enrich and standardize customer data."""
    # Make a copy to avoid modifying the original
    processed_df = df.copy()

    # Convert date string to datetime if needed
    if not pd.api.types.is_datetime64_any_dtype(processed_df['signup_date']):
        processed_df['signup_date'] = pd.to_datetime(processed_df['signup_date'])

    # Calculate customer tenure in days
    processed_df['tenure_days'] = (pd.Timestamp('2024-01-01') - processed_df['signup_date']).dt.days

    # Group customers by tenure
    def tenure_group(days):
        if days < 90:
            return 'New'
        elif days < 365:
            return 'Regular'
        else:
            return 'Loyal'

    processed_df['customer_segment'] = processed_df['tenure_days'].apply(tenure_group)

    # Standardize country names
    country_mapping = {
        'USA': 'United States',
        'US': 'United States',
        'U.S.A.': 'United States',
        'UK': 'United Kingdom',
        'U.K.': 'United Kingdom'
    }
    processed_df['country'] = processed_df['country'].map(lambda x: country_mapping.get(x, x))

    # Add geographic region
    region_mapping = {
        'United States': 'North America',
        'Canada': 'North America',
        'United Kingdom': 'Europe',
        'Germany': 'Europe',
        'France': 'Europe'
    }
    processed_df['region'] = processed_df['country'].map(region_mapping)

    return processed_df

def standardize_product_data(df):
    """Standardize and enrich product data."""
    # Make a copy to avoid modifying the original
    processed_df = df.copy()

    # Standardize categories
    processed_df['category'] = processed_df['category'].str.title()

    # Add product tier based on price
    def price_tier(price):
        if price < 20:
            return 'Budget'
        elif price < 50:
            return 'Standard'
        else:
            return 'Premium'

    processed_df['price_tier'] = processed_df['price'].apply(price_tier)

    # Convert boolean to string for consistency
    processed_df['availability'] = processed_df['in_stock'].map({True: 'In Stock', False: 'Out of Stock'})

    return processed_df

def create_transaction_product_view(transactions_df, products_df):
    """Create a joined view of transactions and products."""
    # Merge transactions with product information
    merged_df = pd.merge(
        transactions_df,
        products_df[['product_id', 'product_name', 'category', 'price_tier']],
        on='product_id',
        how='left'
    )

    # Add some calculated fields
    merged_df['month_year'] = merged_df['transaction_date'].dt.strftime('%Y-%m')

    return merged_df

def main():
    print("Starting data processing stage...")

    # 1. Download data from raw-ingestion-zone
    print("\nDownloading data from raw-ingestion-zone...")
    try:
        transactions_df = download_dataframe_from_minio('raw-ingestion-zone', 'sales/transactions.csv')
        customers_df = download_dataframe_from_minio('raw-ingestion-zone', 'crm/customers.csv')
        products_df = download_dataframe_from_minio('raw-ingestion-zone', 'inventory/products.csv')
        print(f"Downloaded {len(transactions_df)} transactions, {len(customers_df)} customers, {len(products_df)} products")
    except Exception as e:
        print(f"Error downloading data: {e}")
        return

    # 2. Process and transform the data
    print("\nTransforming data...")

    # Process transactions
    processed_transactions = standardize_transaction_data(transactions_df)
    print("Transaction data processed and standardized")

    # Validate transaction data quality
    transaction_rules = {
        'no_nulls': ['transaction_id', 'customer_id', 'product_id', 'amount', 'payment_method'],
        'unique': ['transaction_id']
    }
    validate_data_quality(processed_transactions, 'processed_transactions', transaction_rules)

    # Process customers
    processed_customers = enrich_customer_data(customers_df)
    print("Customer data processed and enriched")

    # Validate customer data quality
    customer_rules = {
        'no_nulls': ['customer_id', 'email'],
        'unique': ['customer_id', 'email']
    }
    validate_data_quality(processed_customers, 'processed_customers', customer_rules)

    # Process products
    processed_products = standardize_product_data(products_df)
    print("Product data processed and standardized")

    # Validate product data quality
    product_rules = {
        'no_nulls': ['product_id', 'product_name', 'category'],
        'unique': ['product_id']
    }
    validate_data_quality(processed_products, 'processed_products', product_rules)

    # Create transaction-product view
    transaction_product_view = create_transaction_product_view(processed_transactions, processed_products)
    print("Created transaction-product integrated view")

    # 3. Upload to process-zone in Parquet format (columnar storage for better performance)
    print("\nUploading processed data to process-zone...")

    # Upload processed transactions
    transaction_meta = {
        'description': 'Standardized transaction data with derived fields',
        'primary_keys': ['transaction_id'],
        'foreign_keys': ['customer_id', 'product_id'],
        'transformations': 'Added date components, standardized payment methods, added amount categories'
    }
    upload_dataframe_to_minio(
        processed_transactions,
        'process-zone',
        'sales/transactions.parquet',
        format='parquet',
        metadata=transaction_meta
    )
    log_data_transformation(
        'raw-ingestion-zone', 'sales/transactions.csv',
        'process-zone', 'sales/transactions.parquet',
        'Standardized transaction data and converted to Parquet format'
    )

    # Upload processed customers
    customer_meta = {
        'description': 'Enriched customer data with derived fields',
        'primary_keys': ['customer_id'],
        'transformations': 'Added tenure calculation, customer segments, standardized countries, added regions'
    }
    upload_dataframe_to_minio(
        processed_customers,
        'process-zone',
        'crm/customers.parquet',
        format='parquet',
        metadata=customer_meta
    )
    log_data_transformation(
        'raw-ingestion-zone', 'crm/customers.csv',
        'process-zone', 'crm/customers.parquet',
        'Enriched customer data and converted to Parquet format'
    )

    # Upload processed products
    product_meta = {
        'description': 'Standardized product data with derived fields',
        'primary_keys': ['product_id'],
        'transformations': 'Added price tiers, standardized categories, improved availability status'
    }
    upload_dataframe_to_minio(
        processed_products,
        'process-zone',
        'inventory/products.parquet',
        format='parquet',
        metadata=product_meta
    )
    log_data_transformation(
        'raw-ingestion-zone', 'inventory/products.csv',
        'process-zone', 'inventory/products.parquet',
        'Standardized product data and converted to Parquet format'
    )

    # Upload transaction-product view
    view_meta = {
        'description': 'Integrated view of transactions and products',
        'source_tables': ['transactions', 'products'],
        'join_keys': ['product_id'],
        'transformations': 'Joined transaction data with product information'
    }
    upload_dataframe_to_minio(
        transaction_product_view,
        'process-zone',
        'integrated/transaction_product_view.parquet',
        format='parquet',
        metadata=view_meta
    )
    log_data_transformation(
        'multiple', 'multiple',
        'process-zone', 'integrated/transaction_product_view.parquet',
        'Created integrated view joining transactions and products'
    )

    print("\nData processing complete!")
    print("Note: In the Process Zone, data has been cleaned, standardized, and enriched.")
    print("The data is now stored in Parquet format for better query performance.")
    print("Metadata and transformation logs have been recorded in the govern-zone.")

if __name__ == "__main__":
    main()
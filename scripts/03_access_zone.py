"""
This script demonstrates preparation of analytics-ready data in the access zone.
It aggregates and structures data from the process-zone into ready-to-consume datasets.

Access Zone: Where processed data is made accessible and consumable for analytics,
visualization, and decision support.
"""
from utils import (
    download_dataframe_from_minio,
    upload_dataframe_to_minio,
    log_data_transformation,
    execute_trino_query
)
import pandas as pd

def create_sales_by_product_category():
    """Create aggregated sales data by product category."""
    print("Creating sales by product category aggregation...")

    # Load the transaction-product view from process zone
    tp_view = download_dataframe_from_minio(
        'process-zone',
        'integrated/transaction_product_view.parquet',
        format='parquet'
    )

    # Aggregate by category and month
    sales_by_category = tp_view.groupby(['category', 'month_year']).agg({
        'amount': ['sum', 'mean', 'count'],
        'transaction_id': 'nunique'
    }).reset_index()

    # Flatten the multi-level columns
    sales_by_category.columns = [
        'product_category', 'month_year', 'total_sales',
        'average_sale', 'transaction_count', 'unique_transaction_count'
    ]

    # Sort by month and category
    sales_by_category = sales_by_category.sort_values(['month_year', 'product_category'])

    return sales_by_category

def create_customer_sales_summary():
    """Create customer-centric sales summary for analytics."""
    print("Creating customer sales summary...")

    # Load the data from process zone
    transactions = download_dataframe_from_minio(
        'process-zone',
        'sales/transactions.parquet',
        format='parquet'
    )

    customers = download_dataframe_from_minio(
        'process-zone',
        'crm/customers.parquet',
        format='parquet'
    )

    # Aggregate transactions by customer
    customer_transactions = transactions.groupby('customer_id').agg({
        'transaction_id': 'count',
        'amount': ['sum', 'mean', 'min', 'max'],
        'transaction_date': ['min', 'max']
    }).reset_index()

    # Flatten the multi-level columns
    customer_transactions.columns = [
        'customer_id', 'transaction_count', 'total_spend',
        'average_spend', 'min_spend', 'max_spend',
        'first_purchase_date', 'last_purchase_date'
    ]

    # Calculate days since last purchase
    customer_transactions['days_since_last_purchase'] = (
            pd.Timestamp('2024-01-31') - customer_transactions['last_purchase_date']
    ).dt.days

    # Join with customer information
    customer_summary = pd.merge(
        customer_transactions,
        customers[['customer_id', 'first_name', 'last_name', 'email', 'country', 'region', 'customer_segment']],
        on='customer_id',
        how='left'
    )

    # Add RFM (Recency, Frequency, Monetary) segments using a simpler approach
    # Instead of quantiles, use fixed thresholds

    # Recency - days since last purchase
    def recency_score(days):
        if days <= 10:
            return '3-Recent'
        elif days <= 20:
            return '2-Moderate'
        else:
            return '1-Inactive'

    # Frequency - transaction count
    def frequency_score(count):
        if count >= 15:
            return '3-Frequent'
        elif count >= 10:
            return '2-Regular'
        else:
            return '1-Rare'

    # Monetary - total spend
    def monetary_score(amount):
        if amount >= 800:
            return '3-High'
        elif amount >= 500:
            return '2-Medium'
        else:
            return '1-Low'

    customer_summary['recency_score'] = customer_summary['days_since_last_purchase'].apply(recency_score)
    customer_summary['frequency_score'] = customer_summary['transaction_count'].apply(frequency_score)
    customer_summary['monetary_score'] = customer_summary['total_spend'].apply(monetary_score)

    # Combine RFM scores
    customer_summary['rfm_segment'] = (
            customer_summary['recency_score'].astype(str) + '_' +
            customer_summary['frequency_score'].astype(str) + '_' +
            customer_summary['monetary_score'].astype(str)
    )

    return customer_summary

def create_product_performance_metrics():
    """Create product performance metrics for business intelligence."""
    print("Creating product performance metrics...")

    # Load data from process zone
    tp_view = download_dataframe_from_minio(
        'process-zone',
        'integrated/transaction_product_view.parquet',
        format='parquet'
    )

    products = download_dataframe_from_minio(
        'process-zone',
        'inventory/products.parquet',
        format='parquet'
    )

    # Aggregate by product
    product_metrics = tp_view.groupby('product_id').agg({
        'transaction_id': 'count',
        'amount': ['sum', 'mean']
    }).reset_index()

    # Flatten columns
    product_metrics.columns = [
        'product_id', 'sales_count', 'total_revenue', 'average_price'
    ]

    # Join with product information
    product_performance = pd.merge(
        product_metrics,
        products[['product_id', 'product_name', 'category', 'price_tier', 'availability']],
        on='product_id',
        how='left'
    )

    # Calculate performance metrics
    # Rank products by sales count within category
    product_performance['sales_rank_in_category'] = product_performance.groupby('category')['sales_count'].rank(ascending=False)

    # Calculate percentage of category sales
    category_totals = product_performance.groupby('category')['total_revenue'].transform('sum')
    product_performance['percent_of_category_sales'] = product_performance['total_revenue'] / category_totals * 100

    return product_performance

def main():
    print("Starting data preparation for the Access Zone...")

    # 1. Create analytics-ready datasets
    sales_by_category = create_sales_by_product_category()
    customer_summary = create_customer_sales_summary()
    product_performance = create_product_performance_metrics()

    # 2. Upload to access-zone
    print("\nUploading analytics-ready data to access-zone...")

    # Sales by category aggregation
    sales_meta = {
        'description': 'Monthly sales aggregated by product category',
        'purpose': 'Sales trend analysis and reporting',
        'refresh_frequency': 'Daily',
        'target_users': 'Sales Analysts, Business Intelligence'
    }
    upload_dataframe_to_minio(
        sales_by_category,
        'access-zone',
        'analytics/sales_by_category.parquet',
        format='parquet',
        metadata=sales_meta
    )
    log_data_transformation(
        'process-zone', 'integrated/transaction_product_view.parquet',
        'access-zone', 'analytics/sales_by_category.parquet',
        'Created sales by category aggregation for analytics'
    )

    # Also save a CSV version for direct use in tools that prefer CSV
    upload_dataframe_to_minio(
        sales_by_category,
        'access-zone',
        'analytics/sales_by_category.csv',
        format='csv',
        metadata=sales_meta
    )

    # Customer summary
    customer_meta = {
        'description': 'Customer-centric sales summary with RFM segmentation',
        'purpose': 'Customer segmentation, targeting, and retention analysis',
        'refresh_frequency': 'Weekly',
        'target_users': 'Marketing Team, Customer Success'
    }
    upload_dataframe_to_minio(
        customer_summary,
        'access-zone',
        'analytics/customer_summary.parquet',
        format='parquet',
        metadata=customer_meta
    )
    log_data_transformation(
        'multiple', 'multiple',
        'access-zone', 'analytics/customer_summary.parquet',
        'Created customer summary with RFM segmentation for marketing analysis'
    )

    # Product performance
    product_meta = {
        'description': 'Product performance metrics with category ranking',
        'purpose': 'Product performance assessment and inventory planning',
        'refresh_frequency': 'Weekly',
        'target_users': 'Product Managers, Inventory Planners'
    }
    upload_dataframe_to_minio(
        product_performance,
        'access-zone',
        'analytics/product_performance.parquet',
        format='parquet',
        metadata=product_meta
    )
    log_data_transformation(
        'multiple', 'multiple',
        'access-zone', 'analytics/product_performance.parquet',
        'Created product performance metrics for business intelligence'
    )

    print("\nAccess Zone preparation complete!")
    print("Note: The Access Zone now contains analytics-ready datasets optimized for:")
    print("  - Business Intelligence dashboards")
    print("  - Data analysis and visualization")
    print("  - Machine learning model development")
    print("  - Executive reporting")
    print("\nThese datasets are structured for easy consumption by various tools and users.")

if __name__ == "__main__":
    main()
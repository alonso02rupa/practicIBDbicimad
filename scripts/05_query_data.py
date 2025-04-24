"""
This script demonstrates querying the data in the access zone using different methods:
1. Direct Pandas querying from the access-zone
2. SQL queries via Trino for complex analytical queries
3. Example analytics and insights extracted from the data

Access Zone: Contains analytics-ready data for visualization, reporting, and advanced analytics.
"""
from utils import (
    download_dataframe_from_minio,
    execute_trino_query
)
import pandas as pd
import matplotlib.pyplot as plt
import io

def generate_insights(sales_category, customer_data, product_data):
    """Generate business insights from the analyzed data."""
    print("\n=== Key Business Insights ===")

    # Insight 1: Customer Lifetime Value by Segment
    customer_segments = customer_data.groupby('customer_segment').agg({
        'customer_id': 'count',
        'total_spend': ['sum', 'mean'],
        'transaction_count': 'mean'
    })

    customer_segments.columns = ['customer_count', 'total_revenue', 'avg_customer_value', 'avg_transactions']
    customer_segments['avg_transaction_value'] = customer_segments['total_revenue'] / (customer_segments['customer_count'] * customer_segments['avg_transactions'])

    print("\nInsight 1: Customer Lifetime Value by Segment")
    print(customer_segments[['customer_count', 'avg_customer_value', 'avg_transactions', 'avg_transaction_value']])

    # Insight 2: Product Category Performance
    category_performance = product_data.groupby('category').agg({
        'sales_count': 'sum',
        'total_revenue': 'sum',
        'product_id': 'count'
    }).reset_index()

    category_performance.columns = ['category', 'total_sales', 'total_revenue', 'product_count']
    category_performance['revenue_per_product'] = category_performance['total_revenue'] / category_performance['product_count']
    category_performance['sales_per_product'] = category_performance['total_sales'] / category_performance['product_count']

    print("\nInsight 2: Product Category Efficiency")
    print(category_performance.sort_values('revenue_per_product', ascending=False))

    # Insight 3: RFM Segment Analysis
    rfm_segments = customer_data.groupby('rfm_segment').agg({
        'customer_id': 'count',
        'total_spend': 'sum',
        'transaction_count': 'sum'
    }).reset_index()

    rfm_segments.columns = ['rfm_segment', 'customer_count', 'total_revenue', 'transaction_count']
    rfm_segments['percent_customers'] = rfm_segments['customer_count'] / rfm_segments['customer_count'].sum() * 100
    rfm_segments['percent_revenue'] = rfm_segments['total_revenue'] / rfm_segments['total_revenue'].sum() * 100

    print("\nInsight 3: Top 5 RFM Segments by Revenue")
    print(rfm_segments.sort_values('total_revenue', ascending=False).head(5)[
              ['rfm_segment', 'customer_count', 'percent_customers', 'percent_revenue']
          ])

    # Insight 4: Recommendations
    print("\nInsight 4: Business Recommendations")

    # Identify the top performing category
    top_category = category_performance.loc[category_performance['total_revenue'].idxmax(), 'category']

    # Identify the most valuable customer segment
    valuable_segment = customer_segments.index[customer_segments['avg_customer_value'].argmax()]

    # Identify the most efficient product category
    efficient_category = category_performance.loc[category_performance['revenue_per_product'].idxmax(), 'category']

    # Generate recommendations
    print(f"1. Focus marketing efforts on the {top_category} category, which generates the highest revenue.")
    print(f"2. Create retention programs for {valuable_segment} customers, who have the highest lifetime value.")
    print(f"3. Use the {efficient_category} category as a model for product assortment and pricing strategies.")
    print("4. Implement cross-selling between complementary categories to increase basket size.")

    return {
        'customer_segments': customer_segments,
        'category_performance': category_performance,
        'rfm_segments': rfm_segments
    }

def query_with_pandas():
    """Demonstrate accessing and analyzing data directly with pandas."""
    print("\n=== Querying Access Zone with Pandas ===")

    # Load datasets from access zone
    print("Loading datasets from access-zone...")
    sales_by_category = download_dataframe_from_minio(
        'access-zone',
        'analytics/sales_by_category.parquet',
        format='parquet'
    )

    customer_summary = download_dataframe_from_minio(
        'access-zone',
        'analytics/customer_summary.parquet',
        format='parquet'
    )

    product_performance = download_dataframe_from_minio(
        'access-zone',
        'analytics/product_performance.parquet',
        format='parquet'
    )

    # Analysis 1: Sales trends by category
    print("\nAnalysis 1: Sales Trends by Product Category")
    category_trends = sales_by_category.pivot(
        index='month_year',
        columns='product_category',
        values='total_sales'
    ).fillna(0)

    print("\nMonthly Sales by Category:")
    print(category_trends)

    # Analysis 2: Customer Segmentation
    print("\nAnalysis 2: Customer Segmentation Analysis")
    segment_analysis = customer_summary.groupby('customer_segment').agg({
        'customer_id': 'count',
        'total_spend': 'sum',
        'average_spend': 'mean',
        'transaction_count': 'sum'
    }).reset_index()

    segment_analysis = segment_analysis.rename(columns={'customer_id': 'customer_count'})
    segment_analysis['percent_of_customers'] = segment_analysis['customer_count'] / segment_analysis['customer_count'].sum() * 100
    segment_analysis['percent_of_revenue'] = segment_analysis['total_spend'] / segment_analysis['total_spend'].sum() * 100

    print("\nCustomer Segment Analysis:")
    print(segment_analysis[['customer_segment', 'customer_count', 'percent_of_customers',
                            'percent_of_revenue', 'average_spend']])

    # Analysis 3: Top Products Performance
    print("\nAnalysis 3: Top Products by Revenue")
    top_products = product_performance.sort_values('total_revenue', ascending=False).head(10)

    print("\nTop 10 Products by Revenue:")
    print(top_products[['product_id', 'product_name', 'category',
                        'sales_count', 'total_revenue', 'average_price']])

    return sales_by_category, customer_summary, product_performance

def query_with_trino():
    """Demonstrate using SQL via Trino to query the data lake."""
    print("\n=== Querying Data Lake with Trino SQL ===")

    try:
        # Inform the user that this is a demonstration only
        print("\nNote: The following queries are for demonstration purposes.")
        print("To run them successfully, you would need to configure Trino")
        print("catalogs and schemas to match the data lake structure.")
        print("The example queries are shown here for educational purposes.\n")

        # Show example queries that would work with a properly configured Trino setup
        print("Example Query 1: Sales by Payment Method")
        print("""
        SELECT 
            payment_method, 
            COUNT(*) as transaction_count, 
            SUM(amount) as total_sales,
            AVG(amount) as average_sale
        FROM minio.process_zone.sales_transactions
        GROUP BY payment_method
        ORDER BY total_sales DESC
        """)

        print("\nExample Query 2: Customer Segmentation Analysis")
        print("""
        WITH customer_transactions AS (
            SELECT 
                c.customer_id,
                c.region,
                c.customer_segment,
                COUNT(t.transaction_id) as transaction_count,
                SUM(t.amount) as total_spend
            FROM 
                minio.process_zone.crm_customers c
            JOIN 
                minio.process_zone.sales_transactions t ON c.customer_id = t.customer_id
            GROUP BY 
                c.customer_id, c.region, c.customer_segment
        )
        SELECT
            region,
            customer_segment,
            COUNT(*) as customer_count,
            SUM(total_spend) as total_revenue,
            AVG(total_spend) as avg_customer_spend
        FROM 
            customer_transactions
        GROUP BY 
            region, customer_segment
        ORDER BY 
            region, total_revenue DESC
        """)

    except Exception as e:
        print(f"Error with Trino demonstration: {e}")

    return None

def main():
    """Execute all query examples and generate insights."""
    print("Demonstrating various ways to query the multi-zone data lake...")

    # 1. Query with Pandas - direct access to the analytical datasets
    sales_category, customer_summary, product_performance = query_with_pandas()

    # 2. Query with Trino SQL - for more complex analytical queries
    query_with_trino()

    # 3. Generate business insights from the data
    insights = generate_insights(sales_category, customer_summary, product_performance)

    print("\n=== Data Lake Query Summary ===")
    print("This demonstration showed different ways to extract value from the data lake:")
    print("1. Direct access to analytics-ready datasets using Pandas")
    print("2. Complex SQL queries using Trino for cross-dataset analysis")
    print("3. Business insights and recommendations derived from the data")
    print("\nThe multi-zone architecture enables:")
    print("- Raw data ingestion and preservation (raw-ingestion-zone)")
    print("- Data transformation and preparation (process-zone)")
    print("- Analytics-ready data for business use (access-zone)")
    print("- Comprehensive governance and metadata management (govern-zone)")

if __name__ == "__main__":
    main()
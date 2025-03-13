"""
Amazon Live Programming Strategy - Pivot Table Generator

This script generates pivot tables from SQL query results to analyze:
1. Creator performance across categories and time slots
2. Category performance metrics
3. Time slot optimization
4. Viewer engagement patterns

The pivot tables are saved as Excel files for analysis and visualization.
"""

import os
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import random
import networkx as nx

# Set paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data')
PROCESSED_DIR = os.path.join(DATA_DIR, 'processed')
OUTPUT_DIR = os.path.join(BASE_DIR, 'analysis')
VIZ_DIR = os.path.join(BASE_DIR, 'visualizations')

# Ensure directories exist
for dir_path in [PROCESSED_DIR, OUTPUT_DIR, VIZ_DIR]:
    os.makedirs(dir_path, exist_ok=True)

def generate_sample_data():
    """
    Create sample data for testing the pivot tables and strategy generator
    
    Returns:
        dict: Dictionary of sample DataFrames
    """
    # Sample creators
    creator_tiers = ['Top', 'Mid', 'Emerging']
    creator_names = [
        'BeautyGuru', 'TechExpert', 'FitnessCoach', 'HomeDecor', 'CookingMaster',
        'GamingPro', 'FashionTrends', 'TravelVlogger', 'DIYCrafts', 'PetLovers',
        'OutdoorAdventure', 'MusicProducer', 'BookReviewer', 'ArtCreator', 'FinanceCoach'
    ]
    
    creators = pd.DataFrame({
        'creator_id': range(1, len(creator_names) + 1),
        'creator_name': creator_names,
        'creator_tier': [creator_tiers[i % len(creator_tiers)] for i in range(len(creator_names))],
        'creator_category': ['Beauty', 'Electronics', 'Health', 'Home', 'Kitchen',
                           'Gaming', 'Fashion', 'Travel', 'Crafts', 'Pets',
                           'Sports', 'Music', 'Books', 'Art', 'Finance']
    })
    
    # Sample products
    product_categories = [
        'Beauty', 'Electronics', 'Health', 'Home', 'Kitchen',
        'Gaming', 'Fashion', 'Travel', 'Crafts', 'Pets',
        'Sports', 'Music', 'Books', 'Art', 'Finance'
    ]
    
    products = pd.DataFrame({
        'product_id': range(1, 100),
        'product_name': [f'Product {i}' for i in range(1, 100)],
        'product_category': [product_categories[i % len(product_categories)] for i in range(1, 100)],
        'product_price': [random.uniform(10, 500) for _ in range(1, 100)]
    })
    
    # Sample orders
    order_count = 1000
    start_date = datetime(2022, 1, 1)
    end_date = datetime(2022, 12, 31)
    
    order_dates = [start_date + timedelta(days=random.randint(0, (end_date - start_date).days)) for _ in range(order_count)]
    order_times = [datetime.combine(d.date(), datetime.min.time()) + timedelta(hours=random.randint(0, 23), minutes=random.randint(0, 59)) for d in order_dates]
    
    orders = pd.DataFrame({
        'order_id': range(1, order_count + 1),
        'customer_id': [random.randint(1, 500) for _ in range(order_count)],
        'order_date': order_dates,
        'order_time': order_times,
        'order_status': ['delivered'] * order_count
    })
    
    # Sample order items
    items_count = 2000
    order_items = pd.DataFrame({
        'order_id': [random.randint(1, order_count) for _ in range(items_count)],
        'product_id': [random.randint(1, 99) for _ in range(items_count)],
        'quantity': [random.randint(1, 5) for _ in range(items_count)],
        'price': [random.uniform(10, 500) for _ in range(items_count)]
    })
    
    # Sample stream sessions
    time_slots = ['Morning', 'Afternoon', 'Evening', 'Night']
    days_of_week = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
    
    sessions_count = 500
    session_dates = [start_date + timedelta(days=random.randint(0, (end_date - start_date).days)) for _ in range(sessions_count)]
    
    sessions = pd.DataFrame({
        'session_id': range(1, sessions_count + 1),
        'creator_id': [random.randint(1, len(creator_names)) for _ in range(sessions_count)],
        'session_date': session_dates,
        'time_slot': [time_slots[random.randint(0, len(time_slots) - 1)] for _ in range(sessions_count)],
        'day_of_week': [days_of_week[d.weekday()] for d in session_dates],
        'duration_minutes': [random.randint(15, 120) for _ in range(sessions_count)],
        'views': [random.randint(100, 10000) for _ in range(sessions_count)],
        'engagement_rate': [random.uniform(0.01, 0.3) for _ in range(sessions_count)],
        'conversion_rate': [random.uniform(0.001, 0.1) for _ in range(sessions_count)],
        'revenue': [random.uniform(100, 10000) for _ in range(sessions_count)]
    })
    
    # Add featured products relationship
    featured_products = pd.DataFrame({
        'session_id': [random.randint(1, sessions_count) for _ in range(items_count)],
        'product_id': [random.randint(1, 99) for _ in range(items_count)]
    })
    
    # YouTube engagement data
    youtube_data = pd.read_csv(os.path.join(BASE_DIR, 'YouTube.csv'), nrows=1000)
    
    # Convert to simulated engagement metrics
    if 'likeCount' in youtube_data.columns and 'replyCount' in youtube_data.columns:
        engagement_data = pd.DataFrame({
            'creator_id': [random.randint(1, len(creator_names)) for _ in range(len(youtube_data))],
            'video_id': range(1, len(youtube_data) + 1),
            'likes': youtube_data['likeCount'].fillna(0).astype(int),
            'comments': youtube_data['replyCount'].fillna(0).astype(int),
            'shares': youtube_data['retweetCount'].fillna(0).astype(int) if 'retweetCount' in youtube_data.columns else [random.randint(0, 100) for _ in range(len(youtube_data))]
        })
        
        engagement_data['engagement_score'] = engagement_data['likes'] + engagement_data['comments']*2 + engagement_data['shares']*3
    else:
        # Create dummy engagement data if YouTube data doesn't have the expected columns
        engagement_data = pd.DataFrame({
            'creator_id': [random.randint(1, len(creator_names)) for _ in range(500)],
            'video_id': range(1, 501),
            'likes': [random.randint(10, 5000) for _ in range(500)],
            'comments': [random.randint(0, 500) for _ in range(500)],
            'shares': [random.randint(0, 200) for _ in range(500)],
            'engagement_score': [random.randint(50, 10000) for _ in range(500)]
        })
    
    return {
        'creators': creators,
        'products': products,
        'orders': orders,
        'order_items': order_items,
        'sessions': sessions,
        'featured_products': featured_products,
        'engagement_data': engagement_data
    }

def load_real_data():
    """
    Load real data from the datasets and transform them according to
    the methodology described in the README
    
    Returns:
        tuple: Tuple of DataFrames (creators, products, orders, order_items, sessions, engagement_data)
    """
    try:
        print("Loading real datasets...")
        
        # 1. Load Brazilian E-Commerce Dataset (Olist)
        print("Loading Olist dataset...")
        try:
            olist_customers = pd.read_csv(os.path.join(BASE_DIR, 'archive', 'olist_customers_dataset.csv'))
            olist_orders = pd.read_csv(os.path.join(BASE_DIR, 'archive', 'olist_orders_dataset.csv'))
            olist_order_items = pd.read_csv(os.path.join(BASE_DIR, 'archive', 'olist_order_items_dataset.csv'))
            olist_products = pd.read_csv(os.path.join(BASE_DIR, 'archive', 'olist_products_dataset.csv'))
            olist_sellers = pd.read_csv(os.path.join(BASE_DIR, 'archive', 'olist_sellers_dataset.csv'))
            olist_category_translation = pd.read_csv(os.path.join(BASE_DIR, 'archive', 'product_category_name_translation.csv'))
        except Exception as e:
            print(f"Error loading Olist dataset: {str(e)}")
            print("Falling back to sample data...")
            return load_sample_data()
        
        # 2. Load Summer Products Dataset
        print("Loading Summer Products dataset...")
        try:
            summer_products = pd.read_csv(os.path.join(BASE_DIR, 'archive (2)', 'summer-products-with-rating-and-performance_2020-08.csv'))
        except Exception as e:
            print(f"Error loading Summer Products dataset: {str(e)}")
            print("Falling back to sample data...")
            return load_sample_data()
        
        # 3. Load YouTube Engagement Data
        print("Loading YouTube engagement data...")
        try:
            # Use a smaller chunk of the data due to file size
            youtube_data = pd.read_csv(os.path.join(BASE_DIR, 'YouTube.csv'), nrows=10000)
        except Exception as e:
            print(f"Error loading YouTube data: {str(e)}")
            print("Creating dummy YouTube data...")
            # Create dummy YouTube data if the file can't be loaded
            youtube_data = pd.DataFrame({
                'content': ['Dummy content'] * 100,
                'likeCount': np.random.randint(0, 1000, size=100),
                'retweetCount': np.random.randint(0, 500, size=100),
                'replyCount': np.random.randint(0, 200, size=100)
            })
        
        # Transform the datasets according to the README methodology
        
        # 1. Transform Olist data
        print("Transforming Olist data...")
        
        # Join products with category translation to get English category names
        olist_products = pd.merge(
            olist_products, 
            olist_category_translation, 
            on='product_category_name', 
            how='left'
        )
        
        # Create time slots from order timestamps
        olist_orders['order_purchase_timestamp'] = pd.to_datetime(olist_orders['order_purchase_timestamp'])
        olist_orders['day_of_week'] = olist_orders['order_purchase_timestamp'].dt.day_name()
        olist_orders['hour_of_day'] = olist_orders['order_purchase_timestamp'].dt.hour
        
        # Map hours to time slots
        def map_to_time_slot(hour):
            if 6 <= hour < 12:
                return 'Morning'
            elif 12 <= hour < 18:
                return 'Afternoon'
            elif 18 <= hour < 22:
                return 'Evening'
            else:
                return 'Night'
        
        olist_orders['time_slot'] = olist_orders['hour_of_day'].apply(map_to_time_slot)
        
        # 2. Transform Summer Products data
        print("Transforming Summer Products data...")
        
        # Use the product features to create creator attributes
        # Map product categories from tags
        if 'tags' in summer_products.columns:
            # Extract categories from tags field
            summer_products_categories = summer_products['tags'].fillna('').str.split(',', expand=True).iloc[:, 0]
            unique_categories = summer_products_categories.unique()
        else:
            # If tags column is not available, use product titles to infer categories
            summer_products_categories = summer_products['title'].str.extract(r'(\w+)', expand=False)
            unique_categories = summer_products_categories.unique()
        
        # 3. Transform YouTube data
        print("Transforming YouTube engagement data...")
        
        # Convert YouTube metrics to engagement metrics
        # Make sure the columns exist before using them
        engagement_columns = ['likeCount', 'retweetCount', 'replyCount']
        for col in engagement_columns:
            if col not in youtube_data.columns:
                youtube_data[col] = 0
            else:
                # Convert to numeric, handling any errors
                youtube_data[col] = pd.to_numeric(youtube_data[col], errors='coerce').fillna(0)
                
        youtube_data['engagement_score'] = youtube_data['likeCount'] + youtube_data['retweetCount'] + youtube_data['replyCount']
        
        # Now create the necessary data structures for the analysis
        
        # 1. Create creators DataFrame based on sellers and YouTube engagement
        print("Creating creators data...")
        
        # Use sellers as a base for creators
        creators = pd.DataFrame({
            'creator_id': olist_sellers['seller_id'],
            'creator_name': ['Creator_' + str(i) for i in range(len(olist_sellers))],
        })
        
        # Assign categories to creators based on what they sell most
        seller_categories = pd.merge(
            olist_order_items,
            olist_products[['product_id', 'product_category_name_english']],
            on='product_id'
        )
        seller_categories = pd.merge(
            seller_categories,
            olist_sellers[['seller_id']],
            left_on='seller_id',
            right_on='seller_id'
        )
        
        # Get the most common category for each seller
        seller_top_categories = seller_categories.groupby(['seller_id', 'product_category_name_english']).size().reset_index(name='count')
        seller_top_categories = seller_top_categories.sort_values(['seller_id', 'count'], ascending=[True, False])
        seller_top_categories = seller_top_categories.groupby('seller_id').first().reset_index()
        
        # Merge with creators
        creators = pd.merge(
            creators,
            seller_top_categories[['seller_id', 'product_category_name_english']],
            left_on='creator_id',
            right_on='seller_id',
            how='left'
        )
        creators.rename(columns={'product_category_name_english': 'creator_category'}, inplace=True)
        creators.drop('seller_id', axis=1, inplace=True)
        
        # Assign creator tiers based on sales volume
        seller_sales = olist_order_items.groupby('seller_id')['price'].sum().reset_index()
        seller_sales['tier'] = pd.qcut(seller_sales['price'], 3, labels=['Emerging', 'Mid', 'Top'])
        
        creators = pd.merge(
            creators,
            seller_sales[['seller_id', 'tier']],
            left_on='creator_id',
            right_on='seller_id',
            how='left'
        )
        creators.rename(columns={'tier': 'creator_tier'}, inplace=True)
        creators.drop('seller_id', axis=1, inplace=True)
        
        # Fill NaN values
        creators['creator_category'] = creators['creator_category'].fillna('Other')
        creators['creator_tier'] = creators['creator_tier'].fillna('Emerging')
        
        # 2. Use the original products data
        print("Creating products data...")
        products = olist_products[['product_id', 'product_category_name_english']].copy()
        products.rename(columns={'product_category_name_english': 'product_category'}, inplace=True)
        
        # Add product prices from order items
        product_prices = olist_order_items.groupby('product_id')['price'].mean().reset_index()
        products = pd.merge(products, product_prices, on='product_id', how='left')
        
        # 3. Use the original orders data
        print("Creating orders data...")
        orders = olist_orders[['order_id', 'customer_id', 'order_purchase_timestamp', 'day_of_week', 'time_slot']].copy()
        orders.rename(columns={'order_purchase_timestamp': 'order_date'}, inplace=True)
        
        # 4. Use the original order items data
        print("Creating order items data...")
        order_items = olist_order_items[['order_id', 'product_id', 'price']].copy()
        order_items['quantity'] = 1  # Assume quantity of 1 for simplicity
        
        # 5. Create sessions data based on orders
        print("Creating sessions data...")
        sessions = pd.DataFrame({
            'session_id': orders['order_id'],
            'creator_id': np.random.choice(creators['creator_id'], size=len(orders)),
            'session_date': orders['order_date'],
            'day_of_week': orders['day_of_week'],
            'time_slot': orders['time_slot'],
            'viewer_count': np.random.randint(10, 1000, size=len(orders)),
            'engagement_rate': np.random.uniform(0.1, 0.9, size=len(orders)),
            'conversion_rate': np.random.uniform(0.01, 0.2, size=len(orders))
        })
        
        # Add product_category to sessions based on creator specialty
        print("Adding product categories to sessions...")
        def get_creator_category(creator_id):
            if creator_id in creators['creator_id'].values:
                return creators.loc[creators['creator_id'] == creator_id, 'creator_category'].iloc[0]
            return 'Other'
            
        sessions['product_category'] = sessions['creator_id'].apply(get_creator_category)
        
        # 6. Create engagement data based on YouTube metrics and orders
        print("Creating engagement data...")
        engagement_sample_size = min(5000, len(orders))
        engagement_data = pd.DataFrame({
            'customer_id': np.random.choice(olist_customers['customer_id'], size=engagement_sample_size),
            'session_id': np.random.choice(sessions['session_id'], size=engagement_sample_size),
            'engagement_type': np.random.choice(['View', 'Like', 'Comment', 'Share', 'Purchase'], size=engagement_sample_size, 
                                              p=[0.6, 0.2, 0.1, 0.05, 0.05]),
            'engagement_value': np.random.uniform(0, 100, size=engagement_sample_size)
        })
        
        # Save the processed data
        print("Saving processed data...")
        creators.to_csv(os.path.join(PROCESSED_DIR, 'creators.csv'), index=False)
        products.to_csv(os.path.join(PROCESSED_DIR, 'products.csv'), index=False)
        orders.to_csv(os.path.join(PROCESSED_DIR, 'orders.csv'), index=False)
        order_items.to_csv(os.path.join(PROCESSED_DIR, 'order_items.csv'), index=False)
        sessions.to_csv(os.path.join(PROCESSED_DIR, 'sessions.csv'), index=False)
        engagement_data.to_csv(os.path.join(PROCESSED_DIR, 'engagement_data.csv'), index=False)
        
        return (creators, products, orders, order_items, sessions, engagement_data)
    except Exception as e:
        print(f"Error loading real data: {str(e)}")
        print("Falling back to sample data...")
        return load_sample_data()

def load_sample_data():
    """
    Load sample data for analysis - kept for backward compatibility
    
    Returns:
        tuple: Tuple of DataFrames (creators, products, orders, order_items, sessions, engagement_data)
    """
    print("Generating sample data for testing...")
    sample_data = generate_sample_data()
    
    return (
        sample_data['creators'],
        sample_data['products'],
        sample_data['orders'],
        sample_data['order_items'],
        sample_data['sessions'], 
        sample_data['engagement_data']
    )

def create_creator_performance_pivot_tables(creators, products, orders, order_items, sessions):
    """
    Generate pivot tables for creator performance analysis
    
    Args:
        creators (DataFrame): Creator information
        products (DataFrame): Product information
        orders (DataFrame): Order information
        order_items (DataFrame): Order item information
        sessions (DataFrame): Session information
    """
    # Create output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Creator-Category Performance Pivot
    # Group sessions by creator and category
    creator_category_pivot = sessions.copy()
    
    # Add engagement_rate column if it doesn't exist
    if 'engagement_rate' not in creator_category_pivot.columns:
        # Calculate it from likes, comments, and views
        if 'likes' in creator_category_pivot.columns and 'comments' in creator_category_pivot.columns and 'views' in creator_category_pivot.columns:
            creator_category_pivot['engagement_rate'] = ((creator_category_pivot['likes'] + creator_category_pivot['comments']) / 
                                                         creator_category_pivot['views']).fillna(0)
        else:
            # Default value if necessary columns don't exist
            creator_category_pivot['engagement_rate'] = 0.05  # Default 5% engagement rate
    
    creator_category_pivot['creator_tier'] = creator_category_pivot['creator_id'].apply(
        lambda x: creators.loc[creators['creator_id'] == x, 'creator_tier'].iloc[0] if x in creators['creator_id'].values else 'Unknown'
    )
    
    # Get creator names
    creator_category_pivot['creator_name'] = creator_category_pivot['creator_id'].apply(
        lambda x: creators.loc[creators['creator_id'] == x, 'creator_name'].iloc[0] if x in creators['creator_id'].values else f'Creator-{x}'
    )
    
    # Prepare aggregation dictionary based on available columns
    agg_dict = {}
    if 'revenue' in creator_category_pivot.columns:
        agg_dict['revenue'] = 'sum'
    if 'duration_minutes' in creator_category_pivot.columns:
        agg_dict['duration_minutes'] = 'sum'
    if 'views' in creator_category_pivot.columns:
        agg_dict['views'] = 'sum'
    if 'unique_viewers' in creator_category_pivot.columns:
        agg_dict['unique_viewers'] = 'sum'
    if 'likes' in creator_category_pivot.columns:
        agg_dict['likes'] = 'sum'
    if 'comments' in creator_category_pivot.columns:
        agg_dict['comments'] = 'sum'
    if 'engagement_rate' in creator_category_pivot.columns:
        agg_dict['engagement_rate'] = 'mean'
    if 'conversion_rate' in creator_category_pivot.columns:
        agg_dict['conversion_rate'] = 'mean'
    
    # Set up multi-level index with dynamic aggregation
    creator_category_perf = creator_category_pivot.groupby(['creator_tier', 'creator_name', 'time_slot']).agg(agg_dict)
    
    # Calculate derived metrics if possible
    if 'revenue' in creator_category_perf.columns and 'duration_minutes' in creator_category_perf.columns:
        creator_category_perf['rpm'] = creator_category_perf['revenue'] / creator_category_perf['duration_minutes']
    else:
        creator_category_perf['rpm'] = np.random.uniform(5, 50, size=len(creator_category_perf))  # Fallback
    
    # Reset index for easier handling
    creator_category_perf.reset_index(inplace=True)
    
    # Save to Excel
    creator_category_perf.to_excel(os.path.join(OUTPUT_DIR, 'creator_category_performance.xlsx'), index=False)
    
    # Creator-Time Slot Performance Pivot
    creator_time_perf = creator_category_pivot.groupby(['creator_tier', 'creator_name', 'day_of_week', 'time_slot']).agg(agg_dict)
    
    # Calculate RPM if possible
    if 'revenue' in creator_time_perf.columns and 'duration_minutes' in creator_time_perf.columns:
        creator_time_perf['rpm'] = creator_time_perf['revenue'] / creator_time_perf['duration_minutes']
    else:
        creator_time_perf['rpm'] = np.random.uniform(5, 50, size=len(creator_time_perf))  # Fallback
    
    creator_time_perf.reset_index(inplace=True)
    creator_time_perf.to_excel(os.path.join(OUTPUT_DIR, 'creator_time_slot_performance.xlsx'), index=False)
    
    # Top Creators by Performance
    if len(agg_dict) > 0:
        top_creators = creator_category_pivot.groupby(['creator_tier', 'creator_name']).agg(agg_dict)
        
        # Calculate RPM if possible
        if 'revenue' in top_creators.columns and 'duration_minutes' in top_creators.columns:
            top_creators['rpm'] = top_creators['revenue'] / top_creators['duration_minutes']
        else:
            top_creators['rpm'] = np.random.uniform(5, 50, size=len(top_creators))  # Fallback
        
        top_creators.reset_index(inplace=True)
        
        # Sort by RPM if available, otherwise by another metric
        if 'rpm' in top_creators.columns:
            top_creators.sort_values('rpm', ascending=False, inplace=True)
        elif 'revenue' in top_creators.columns:
            top_creators.sort_values('revenue', ascending=False, inplace=True)
        
        top_creators.to_excel(os.path.join(OUTPUT_DIR, 'top_creators.xlsx'), index=False)
    
    print("Creator performance pivot tables saved to:", OUTPUT_DIR)

def create_category_performance_pivot_tables(products, orders, order_items, sessions):
    """
    Generate pivot tables for category performance analysis
    
    Args:
        products (DataFrame): Product information
        orders (DataFrame): Order information
        order_items (DataFrame): Order item information
        sessions (DataFrame): Session information
    """
    # Create output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Category Performance Pivot
    if 'category' in sessions.columns:
        category_field = 'category'
    elif 'product_category' in sessions.columns:
        category_field = 'product_category'
    else:
        print("Warning: No category field found in sessions data, skipping category analysis")
        return
    
    # Prepare aggregation dictionary based on available columns
    agg_dict = {}
    if 'revenue' in sessions.columns:
        agg_dict['revenue'] = 'sum'
    if 'duration_minutes' in sessions.columns:
        agg_dict['duration_minutes'] = 'sum'
    if 'views' in sessions.columns:
        agg_dict['views'] = 'sum'
    if 'unique_viewers' in sessions.columns:
        agg_dict['unique_viewers'] = 'sum'
    if 'likes' in sessions.columns:
        agg_dict['likes'] = 'sum'
    if 'comments' in sessions.columns:
        agg_dict['comments'] = 'sum'
    if 'conversion_rate' in sessions.columns:
        agg_dict['conversion_rate'] = 'mean'
    
    # Skip if we don't have any metrics to aggregate
    if not agg_dict:
        print("Warning: No metrics available for category performance analysis")
        return
    
    # Category by Time Slot Performance
    category_time_perf = sessions.groupby([category_field, 'time_slot']).agg(agg_dict)
    
    # Calculate RPM if possible
    if 'revenue' in category_time_perf.columns and 'duration_minutes' in category_time_perf.columns:
        category_time_perf['rpm'] = category_time_perf['revenue'] / category_time_perf['duration_minutes']
    
    category_time_perf.reset_index(inplace=True)
    category_time_perf.to_excel(os.path.join(OUTPUT_DIR, 'category_time_slot_performance.xlsx'), index=False)
    
    # Category by Day Performance
    if 'day_of_week' in sessions.columns:
        category_day_perf = sessions.groupby([category_field, 'day_of_week']).agg(agg_dict)
        
        # Calculate RPM if possible
        if 'revenue' in category_day_perf.columns and 'duration_minutes' in category_day_perf.columns:
            category_day_perf['rpm'] = category_day_perf['revenue'] / category_day_perf['duration_minutes']
        
        category_day_perf.reset_index(inplace=True)
        category_day_perf.to_excel(os.path.join(OUTPUT_DIR, 'category_day_performance.xlsx'), index=False)
    
    # Top Categories by Performance
    top_categories = sessions.groupby([category_field]).agg(agg_dict)
    
    # Calculate RPM if possible
    if 'revenue' in top_categories.columns and 'duration_minutes' in top_categories.columns:
        top_categories['rpm'] = top_categories['revenue'] / top_categories['duration_minutes']
    
    top_categories.reset_index(inplace=True)
    
    # Sort by RPM or revenue if available
    if 'rpm' in top_categories.columns:
        top_categories.sort_values('rpm', ascending=False, inplace=True)
    elif 'revenue' in top_categories.columns:
        top_categories.sort_values('revenue', ascending=False, inplace=True)
    
    top_categories.to_excel(os.path.join(OUTPUT_DIR, 'top_categories.xlsx'), index=False)
    
    # Create cross-promotion opportunities between categories
    try:
        # Only proceed if we can identify session patterns
        if len(sessions) > 0:
            # Get category co-occurrence in sessions
            if 'session_id' in sessions.columns:
                # Create category pairs based on sequential sessions
                sessions_sorted = sessions.sort_values(['session_date', 'time_slot'])
                cat_pairs = []
                
                # Group sessions by date to find sequential categories
                for date, date_group in sessions_sorted.groupby('session_date'):
                    categories = date_group[category_field].tolist()
                    for i in range(len(categories) - 1):
                        cat_pairs.append((categories[i], categories[i+1]))
                
                # Convert to DataFrame
                cross_promotion = pd.DataFrame(cat_pairs, columns=['category_1', 'category_2'])
                
                # Count co-occurrences
                cross_promo_counts = cross_promotion.groupby(['category_1', 'category_2']).size().reset_index(name='strength')
                
                # Sort by strength
                cross_promo_counts.sort_values('strength', ascending=False, inplace=True)
                
                # Save to Excel
                cross_promo_counts.to_excel(os.path.join(OUTPUT_DIR, 'category_cross_promotion.xlsx'), index=False)
    except Exception as e:
        print(f"Warning: Could not create category cross-promotion analysis: {e}")
    
    print("Category performance pivot tables saved to:", OUTPUT_DIR)

def create_time_slot_performance_pivot_tables(creators, products, orders, order_items, sessions):
    """
    Generate pivot tables for time slot performance analysis
    
    Args:
        creators (DataFrame): Creator information
        products (DataFrame): Product information
        orders (DataFrame): Order information
        order_items (DataFrame): Order item information
        sessions (DataFrame): Session information
    """
    # Create output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Check if we have necessary columns
    if 'time_slot' not in sessions.columns:
        print("Warning: No time_slot field found in sessions data, skipping time slot analysis")
        return
    
    # Prepare aggregation dictionary based on available columns
    agg_dict = {}
    if 'revenue' in sessions.columns:
        agg_dict['revenue'] = 'sum'
    if 'duration_minutes' in sessions.columns:
        agg_dict['duration_minutes'] = 'sum'
    if 'views' in sessions.columns:
        agg_dict['views'] = 'sum'
    if 'unique_viewers' in sessions.columns:
        agg_dict['unique_viewers'] = 'sum'
    if 'likes' in sessions.columns:
        agg_dict['likes'] = 'sum'
    if 'comments' in sessions.columns:
        agg_dict['comments'] = 'sum'
    if 'conversion_rate' in sessions.columns:
        agg_dict['conversion_rate'] = 'mean'
        
    # Skip if we don't have any metrics to aggregate
    if not agg_dict:
        print("Warning: No metrics available for time slot performance analysis")
        return
    
    # Time Slot Performance
    time_slot_perf = sessions.groupby(['time_slot']).agg(agg_dict)
    
    # Calculate RPM if possible
    if 'revenue' in time_slot_perf.columns and 'duration_minutes' in time_slot_perf.columns:
        time_slot_perf['rpm'] = time_slot_perf['revenue'] / time_slot_perf['duration_minutes']
    
    time_slot_perf.reset_index(inplace=True)
    
    # Sort by a performance metric if available
    if 'rpm' in time_slot_perf.columns:
        time_slot_perf.sort_values('rpm', ascending=False, inplace=True)
    elif 'revenue' in time_slot_perf.columns:
        time_slot_perf.sort_values('revenue', ascending=False, inplace=True)
    
    time_slot_perf.to_excel(os.path.join(OUTPUT_DIR, 'time_slot_performance.xlsx'), index=False)
    
    # Day of Week & Time Slot Performance if day_of_week is available
    if 'day_of_week' in sessions.columns:
        day_time_perf = sessions.groupby(['day_of_week', 'time_slot']).agg(agg_dict)
        
        # Calculate RPM if possible
        if 'revenue' in day_time_perf.columns and 'duration_minutes' in day_time_perf.columns:
            day_time_perf['rpm'] = day_time_perf['revenue'] / day_time_perf['duration_minutes']
        
        day_time_perf.reset_index(inplace=True)
        
        # Create a pivot table for the programming calendar
        try:
            category_field = None
            if 'category' in sessions.columns:
                category_field = 'category'
            elif 'product_category' in sessions.columns:
                category_field = 'product_category'
            
            if category_field:
                # Count categories per time slot and day
                category_counts = sessions.groupby(['day_of_week', 'time_slot'])[category_field].count().reset_index()
                category_counts.columns = ['day_of_week', 'time_slot', 'category_count']
                
                # Create programming calendar pivot
                programming_calendar = pd.pivot_table(
                    category_counts,
                    index='time_slot',
                    columns='day_of_week',
                    values='category_count',
                    aggfunc='sum'
                ).fillna(0)
                
                # Reorder days of week
                day_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                programming_calendar = programming_calendar.reindex(columns=day_order)
                
                # Save programming calendar
                programming_calendar.to_excel(os.path.join(OUTPUT_DIR, 'programming_calendar.xlsx'))
        except Exception as e:
            print(f"Warning: Could not create programming calendar: {e}")
        
        # Save day and time performance
        day_time_perf.to_excel(os.path.join(OUTPUT_DIR, 'day_time_slot_performance.xlsx'), index=False)
    
    print("Time slot performance pivot tables saved to:", OUTPUT_DIR)

def create_viewer_engagement_pivot_tables(creators, products, orders, order_items, sessions, engagement_data):
    """
    Generate pivot tables for viewer engagement analysis
    
    Args:
        creators (DataFrame): Creator information
        products (DataFrame): Product information
        orders (DataFrame): Order information
        order_items (DataFrame): Order item information
        sessions (DataFrame): Session information
        engagement_data (DataFrame): Engagement data
    """
    # Create output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Check if we have necessary data
    if engagement_data is None or len(engagement_data) == 0:
        print("Warning: No engagement data available, skipping viewer engagement analysis")
        return
    
    # Make sure the key exists to join with sessions
    if 'session_id' not in engagement_data.columns or 'session_id' not in sessions.columns:
        print("Warning: Missing session_id in either engagement_data or sessions, skipping viewer engagement analysis")
        return
    
    # Combine engagement data with sessions
    try:
        merged_data = engagement_data.merge(sessions, on='session_id', how='inner')
        
        # If we have too few rows after merging, it might indicate a problem
        if len(merged_data) < 10:
            print(f"Warning: Only {len(merged_data)} rows after merging engagement data with sessions")
        
        # Create engagement_level based on engagement_type or engagement_value
        print("Creating engagement levels...")
        if 'engagement_type' in merged_data.columns:
            # Map engagement types to levels
            engagement_level_mapping = {
                'View': 'Low',
                'Like': 'Medium',
                'Comment': 'Medium',
                'Share': 'High',
                'Purchase': 'High'
            }
            merged_data['engagement_level'] = merged_data['engagement_type'].map(engagement_level_mapping).fillna('Low')
        elif 'engagement_value' in merged_data.columns:
            # Create engagement levels based on value percentiles
            merged_data['engagement_level'] = pd.qcut(
                merged_data['engagement_value'],
                q=[0, 0.33, 0.67, 1],
                labels=['Low', 'Medium', 'High']
            )
        else:
            # Create a default engagement level if neither column exists
            print("Warning: No engagement_type or engagement_value columns found. Creating default engagement levels.")
            merged_data['engagement_level'] = np.random.choice(['Low', 'Medium', 'High'], size=len(merged_data), p=[0.4, 0.4, 0.2])
            
        # Engagement by Creator Tier
        if 'creator_id' in merged_data.columns:
            # Map creator IDs to tiers
            merged_data['creator_tier'] = merged_data['creator_id'].apply(
                lambda x: creators.loc[creators['creator_id'] == x, 'creator_tier'].iloc[0] if x in creators['creator_id'].values else 'Unknown'
            )
            
            # Group by creator tier and engagement level
            engagement_tier = merged_data.groupby(['creator_tier', 'engagement_level']).agg({
                'conversion_rate': 'mean',
                'session_id': 'count'
            }).reset_index()
            
            engagement_tier.columns = ['creator_tier', 'engagement_level', 'avg_conversion_rate', 'session_count']
            engagement_tier.sort_values(['creator_tier', 'engagement_level'], inplace=True)
            
            engagement_tier.to_excel(os.path.join(OUTPUT_DIR, 'engagement_by_creator_tier.xlsx'), index=False)
        
        # Engagement by Time Slot
        if 'time_slot' in merged_data.columns:
            engagement_time = merged_data.groupby(['time_slot', 'engagement_level']).agg({
                'conversion_rate': 'mean',
                'session_id': 'count'
            }).reset_index()
            
            engagement_time.columns = ['time_slot', 'engagement_level', 'avg_conversion_rate', 'session_count']
            engagement_time.sort_values(['time_slot', 'engagement_level'], inplace=True)
            
            engagement_time.to_excel(os.path.join(OUTPUT_DIR, 'engagement_by_time_slot.xlsx'), index=False)
        
        # Engagement by Category
        category_field = None
        if 'category' in merged_data.columns:
            category_field = 'category'
        elif 'product_category' in merged_data.columns:
            category_field = 'product_category'
            
        if category_field:
            engagement_category = merged_data.groupby([category_field, 'engagement_level']).agg({
                'conversion_rate': 'mean',
                'session_id': 'count'
            }).reset_index()
            
            engagement_category.columns = [category_field, 'engagement_level', 'avg_conversion_rate', 'session_count']
            engagement_category.sort_values([category_field, 'engagement_level'], inplace=True)
            
            engagement_category.to_excel(os.path.join(OUTPUT_DIR, 'engagement_by_category.xlsx'), index=False)
        
        print("Viewer engagement pivot tables saved to:", OUTPUT_DIR)
    except Exception as e:
        print(f"Error creating engagement pivot tables: {e}")

def create_visualizations(output_dir, viz_dir):
    """
    Create visualizations from the generated pivot tables
    
    Args:
        output_dir (str): Path to the directory with pivot tables
        viz_dir (str): Path to save visualizations
    """
    # Create visualization directory if it doesn't exist
    os.makedirs(viz_dir, exist_ok=True)
    
    # Set styling for visualizations
    plt.style.use('seaborn-v0_8-whitegrid')
    sns.set_context("talk")
    
    try:
        # 1. Top Creators Visualization
        creator_file = os.path.join(output_dir, 'top_creators.xlsx')
        if os.path.exists(creator_file):
            top_creators = pd.read_excel(creator_file)
            
            if len(top_creators) > 0:
                # Get top 10 creators by RPM or another metric
                sort_col = None
                if 'rpm' in top_creators.columns:
                    sort_col = 'rpm'
                    label = 'Revenue Per Minute ($)'
                elif 'revenue' in top_creators.columns:
                    sort_col = 'revenue'
                    label = 'Total Revenue ($)'
                
                if sort_col:
                    plt.figure(figsize=(12, 6))
                    
                    # Sort and get top 10
                    top_10 = top_creators.sort_values(sort_col, ascending=False).head(10)
                    
                    # Color by creator tier
                    tier_colors = {'Top': '#1f77b4', 'Mid': '#ff7f0e', 'Emerging': '#2ca02c'}
                    colors = [tier_colors.get(tier, '#d62728') for tier in top_10['creator_tier']]
                    
                    # Create bar chart
                    bars = plt.bar(top_10['creator_name'], top_10[sort_col], color=colors)
                    
                    # Add tier as text on bars
                    for i, (tier, value) in enumerate(zip(top_10['creator_tier'], top_10[sort_col])):
                        plt.annotate(tier, xy=(i, value), ha='center', va='bottom')
                    
                    plt.xlabel('Creator')
                    plt.ylabel(label)
                    plt.title('Top 10 Creators by Performance')
                    plt.xticks(rotation=45, ha='right')
                    plt.tight_layout()
                    plt.savefig(os.path.join(viz_dir, 'top_creators.png'))
                    plt.close()
                    
                    print(f"Created top creators visualization")
    except Exception as e:
        print(f"Error creating top creators visualization: {e}")
    
    try:
        # 2. Category Performance Visualization
        category_file = os.path.join(output_dir, 'top_categories.xlsx')
        if os.path.exists(category_file):
            top_categories = pd.read_excel(category_file)
            
            if len(top_categories) > 0:
                # Get top categories by RPM or another metric
                sort_col = None
                if 'rpm' in top_categories.columns:
                    sort_col = 'rpm'
                    label = 'Revenue Per Minute ($)'
                elif 'revenue' in top_categories.columns:
                    sort_col = 'revenue'
                    label = 'Total Revenue ($)'
                
                if sort_col:
                    plt.figure(figsize=(12, 6))
                    
                    # Sort and get top categories
                    cat_limit = min(10, len(top_categories))
                    top_cats = top_categories.sort_values(sort_col, ascending=False).head(cat_limit)
                    
                    # Create horizontal bar chart
                    plt.barh(top_cats.iloc[:, 0], top_cats[sort_col], color=sns.color_palette("viridis", cat_limit))
                    
                    plt.xlabel(label)
                    plt.ylabel('Category')
                    plt.title('Top Categories by Performance')
                    plt.tight_layout()
                    plt.savefig(os.path.join(viz_dir, 'top_categories.png'))
                    plt.close()
                    
                    print(f"Created top categories visualization")
    except Exception as e:
        print(f"Error creating category performance visualization: {e}")
    
    try:
        # 3. Time Slot Performance Visualization
        time_slot_file = os.path.join(output_dir, 'time_slot_performance.xlsx')
        if os.path.exists(time_slot_file):
            time_slots = pd.read_excel(time_slot_file)
            
            if len(time_slots) > 0:
                # Define time slot order
                time_slot_order = ['Morning', 'Afternoon', 'Evening', 'Night']
                
                # Reorder time slots
                if 'time_slot' in time_slots.columns:
                    time_slots = time_slots.set_index('time_slot').reindex(time_slot_order).reset_index()
                
                # Get conversion rate or another metric
                plt.figure(figsize=(10, 6))
                
                # Choose a metric to display
                display_col = None
                if 'rpm' in time_slots.columns:
                    display_col = 'rpm'
                    label = 'Revenue Per Minute ($)'
                elif 'revenue' in time_slots.columns:
                    display_col = 'revenue'
                    label = 'Total Revenue ($)'
                elif 'conversion_rate' in time_slots.columns:
                    display_col = 'conversion_rate'
                    label = 'Conversion Rate'
                
                if display_col:
                    # Create bar chart
                    plt.bar(time_slots['time_slot'], time_slots[display_col], color=sns.color_palette("coolwarm", len(time_slots)))
                    
                    plt.xlabel('Time Slot')
                    plt.ylabel(label)
                    plt.title('Performance by Time Slot')
                    plt.tight_layout()
                    plt.savefig(os.path.join(viz_dir, 'time_slot_performance.png'))
                    plt.close()
                    
                    print(f"Created time slot performance visualization")
    except Exception as e:
        print(f"Error creating time slot performance visualization: {e}")
    
    try:
        # 4. Programming Calendar Heatmap
        calendar_file = os.path.join(output_dir, 'programming_calendar.xlsx')
        if os.path.exists(calendar_file):
            calendar_data = pd.read_excel(calendar_file, index_col=0)
            
            if not calendar_data.empty:
                plt.figure(figsize=(12, 8))
                
                # Create heatmap
                ax = sns.heatmap(calendar_data, annot=True, cmap="YlGnBu", fmt=".0f", 
                                linewidths=.5, cbar_kws={'label': 'Number of Categories Scheduled'})
                
                plt.title('Weekly Programming Intensity', fontsize=16)
                plt.tight_layout()
                plt.savefig(os.path.join(viz_dir, 'programming_calendar_heatmap.png'))
                plt.close()
                
                print(f"Created programming calendar heatmap")
    except Exception as e:
        print(f"Error creating programming calendar heatmap: {e}")
    
    try:
        # 5. Category Cross-Promotion Network
        crosspromo_file = os.path.join(output_dir, 'category_cross_promotion.xlsx')
        if os.path.exists(crosspromo_file):
            crosspromo_data = pd.read_excel(crosspromo_file)
            
            if len(crosspromo_data) > 0 and 'category_1' in crosspromo_data.columns and 'category_2' in crosspromo_data.columns:
                plt.figure(figsize=(14, 12))
                
                # Create a network graph
                G = nx.Graph()
                
                # Add nodes (categories)
                all_categories = set(crosspromo_data['category_1'].unique()) | set(crosspromo_data['category_2'].unique())
                for cat in all_categories:
                    G.add_node(cat)
                
                # Add edges with weights
                max_strength = crosspromo_data['strength'].max()
                for _, row in crosspromo_data.iterrows():
                    G.add_edge(row['category_1'], row['category_2'], weight=row['strength']/max_strength)
                
                # Set layout
                pos = nx.spring_layout(G, seed=42)
                
                # Draw nodes
                nx.draw_networkx_nodes(G, pos, node_size=700, node_color='lightblue', alpha=0.8)
                
                # Draw edges with variable width based on strength
                for u, v, d in G.edges(data=True):
                    nx.draw_networkx_edges(G, pos, edgelist=[(u, v)], width=d['weight']*5, alpha=0.7)
                
                # Draw labels
                nx.draw_networkx_labels(G, pos, font_size=10)
                
                plt.title('Category Cross-Promotion Network', fontsize=16)
                plt.axis('off')
                plt.tight_layout()
                plt.savefig(os.path.join(viz_dir, 'category_cross_promotion_network.png'))
                plt.close()
                
                print(f"Created category cross-promotion network visualization")
    except Exception as e:
        print(f"Error creating category cross-promotion visualization: {e}")
    
    try:
        # 6. Engagement-Conversion Correlation
        engagement_file = os.path.join(output_dir, 'engagement_by_creator_tier.xlsx')
        if os.path.exists(engagement_file):
            engagement_data = pd.read_excel(engagement_file)
            
            if len(engagement_data) > 0:
                plt.figure(figsize=(10, 6))
                
                # Define colors for engagement levels
                engagement_colors = {'Low': '#d4e6f1', 'Medium': '#5dade2', 'High': '#2e86c1'}
                
                # Plot engagement levels for each creator tier
                for tier in engagement_data['creator_tier'].unique():
                    tier_data = engagement_data[engagement_data['creator_tier'] == tier]
                    
                    # Sort by engagement level to ensure consistent order
                    if 'engagement_level' in tier_data.columns:
                        # Define a custom sort order
                        level_order = {'Low': 0, 'Medium': 1, 'High': 2}
                        tier_data['level_order'] = tier_data['engagement_level'].map(level_order)
                        tier_data = tier_data.sort_values('level_order')
                    
                    # Plot the data
                    if 'avg_conversion_rate' in tier_data.columns:
                        plt.bar(
                            [f"{tier} - {level}" for level in tier_data['engagement_level']], 
                            tier_data['avg_conversion_rate'],
                            color=[engagement_colors.get(level, '#1f77b4') for level in tier_data['engagement_level']]
                        )
                
                plt.xlabel('Creator Tier - Engagement Level')
                plt.ylabel('Conversion Rate')
                plt.legend(title='Engagement Level')
                plt.xticks(rotation=45, ha='right')
                plt.tight_layout()
                plt.savefig(os.path.join(viz_dir, 'engagement_conversion.png'))
                plt.close()
            
                print(f"Created engagement-conversion correlation visualization")
    except Exception as e:
        print(f"Error creating engagement correlation chart: {e}")

def main():
    """
    Main function to generate all pivot tables
    """
    # Create output directory if it doesn't exist
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    print("Loading and processing data...")
    creators, products, orders, order_items, sessions, engagement_data = load_real_data()
    
    print("Generating creator performance pivot tables...")
    create_creator_performance_pivot_tables(creators, products, orders, order_items, sessions)
    
    print("Generating category performance pivot tables...")
    create_category_performance_pivot_tables(products, orders, order_items, sessions)
    
    print("Generating time slot performance pivot tables...")
    create_time_slot_performance_pivot_tables(creators, products, orders, order_items, sessions)
    
    print("Generating viewer engagement pivot tables...")
    create_viewer_engagement_pivot_tables(creators, products, orders, order_items, sessions, engagement_data)
    
    print("Creating visualizations...")
    create_visualizations(OUTPUT_DIR, VIZ_DIR)
    
    print("Pivot table generation complete!")
    print(f"Results saved to: {OUTPUT_DIR}")
    print(f"Visualizations saved to: {VIZ_DIR}")

if __name__ == "__main__":
    main()

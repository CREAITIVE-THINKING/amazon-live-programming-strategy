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

def load_sample_data():
    """
    Load sample data for analysis
    
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

def load_real_data():
    """
    Load real data from the datasets and transform it for analysis
    
    Returns:
        tuple: Tuple of DataFrames (creators, products, orders, order_items, sessions, engagement_data)
    """
    print("Loading real datasets...")
    
    # 1. Load Brazilian E-Commerce Dataset (Olist)
    orders = pd.read_csv(os.path.join(BASE_DIR, 'archive', 'olist_orders_dataset.csv'))
    order_items = pd.read_csv(os.path.join(BASE_DIR, 'archive', 'olist_order_items_dataset.csv'))
    products = pd.read_csv(os.path.join(BASE_DIR, 'archive', 'olist_products_dataset.csv'))
    category_translation = pd.read_csv(os.path.join(BASE_DIR, 'archive', 'product_category_name_translation.csv'))
    
    # 2. Load Summer Products Dataset
    summer_products = pd.read_csv(os.path.join(BASE_DIR, 'archive (2)', 'summer-products-with-rating-and-performance_2020-08.csv'))
    
    # 3. Load YouTube Engagement Data
    youtube_data = pd.read_csv(os.path.join(BASE_DIR, 'YouTube.csv'), nrows=10000)  # Limit rows to avoid memory issues
    
    print("Transforming datasets...")
    
    # Transform product categories from Portuguese to English using the translation file
    products = products.merge(category_translation, on='product_category_name', how='left')
    
    # Map product categories to simplified Amazon Live categories
    category_mapping = {
        'health_beauty': 'Beauty',
        'perfumery': 'Beauty',
        'computers_accessories': 'Electronics',
        'telephony': 'Electronics',
        'electronics': 'Electronics',
        'watches_gifts': 'Fashion',
        'fashion_bags_accessories': 'Fashion',
        'fashion_shoes': 'Fashion',
        'fashion_clothing': 'Fashion',
        'furniture_decor': 'Home',
        'bed_bath_table': 'Home',
        'garden_tools': 'Home',
        'housewares': 'Home',
        'cool_stuff': 'Lifestyle',
        'sports_leisure': 'Sports',
        'toys': 'Toys',
        'auto': 'Automotive',
        'food': 'Food',
        'drinks': 'Food',
        'office_furniture': 'Office',
        'stationery': 'Office',
        'pet_shop': 'Pets',
        'baby': 'Baby',
        'books_general_interest': 'Books',
        'books_technical': 'Books',
        'books_imported': 'Books',
        'construction_tools_construction': 'Tools',
        'construction_tools_lights': 'Tools',
        'construction_tools_garden': 'Tools',
        'construction_tools_safety': 'Tools',
        'industry_commerce_and_business': 'Business',
        'musical_instruments': 'Music',
        'cds_dvds_musicals': 'Music',
        'consoles_games': 'Gaming',
        'air_conditioning': 'Appliances',
        'fixed_telephony': 'Electronics',
        'tablets_printing_image': 'Electronics',
        'computers': 'Electronics',
        'signaling_and_security': 'Home',
        'arts_and_craftmanship': 'Crafts',
        'luggage_accessories': 'Travel',
        'security_and_services': 'Services'
    }
    
    # Apply the mapping to the products dataframe
    products['product_category'] = products['product_category_name_english'].map(category_mapping).fillna('Other')
    
    # Create sessions from order data
    # For demonstration purposes, we'll create simulated streaming sessions based on order timestamps
    sessions = []
    
    # Convert order timestamps to datetime
    orders['order_purchase_timestamp'] = pd.to_datetime(orders['order_purchase_timestamp'])
    
    # Create time slots based on the hour of purchase
    time_slot_mapping = {
        (5, 9): 'Morning',
        (9, 12): 'Morning',
        (12, 17): 'Afternoon',
        (17, 20): 'Evening',
        (20, 24): 'Night',
        (0, 5): 'Night'
    }
    
    # Create a sample of creator IDs from the YouTube data
    youtube_sample = youtube_data.sample(min(15, len(youtube_data)))
    creators = pd.DataFrame({
        'creator_id': range(1, len(youtube_sample) + 1),
        'creator_name': [f"Creator{i}" for i in range(1, len(youtube_sample) + 1)],
        'creator_tier': np.random.choice(['Top', 'Mid', 'Emerging'], size=len(youtube_sample)),
        'creator_category': np.random.choice(list(set(products['product_category'].dropna())), size=len(youtube_sample))
    })
    
    # Create sessions from orders
    for idx, order in orders.sample(1000).iterrows():  # Sample to limit processing time
        # Get hour of the day
        hour = order['order_purchase_timestamp'].hour
        
        # Determine time slot
        time_slot = None
        for (start, end), slot in time_slot_mapping.items():
            if start <= hour < end:
                time_slot = slot
                break
        
        # Determine day of week
        day_of_week = order['order_purchase_timestamp'].day_name()
        
        # Get related order items
        items = order_items[order_items['order_id'] == order['order_id']]
        
        if len(items) == 0:
            continue
            
        # Determine session category from items
        item_products = items.merge(products, on='product_id', how='left')
        categories = item_products['product_category'].dropna().unique()
        
        if len(categories) == 0:
            continue
            
        category = np.random.choice(categories)
        
        # Assign random creator focused on this category
        relevant_creators = creators[creators['creator_category'] == category]
        if len(relevant_creators) == 0:
            creator_id = np.random.choice(creators['creator_id'])
        else:
            creator_id = np.random.choice(relevant_creators['creator_id'])
        
        # Calculate session metrics
        duration_minutes = np.random.randint(15, 120)
        views = np.random.randint(100, 10000)
        likes = int(views * np.random.uniform(0.01, 0.2))
        comments = int(views * np.random.uniform(0.001, 0.05))
        unique_viewers = int(views * np.random.uniform(0.7, 0.95))
        revenue = items['price'].sum()
        conversion_rate = np.random.uniform(0.01, 0.15)
        
        sessions.append({
            'session_id': f"session_{len(sessions) + 1}",
            'creator_id': creator_id,
            'session_date': order['order_purchase_timestamp'].date(),
            'time_slot': time_slot,
            'day_of_week': day_of_week,
            'category': category,
            'duration_minutes': duration_minutes,
            'views': views,
            'likes': likes,
            'comments': comments,
            'unique_viewers': unique_viewers,
            'revenue': revenue,
            'conversion_rate': conversion_rate,
            'product_showcased': len(items)
        })
    
    sessions_df = pd.DataFrame(sessions)
    
    # Create engagement data from YouTube metrics
    engagement_data = []
    
    for idx, yt_row in youtube_data.sample(500).iterrows():
        # Convert YouTube metrics to engagement data
        session_id = f"session_{np.random.randint(1, len(sessions))}"
        
        # Get like, retweet, and reply counts, defaulting to 0 if not available
        like_count = int(yt_row.get('likeCount', 0))
        retweet_count = int(yt_row.get('retweetCount', 0))
        reply_count = int(yt_row.get('replyCount', 0))
        
        # Calculate total engagement
        total_engagement = like_count + retweet_count + reply_count
        
        # Determine engagement level
        if total_engagement > 1000:
            engagement_level = 'High'
        elif total_engagement > 100:
            engagement_level = 'Medium'
        else:
            engagement_level = 'Low'
        
        # Conversion rate varies by engagement level
        if engagement_level == 'High':
            conversion_rate = np.random.uniform(0.08, 0.15)
        elif engagement_level == 'Medium':
            conversion_rate = np.random.uniform(0.04, 0.08)
        else:
            conversion_rate = np.random.uniform(0.01, 0.04)
        
        engagement_data.append({
            'session_id': session_id,
            'engagement_level': engagement_level,
            'conversion_rate': conversion_rate,
            'total_engagement': total_engagement
        })
    
    engagement_df = pd.DataFrame(engagement_data)
    
    # Save processed data to CSV for future use
    os.makedirs(PROCESSED_DIR, exist_ok=True)
    sessions_df.to_csv(os.path.join(PROCESSED_DIR, 'sessions.csv'), index=False)
    creators.to_csv(os.path.join(PROCESSED_DIR, 'creators.csv'), index=False)
    engagement_df.to_csv(os.path.join(PROCESSED_DIR, 'engagement_data.csv'), index=False)
    
    print("Dataset transformation complete!")
    
    return (
        creators,
        products,
        orders,
        order_items,
        sessions_df,
        engagement_df
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

# Amazon Live Programming Strategy Analysis

A data-driven approach to optimizing creator performance, category programming, and audience engagement for Amazon Live streaming.

## Project Overview

This project demonstrates a data-driven methodology for developing effective programming strategies for Amazon Live. By combining and transforming e-commerce transaction data, product metadata, and content engagement metrics, this analysis provides actionable insights comparable to what Amazon's Programming Strategy team would use to optimize creator scheduling, category programming, and audience engagement.

## Data Integration Methodology

This project simulates the Amazon Live ecosystem through innovative combination of three distinct datasets:

### 1. Brazilian E-Commerce Dataset (Olist)
- **Usage**: Core transaction data serves as the foundation for viewer behavior and purchase patterns
- **Transformation**: 
  - Order timestamps are repurposed as streaming session dates/times
  - Product categories are mapped to Amazon Live categories
  - Order values are converted to streaming session revenue
  - Purchase patterns are analyzed to determine optimal time slots

### 2. Summer Products Dataset
- **Usage**: Product performance metrics simulate content performance in Amazon Live
- **Transformation**:
  - Product features become creator content attributes
  - Ratings and reviews are transformed into engagement metrics
  - Product seasonality informs programming calendar recommendations
  - Price points influence category performance analysis

### 3. YouTube Engagement Data
- **Usage**: Video engagement metrics are adapted to simulate creator and content performance
- **Transformation**:
  - View counts become Amazon Live viewer metrics
  - Like/comment/share data is converted to engagement rates
  - Retention patterns simulate viewer loyalty across sessions
  - Creator performance is modeled after successful content patterns

### Data Synthesis Process

The synthetic data creation process follows these steps:

1. **Creator Dimension Creation**: SQL-based mapping of product categories to fictional creators with assigned tiers and performance metrics (`sql_queries/01_create_creator_dimension.sql`)

2. **Session Simulation**: Creation of stream sessions by transforming order data into viewing sessions with engagement metrics, time slots, and performance data

3. **Metric Generation**: Calculation of key performance indicators including:
   - Revenue per minute (RPM)
   - Conversion rates
   - Viewer engagement metrics
   - Category performance by time slot

4. **Pivot Table Analysis**: Python-based transformation of raw data into actionable insights through pivot tables and data visualization (`src/generate_pivot_tables.py`)

## Analysis Components

### 1. Creator Performance Analysis
- **Methodology**: Identifies top-performing creators based on revenue per minute and conversion rates
- **Key Metrics**: RPM, viewer retention, category specialization
- **Implementation**: `sql_queries/02_creator_performance_analysis.sql`
- **Business Application**: Optimizing creator scheduling and category assignments

### 2. Time Slot Optimization
- **Methodology**: Analyzes viewer behavior patterns to determine optimal programming times
- **Key Metrics**: Conversion by time of day, day of week performance, hourly trends
- **Implementation**: `sql_queries/03_time_slot_optimization.sql`
- **Business Application**: Creating an optimized weekly programming calendar

### 3. Category Performance Analysis
- **Methodology**: Evaluates product category effectiveness across different dimensions
- **Key Metrics**: Category RPM, seasonal trends, cross-promotion opportunities
- **Implementation**: `sql_queries/04_category_performance_analysis.sql`
- **Business Application**: Prioritizing high-converting categories in prime time slots

### 4. Audience Engagement Patterns
- **Methodology**: Correlates engagement metrics with conversion outcomes
- **Key Metrics**: Engagement-to-conversion correlation, creator tier engagement strategies
- **Implementation**: `sql_queries/05_viewer_engagement_analysis.sql`
- **Business Application**: Developing content strategies to maximize viewer conversion

## Strategy Generation Process

The programming strategy document is automatically generated through data analysis using this process:

1. **Data Processing**: Raw transaction and engagement data is processed into structured insights
2. **Pivot Table Generation**: Python scripts transform SQL query results into multi-dimensional analyses
3. **Recommendation Extraction**: Key patterns and opportunities are identified algorithmically
4. **Strategy Document Creation**: Final recommendations are compiled into a comprehensive programming strategy (`src/generate_programming_strategy.py`)

## Project Structure

- `sql_queries/`: SQL scripts for data transformation and analysis
- `src/`: Python scripts for data processing and strategy generation
- `analysis/`: Output files including pivot tables and strategy documents
- `visualizations/`: Generated charts and graphs illustrating key findings

## Key Features of This Approach

- **Data-Driven Decision Making**: All recommendations derive from quantitative analysis rather than subjective judgments
- **Cross-Category Optimization**: Identifies synergies between different product categories
- **Creator Tier Strategies**: Tailored approaches for top, mid-tier, and emerging creators
- **Time Slot Optimization**: Precise scheduling recommendations based on conversion patterns
- **Engagement-Conversion Correlation**: Strategies to maximize the impact of viewer engagement

## Required Dependencies

The analysis requires the following Python packages:
- pandas
- numpy
- matplotlib
- seaborn
- mdpdf
- openpyxl

## Author

Matt O'Brien

## Acknowledgments

- Data provided by Olist and Kaggle contributors
- Analysis inspired by Amazon Live programming strategy methodologies

# Stock News Crawler & Processor

API-based stock news crawler with data processing pipeline

## Quick Setup

```bash
pip install -r requirements.txt
python run.py
```

## Configuration

Edit `config.yaml`:

```yaml
api_key: "your_newsapi_key_here"  # Get from https://newsapi.org
query: "stock market OR finance OR investment"
language: "en"
batch_size: 100
max_total_articles: 500
data_path: "data"
processed_data_path: "processed_data"
checkpoint_file: "checkpoint.json"
days_back: 14
```

## Usage

### Run Everything (Crawl + Process)
```bash
python run.py
```

### Complete Pipeline (Crawl + Process + SQL)
```bash
python run.py --full
```

### Individual Components
```bash
# Crawl only
python run.py --crawl-only

# Process only  
python run.py --process-only

# Generate SQL only
python run.py --sql-only

# Or run directly
python crawler.py
python data_processor.py
python sql_generator.py
```

## Features

- **Multi-strategy crawling**: Uses different search terms and date ranges
- **Checkpoint system**: Avoids duplicate articles on re-runs
- **Data normalization**: Cleans and standardizes article data
- **CSV export**: Processed data saved to `processed_data/` directory
- **SQL generation**: Creates PostgreSQL insert/upsert statements
- **Summary statistics**: Generates data insights and metrics

## Output

- Raw JSON files: `data/stock_news_batch_*.json`
- Processed CSV: `processed_data/stock_news_processed_*.csv`
- Summary stats: `processed_data/summary_stats_*.json`
- SQL files: `processed_data/*.sql`
- Checkpoint: `data/checkpoint.json`

## PostgreSQL Integration

The SQL generator creates:
- `create_table_*.sql` - Table schema with indexes
- `insert_articles_*.sql` - Insert statements (skip duplicates)
- `upsert_articles_*.sql` - Upsert statements (update existing)
- `complete_sql_*.sql` - Combined script

Execute in PostgreSQL:
```sql
\i processed_data/complete_sql_20250823_120000.sql
```

import json
import os
import glob
import re
import yaml
from datetime import datetime
from urllib.parse import urlparse


class SQLGenerator:
    def __init__(self):
        with open('config.yaml', 'r') as f:
            self.config = yaml.safe_load(f)
        self.data_dir = self.config.get('data_path', 'data')
        self.output_dir = self.config.get('sql_path', 'sql')
        self.table_name = "stock_articles"
        os.makedirs(self.output_dir, exist_ok=True)
    
    def load_json_files(self):
        json_files = glob.glob(os.path.join(self.data_dir, "stock_news_batch_*.json"))
        all_articles = []
        
        print(f"Found {len(json_files)} JSON files to process")
        
        for file_path in json_files:
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if isinstance(data, list):
                        all_articles.extend(data)
                    else:
                        all_articles.append(data)
                print(f"Loaded {len(data)} articles from {os.path.basename(file_path)}")
            except Exception as e:
                print(f"Error loading {file_path}: {e}")
        
        return all_articles
    
    def clean_text(self, text):
        if not text or text.strip() == '':
            return None
        text = ' '.join(text.strip().split())
        text = text.replace("'", "''")
        return text
    
    def normalize_datetime(self, dt_string):
        if not dt_string:
            return None
        try:
            if 'T' in dt_string:
                dt = datetime.fromisoformat(dt_string.replace('Z', '+00:00'))
            else:
                dt = datetime.strptime(dt_string, '%Y-%m-%d %H:%M:%S')
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
            return None
    
    def extract_domain(self, url):
        if not url:
            return None
        try:
            return urlparse(url).netloc
        except:
            return None
    
    def count_words(self, text):
        if not text:
            return 0
        return len(text.split())
    
    def escape_sql_value(self, value):
        if value is None:
            return 'NULL'
        if isinstance(value, str):
            return f"'{value}'"
        if isinstance(value, bool):
            return 'TRUE' if value else 'FALSE'
        return str(value)
    
    def transform_article(self, article):
        return {
            'url': self.clean_text(article.get('url', '')),
            'title': self.clean_text(article.get('title', '')),
            'description': self.clean_text(article.get('description', '')),
            'source': self.clean_text(article.get('source', '')),
            'author': self.clean_text(article.get('author', '')),
            'published_at': self.normalize_datetime(article.get('published_at', '')),
            'scraped_at': self.normalize_datetime(article.get('scraped_at', '')),
            'image_url': self.clean_text(article.get('image_url', '')),
            'domain': self.extract_domain(article.get('url', '')),
            'word_count': self.count_words(article.get('description', '')),
            'has_image': bool(article.get('image_url', '')),
            'has_author': bool(article.get('author', ''))
        }
    
    def generate_create_table_sql(self):
        sql = f"""
CREATE TABLE IF NOT EXISTS {self.table_name} (
    id SERIAL PRIMARY KEY,
    url TEXT UNIQUE NOT NULL,
    title TEXT,
    description TEXT,
    source VARCHAR(255),
    author VARCHAR(255),
    published_at TIMESTAMP,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    image_url TEXT,
    domain VARCHAR(255),
    word_count INTEGER DEFAULT 0,
    has_image BOOLEAN DEFAULT FALSE,
    has_author BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_stock_articles_url ON {self.table_name}(url);
CREATE INDEX IF NOT EXISTS idx_stock_articles_source ON {self.table_name}(source);
CREATE INDEX IF NOT EXISTS idx_stock_articles_published_at ON {self.table_name}(published_at);
CREATE INDEX IF NOT EXISTS idx_stock_articles_domain ON {self.table_name}(domain);
"""
        return sql
    
    def generate_insert_sql(self, articles):
        if not articles:
            return ""
        
        columns = [
            'url', 'title', 'description', 'source', 'author',
            'published_at', 'scraped_at', 'image_url', 'domain',
            'word_count', 'has_image', 'has_author'
        ]
        
        sql_parts = [f"INSERT INTO {self.table_name} ({', '.join(columns)}) VALUES"]
        
        values = []
        for article in articles:
            transformed = self.transform_article(article)
            row_values = []
            for col in columns:
                row_values.append(self.escape_sql_value(transformed.get(col)))
            values.append(f"({', '.join(row_values)})")
        
        sql_parts.append(',\n'.join(values))
        sql_parts.append("ON CONFLICT (url) DO NOTHING;")
        
        return '\n'.join(sql_parts)
    
    def generate_upsert_sql(self, articles):
        if not articles:
            return ""
        
        columns = [
            'url', 'title', 'description', 'source', 'author',
            'published_at', 'scraped_at', 'image_url', 'domain',
            'word_count', 'has_image', 'has_author'
        ]
        
        update_columns = [col for col in columns if col != 'url']
        
        sql_parts = [f"INSERT INTO {self.table_name} ({', '.join(columns)}) VALUES"]
        
        values = []
        for article in articles:
            transformed = self.transform_article(article)
            row_values = []
            for col in columns:
                row_values.append(self.escape_sql_value(transformed.get(col)))
            values.append(f"({', '.join(row_values)})")
        
        sql_parts.append(',\n'.join(values))
        
        update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_columns])
        sql_parts.append(f"ON CONFLICT (url) DO UPDATE SET {update_clause}, updated_at = CURRENT_TIMESTAMP;")
        
        return '\n'.join(sql_parts)
    
    def save_sql_file(self, content, filename):
        filepath = os.path.join(self.output_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        print(f"Saved SQL to {filepath}")
        return filepath
    
    def generate_all_sql(self):
        print("Starting SQL generation...")
        
        articles = self.load_json_files()
        
        if not articles:
            print("No articles found to process")
            return
        
        print(f"Processing {len(articles)} articles...")
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        create_table_sql = self.generate_create_table_sql()
        create_table_file = self.save_sql_file(create_table_sql, f"create_table_{timestamp}.sql")
        
        insert_sql = self.generate_insert_sql(articles)
        insert_file = self.save_sql_file(insert_sql, f"insert_articles_{timestamp}.sql")
        
        upsert_sql = self.generate_upsert_sql(articles)
        upsert_file = self.save_sql_file(upsert_sql, f"upsert_articles_{timestamp}.sql")
        
        all_sql = f"""-- Complete SQL script for stock articles
-- Generated on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

{create_table_sql}

-- Insert new articles only (skip duplicates)
{insert_sql}
"""
        
        combined_file = self.save_sql_file(all_sql, f"complete_sql_{timestamp}.sql")
        
        print(f"\nSQL generation completed!")
        print(f"Total articles processed: {len(articles)}")
        print(f"Files generated:")
        print(f"  - {create_table_file}")
        print(f"  - {insert_file}")
        print(f"  - {upsert_file}")
        print(f"  - {combined_file}")
        
        return {
            'create_table': create_table_file,
            'insert': insert_file,
            'upsert': upsert_file,
            'combined': combined_file
        }


if __name__ == "__main__":
    generator = SQLGenerator()
    files = generator.generate_all_sql()

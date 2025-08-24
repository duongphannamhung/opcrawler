import pandas as pd
import json
import os
import glob
import yaml
from datetime import datetime


class StockNewsProcessor:
    def __init__(self):
        with open('config.yaml', 'r') as f:
            self.config = yaml.safe_load(f)
        self.data_dir = self.config.get('data_path', 'data')
        self.processed_dir = self.config.get('processed_data_path', 'processed_data')
        self.ensure_directories()
    
    def ensure_directories(self):
        os.makedirs(self.processed_dir, exist_ok=True)
    
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
    
    def normalize_data(self, articles):
        print(f"Normalizing {len(articles)} articles...")
        
        normalized_data = []
        
        for article in articles:
            normalized_article = {
                'title': self.clean_text(article.get('title', '')),
                'description': self.clean_text(article.get('description', '')),
                'url': article.get('url', ''),
                'source': article.get('source', ''),
                'published_at': self.normalize_datetime(article.get('published_at', '')),
                'author': self.clean_text(article.get('author', '')),
                'image_url': article.get('image_url', ''),
                'scraped_at': self.normalize_datetime(article.get('scraped_at', '')),
                'date_range': article.get('date_range', ''),
                'has_image': bool(article.get('image_url', '')),
                'has_author': bool(article.get('author', '')),
                'domain': self.extract_domain(article.get('url', '')),
                'published_date': self.extract_date_only(article.get('published_at', '')),
                'scraped_date': self.extract_date_only(article.get('scraped_at', '')),
                'word_count': self.count_words(article.get('description', ''))
            }
            normalized_data.append(normalized_article)
        
        return normalized_data
    
    def clean_text(self, text):
        if not text or text.strip() == '':
            return ''
        return ' '.join(text.strip().split())
    
    def normalize_datetime(self, dt_string):
        if not dt_string:
            return ''
        try:
            if 'T' in dt_string:
                dt = datetime.fromisoformat(dt_string.replace('Z', '+00:00'))
            else:
                dt = datetime.strptime(dt_string, '%Y-%m-%d %H:%M:%S')
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
            return dt_string
    
    def extract_domain(self, url):
        if not url:
            return ''
        try:
            from urllib.parse import urlparse
            return urlparse(url).netloc
        except:
            return ''
    
    def extract_date_only(self, dt_string):
        if not dt_string:
            return ''
        try:
            if 'T' in dt_string:
                dt = datetime.fromisoformat(dt_string.replace('Z', '+00:00'))
            else:
                dt = datetime.strptime(dt_string, '%Y-%m-%d %H:%M:%S')
            return dt.strftime('%Y-%m-%d')
        except:
            return ''
    
    def count_words(self, text):
        if not text:
            return 0
        return len(text.split())
    
    def create_dataframe(self, normalized_data):
        df = pd.DataFrame(normalized_data)
        
        datetime_columns = ['published_at', 'scraped_at']
        for col in datetime_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce')
        
        date_columns = ['published_date', 'scraped_date']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.date
        
        original_count = len(df)
        df = df.drop_duplicates(subset=['url'], keep='first')
        removed_count = original_count - len(df)
        
        if removed_count > 0:
            print(f"Removed {removed_count} duplicate articles")
        
        return df
    
    def generate_summary_stats(self, df):
        stats = {
            'total_articles': len(df),
            'unique_sources': df['source'].nunique(),
            'date_range': f"{df['published_date'].min()} to {df['published_date'].max()}",
            'articles_with_images': df['has_image'].sum(),
            'articles_with_authors': df['has_author'].sum(),
            'top_sources': df['source'].value_counts().head(5).to_dict(),
            'articles_by_date': {str(k): int(v) for k, v in df['published_date'].value_counts().sort_index().head(10).to_dict().items()}
        }
        return stats
    
    def save_to_csv(self, df, filename=None):
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"stock_news_processed_{timestamp}.csv"
        
        filepath = os.path.join(self.processed_dir, filename)
        df.to_csv(filepath, index=False, encoding='utf-8', sep='\t')
        print(f"Saved processed data to {filepath}")
        return filepath
    
    def save_summary_stats(self, stats, filename=None):
        if filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"summary_stats_{timestamp}.json"
        
        filepath = os.path.join(self.processed_dir, filename)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(stats, f, indent=2, default=str)
        print(f"Saved summary statistics to {filepath}")
        return filepath
    
    def process_all(self):
        print("Starting data processing...")
        
        articles = self.load_json_files()
        
        if not articles:
            print("No articles found to process")
            return
        
        normalized_data = self.normalize_data(articles)
        
        df = self.create_dataframe(normalized_data)
        
        stats = self.generate_summary_stats(df)
        
        csv_file = self.save_to_csv(df)
        
        stats_file = self.save_summary_stats(stats)
        
        print(f"\nProcessing completed!")
        print(f"Total articles processed: {stats['total_articles']}")
        print(f"Unique sources: {stats['unique_sources']}")
        print(f"Date range: {stats['date_range']}")
        print(f"Files created:")
        print(f"  - {csv_file}")
        print(f"  - {stats_file}")
        
        return df, stats


if __name__ == "__main__":
    processor = StockNewsProcessor()
    df, stats = processor.process_all()

import json
import yaml
import requests
import os
import time
from datetime import datetime, timedelta


class StockNewsCrawler:
    def __init__(self):
        with open('config.yaml', 'r') as f:
            self.config = yaml.safe_load(f)
        self.checkpoint_file = os.path.join(self.config.get('data_path', 'data'), self.config.get('checkpoint_file', 'checkpoint.json'))
        self.collected_urls = set()
        self.load_checkpoint()
    
    def load_checkpoint(self):
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r') as f:
                    checkpoint = json.load(f)
                    self.collected_urls = set(checkpoint.get('collected_urls', []))
                    print(f"Loaded checkpoint with {len(self.collected_urls)} existing URLs")
            except Exception as e:
                print(f"Error loading checkpoint: {e}")
                self.collected_urls = set()
    
    def save_checkpoint(self):
        os.makedirs(os.path.dirname(self.checkpoint_file), exist_ok=True)
        checkpoint = {
            'collected_urls': list(self.collected_urls),
            'last_run': datetime.now().isoformat(),
            'total_articles': len(self.collected_urls)
        }
        with open(self.checkpoint_file, 'w') as f:
            json.dump(checkpoint, f, indent=2)
    
    def get_articles_batch(self, page=1, date_offset=0):
        url = "https://newsapi.org/v2/everything"
        
        # For free tier, we'll use date ranges instead of pagination
        from_date = (datetime.now() - timedelta(days=self.config.get('days_back', 7) + date_offset)).strftime('%Y-%m-%d')
        to_date = (datetime.now() - timedelta(days=date_offset)).strftime('%Y-%m-%d')
        
        params = {
            'q': self.config['query'],
            'language': self.config['language'],
            'pageSize': min(self.config.get('batch_size', 20), 100),  # Max 100 per request
            'from': from_date,
            'to': to_date,
            'sortBy': 'publishedAt',
            'apiKey': self.config['api_key']
        }
        
        # Only add page parameter if it's page 1 (free tier limitation)
        if page == 1:
            params['page'] = page
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            data = response.json()
            articles = []
            new_articles = 0
            
            if data['status'] == 'ok':
                total_results = data.get('totalResults', 0)
                print(f"Date range {from_date} to {to_date}: Found {len(data['articles'])} articles (Total available: {total_results})")
                
                for article in data['articles']:
                    article_url = article.get('url', '')
                    if article_url and article_url not in self.collected_urls:
                        articles.append({
                            'title': article.get('title', ''),
                            'description': article.get('description', ''),
                            'url': article_url,
                            'source': article.get('source', {}).get('name', ''),
                            'published_at': article.get('publishedAt', ''),
                            'author': article.get('author', ''),
                            'image_url': article.get('urlToImage', ''),
                            'scraped_at': datetime.now().isoformat(),
                            'date_range': f"{from_date}_to_{to_date}"
                        })
                        self.collected_urls.add(article_url)
                        new_articles += 1
                
                print(f"New articles in this batch: {new_articles}")
            
            return articles, total_results
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching articles: {e}")
            return [], 0
        except Exception as e:
            print(f"Error processing articles: {e}")
            return [], 0
    
    def save_json(self, articles, batch_num):
        data_path = self.config.get('data_path', 'data')
        os.makedirs(data_path, exist_ok=True)
        
        filename = f"stock_news_batch_{batch_num}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        filepath = os.path.join(data_path, filename)
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(articles, f, ensure_ascii=False, indent=2)
        return filepath
    
    def run(self):
        all_articles = []
        batch_num = 1
        date_offset = 0
        max_total = self.config.get('max_total_articles', 1000)
        batch_size = self.config.get('batch_size', 20)
        max_days_back = self.config.get('days_back', 7)
        
        print(f"Starting crawl with checkpoint of {len(self.collected_urls)} existing URLs")
        print(f"Target: {max_total} total articles, {batch_size} per batch")
        print(f"Strategy: Using date ranges over {max_days_back} days to get more articles")
        
        # First, try to get articles from current date range
        articles, total_results = self.get_articles_batch(1, 0)
        if articles:
            all_articles.extend(articles)
            filename = self.save_json(articles, batch_num)
            print(f"Batch {batch_num}: Saved {len(articles)} new articles to {filename}")
            self.save_checkpoint()
            batch_num += 1
        
        # Then use different search strategies to get more articles
        search_strategies = [
            "stock market news",
            "financial news",
            "investment news", 
            "market analysis",
            "economic news",
            "trading news",
            "business news"
        ]
        
        original_query = self.config['query']
        
        for strategy in search_strategies:
            if len(all_articles) >= max_total:
                break
                
            print(f"\n--- Trying search strategy: '{strategy}' ---")
            self.config['query'] = strategy
            
            articles, _ = self.get_articles_batch(1, 0)
            
            if articles:
                all_articles.extend(articles)
                filename = self.save_json(articles, batch_num)
                print(f"Batch {batch_num}: Saved {len(articles)} new articles to {filename}")
                self.save_checkpoint()
                batch_num += 1
            
            time.sleep(2)  # Rate limiting
        
        # Restore original query
        self.config['query'] = original_query
        
        # Try different date ranges if we still need more articles
        date_offset = 1
        while len(all_articles) < max_total and date_offset <= max_days_back:
            print(f"\n--- Fetching older articles (offset: {date_offset} days) ---")
            
            articles, total_results = self.get_articles_batch(1, date_offset)
            
            if not articles:
                print("No new articles found in this date range")
                date_offset += 1
                continue
            
            all_articles.extend(articles)
            
            filename = self.save_json(articles, batch_num)
            print(f"Batch {batch_num}: Saved {len(articles)} new articles to {filename}")
            
            self.save_checkpoint()
            
            print(f"Progress: {len(all_articles)}/{max_total} articles collected")
            batch_num += 1
            date_offset += 1
            time.sleep(2)  # Rate limiting
        
        print(f"\nCrawling completed:")
        print(f"Total new articles collected: {len(all_articles)}")
        print(f"Total unique URLs in checkpoint: {len(self.collected_urls)}")
        
        return all_articles


if __name__ == "__main__":
    crawler = StockNewsCrawler()
    crawler.run()
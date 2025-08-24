#!/usr/bin/env python3
"""
Main script to run the stock news crawler and data processor
"""

import sys
import os
from crawler import StockNewsCrawler
from data_processor import StockNewsProcessor
from sql_generator import SQLGenerator


def main():
    print("=== Stock News Crawler & Processor ===\n")
    
    if len(sys.argv) > 1:
        if sys.argv[1] == '--process-only':
            print("Running data processor only...")
            processor = StockNewsProcessor()
            processor.process_all()
            return
        elif sys.argv[1] == '--crawl-only':
            print("Running crawler only...")
            crawler = StockNewsCrawler()
            crawler.run()
            return
        elif sys.argv[1] == '--sql-only':
            print("Generating SQL only...")
            generator = SQLGenerator()
            generator.generate_all_sql()
            return
        elif sys.argv[1] == '--full':
            print("Running complete pipeline...")
            crawler = StockNewsCrawler()
            articles = crawler.run()
            
            if articles:
                print(f"\nStep 2: Processing {len(articles)} articles...")
                processor = StockNewsProcessor()
                df, stats = processor.process_all()
                
                print(f"\nStep 3: Generating SQL...")
                generator = SQLGenerator()
                sql_files = generator.generate_all_sql()
                
                print("\n=== Pipeline Complete ===")
                print(f"Crawled: {len(articles)} new articles")
                print(f"Processed: {stats['total_articles']} total articles")
                print(f"SQL files generated in 'processed_data' directory")
            return
    
    print("Running crawler and processor...")
    crawler = StockNewsCrawler()
    articles = crawler.run()
    
    if articles:
        print(f"\nStep 2: Processing {len(articles)} articles...")
        processor = StockNewsProcessor()
        df, stats = processor.process_all()
        
        print("\n=== Final Summary ===")
        print(f"Crawled: {len(articles)} new articles")
        print(f"Processed: {stats['total_articles']} total articles")
        print(f"CSV file saved in 'processed_data' directory")
        print("\nTo generate SQL files, run: python run.py --sql-only")
    else:
        print("No new articles to process")


if __name__ == "__main__":
    main()

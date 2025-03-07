import requests
from bs4 import BeautifulSoup
import schedule
import time
from datetime import datetime

class Scraper:
    def __init__(self, manager):
        """Initialize scraper with news sources and stocks to track."""
        self.NEWS_SOURCES = {
            "Yahoo Finance": "https://finance.yahoo.com/quote/{}/news",
            "Google News": "https://news.google.com/search?q={}%20stock",
        }
        self.STOCKS = ["NVDA", "TSLA", "GOOG"]
        self.manager = manager  #  Initialize storage manager

    def scrape_news_content(self, news_url):
        """Extracts main content from the news page."""
        try:
            response = requests.get(news_url, headers={"User-Agent": "Mozilla/5.0"})
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")
                paragraphs = soup.find_all("p")
                content = " ".join([p.get_text() for p in paragraphs[:5]])  # Extract first 5 paragraphs
                return content
        except Exception as e:
            print(f"⚠️ Error fetching content from {news_url}: {e}")
        return "Content unavailable"

    def scrape_news(self, stock):
        """Scrapes news articles for a given stock."""
        collected_news = []
        for source, url in self.NEWS_SOURCES.items():
            full_url = url.format(stock)
            print("URL is: " + full_url)
            response = requests.get(full_url, headers={"User-Agent": "Mozilla/5.0"})

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, "html.parser")
                articles = soup.find_all("a", limit=5)  # Extract first 5 articles
            
                for article in articles:
                    title = article.get_text(strip=True)
                    link = article.get("href")

                    if link:
                        link = link if "http" in link else f"https://{source}{link}"  # Ensure absolute URL
                        content = self.scrape_news_content(link)  # Extract full article content
                        collected_news.append({
                            "Stock": stock,
                            "Source": source,
                            "Title": title,
                            "Link": link,
                            "Content": content,
                            "Time": datetime.now()
                        })
                        print("scraped news:" + 
                            "\nStock:" + stock +
                            "\nSource:" + source +
                            "\nTitle:" + title +
                            "\nLink:"  + link +
                            "\nContent:" + content
                        )
                    else:
                        print(f"⚠️ Skipping article with missing href: {title}")

            else:
                print(f"❌ Failed to scrape {source} for {stock}")
        return collected_news

    def collect_data(self):
        """Collects data for all tracked stocks and stores it in Pinecone + SQLite."""
        for stock in self.STOCKS:
            news_list = self.scrape_news(stock)
            for news in news_list:
                self.manager.store_news(
                    stock=news["Stock"],
                    source=news["Source"],
                    title=news["Title"],
                    link=news["Link"],
                    content=news["Content"]
                )
        print(f"✅ [{datetime.now()}] Data collected & stored successfully.")

    def schedule_scraping(self):
        """Schedules the scraper to run every 6 hours."""
        schedule.every(6).hours.do(self.collect_data)
        print("⏳ Scraper scheduled every 6 hours.")
        while True:
            schedule.run_pending()
            time.sleep(60)

if __name__ == "__main__":
    scraper = Scraper()
    scraper.schedule_scraping()

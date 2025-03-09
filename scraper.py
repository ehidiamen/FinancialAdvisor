import requests
from bs4 import BeautifulSoup
import time
import schedule

class Scraper:
    def __init__(self, manager):
        self.manager = manager
        self.STOCKS = ["TSLA", "Tesla", "NVDA", "NVIDIA", "GOOG", "Google"]
        self.HEADERS = {"User-Agent": "Mozilla/5.0"}

    def is_financial_news(self, content):
        """Checks if an article is related to stock or financial markets."""
        keywords = ["stock", "market", "shares", "investment", "price", "earnings", "revenue", "trading"]
        return any(word in content.lower() for word in keywords)

    def get_article_content(self, url):
        """Fetches the content of an article from a URL."""
        try:
            response = requests.get(url, headers=self.HEADERS, timeout=10)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            paragraphs = soup.find_all("p")
            return " ".join([p.get_text() for p in paragraphs])
        except requests.RequestException:
            return "Content unavailable"

    def scrape_yahoo_finance(self, stock):
        """Scrapes Yahoo Finance for stock-related news."""
        url = f"https://finance.yahoo.com/quote/{stock}/news"
        response = requests.get(url, headers=self.HEADERS)
        soup = BeautifulSoup(response.text, "html.parser")
        articles = soup.find_all("a", href=True)

        news_list = []
        for article in articles:
            title = article.get_text().strip()
            link = article["href"]
            full_link = f"https://finance.yahoo.com{link}" if not link.startswith("http") else link
            article_content = self.get_article_content(full_link)

            if self.is_financial_news(article_content):
                news_list.append({
                    "stock": stock,
                    "source": "Yahoo Finance",
                    "title": title,
                    "link": full_link,
                    "content": article_content
                })
                print({
                    "stock": stock,
                    "source": "Yahoo Finance",
                    "title": title,
                    "link": full_link,
                    "content": article_content
                })

        return news_list

    def scrape_google_news(self, stock):
        """Scrapes Google News for stock-related articles."""
        url = f"https://news.google.com/search?q={stock}%20stock"
        try:
            response = requests.get(url, headers=self.HEADERS, timeout=10)
            response.raise_for_status()
        except requests.RequestException as e:
            print(f"⚠️ Google News request failed for {stock}: {e}")
            return []

        soup = BeautifulSoup(response.text, "html.parser")
        articles = soup.find_all("a", href=True)

        news_list = []
        for article in articles:
            title = article.get_text().strip()
            link = article["href"]
            full_link = f"https://news.google.com{link}" if not link.startswith("http") else link
            article_content = self.get_article_content(full_link)

            if self.is_financial_news(article_content):
                news_list.append({
                    "stock": stock,
                    "source": "Google News",
                    "title": title,
                    "link": full_link,
                    "content": article_content
                })
                print({
                    "stock": stock,
                    "source": "Google News",
                    "title": title,
                    "link": full_link,
                    "content": article_content
                })

        return news_list

    def collect_data(self):
        """Runs the scrapers and stores the collected news."""
        print("Scraping data")
        for stock in self.STOCKS:
            yahoo_news = self.scrape_yahoo_finance(stock)
            google_news = self.scrape_google_news(stock)
            all_news = yahoo_news + google_news
            print(all_news)

            for article in all_news:
                self.manager.store_news(
                    stock=article["stock"],
                    source=article["source"],
                    title=article["title"],
                    link=article["link"],
                    content=article["content"]
                )
        print("✅ Scraping completed successfully!")
    
    def schedule_scraping(self):
        """Schedules scraping every 6 hours."""
        schedule.every(6).hours.do(lambda: (self.collect_data(), self.manager.delete_old_news()))

        print("⏳ Scraper and old news deletion scheduled every 6 hours.")
        while True:
            schedule.run_pending()
            time.sleep(60)

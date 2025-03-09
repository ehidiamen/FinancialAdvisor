import os
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from pinecone import Pinecone, ServerlessSpec
import sqlite3
from datetime import datetime, timedelta
from scraper import Scraper
import schedule
import time

from dotenv import load_dotenv

# ✅ Load environment variables
load_dotenv()
PINECONE_API_KEY = os.getenv("PINECONE_API_KEY")
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")

# ✅ Initialize Pinecone Client
pinecone = Pinecone(api_key=PINECONE_API_KEY)
index_name = "financial-news"

#if index_name not in pinecone.list_indexes():
#    pinecone.create_index(index_name, dimension=768, spec=ServerlessSpec(cloud='aws', region='us-east-1'))  # ✅ GoogleGenerativeAI uses 768 dimensions

index = pinecone.Index(index_name)

# ✅ Initialize Google Generative AI Embeddings
embedding_model = GoogleGenerativeAIEmbeddings(model="models/embedding-001", google_api_key=GOOGLE_API_KEY)

# ✅ Initialize text splitter
text_splitter = RecursiveCharacterTextSplitter(chunk_size=1000, chunk_overlap=50)

class PineconeNewsManager:
    def __init__(self):
        self.db = "news.db"
        self.setup_database()

    def setup_database(self):
        """Ensures the SQLite database structure is correct."""
        conn = sqlite3.connect(self.db)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS news (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                stock TEXT,
                source TEXT,
                title TEXT,
                link TEXT UNIQUE,
                content TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        conn.close()

    def store_news(self, stock, source, title, link, content):
        """Stores news articles in SQLite and Pinecone."""
        print("Storing content on: " + stock + ", Content: \n")
        print(content)
        conn = sqlite3.connect(self.db)
        cursor = conn.cursor()

        # ✅ Check for duplicates before inserting
        cursor.execute("SELECT COUNT(*) FROM news WHERE link = ?", (link,))
        if cursor.fetchone()[0] > 0:
            print(f"⚠️ Skipping duplicate news article: {title}")
            return

        cursor.execute("INSERT INTO news (stock, source, title, link, content) VALUES (?, ?, ?, ?, ?)",
                       (stock, source, title, link, content))
        conn.commit()
        conn.close()

        # ✅ Store embeddings in Pinecone
        chunks = text_splitter.split_text(content)
        pinecone_vectors = [
            (f"{title}_{i}", embedding_model.embed_query(chunk), {"content": chunk, "stock": stock})
            for i, chunk in enumerate(chunks)
        ]
        index.upsert(pinecone_vectors)
        print(f"✅ Stored {len(chunks)} chunks for: {title}")

    def retrieve_news(self, stock, limit=5):
        """Fetches news from SQLite and Pinecone, returning the most relevant results."""
        retrieved_news = []

        # ✅ Fetch from SQLite
        try:
            conn = sqlite3.connect(self.db)
            cursor = conn.cursor()
            cursor.execute("SELECT title, content, source, link, timestamp FROM news WHERE stock LIKE ? ORDER BY timestamp DESC LIMIT ?",
                           (f"%{stock}%", limit))
            sqlite_results = cursor.fetchall()
            conn.close()

            for row in sqlite_results:
                retrieved_news.append({
                    "title": row[0],
                    "content": row[1],
                    "source": row[2],
                    "link": row[3],
                    "timestamp": row[4],
                    "source_type": "SQLite"
                })

        except Exception as e:
            print(f"⚠️ Error retrieving news from SQLite: {e}")

        # ✅ Fetch from Pinecone
        try:
            stock_embedding = embedding_model.embed_query(stock)
            pinecone_results = index.query(vector=stock_embedding, top_k=limit, include_metadata=True)

            for match in pinecone_results["matches"]:
                metadata = match["metadata"]
                retrieved_news.append({
                    "title": metadata.get("title", "Unknown Title"),
                    "content": metadata.get("content", "Content Unavailable"),
                    "source": metadata.get("source", "Unknown Source"),
                    "link": metadata.get("link", "#"),
                    "timestamp": metadata.get("timestamp", "Unknown"),
                    "relevance_score": match.get("score", 0),
                    "source_type": "Pinecone"
                })

        except Exception as e:
            print(f"⚠️ Error retrieving news from Pinecone: {e}")

        return sorted(retrieved_news, key=lambda x: x.get("timestamp", ""), reverse=True)[:limit]

    def delete_old_news(self):
        """Deletes news older than 24 hours from Pinecone and SQLite."""
        threshold = datetime.now() - timedelta(days=1)

        # ✅ Delete from SQLite
        conn = sqlite3.connect("news.db")
        cursor = conn.cursor()
        cursor.execute("DELETE FROM news WHERE timestamp < ?", (threshold,))
        conn.commit()
        conn.close()

        # ✅ Fetch and Delete Old News from Pinecone
        pinecone_news = index.query(vector=[0] * 768, top_k=1000, include_metadata=True) 
        for item in pinecone_news["matches"]:
            if "timestamp" in item["metadata"]:
                timestamp = datetime.fromisoformat(item["metadata"]["timestamp"])
                if timestamp < threshold:
                    index.delete(id=item["id"])
                    print(f"🗑️ Deleted: {item['metadata']['title']} from Pinecone")

    def schedule_scraping(self):
        """Schedules scraping every 6 hours."""
        schedule.every(3).minutes.do(lambda: (self.scraper.collect_data(), self.delete_old_news()))

        print("⏳ Scraper and old news deletion scheduled every 6 hours.")
        while True:
            schedule.run_pending()
            time.sleep(60)

if __name__ == "__main__":
    manager = PineconeNewsManager()
    manager.schedule_scraping()  # Start scheduled scraping

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import firebase_admin
from firebase_admin import credentials, firestore
from langchain_groq import ChatGroq
from PineconeNewsManager import PineconeNewsManager 
import threading

from dotenv import load_dotenv

from scraper import Scraper

load_dotenv()

# Initialize Firebase
cred = credentials.Certificate("firebase_credentials.json")  
firebase_admin.initialize_app(cred)
db = firestore.client()

# Initialize FastAPI
app = FastAPI()

# ✅ Enable CORS (Allow frontend access)
app.add_middleware(
    CORSMiddleware,
    allow_origins= ["*"],# ["http://localhost:3000", "https://betterchat-frontend.vercel.app"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ✅ Initialize AI Model
llm = ChatGroq(
    model="llama3-8b-8192",
    temperature=0,
    max_tokens=None,
    timeout=None,
    max_retries=2,
)

# ✅ Initialize PineconeNewsManager
news_manager = PineconeNewsManager()
scraper = Scraper(news_manager)
threading.Thread(target=scraper.schedule_scraping, daemon=True).start()
# news_manager.schedule_scraping() 

# ✅ Stocks to Check in User Queries
STOCKS = {
    "TSLA": "Tesla",
    "TESLA": "Tesla",
    "GOOG": "Google",
    "GOOGLE": "Google",
    "NVDA": "Nvidia",
    "NVIDIA": "Nvidia"
}

# ✅ Request Model
class AdviceRequest(BaseModel):
    user_id: str
    query: str

# ✅ Helper Function: Identify Stock in Query
def find_stock_in_query(query):
    for stock_symbol, stock_name in STOCKS.items():
        if stock_symbol.lower() in query.lower():
            return stock_name  # Return clean stock name (Tesla, Google, Nvidia)
    return None

# ✅ Endpoint: Get Financial Advice
@app.post("/financial_advice")
async def get_financial_advice(request: AdviceRequest):
    stock = find_stock_in_query(request.query)

    if stock:
        # ✅ Retrieve relevant news from Pinecone for RAG
        news_results = news_manager.retrieve_news(stock)
        retrieved_news = "\n".join([f"- {news['title']}: {news['content']}" for news in news_results])

        prompt = f"""
        You are a financial assistant providing stock insights. 
        The user asked: {request.query}
        Here are relevant news articles on {stock}:
        {retrieved_news}
        Now, generate a professional financial response based on this data.
        """

    else:
        # ✅ No stock found, proceed without RAG
        prompt = f"""
        You are a financial assistant. The user asked: {request.query}
        Provide a financial insight based on general knowledge.
        """

    response = llm.invoke([{"role": "system", "content": prompt}]).content.strip()

    return {"stock": stock, "response": response}

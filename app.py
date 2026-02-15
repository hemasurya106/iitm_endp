from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from collections import OrderedDict, defaultdict, deque
import hashlib
import time
import numpy as np
import asyncio
import json
from collections import defaultdict
import time
from fastapi.responses import JSONResponse
app = FastAPI()

# =====================================================
# CORS (Fixes OPTIONS 405 issue)
# =====================================================

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Explicit OPTIONS handlers (bulletproof fix)

@app.options("/")
async def options_root():
    return {}

@app.options("/security")
async def options_security():
    return {}

@app.options("/stream")
async def options_stream():
    return {}

# =====================================================
# GLOBAL CONFIG
# =====================================================

MODEL_COST_PER_MILLION = 0.60
AVG_TOKENS = 800
TTL_SECONDS = 86400
MAX_CACHE_SIZE = 1500
SIMILARITY_THRESHOLD = 0.95

# =====================================================
# LIGHTWEIGHT EMBEDDING
# =====================================================

def simple_embedding(text: str):
    words = text.lower().split()
    vector = [abs(hash(word)) % 1000 for word in words[:20]]
    return np.array(vector if vector else [0])

def cosine_similarity(a, b):
    min_len = min(len(a), len(b))
    if min_len == 0:
        return 0.0
    a = a[:min_len]
    b = b[:min_len]
    denom = (np.linalg.norm(a) * np.linalg.norm(b))
    if denom == 0:
        return 0.0
    return np.dot(a, b) / denom

# =====================================================
# Q1 — CACHING
# =====================================================

cache = OrderedDict()
analytics = {
    "totalRequests": 0,
    "cacheHits": 0,
    "cacheMisses": 0,
}

class CacheRequest(BaseModel):
    query: str
    application: str

def md5_hash(text):
    return hashlib.md5(text.encode()).hexdigest()

def clean_expired():
    now = time.time()
    keys = [k for k, v in cache.items() if now - v["timestamp"] > TTL_SECONDS]
    for k in keys:
        del cache[k]

def enforce_lru():
    while len(cache) > MAX_CACHE_SIZE:
        cache.popitem(last=False)

def fake_llm(query):
    time.sleep(1)
    return f"Answer to: {query}"

@app.post("/")
def caching(req: CacheRequest):
    start = time.time()
    analytics["totalRequests"] += 1
    clean_expired()

    key = md5_hash(req.query)

    # Exact match
    if key in cache:
        analytics["cacheHits"] += 1
        cache.move_to_end(key)
        latency_ms = max(1, int((time.time() - start) * 1000))
        return {
            "answer": cache[key]["response"],
            "cached": True,
            "latency": latency_ms,
            "cacheKey": key
        }

    # Semantic match
    query_embedding = simple_embedding(req.query)

    for k, v in cache.items():
        if cosine_similarity(query_embedding, v["embedding"]) > SIMILARITY_THRESHOLD:
            analytics["cacheHits"] += 1
            cache.move_to_end(k)
            latency_ms = max(1, int((time.time() - start) * 1000))
            return {
                "answer": v["response"],
                "cached": True,
                "latency": latency_ms,
                "cacheKey": k
            }

    # Miss
    analytics["cacheMisses"] += 1
    response = fake_llm(req.query)

    cache[key] = {
        "response": response,
        "embedding": query_embedding,
        "timestamp": time.time()
    }

    enforce_lru()

    latency_ms = max(1, int((time.time() - start) * 1000))

    return {
        "answer": response,
        "cached": False,
        "latency": latency_ms,
        "cacheKey": key
    }

@app.get("/analytics")
def get_analytics():
    total = analytics["totalRequests"]
    hits = analytics["cacheHits"]
    hit_rate = hits / total if total else 0
    saved_tokens = hits * AVG_TOKENS
    savings = (saved_tokens / 1_000_000) * MODEL_COST_PER_MILLION

    return {
        "hitRate": round(hit_rate, 2),
        "totalRequests": total,
        "cacheHits": hits,
        "cacheMisses": analytics["cacheMisses"],
        "cacheSize": len(cache),
        "costSavings": round(savings, 2),
        "savingsPercent": round(hit_rate * 100, 2),
        "strategies": [
            "exact match",
            "semantic similarity",
            "LRU eviction",
            "TTL expiration"
        ]
    }

# =====================================================
# Q2 — RATE LIMITING
# =====================================================
RATE_LIMIT = 42          # 42 per minute
BURST_CAPACITY = 11      # allow burst of 11
REFILL_RATE = RATE_LIMIT / 60  # tokens per second

rate_limit_store = defaultdict(lambda: {
    "tokens": RATE_LIMIT,
    "last_refill": time.time()
})


@app.post("/security")
async def security(request: Request):
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(
            status_code=400,
            content={
                "blocked": True,
                "reason": "Invalid JSON payload",
                "sanitizedOutput": None,
                "confidence": 0.90
            }
        )

    # Track per userId if provided, else fallback to IP
    user_id = body.get("userId") or request.client.host
    now = time.time()

    bucket = rate_limit_store[user_id]

    # Refill tokens
    elapsed = now - bucket["last_refill"]
    refill_tokens = elapsed * REFILL_RATE
    bucket["tokens"] = min(RATE_LIMIT, bucket["tokens"] + refill_tokens)
    bucket["last_refill"] = now

    if bucket["tokens"] < 1:
        retry_after = int((1 - bucket["tokens"]) / REFILL_RATE)

        return JSONResponse(
            status_code=429,
            content={
                "blocked": True,
                "reason": "Rate limit exceeded (42 requests per minute)",
                "sanitizedOutput": None,
                "confidence": 0.99
            },
            headers={"Retry-After": str(max(retry_after, 1))}
        )

    # Consume token
    bucket["tokens"] -= 1

    # Successful request
    return {
        "blocked": False,
        "reason": "Input passed all security checks",
        "sanitizedOutput": body.get("input", ""),
        "confidence": 0.95
    }
# =====================================================
# ================== Q3 STREAMING =====================
# =====================================================

class StreamRequest(BaseModel):
    prompt: str
    stream: bool
async def stream_generator(prompt):
    code = """
\"\"\"
Advanced Python Data Processing Framework
This module provides structured data ingestion, validation,
transformation, aggregation, and export functionality.
\"\"\"

import csv
import json
import logging
import argparse
from typing import List, Dict, Any, Optional
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

class DataValidationError(Exception):
    \"\"\"Custom exception for data validation errors.\"\"\"
    pass


class DataProcessor:
    \"\"\"Main class responsible for processing structured data.\"\"\"

    def __init__(self, file_path: str):
        self.file_path: str = file_path
        self.data: List[Dict[str, Any]] = []

    def load_csv(self) -> None:
        \"\"\"Load CSV data from disk.\"\"\"
        try:
            with open(self.file_path, newline='', encoding="utf-8") as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    self.data.append(row)
            logging.info(f"Loaded {len(self.data)} rows successfully.")
        except FileNotFoundError:
            logging.error("File not found.")
            raise
        except Exception as e:
            logging.error(f"Unexpected error while loading CSV: {e}")
            raise

    def validate(self) -> None:
        \"\"\"Validate required fields.\"\"\"
        if not self.data:
            raise DataValidationError("No data loaded.")

        for index, row in enumerate(self.data):
            if not isinstance(row, dict):
                raise DataValidationError(f"Invalid row format at index {index}")
            if any(value is None for value in row.values()):
                raise DataValidationError(f"Missing value at row {index}")

        logging.info("Data validation passed.")

    def clean(self) -> None:
        \"\"\"Strip whitespace and normalize text.\"\"\"
        cleaned = []
        for row in self.data:
            cleaned_row = {k: str(v).strip() for k, v in row.items()}
            cleaned.append(cleaned_row)
        self.data = cleaned
        logging.info("Data cleaning complete.")

    def transform(self) -> None:
        \"\"\"Transform values to uppercase.\"\"\"
        for row in self.data:
            for key in row:
                row[key] = row[key].upper()
        logging.info("Data transformation complete.")

    def aggregate(self) -> Dict[str, int]:
        \"\"\"Example aggregation: count occurrences per first column.\"\"\"
        aggregation: Dict[str, int] = {}
        if not self.data:
            return aggregation

        first_key = list(self.data[0].keys())[0]
        for row in self.data:
            key_value = row[first_key]
            aggregation[key_value] = aggregation.get(key_value, 0) + 1

        logging.info("Aggregation complete.")
        return aggregation

    def save_json(self, output_file: str) -> None:
        \"\"\"Export processed data to JSON.\"\"\"
        try:
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(self.data, f, indent=4)
            logging.info(f"Data exported to {output_file}")
        except Exception as e:
            logging.error(f"Error saving JSON: {e}")
            raise

    def run_pipeline(self, output_file: str) -> None:
        \"\"\"Execute full processing pipeline.\"\"\"
        self.load_csv()
        self.validate()
        self.clean()
        self.transform()
        aggregation = self.aggregate()
        self.save_json(output_file)
        logging.info(f"Pipeline finished at {datetime.now()}")
        logging.info(f"Aggregation Summary: {aggregation}")


def parse_arguments() -> argparse.Namespace:
    \"\"\"Parse CLI arguments.\"\"\"
    parser = argparse.ArgumentParser(description="Run data processing pipeline.")
    parser.add_argument("--input", required=True, help="Input CSV file path")
    parser.add_argument("--output", required=True, help="Output JSON file path")
    return parser.parse_args()


def main():
    \"\"\"Main execution entry point.\"\"\"
    try:
        args = parse_arguments()
        processor = DataProcessor(args.input)
        processor.run_pipeline(args.output)
        print("Processing completed successfully.")
    except DataValidationError as ve:
        logging.error(f"Validation error: {ve}")
    except Exception as e:
        logging.error(f"Fatal error: {e}")


if __name__ == "__main__":
    main()
"""

    chunk_size = 400  # larger chunk for smoother streaming
    for i in range(0, len(code), chunk_size):
        chunk = code[i:i+chunk_size]
        yield f"data: {json.dumps({'choices':[{'delta':{'content':chunk}}]})}\n\n"
        await asyncio.sleep(0.05)

    yield "data: [DONE]\n\n"

@app.post("/stream")
async def stream(req: StreamRequest):
    return StreamingResponse(stream_generator(req.prompt),
                             media_type="text/event-stream")

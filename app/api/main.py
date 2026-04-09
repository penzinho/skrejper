from fastapi import FastAPI
from datetime import datetime

app = FastAPI(
    title="Lead Generation API",
    version="1.0"
)

# health check
@app.get("/health")
def health():
    return {
        "status": "ok",
        "time": datetime.utcnow()
    }
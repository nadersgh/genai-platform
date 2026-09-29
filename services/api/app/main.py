from fastapi import FastAPI

app = FastAPI(title="GenAI Platform API")


@app.get("/healthz")
def healthz() -> dict:
    return {"status": "ok"}

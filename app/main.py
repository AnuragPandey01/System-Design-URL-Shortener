from fastapi import FastAPI
import uvicorn
from app.api.endpoints import router as api_router

app = FastAPI()

app.include_router(api_router)


@app.get("/status")
def read_root():
    return {"message": "ok"}


if __name__ == "__main__":
    uvicorn.run("app.main:app", port=8000, reload=True)
from fastapi import FastAPI
import uvicorn
from api.endpoints import router as api_router

app = FastAPI()

app.include_router(api_router)

if __name__ == "__main__":
    uvicorn.run("main:app", port=8000, reload=True)
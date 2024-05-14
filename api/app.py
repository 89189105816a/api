

from fastapi import FastAPI
from router import api_router
import uvicorn

app = FastAPI(
    title="server market api",
    version="0.1"
)
app.include_router(api_router)

if __name__ == '__main__':
    uvicorn.run("main:app", host='127.0.0.1', port=8084, reload=True)
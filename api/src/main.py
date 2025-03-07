from fastapi import FastAPI

from .routers import rescue_predict_router

tags_metadata = [
    {
        "name": "data",
        "description": "Show data",
    },
    {"name": "machine-learning", "description": "Prediction Endpoint."},
]

app = FastAPI(
    title="🪐 Getaround API",
    description="API for Getaround",
    version="0.1",
    contact={
        "name": "Alexandre",
        "url": "https://github.com/littlerobinson",
    },
    openapi_tags=tags_metadata,
)

app.include_router(rescue_predict_router.router)


@app.get("/")
async def root():
    return {"message": "Hello Rescue Predict API!"}

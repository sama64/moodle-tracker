from __future__ import annotations

from fastapi import FastAPI

from uni_tracker.api.routes import router


app = FastAPI(title="uni-tracker", version="0.1.0")
app.include_router(router)

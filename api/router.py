

from fastapi import APIRouter, HTTPException, Depends
from typing import List, Union, Optional
from pydantic import UUID4, PositiveFloat


api_router = APIRouter(
    prefix="/api",
    tags=["manager router"]
)
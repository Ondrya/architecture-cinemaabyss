from pydantic import BaseModel, Field
from typing import Optional, List
from datetime import datetime
from uuid import uuid4


class MovieEventPayload(BaseModel):
    movie_id: int
    title: str
    action: str
    user_id: Optional[int] = None
    rating: Optional[float] = None
    genres: Optional[List[str]] = None
    description: Optional[str] = None


class UserEventPayload(BaseModel):
    user_id: int
    action: str
    timestamp: datetime
    username: Optional[str] = None
    email: Optional[str] = None


class PaymentEventPayload(BaseModel):
    payment_id: int
    user_id: int
    amount: float
    status: str
    timestamp: datetime
    method_type: Optional[str] = None


class EventResponse(BaseModel):
    status: str = "success"
    partition: int
    offset: int
    event: dict  # или можно создать BaseEvent, но для MVP — dict
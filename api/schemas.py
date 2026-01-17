
from pydantic import BaseModel

class TopProduct(BaseModel):
    term: str  # The product/term mentioned
    count: int  # Number of times mentioned

    class Config:
        schema_extra = {
            "example": {"term": "paracetamol", "count": 123}
        }

class ChannelActivity(BaseModel):
    date: str  # Date in YYYY-MM-DD
    message_count: int  # Number of messages posted on that date

    class Config:
        schema_extra = {
            "example": {"date": "2026-01-17", "message_count": 42}
        }

class MessageSearchResult(BaseModel):
    message_id: int  # Unique message ID
    channel_name: str  # Channel where message was posted
    message_text: str  # The message content
    message_date: str  # Date of the message

    class Config:
        schema_extra = {
            "example": {
                "message_id": 12345,
                "channel_name": "CheMed123",
                "message_text": "Paracetamol is available now!",
                "message_date": "2026-01-17"
            }
        }

class VisualContentStats(BaseModel):
    channel_name: str  # Channel name
    total_images: int  # Total images posted
    unique_images: int  # Unique image files
    messages_with_images: int  # Messages containing images

    class Config:
        schema_extra = {
            "example": {
                "channel_name": "CheMed123",
                "total_images": 100,
                "unique_images": 80,
                "messages_with_images": 90
            }
        }

class TrendingChannel(BaseModel):
    channel_name: str  # Channel name
    last_7_days: int  # Posts in last 7 days
    prev_7_days: int  # Posts in previous 7 days
    increase: int  # Increase in posts

    class Config:
        schema_extra = {
            "example": {
                "channel_name": "CheMed123",
                "last_7_days": 30,
                "prev_7_days": 10,
                "increase": 20
            }
        }

"""
Telegram Scraper for Medical Telegram Warehouse
Extracts messages and images from specified channels and stores them in a raw data lake.

Features:
- Rate limiting to avoid Telegram bans
- Error recovery and retry logic
- Progress tracking
- Data validation
- Configurable limits
"""

import os
import json
import logging
import asyncio
import random
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from telethon import TelegramClient, errors
from telethon.tl.types import MessageMediaPhoto, Channel
from dotenv import load_dotenv
import hashlib
import traceback
from tqdm import tqdm
import csv

# Load environment variables
load_dotenv()

CHANNELS_CONFIG_PATH = os.getenv("CHANNELS_CONFIG_PATH", "channels.json")

def load_channels():
    if os.path.exists(CHANNELS_CONFIG_PATH):
        with open(CHANNELS_CONFIG_PATH, "r") as f:
            return json.load(f)
    # fallback to hardcoded list
    return [
        "lobelia4cosmetics",
        "tikvahpharma",
    ]

class TelegramScraper:
    def __init__(self, session_name: str = "medical_telegram"):
        self.api_id = int(os.getenv("TELEGRAM_API_ID"))
        self.api_hash = os.getenv("TELEGRAM_API_HASH")
        self.session_name = session_name
        
        # Configuration
        self.channels = load_channels()
        
        # Rate limiting - CRITICAL to avoid bans
        self.max_messages_per_channel = 1000  # Start small
        self.delay_between_messages = 0.5  # seconds
        self.max_retries = 3
        self.request_timeout = 30
        
        # Directories
        self.raw_data_dir = "data/raw/telegram_messages"
        self.image_dir = "data/raw/images"
        self.log_dir = "logs"
        
        # Create directories
        for directory in [self.raw_data_dir, self.image_dir, self.log_dir]:
            os.makedirs(directory, exist_ok=True)
        
        self.setup_logging()
        self.client = None
        
    def setup_logging(self):
        """Configure logging with file and console output"""
        log_file = os.path.join(
            self.log_dir, 
            f"scraper_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
        )
        
        # Clear any existing handlers
        logging.getLogger().handlers = []
        
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            handlers=[
                logging.FileHandler(log_file, encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
        
    async def connect(self) -> bool:
        """Establish connection to Telegram"""
        try:
            self.client = TelegramClient(
                self.session_name, 
                self.api_id, 
                self.api_hash
            )
            
            await self.client.connect()
            
            if not await self.client.is_user_authorized():
                await self.client.send_code_request(os.getenv("TELEGRAM_PHONE"))
                code = input("Enter the code you received: ")
                await self.client.sign_in(os.getenv("TELEGRAM_PHONE"), code)
            
            self.logger.info("Successfully connected to Telegram")
            return True
            
        except Exception as e:
            self.logger.error(f"Connection failed: {e}")
            return False
    
    async def safe_get_messages(self, channel_entity, limit: int = 100) -> List:
        """Safely fetch messages with retry logic"""
        messages = []
        retry_count = 0
        
        while retry_count < self.max_retries:
            try:
                async for message in self.client.iter_messages(
                    channel_entity, 
                    limit=limit,
                    wait_time=self.delay_between_messages  # Rate limiting
                ):
                    messages.append(message)
                    
                    # Add random delay to avoid detection
                    if len(messages) % 50 == 0:
                        delay = random.uniform(1, 3)
                        await asyncio.sleep(delay)
                        self.logger.info(f"Fetched {len(messages)} messages...")
                
                break  # Success, exit retry loop
                
            except errors.FloodWaitError as e:
                self.logger.warning(f"Flood wait: {e.seconds} seconds")
                await asyncio.sleep(e.seconds + 5)
                retry_count += 1
                
            except errors.RPCError as e:
                self.logger.error(f"RPC Error: {e}")
                retry_count += 1
                await asyncio.sleep(5)
                
            except Exception as e:
                self.logger.error(f"Unexpected error: {e}")
                retry_count += 1
                await asyncio.sleep(10)
        
        return messages
    
    async def process_message(self, message, channel_name: str) -> Optional[Dict]:
        """Convert Telegram message to structured data"""
        try:
            # Basic message data
            msg_data = {
                "message_id": message.id,
                "channel_name": channel_name,
                "message_date": message.date.isoformat() if message.date else None,
                "message_text": message.text or "",
                "has_media": bool(message.media),
                "views": getattr(message, "views", 0) or 0,
                "forwards": getattr(message, "forwards", 0) or 0,
                "image_path": None,
                "scraped_at": datetime.now().isoformat(),
                "message_length": len(message.text or ""),
                "contains_price": self._contains_price(message.text or ""),
                "contains_contact": self._contains_contact(message.text or "")
            }
            
            # Handle media
            if message.media and isinstance(message.media, MessageMediaPhoto):
                img_path = await self.download_image(message, channel_name)
                if img_path:
                    msg_data["image_path"] = img_path
            
            return msg_data
            
        except Exception as e:
            import traceback
            self.logger.error(f"Error processing message {message.id}: {e}\n{traceback.format_exc()}")
            return None
    
    def _contains_price(self, text: str) -> bool:
        """Check if message contains price information"""
        price_patterns = ['ETB', 'birr', 'USD', '$', 'price', 'ህ']
        return any(pattern.lower() in text.lower() for pattern in price_patterns)
    
    def _contains_contact(self, text: str) -> bool:
        """Check if message contains contact information"""
        contact_patterns = ['09', '+251', '@', 'telegram', 'call', 'ጥያቄ']
        return any(pattern.lower() in text.lower() for pattern in contact_patterns)
    
    async def download_image(self, message, channel_name: str) -> Optional[str]:
        """Download image from message"""
        try:
            img_folder = os.path.join(self.image_dir, channel_name.replace('@', ''))
            os.makedirs(img_folder, exist_ok=True)
            
            # Create unique filename with hash to avoid duplicates
            filename_hash = hashlib.md5(
                f"{channel_name}_{message.id}_{datetime.now().timestamp()}".encode()
            ).hexdigest()[:8]
            
            img_path = os.path.join(img_folder, f"{message.id}_{filename_hash}.jpg")
            
            # Download with timeout
            await asyncio.wait_for(
                message.download_media(file=img_path),
                timeout=self.request_timeout
            )
            
            if os.path.exists(img_path) and os.path.getsize(img_path) > 0:
                self.logger.debug(f"Downloaded image: {img_path}")
                return img_path
                
        except asyncio.TimeoutError:
            self.logger.warning(f"Timeout downloading image for message {message.id}")
        except Exception as e:
            self.logger.error(f"Error downloading image: {e}")
        
        return None
    
    def save_messages(self, messages: List[Dict], channel_name: str, output_format: str = "json"):
        """Save messages to JSON file with partitioned structure"""
        if not messages:
            self.logger.warning(f"No messages to save for {channel_name}")
            return
        
        # Partition by date (use first message's date or today)
        if messages[0].get('message_date'):
            date_part = messages[0]['message_date'][:10]  # YYYY-MM-DD
        else:
            date_part = datetime.now().strftime("%Y-%m-%d")
        
        out_folder = os.path.join(self.raw_data_dir, date_part)
        os.makedirs(out_folder, exist_ok=True)
        
        # Sanitize filename
        safe_channel_name = channel_name.replace('@', '').replace('/', '_')
        out_path = os.path.join(out_folder, f"{safe_channel_name}.json")
        
        # Save with pretty formatting
        if output_format == "json":
            with open(out_path, 'w', encoding='utf-8') as f:
                json.dump(messages, f, indent=2, ensure_ascii=False)
            self.logger.info(f"Saved {len(messages)} messages to {out_path}")
        elif output_format == "csv":
            csv_path = out_path.replace('.json', '.csv')
            with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=messages[0].keys())
                writer.writeheader()
                writer.writerows(messages)
            self.logger.info(f"Saved {len(messages)} messages to {csv_path}")
        
        # Also save a summary
        summary = {
            "channel": channel_name,
            "total_messages": len(messages),
            "date_scraped": datetime.now().isoformat(),
            "date_range": {
                "earliest": messages[-1].get('message_date') if messages else None,
                "latest": messages[0].get('message_date') if messages else None
            },
            "with_images": sum(1 for m in messages if m.get('image_path')),
            "total_views": sum(m.get('views', 0) for m in messages),
            "file_path": out_path
        }
        
        summary_path = os.path.join(out_folder, f"{safe_channel_name}_summary.json")
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
    
    async def scrape_channel(self, channel_identifier: str):
        """Main scraping function for a single channel"""
        self.logger.info(f"Starting scrape for: {channel_identifier}")
        
        try:
            # Get channel entity
            channel_entity = await self.client.get_entity(channel_identifier)
            channel_name = getattr(channel_entity, 'title', channel_identifier)
            channel_username = getattr(channel_entity, 'username', channel_identifier)
            
            self.logger.info(f"Found channel: {channel_name} (@{channel_username})")
            
            # Fetch messages
            raw_messages = await self.safe_get_messages(
                channel_entity, 
                self.max_messages_per_channel
            )
            
            if not raw_messages:
                self.logger.warning(f"No messages found for {channel_identifier}")
                return 0
            
            # Process messages
            from tqdm import tqdm
            processed_messages = await asyncio.gather(*[
                self.process_message(msg, channel_username) for msg in tqdm(raw_messages, desc=f"Processing {channel_identifier}")
            ])
            processed_messages = [m for m in processed_messages if m]
            
            # Save to file
            self.save_messages(processed_messages, channel_username, output_format="json")
            
            self.logger.info(
                f"Completed {channel_identifier}: "
                f"{len(processed_messages)} messages processed"
            )
            
            return len(processed_messages)
            
        except errors.ChannelInvalidError:
            self.logger.error(f"Channel not found or inaccessible: {channel_identifier}")
        except Exception as e:
            self.logger.error(f"Error scraping {channel_identifier}: {e}")
        
        return 0
    
    async def scrape_all(self):
        """Scrape all configured channels"""
        if not await self.connect():
            self.logger.error("Cannot proceed without Telegram connection")
            return
        
        total_messages = 0
        
        for channel in self.channels:
            try:
                count = await self.scrape_channel(channel)
                total_messages += count
                
                # Random delay between channels
                if channel != self.channels[-1]:
                    delay = random.uniform(10, 30)
                    self.logger.info(f"Waiting {delay:.1f}s before next channel...")
                    await asyncio.sleep(delay)
                    
            except Exception as e:
                self.logger.error(f"Fatal error with channel {channel}: {e}")
                continue
        
        self.logger.info(f"Total scraped: {total_messages} messages")
        
        # Generate final report
        self.generate_scrape_report(total_messages)
        
    def generate_scrape_report(self, total_messages: int):
        """Generate a summary report of the scraping session"""
        report = {
            "scrape_session": {
                "date": datetime.now().isoformat(),
                "total_channels": len(self.channels),
                "total_messages": total_messages,
                "channels_scraped": self.channels,
                "rate_limit_settings": {
                    "max_per_channel": self.max_messages_per_channel,
                    "delay": self.delay_between_messages
                }
            },
            "data_location": {
                "raw_messages": self.raw_data_dir,
                "images": self.image_dir,
                "logs": self.log_dir
            }
        }
        
        report_path = os.path.join(self.log_dir, f"scrape_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        self.logger.info(f"Report saved: {report_path}")

# Main execution
async def main():
    scraper = TelegramScraper()
    await scraper.scrape_all()

if __name__ == "__main__":
    # Check for required environment variables
    required_vars = ["TELEGRAM_API_ID", "TELEGRAM_API_HASH", "TELEGRAM_PHONE"]
    missing = [var for var in required_vars if not os.getenv(var)]
    
    if missing:
        print(f"Missing environment variables: {', '.join(missing)}")
        print("Please set them in your .env file:")
        print("TELEGRAM_API_ID=your_id")
        print("TELEGRAM_API_HASH=your_hash")
        print("TELEGRAM_PHONE=+251...")
        exit(1)
    
    # Run the scraper
    asyncio.run(main())

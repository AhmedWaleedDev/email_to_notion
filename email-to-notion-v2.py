import imaplib
import email
from email.header import decode_header
import os
from datetime import datetime, timedelta
import re
from notion_client import Client
import json
from typing import List, Dict, Optional
import time
import logging
from pathlib import Path
import sqlite3
import schedule
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

class ConfigManager:
    def __init__(self, config_dir: str = "config"):
        self.config_dir = Path(config_dir)
        self.config_dir.mkdir(exist_ok=True)
        self.load_configs()

    def load_configs(self):
        """Load all configuration files"""
        self.keywords = self.load_json_config("keywords.json", {
            'assignment': ['assignment', 'homework', 'hw', 'project', 'submit', 'submission'],
            'exam': ['exam', 'test', 'quiz', 'midterm', 'final', 'assessment'],
            'deadline': ['deadline', 'due', 'due date', 'by', 'until'],
            'meeting': ['meeting', 'class', 'lecture', 'seminar', 'workshop']
        })
        
        self.schedule_config = self.load_json_config("schedule.json", {
            "fixed_times": ["09:00", "12:00", "15:00", "18:00", "21:00"],
            "interval_minutes": 30,
            "catch_up_missed": True,
            "max_catch_up_hours": 24
        })
        
        self.ignore_list = self.load_json_config("ignore_list.json", {
            "emails": [],
            "subjects": [],
            "domains": []
        })

    def load_json_config(self, filename: str, default_config: dict) -> dict:
        """Load a JSON config file or create with defaults if it doesn't exist"""
        file_path = self.config_dir / filename
        if not file_path.exists():
            with open(file_path, 'w') as f:
                json.dump(default_config, f, indent=4)
            return default_config
        
        with open(file_path, 'r') as f:
            return json.load(f)

class EmailProcessor:
    def __init__(self, email_address: str, email_password: str, notion_token: str, database_id: str):
        self.email_address = email_address
        self.email_password = email_password
        # self.credentials = Credentials(email_address, email_password)
        # self.account = Account(email_address, credentials=self.credentials, autodiscover=True, access_type=DELEGATE)

        self.notion = Client(auth=notion_token)
        self.database_id = database_id
        
        # Initialize configuration manager
        self.config = ConfigManager()
        
        # Create necessary directories
        Path("logs").mkdir(exist_ok=True)
        Path("data").mkdir(exist_ok=True)
        
        # Setup logging and database
        self.setup_logging()
        self.init_database()
        
        # Initialize last run tracking
        self.last_run_file = Path("data/last_run.txt")
        self.load_last_run_time()

    def setup_logging(self):
        """Setup rotating log file"""
        log_file = Path("logs/email_processor.log")
        
        handler = RotatingFileHandler(
            log_file,
            maxBytes=1024*1024,  # 1MB
            backupCount=5
        )
        
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        
        self.logger = logging.getLogger("EmailProcessor")
        self.logger.setLevel(logging.INFO)
        self.logger.addHandler(handler)
        
        # Also add console handler for immediate feedback
        console_handler = logging.StreamHandler()
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

    def init_database(self):
        """Initialize SQLite database for tracking processed emails"""
        db_path = Path("data/processed_emails.db")
        self.conn = sqlite3.connect(db_path)
        cursor = self.conn.cursor()
        
        # Create table if it doesn't exist
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS processed_emails (
                message_id TEXT PRIMARY KEY,
                subject TEXT,
                processed_date TIMESTAMP,
                status TEXT
            )
        ''')
        self.conn.commit()

    def load_last_run_time(self):
        """Load the last successful run time"""
        if self.last_run_file.exists():
            with open(self.last_run_file, 'r') as f:
                last_run_str = f.read().strip()
                self.last_run_time = datetime.fromisoformat(last_run_str)
        else:
            self.last_run_time = datetime.now() - timedelta(hours=24)
            self.save_last_run_time()

    def save_last_run_time(self):
        """Save the current run time"""
        with open(self.last_run_file, 'w') as f:
            f.write(datetime.now().isoformat())


    def is_email_processed(self, message_id: str) -> bool:
        """Check if email has already been processed"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT 1 FROM processed_emails WHERE message_id = ?", (message_id,))
        return cursor.fetchone() is not None

    def mark_email_processed(self, message_id: str, subject: str, status: str = "success"):
        """Mark email as processed in database"""
        cursor = self.conn.cursor()
        cursor.execute(
            "INSERT INTO processed_emails (message_id, subject, processed_date, status) VALUES (?, ?, ?, ?)",
            (message_id, subject, datetime.now(), status)
        )
        self.conn.commit()

    def should_ignore(self, from_addr: str, subject: str) -> bool:
        """Check if email should be ignored based on ignore list"""
        # Check if email address is in ignore list
        if from_addr in self.config.ignore_list["emails"]:
            return True
            
        # Check if domain is in ignore list
        domain = from_addr.split('@')[-1]
        if domain in self.config.ignore_list["domains"]:
            return True
            
        # Check if subject contains any ignored phrases
        for ignored_subject in self.config.ignore_list["subjects"]:
            if ignored_subject.lower() in subject.lower():
                return True
                
        return False

    def detect_task_type(self, subject: str, body: str) -> List[str]:
        """Detect task type based on keywords in subject and body"""
        text = f"{subject.lower()} {body.lower()}"
        detected_types = []
        
        for task_type, keywords in self.config.keywords.items():
            if any(keyword in text for keyword in keywords):
                detected_types.append(task_type)
                
        return detected_types if detected_types else ['other']
    
    def parse_due_date(content: str) -> Optional[str]:
        """Extract a due date from the email content if available."""
        try:
            # Define a regex pattern to find potential dates in the content
            date_patterns = [
                r"\b(?:due\s(?:on|by)?\s)?(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})\b",   # Formats like "due on 12/31/2024"
                r"\b(?:due\s(?:on|by)?\s)?((?:\d{1,2}\s)?(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s\d{2,4})\b",  # "due by 31 Dec 2024"
            ]

            # Try to match any of the date patterns
            for pattern in date_patterns:
                match = re.search(pattern, content, re.IGNORECASE)
                if match:
                    # Parse and standardize the found date
                    due_date = date_parse(match.group(1), fuzzy=True)
                    self.logger.error(f"Due_data found in email: {title}")
                    return due_date.strftime("%Y-%m-%d")  # Format as "YYYY-MM-DD" for Notion

        except Exception as e:
            self.logger.error(f"No due_data found: {str(e)}")
        return None  # Return None if no due date found

    def create_notion_page(self, title: str, content: str, task_types: List[str], due_date: Optional[str] = None):
        """Create a new page in Notion database"""
        try:
            properties = {
                "Title": {"title": [{"text": {"content": title}}]},
                "Type": {"multi_select": [{"name": task_type} for task_type in task_types]},
                "Status": {"select": {"name": "New"}},
            }
            
            if due_date:
                properties["Due Date"] = {"date": {"start": due_date}}

            self.notion.pages.create(
                parent={"database_id": self.database_id},
                properties=properties,
                children=[
                    {
                        "object": "block",
                        "type": "paragraph",
                        "paragraph": {
                            "rich_text": [{
                                "type": "text",
                                "text": {"content": content}
                            }]
                        }
                    }
                ]
            )
            self.logger.info(f"Created Notion page: {title}")
        except Exception as e:
            self.logger.error(f"Error creating Notion page: {str(e)}")
            raise

def process_emails(self, since_time: Optional[datetime] = None):
    """Process new emails and create Notion tasks with optional due dates"""
    try:
        self.logger.info("Starting email processing...")
        
        mail = imaplib.IMAP4_SSL("imap.gmail.com")
        mail.login(self.email_address, self.email_password)
        mail.select("inbox")

        # Search criteria based on time
        if since_time:
            date_str = since_time.strftime("%d-%b-%Y")
            search_criteria = f'(SINCE "{date_str}")'
        else:
            date_str = (datetime.now() - timedelta(days=1)).strftime("%d-%b-%Y")
            search_criteria = f'(SINCE "{date_str}")'

        self.logger.info(f"Searching emails with criteria: {search_criteria}")
        _, messages = mail.search(None, search_criteria)

        for message_num in messages[0].split():
            try:
                _, msg_data = mail.fetch(message_num, "(RFC822)")
                email_body = msg_data[0][1]
                email_message = email.message_from_bytes(email_body)
                
                # Get message ID for tracking
                message_id = email_message["Message-ID"]
                
                if not message_id:
                    self.logger.warning("Email without Message-ID found, generating unique ID")
                    message_id = f"generated-{datetime.now().timestamp()}"
                
                # Skip if already processed
                if self.is_email_processed(message_id):
                    continue

                # Process email content
                subject = decode_header(email_message["subject"])[0][0]
                if isinstance(subject, bytes):
                    subject = subject.decode()
                from_addr = email_message.get("from")

                self.logger.info(f"Processing email: {subject}")

                # Skip if email should be ignored
                if self.should_ignore(from_addr, subject):
                    self.logger.info(f"Ignoring email: {subject}")
                    continue

                # Get email content
                content = self.extract_email_content(email_message)

                # Extract due date from content if available
                due_date = parse_due_date(content)

                # Create Notion page
                task_types = self.detect_task_type(subject, content)
                self.create_notion_page(subject, content, task_types, due_date)
                
                # Mark as processed
                self.mark_email_processed(message_id, subject)
                self.logger.info(f"Successfully processed email: {subject}")

            except Exception as e:
                self.logger.error(f"Error processing individual email: {str(e)}")
                continue

        mail.logout()
        self.logger.info("Completed email processing")

    except Exception as e:
        self.logger.error(f"Error in process_emails: {str(e)}")
        raise

    def extract_email_content(self, email_message) -> str:
        """Extract content from email message"""
        content = ""
        if email_message.is_multipart():
            for part in email_message.walk():
                if part.get_content_type() == "text/plain":
                    try:
                        content += part.get_payload(decode=True).decode()
                    except Exception as e:
                        self.logger.error(f"Error decoding email part: {str(e)}")
        else:
            try:
                content = email_message.get_payload(decode=True).decode()
            except Exception as e:
                self.logger.error(f"Error decoding email content: {str(e)}")
        return content

def run_processor():
    """Main function to run the email processor"""
    try:
        # Load environment variables
        email_address = os.getenv("EMAIL_ADDRESS")
        email_password = os.getenv("EMAIL_PASSWORD")
        notion_token = os.getenv("NOTION_TOKEN")
        database_id = os.getenv("DATABASE_ID")

        if not all([email_address, email_password, notion_token, database_id]):
            raise ValueError("Missing required environment variables")

        processor = EmailProcessor(email_address, email_password, notion_token, database_id)
        
        # Initial run
        processor.process_emails()
        
        # Schedule regular runs
        for time_str in processor.config.schedule_config["fixed_times"]:
            schedule.every().day.at(time_str).do(processor.process_emails)
        
        schedule.every(processor.config.schedule_config["interval_minutes"]).minutes.do(
            processor.process_emails
        )

        while True:
            schedule.run_pending()
            time.sleep(60)

    except Exception as e:
        logging.error(f"Fatal error in main process: {str(e)}")
        raise

if __name__ == "__main__":
    run_processor()
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
        self.notion = Client(auth=notion_token)
        self.database_id = database_id
        
        # Initialize configuration manager
        self.config = ConfigManager()
        
        # Setup logging and database
        self.setup_logging()
        self.init_database()
        
        # Initialize last run tracking
        self.last_run_file = Path("last_run.txt")
        self.load_last_run_time()

    def load_last_run_time(self):
        """Load the last successful run time"""
        if self.last_run_file.exists():
            with open(self.last_run_file, 'r') as f:
                last_run_str = f.read().strip()
                self.last_run_time = datetime.fromisoformat(last_run_str)
        else:
            self.last_run_time = datetime.now() - timedelta(hours=24)

    def save_last_run_time(self):
        """Save the current run time"""
        with open(self.last_run_file, 'w') as f:
            f.write(datetime.now().isoformat())

    def calculate_missed_runs(self) -> List[datetime]:
        """Calculate missed runs based on schedule configuration"""
        if not self.config.schedule_config["catch_up_missed"]:
            return []

        now = datetime.now()
        missed_runs = []
        
        # Calculate missed fixed times
        for time_str in self.config.schedule_config["fixed_times"]:
            hour, minute = map(int, time_str.split(":"))
            scheduled_time = now.replace(hour=hour, minute=minute)
            
            if self.last_run_time < scheduled_time < now:
                missed_runs.append(scheduled_time)

        # Calculate missed intervals
        interval_minutes = self.config.schedule_config["interval_minutes"]
        max_catch_up = timedelta(hours=self.config.schedule_config["max_catch_up_hours"])
        
        current_time = max(self.last_run_time, now - max_catch_up)
        while current_time < now:
            current_time += timedelta(minutes=interval_minutes)
            if current_time < now:
                missed_runs.append(current_time)

        return sorted(missed_runs)

    def process_emails_with_catch_up(self):
        """Process emails including catching up on missed runs"""
        missed_runs = self.calculate_missed_runs()
        
        for run_time in missed_runs:
            self.logger.info(f"Catching up on missed run from: {run_time}")
            self.process_emails(since_time=run_time)

        # Process current run
        self.process_emails()
        self.save_last_run_time()

    def process_emails(self, since_time: Optional[datetime] = None):
        """Process new emails and create Notion tasks"""
        try:
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

            _, messages = mail.search(None, search_criteria)

            for message_num in messages[0].split():
                # [Rest of the email processing code remains the same]
                pass

            mail.logout()

        except Exception as e:
            self.logger.error(f"Error in process_emails: {str(e)}")

def run_processor():
    """Main function to run the email processor"""
    # Load environment variables
    email_address = os.getenv("EMAIL_ADDRESS")
    email_password = os.getenv("EMAIL_PASSWORD")
    notion_token = os.getenv("NOTION_TOKEN")
    database_id = os.getenv("DATABASE_ID")

    if not all([email_address, email_password, notion_token, database_id]):
        raise ValueError("Missing required environment variables")

    processor = EmailProcessor(email_address, email_password, notion_token, database_id)
    config = processor.config.schedule_config
    
    # Schedule fixed time checks
    for time_str in config["fixed_times"]:
        schedule.every().day.at(time_str).do(processor.process_emails_with_catch_up)
    
    # Schedule interval checks
    schedule.every(config["interval_minutes"]).minutes.do(processor.process_emails_with_catch_up)

    while True:
        schedule.run_pending()
        time.sleep(60)

if __name__ == "__main__":
    run_processor()

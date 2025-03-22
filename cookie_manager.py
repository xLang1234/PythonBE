import os
import json
import random
import time
from datetime import datetime
from loguru import logger
from pathlib import Path

class TwitterCookieManager:
    """
    Manages multiple Twitter cookie files for rotation during scraping operations.
    This helps avoid rate limits and provides fallback options when cookies expire.
    """
    
    def __init__(self, cookies_dir="twitterCookies"):
        """
        Initialize the cookie manager.
        
        Args:
            cookies_dir (str): Directory where cookie files are stored
        """
        self.cookies_dir = cookies_dir
        self.ensure_cookie_dir_exists()
        self.cookie_files = []
        self.current_cookie_index = 0
        self.last_cookie_switch_time = time.time()
        self.min_time_between_switches = 60  # Minimum time in seconds between cookie switches
        self.load_available_cookies()
        
    def ensure_cookie_dir_exists(self):
        """Create the cookies directory if it doesn't exist"""
        Path(self.cookies_dir).mkdir(parents=True, exist_ok=True)
        logger.info(f"Ensured cookie directory exists: {self.cookies_dir}")
    
    def load_available_cookies(self):
        """Load all available cookie files from the cookie directory"""
        if not os.path.exists(self.cookies_dir):
            logger.warning(f"Cookie directory {self.cookies_dir} does not exist")
            return
        
        self.cookie_files = [
            os.path.join(self.cookies_dir, f) 
            for f in os.listdir(self.cookies_dir) 
            if f.endswith('.json')
        ]
        
        if not self.cookie_files:
            logger.warning(f"No cookie files found in {self.cookies_dir}")
        else:
            logger.info(f"Loaded {len(self.cookie_files)} cookie files")
            
        # Randomize the initial cookie selection
        random.shuffle(self.cookie_files)
    
    def get_next_cookie_file(self, force_rotate=False):
        """
        Get the next cookie file in the rotation.
        
        Args:
            force_rotate (bool): Force rotation even if minimum time hasn't elapsed
            
        Returns:
            str: Path to the next cookie file or None if no cookies are available
        """
        if not self.cookie_files:
            logger.warning("No cookie files available for rotation")
            return None
        
        current_time = time.time()
        time_since_last_switch = current_time - self.last_cookie_switch_time
        
        # Only rotate if forcing or enough time has passed since last switch
        if force_rotate or time_since_last_switch >= self.min_time_between_switches:
            self.current_cookie_index = (self.current_cookie_index + 1) % len(self.cookie_files)
            self.last_cookie_switch_time = current_time
            logger.info(f"Rotated to cookie file {self.current_cookie_index + 1}/{len(self.cookie_files)}")
        
        return self.cookie_files[self.current_cookie_index]
    
    def get_random_cookie_file(self):
        """
        Get a random cookie file from the available cookies.
        
        Returns:
            str: Path to a random cookie file or None if no cookies are available
        """
        if not self.cookie_files:
            return None
        
        random_cookie = random.choice(self.cookie_files)
        logger.debug(f"Randomly selected cookie file: {os.path.basename(random_cookie)}")
        return random_cookie
    
    def mark_cookie_invalid(self, cookie_file):
        """
        Mark a cookie as invalid by moving it to a backup file.
        
        Args:
            cookie_file (str): Path to the cookie file to mark as invalid
        """
        if cookie_file not in self.cookie_files:
            logger.warning(f"Cookie file {cookie_file} not in rotation, cannot mark invalid")
            return
        
        try:
            # Rename cookie file with timestamp to preserve it
            invalid_name = f"{cookie_file}.invalid.{int(time.time())}"
            os.rename(cookie_file, invalid_name)
            
            # Remove from active rotation
            self.cookie_files.remove(cookie_file)
            
            # Reset current index if necessary
            if not self.cookie_files:
                self.current_cookie_index = 0
            else:
                self.current_cookie_index = self.current_cookie_index % len(self.cookie_files)
                
            logger.warning(f"Marked cookie file {os.path.basename(cookie_file)} as invalid and removed from rotation")
            
        except Exception as e:
            logger.error(f"Error marking cookie as invalid: {str(e)}")
    
    def add_cookie_file(self, username, cookie_data):
        """
        Add a new cookie file to the rotation.
        
        Args:
            username (str): Twitter username associated with the cookie
            cookie_data (dict): Cookie data to save
            
        Returns:
            str: Path to the newly created cookie file
        """
        try:
            # Generate filename with timestamp to ensure uniqueness
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{username}_{timestamp}.json"
            file_path = os.path.join(self.cookies_dir, filename)
            
            # Write cookie data to file
            with open(file_path, 'w') as f:
                json.dump(cookie_data, f, indent=2)
            
            # Add to rotation
            self.cookie_files.append(file_path)
            logger.info(f"Added new cookie file for {username} to rotation")
            
            return file_path
            
        except Exception as e:
            logger.error(f"Error adding cookie file: {str(e)}")
            return None
    
    def validate_all_cookies(self):
        """
        Check all cookies and report their status.
        This could be extended to actually validate cookies against Twitter.
        
        Returns:
            dict: Report of cookie statuses
        """
        report = {
            "total_cookies": len(self.cookie_files),
            "cookie_files": []
        }
        
        for cookie_file in self.cookie_files:
            try:
                mtime = os.path.getmtime(cookie_file)
                age_days = (time.time() - mtime) / (60 * 60 * 24)
                
                cookie_info = {
                    "file": os.path.basename(cookie_file),
                    "age_days": round(age_days, 1),
                    "last_modified": datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S")
                }
                
                report["cookie_files"].append(cookie_info)
                
            except Exception as e:
                logger.error(f"Error checking cookie file {cookie_file}: {str(e)}")
        
        return report

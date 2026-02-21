import os
import json
import asyncio
import aiohttp
import time
import re
import socket
import geoip2.database
import logging
import base64
import subprocess
import shutil
from datetime import datetime
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

# ==============================================================================
# --- CONFIGURATION & LOGGING SETUP ---
# ==============================================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("MonsterEngine")

# --- DIRECTORY STRUCTURE ---
DB_DIR = 'database'   
OUTPUT_DIR = 'proxy'  

# Files inside "database"
SOURCE_FILE = 'all_sources.txt'
STATE_FILE = os.path.join(DB_DIR, 'monster_state.json')
GEOIP_DB = os.path.join(DB_DIR, 'GeoLite2-Country.mmdb')
LOCK_FILE = os.path.join(DB_DIR, '.monster.lock')

# Files inside "proxy"
LINKS_INFO_FILE = os.path.join(OUTPUT_DIR, 'LINKS_FOR_CLIENTS.txt')

# --- ENGINE CONSTANTS ---
TIMEOUT = 3              
MAX_CONCURRENCY = 150    
CYCLE_HOURS = 3          
BATCH_INTERVAL_MIN = 20  

# --- NETWORK THRESHOLDS ---
PING_LIMITS = {
    'DEFAULT': 250,
    'US': 300, 
    'HK': 300, 
    'SG': 300, 
    'JP': 300,
    'BY': 200, 
    'KZ': 200, 
    'RU': 250
}

# Специальный порог для "инвалидных" серверов (медленные, но важные)
INVALID_THRESHOLD_MIN = 250
INVALID_THRESHOLD_MAX = 350 # Расширил до 350 для запаса по США/КЗ
INVALID_REGIONS = {'BY', 'KZ', 'US'}

# Приоритетные регионы (сортируются в начало очереди)
PRIORITY_REGIONS = {'BY', 'KZ', 'DE', 'FI', 'SE', 'LV', 'RU', 'US', 'CH', 'FR'}

# --- COUNTRY TO FILE MAPPING (Strict 10 Countries) ---
COUNTRY_MAP = {
    'RU': 'russia.txt', 
    'BY': 'belarus.txt', 
    'DE': 'germany.txt',
    'FR': 'france.txt',
    'FI': 'finland.txt',
    'KZ': 'kazakhstan.txt',
    'LV': 'latvia.txt',
    'CH': 'switzerland.txt',
    'US': 'usa.txt',
    'SE': 'sweden.txt'
}
DEFAULT_MIX = 'mix.txt'
INVALID_FILE = 'invalid.txt' # Тот самый файл для медленных BY, KZ, US
MAX_NODES_PER_FILE = 500

class MonsterParser:
    """
    Advanced engine for parsing, checking, and distributing proxy configurations.
    Maintains strict file structure and updates metadata for GitHub tracking.
    """
    
    def __init__(self):
        self.start_time = time.time()
        self.ensure_structure()
        self.migrate_old_data()
        self.state = self.load_state()
        self.geo_reader = self.init_geo()
        
        # Optimized regex patterns
        self.proxy_pattern = re.compile(r'(vless|vmess|trojan|ss|ssr)://[^\s"\'<>()]+')
        self.ip_pattern = re.compile(r'@?([\w\.-]+):(\d+)')
        
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
        # Список всех файлов, которые МЫ ОБЯЗАНЫ ОБНОВИТЬ (включая invalid.txt)
        self.mandatory_files = set(COUNTRY_MAP.values()) | {DEFAULT_MIX, INVALID_FILE}

    def ensure_structure(self):
        """Ensures directories and basic files exist."""
        os.makedirs(DB_DIR, exist_ok=True)
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        if not os.path.exists(SOURCE_FILE):
            with open(SOURCE_FILE, 'w', encoding='utf-8') as f:
                f.write("# Monster Engine Source List\n")

    def migrate_old_data(self):
        """Moves data from old Cyrillic folders to new Latin structure if needed."""
        old_folders = {'база данных': DB_DIR, 'прокси': OUTPUT_DIR}
        for old, new in old_folders.items():
            if os.path.exists(old) and os.path.isdir(old):
                logger.info(f"🔄 Migrating data from '{old}' to '{new}'...")
                for item in os.listdir(old):
                    src = os.path.join(old, item)
                    dst = os.path.join(new, item)
                    try:
                        if os.path.exists(dst): os.remove(src)
                        else: shutil.move(src, dst)
                    except Exception as e:
                        logger.debug(f"Migration error for {item}: {e}")
                try: os.rmdir(old)
                except: pass

    def init_geo(self):
        """Initializes GeoIP2 Database reader."""
        if os.path.exists(GEOIP_DB):
            try:
                return geoip2.database.Reader(GEOIP_DB)
            except Exception as e:
                logger.error(f"GeoIP Error: {e}")
        return None

    def load_state(self):
        """Loads persistent engine state."""
        default = {"last_index": 0, "processed_total": 0, "dead_total": 0, "history": []}
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return {**default, **data}
            except: pass
        return default

    def save_state(self):
        """Saves current engine state."""
        try:
            with open(STATE_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.state, f, indent=4, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Save State Error: {e}")

    def get_host_port(self, link):
        """Extracts hostname and port from proxy string."""
        try:
            match = self.ip_pattern.search(link)
            if match: return match.group(1), match.group(2)
        except: pass
        return None, None

    async def fetch_subscription(self, session, url):
        """Downloads and extracts proxies from a subscription URL."""
        try:
            async with session.get(url, timeout=15) as response:
                if response.status == 200:
                    text = await response.text()
                    try:
                        decoded = base64.b64decode(text.strip()).decode('utf-8', errors='ignore')
                        if '://' in decoded: text = decoded
                    except: pass
                    return [m.group(0) for m in self.proxy_pattern.finditer(text)]
        except: pass
        return []

    async def check_node(self, session, host, port, ip_cache):
        """Validates proxy node via TCP check and DNS resolution."""
        if not host or not port: return None, 9999
        
        key = f"{host}:{port}"
        if key in ip_cache: return ip_cache[key]
        
        async with self.semaphore:
            start = time.time()
            try:
                ip = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(None, socket.gethostbyname, host),
                    timeout=TIMEOUT
                )
                _, writer = await asyncio.wait_for(
                    asyncio.open_connection(ip, int(port)),
                    timeout=TIMEOUT
                )
                writer.close()
                await writer.wait_closed()
                
                latency = int((time.time() - start) * 1000)
                res = (ip, latency)
                ip_cache[key] = res
                return res
            except:
                ip_cache[key] = (None, 9999)
                return None, 9999

    def get_country_code(self, ip):
        """Returns ISO country code for given IP."""
        if not self.geo_reader or not ip: return None
        try:
            return self.geo_reader.country(ip).country.iso_code
        except: return None

    def apply_fragmentation(self, link):
        """Applies fragmentation and mux to VLESS/VMESS/Trojan for RU bypass."""
        try:
            parsed = urlparse(link)
            if parsed.scheme in ['vless', 'vmess', 'trojan']:
                query = parse_qs(parsed.query)
                if 'reality' in str(query).lower(): return link
                query['fragment'] = ['10-20,30-50']
                query['mux'] = ['enable=true&concurrency=8']
                parts = list(parsed)
                parts[4] = urlencode(query, doseq=True)
                return urlunparse(parts)
        except: pass
        return link

    def update_links_manifest(self):
        """Updates the LINKS_FOR_CLIENTS.txt file with stable Latin URLs."""
        try:
            base_url = "https://raw.githubusercontent.com/USER/REPO/main/proxy"
            try:
                remote = subprocess.check_output(["git", "config", "--get", "remote.origin.url"]).decode().strip()
                path = remote.replace("git@github.com:", "").replace("https://github.com/", "").replace(".git", "")
                base_url = f"https://raw.githubusercontent.com/{path}/main/{OUTPUT_DIR}"
            except: pass

            lines = [
                "🚀 MONSTER ENGINE - LIVE PROXY LINKS",
                "="*40,
                f"Updated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                "Note: These links are static and reliable.",
                ""
            ]
            
            for file in sorted(list(self.mandatory_files)):
                name = file.replace('.txt', '').upper()
                lines.append(f"📍 {name}:")
                lines.append(f"{base_url}/{file}")
                lines.append("")
                
            with open(LINKS_INFO_FILE, 'w', encoding='utf-8') as f:
                f.write('\n'.join(lines))
            logger.info("📄 Manifest updated.")
        except Exception as e:
            logger.error(f"Manifest Error: {e}")

    async def run(self):
        """Execution Entry Point."""
        if os.path.exists(LOCK_FILE):
            if (time.time() - os.path.getmtime(LOCK_FILE)) < 1200:
                logger.warning("Engine locked. Skipping run.")
                return
        
        try:
            with open(LOCK_FILE, 'w') as f: f.write(str(time.time()))
            
            if not os.path.exists(SOURCE_FILE): return
            with open(SOURCE_FILE, 'r', encoding='utf-8', errors='ignore') as f:
                raw = [l.strip() for l in f if len(l.strip()) > 10 and not l.startswith('#')]
            
            subs = [u for u in raw if u.startswith('http')]
            configs = [c for c in raw if '://' in c and not c.startswith('http')]
            
            all_links = configs.copy()
            async with aiohttp.ClientSession() as session:
                sub_data = await asyncio.gather(*[self.fetch_subscription(session, u) for u in subs])
                for links in sub_data: all_links.extend(links)
                
                unique_pool = set(all_links)
                self.update_links_manifest()
                
                total = len(unique_pool)
                if total == 0:
                    for filename in self.mandatory_files:
                        p = os.path.join(OUTPUT_DIR, filename)
                        with open(p, 'w', encoding='utf-8') as f: f.write('')
                        os.utime(p, None)
                    return

                batch_size = max(500, int(total / ((CYCLE_HOURS * 60) / BATCH_INTERVAL_MIN)))
                pool_list = list(unique_pool)
                pool_list.sort(key=lambda x: any(reg in x.upper() for reg in PRIORITY_REGIONS), reverse=True)
                
                idx = self.state.get("last_index", 0)
                if idx >= total: idx = 0
                batch = pool_list[idx : idx + batch_size]
                
                ip_cache = {}
                results = await asyncio.gather(*[self.check_node(session, *self.get_host_port(l), ip_cache) for l in batch])
                
                processed_nodes = [] # Список словарей с метаданными
                dead_set = set()
                
                for i, (ip, ping) in enumerate(results):
                    link = batch[i]
                    cc = self.get_country_code(ip)
                    limit = PING_LIMITS.get(cc, PING_LIMITS['DEFAULT'])
                    
                    # Логика распределения
                    if ip:
                        if ping <= limit:
                            # Хороший сервер
                            final_link = self.apply_fragmentation(link) if cc == 'RU' else link
                            processed_nodes.append({"link": final_link, "cc": cc, "type": "good"})
                        elif cc in INVALID_REGIONS and INVALID_THRESHOLD_MIN <= ping <= INVALID_THRESHOLD_MAX:
                            # Медленный приоритетный (BY, KZ, US) -> в invalid.txt
                            processed_nodes.append({"link": link, "cc": cc, "type": "invalid"})
                        else:
                            # Совсем плохой пинг
                            dead_set.add(link)
                    else:
                        dead_set.add(link)

            # File Distribution
            for f in os.listdir(OUTPUT_DIR):
                if f.endswith('.txt') and f not in self.mandatory_files and f != os.path.basename(LINKS_INFO_FILE):
                    try: os.remove(os.path.join(OUTPUT_DIR, f))
                    except: pass

            for filename in self.mandatory_files:
                target_path = os.path.join(OUTPUT_DIR, filename)
                file_nodes = {}
                
                # Читаем старое содержимое
                if os.path.exists(target_path):
                    with open(target_path, 'r', encoding='utf-8') as f:
                        for line in f:
                            node = line.strip()
                            if node in unique_pool and node not in dead_set:
                                file_nodes[node] = True
                
                # Наполняем новыми результатами
                for res in processed_nodes:
                    if res['type'] == "good":
                        # Обычное распределение по странам или в микс
                        assigned_file = COUNTRY_MAP.get(res['cc'], DEFAULT_MIX)
                        if assigned_file == filename:
                            file_nodes[res['link']] = True
                    elif res['type'] == "invalid":
                        # Все медленные приоритетные - строго в invalid.txt
                        if filename == INVALID_FILE:
                            file_nodes[res['link']] = True
                
                # ВСЕГДА пишем в файл (Touch logic)
                with open(target_path, 'w', encoding='utf-8') as f:
                    content = list(file_nodes.keys())[:MAX_NODES_PER_FILE]
                    if content:
                        f.write('\n'.join(content) + '\n')
                    else:
                        # Обязательная активность для GitHub
                        f.write(f"# Monster Log Update: {datetime.now().strftime('%H:%M:%S')}\n")
                
                os.utime(target_path, None)

            # Finalize
            updated_sources = [s for s in raw if s.startswith('http') or (s in unique_pool and s not in dead_set)]
            with open(SOURCE_FILE, 'w', encoding='utf-8') as f:
                f.write('\n'.join(updated_sources) + '\n')

            self.state.update({
                "last_index": idx + batch_size,
                "processed_total": self.state.get("processed_total", 0) + len(batch),
                "last_sync": datetime.now().isoformat()
            })
            self.save_state()
            logger.info("✅ Full cycle with Invalid-routing finished.")

        except Exception as e:
            logger.error(f"Critical Error: {e}", exc_info=True)
        finally:
            if os.path.exists(LOCK_FILE):
                try: os.remove(LOCK_FILE)
                except: pass

if __name__ == "__main__":
    asyncio.run(MonsterParser().run())

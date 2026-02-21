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

# Files location
SOURCE_FILE = 'all_sources.txt' # В корне
STATE_FILE = os.path.join(DB_DIR, 'monster_state.json')
GEOIP_DB = os.path.join(DB_DIR, 'GeoLite2-Country.mmdb')
LOCK_FILE = os.path.join(DB_DIR, '.monster.lock')

# Files inside "proxy"
LINKS_INFO_FILE = os.path.join(OUTPUT_DIR, 'LINKS_FOR_CLIENTS.txt')

# --- ENGINE CONSTANTS (MAX PERFORMANCE MODE) ---
TIMEOUT = 3              
MAX_CONCURRENCY = 1000   
BATCH_SIZE = 100000      # Берем ВСЁ сразу

# --- NETWORK THRESHOLDS ---
PING_LIMITS = {
    'DEFAULT': 250,
    'US': 350, 
    'HK': 300, 
    'SG': 300, 
    'JP': 300,
    'BY': 200, 
    'KZ': 200, 
    'RU': 250
}

# Пороги для "инвалидных" серверов
INVALID_THRESHOLD_MIN = 250
INVALID_THRESHOLD_MAX = 450 
INVALID_REGIONS = {'BY', 'KZ', 'US', 'RU'}

# Приоритетные регионы
PRIORITY_REGIONS = {'BY', 'KZ', 'DE', 'FI', 'SE', 'LV', 'RU', 'US', 'CH', 'FR'}

# --- COUNTRY TO FILE MAPPING (Strict Latin Only) ---
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
INVALID_FILE = 'invalid.txt' 
MAX_NODES_PER_FILE = 2500 

class MonsterParser:
    """
    Advanced engine for parsing, checking, and distributing proxy configurations.
    Maintains strict file structure and ensures valid distribution by regions.
    """
    
    def __init__(self):
        self.start_time = time.time()
        self.ensure_structure()
        self.clean_cyrillic_files() # Жесткая чистка старых русских файлов
        self.migrate_old_data()
        self.state = self.load_state()
        self.geo_reader = self.init_geo()
        
        self.proxy_pattern = re.compile(r'(vless|vmess|trojan|ss|ssr)://[^\s"\'<>()]+')
        self.ip_pattern = re.compile(r'@?([\w\.-]+):(\d+)')
        
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
        self.mandatory_files = set(COUNTRY_MAP.values()) | {DEFAULT_MIX, INVALID_FILE}

    def ensure_structure(self):
        """Проверяет наличие папок и создает их."""
        for d in [DB_DIR, OUTPUT_DIR]:
            if not os.path.exists(d):
                os.makedirs(d, exist_ok=True)
            
        if not os.path.exists(SOURCE_FILE):
            with open(SOURCE_FILE, 'w', encoding='utf-8') as f:
                f.write("# Monster Engine Source List\n")

    def clean_cyrillic_files(self):
        """Удаляет мусорные файлы с кириллицей из папки proxy (например 'Беларусь.txt')."""
        if os.path.exists(OUTPUT_DIR):
            for file in os.listdir(OUTPUT_DIR):
                if re.search(r'[А-Яа-я]', file):
                    target = os.path.join(OUTPUT_DIR, file)
                    try:
                        os.remove(target)
                        logger.info(f"🧹 Removed cyrillic ghost file: {file}")
                    except: pass

    def migrate_old_data(self):
        """Перенос данных из кириллических папок в латиницу."""
        old_folders = {'база данных': DB_DIR, 'прокси': OUTPUT_DIR}
        for old, new in old_folders.items():
            if os.path.exists(old) and os.path.isdir(old):
                logger.info(f"🔄 Migration: '{old}' -> '{new}'")
                for item in os.listdir(old):
                    src = os.path.join(old, item)
                    dst = os.path.join(new, item)
                    try:
                        if os.path.exists(dst): os.remove(src)
                        else: shutil.move(src, dst)
                    except: pass
                try: os.rmdir(old)
                except: pass

    def init_geo(self):
        if not os.path.exists(GEOIP_DB):
            logger.warning(f"⚠️ GeoIP NOT FOUND at {GEOIP_DB}. Sorting limited.")
            return None
        try:
            file_size = os.path.getsize(GEOIP_DB)
            if file_size < 1048576: 
                logger.error(f"❌ GeoIP file too small. Skipping.")
                return None
            reader = geoip2.database.Reader(GEOIP_DB)
            reader.country('8.8.8.8') 
            logger.info(f"✅ GeoIP Engine ready ({file_size/1024/1024:.2f} MB)")
            return reader
        except Exception as e:
            logger.error(f"❌ GeoIP Init Error: {e}")
            try: os.remove(GEOIP_DB)
            except: pass
            return None

    def load_state(self):
        default = {"last_index": 0, "processed_total": 0, "dead_total": 0}
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, 'r', encoding='utf-8') as f:
                    return {**default, **json.load(f)}
            except: pass
        return default

    def save_state(self):
        try:
            with open(STATE_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.state, f, indent=4, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Save State Error: {e}")

    def get_host_port(self, link):
        try:
            match = self.ip_pattern.search(link)
            if match: return match.group(1), match.group(2)
        except: pass
        return None, None

    async def fetch_subscription(self, session, url):
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
        """Проверка ноды с системой двойного пинга (Double Check) от ложных срабатываний."""
        if not host or not port: return None, 9999
        key = f"{host}:{port}"
        if key in ip_cache: return ip_cache[key]
        
        async with self.semaphore:
            # 2 попытки чтобы не удалять живые конфиги из-за таймаута сети
            for attempt in range(2):
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
                    if attempt == 1: # Если вторая попытка тоже провал
                        ip_cache[key] = (None, 9999)
                        return None, 9999
                    await asyncio.sleep(0.5) # Отдых перед второй попыткой

    def get_country_code(self, ip):
        if not self.geo_reader or not ip: return None
        try: return self.geo_reader.country(ip).country.iso_code
        except: return None

    def apply_fragmentation(self, link):
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
                "Note: Automatic high-speed distribution.",
                ""
            ]
            
            for file in sorted(list(self.mandatory_files)):
                name = file.replace('.txt', '').upper()
                lines.append(f"📍 {name}:")
                lines.append(f"{base_url}/{file}")
                lines.append("")
                
            with open(LINKS_INFO_FILE, 'w', encoding='utf-8') as f:
                f.write('\n'.join(lines))
        except Exception as e:
            logger.error(f"Manifest Error: {e}")

    async def run(self):
        if os.path.exists(LOCK_FILE):
            if (time.time() - os.path.getmtime(LOCK_FILE)) < 300:
                logger.warning("Engine is locked. Parallel run prevented.")
                return
        
        try:
            with open(LOCK_FILE, 'w') as f: f.write(str(time.time()))
            
            if not os.path.exists(SOURCE_FILE): return
            
            # Читаем исходники
            with open(SOURCE_FILE, 'r', encoding='utf-8', errors='ignore') as f:
                raw = [l.strip() for l in f if len(l.strip()) > 10 or l.startswith('#')]
            
            subs = [u for u in raw if u.startswith('http') and not u.startswith('#')]
            configs = [c for c in raw if '://' in c and not c.startswith('http') and not c.startswith('#')]
            
            logger.info(f"📥 Loaded from {SOURCE_FILE}: {len(subs)} Subscriptions | {len(configs)} Static Configs")

            all_links = configs.copy()
            async with aiohttp.ClientSession() as session:
                sub_data = await asyncio.gather(*[self.fetch_subscription(session, u) for u in subs])
                for links in sub_data: all_links.extend(links)
                
                unique_pool = set(all_links)
                total = len(unique_pool)
                logger.info(f"🔍 Discovered total unique nodes: {total}")
                
                self.update_links_manifest()
                
                if total == 0:
                    for filename in self.mandatory_files:
                        p = os.path.join(OUTPUT_DIR, filename)
                        with open(p, 'w', encoding='utf-8') as f: f.write('')
                    return

                pool_list = list(unique_pool)
                pool_list.sort(key=lambda x: any(reg in x.upper() for reg in PRIORITY_REGIONS), reverse=True)
                batch = pool_list[:BATCH_SIZE]
                
                logger.info(f"⚡ Testing {len(batch)} nodes with concurrency {MAX_CONCURRENCY}...")
                
                ip_cache = {}
                results = await asyncio.gather(*[self.check_node(session, *self.get_host_port(l), ip_cache) for l in batch])
                
                valid_nodes = []
                dead_set = set()
                
                for i, (ip, ping) in enumerate(results):
                    link = batch[i]
                    if not ip:
                        dead_set.add(link)
                        continue
                        
                    cc = self.get_country_code(ip)
                    limit = PING_LIMITS.get(cc, PING_LIMITS['DEFAULT'])
                    
                    if ping <= limit:
                        final_link = self.apply_fragmentation(link) if cc == 'RU' else link
                        valid_nodes.append({"link": final_link, "cc": cc, "type": "good"})
                    elif cc in INVALID_REGIONS and INVALID_THRESHOLD_MIN <= ping <= INVALID_THRESHOLD_MAX:
                        valid_nodes.append({"link": link, "cc": cc, "type": "invalid"})
                    else:
                        dead_set.add(link)

            # --- Распределение по файлам ---
            file_stats = {f: 0 for f in self.mandatory_files}
            
            for filename in self.mandatory_files:
                target_path = os.path.join(OUTPUT_DIR, filename)
                file_content = {}
                fallback_node = None

                if os.path.exists(target_path):
                    try:
                        with open(target_path, 'r', encoding='utf-8') as f:
                            for line in f:
                                line = line.strip()
                                if "://" in line:
                                    if not fallback_node and line not in dead_set:
                                        fallback_node = line 
                                    if line in unique_pool and line not in dead_set:
                                        file_content[line] = True
                    except: pass
                
                for node_data in valid_nodes:
                    node_cc, node_link = node_data['cc'], node_data['link']
                    if node_data['type'] == "invalid" and filename == INVALID_FILE:
                        file_content[node_link] = True
                    elif node_data['type'] == "good":
                        if COUNTRY_MAP.get(node_cc, DEFAULT_MIX) == filename:
                            file_content[node_link] = True

                nodes_list = list(file_content.keys())[:MAX_NODES_PER_FILE]
                
                if not nodes_list and fallback_node:
                    nodes_list = [fallback_node]
                    logger.warning(f"⚠️ {filename} is empty. Active fallback applied.")

                file_stats[filename] = len(nodes_list)

                with open(target_path, 'w', encoding='utf-8') as f:
                    if nodes_list:
                        f.write('\n'.join(nodes_list) + '\n')
                    else:
                        f.write(f"# Monster Update: {datetime.now().strftime('%H:%M:%S')} | Offline\n")
                
                os.utime(target_path, None)

            # --- ЗАЩИТА ИСХОДНИКОВ (Не удаляем просто так) ---
            # Оставляем: подписки (http), комменты (#), и конфиги, которые НЕ попали в 100% мертвые
            updated_sources = []
            for s in raw:
                if s.startswith('http') or s.startswith('#'):
                    updated_sources.append(s)
                elif s in configs and s not in dead_set:
                    updated_sources.append(s)
            
            with open(SOURCE_FILE, 'w', encoding='utf-8') as f:
                f.write('\n'.join(updated_sources) + '\n')

            # --- ИТОГОВЫЙ ДАШБОРД В КОНСОЛЬ ---
            logger.info("="*40)
            logger.info("📊 ИТОГИ РАСПРЕДЕЛЕНИЯ (DASHBOARD):")
            logger.info(f"   🔎 Всего проверено: {len(batch)}")
            logger.info(f"   🟢 Успешно (Alive): {len(valid_nodes)}")
            logger.info(f"   🔴 Отклонено (Dead): {len(dead_set)}")
            logger.info("-" * 40)
            for f_name, count in sorted(file_stats.items(), key=lambda x: x[1], reverse=True):
                icon = "📁" if count > 0 else "🪫"
                logger.info(f"   {icon} {f_name.ljust(15)} : {count} nodes")
            logger.info("="*40)

            self.state.update({
                "processed_total": self.state.get("processed_total", 0) + len(batch),
                "last_run": datetime.now().isoformat()
            })
            self.save_state()

        except Exception as e:
            logger.error(f"Critical System Error: {e}", exc_info=True)
        finally:
            if os.path.exists(LOCK_FILE):
                try: os.remove(LOCK_FILE)
                except: pass

if __name__ == "__main__":
    asyncio.run(MonsterParser().run())

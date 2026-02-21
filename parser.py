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

# Пороги для "инвалидных" серверов (медленные, но из приоритетных регионов)
INVALID_THRESHOLD_MIN = 250
INVALID_THRESHOLD_MAX = 350 
INVALID_REGIONS = {'BY', 'KZ', 'US'}

# Приоритетные регионы для сортировки очереди
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
INVALID_FILE = 'invalid.txt' 
MAX_NODES_PER_FILE = 500

class MonsterParser:
    """
    Advanced engine for parsing, checking, and distributing proxy configurations.
    Maintains strict file structure and ensures valid distribution by regions.
    """
    
    def __init__(self):
        self.start_time = time.time()
        self.ensure_structure()
        self.migrate_old_data()
        self.state = self.load_state()
        self.geo_reader = self.init_geo()
        
        # Регулярные выражения для парсинга
        self.proxy_pattern = re.compile(r'(vless|vmess|trojan|ss|ssr)://[^\s"\'<>()]+')
        self.ip_pattern = re.compile(r'@?([\w\.-]+):(\d+)')
        
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
        # 12 обязательных файлов: 10 стран + микс + отстойник
        self.mandatory_files = set(COUNTRY_MAP.values()) | {DEFAULT_MIX, INVALID_FILE}

    def ensure_structure(self):
        """Проверяет наличие папок и создает их только при необходимости."""
        if not os.path.exists(DB_DIR):
            os.makedirs(DB_DIR, exist_ok=True)
        if not os.path.exists(OUTPUT_DIR):
            os.makedirs(OUTPUT_DIR, exist_ok=True)
            
        if not os.path.exists(SOURCE_FILE):
            with open(SOURCE_FILE, 'w', encoding='utf-8') as f:
                f.write("# Monster Engine Source List\n")

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
                    except Exception as e:
                        logger.debug(f"Migration error: {e}")
                try: os.rmdir(old)
                except: pass

    def init_geo(self):
        """Инициализация базы GeoIP2."""
        if not os.path.exists(GEOIP_DB):
            logger.warning(f"GeoIP Database not found at {GEOIP_DB}. Sorting by countries will be limited.")
            return None
        try:
            return geoip2.database.Reader(GEOIP_DB)
        except Exception as e:
            logger.error(f"GeoIP Error: {e}")
            return None

    def load_state(self):
        """Загрузка состояния парсера."""
        default = {"last_index": 0, "processed_total": 0, "dead_total": 0}
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return {**default, **data}
            except: pass
        return default

    def save_state(self):
        """Сохранение состояния парсера."""
        try:
            with open(STATE_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.state, f, indent=4, ensure_ascii=False)
        except Exception as e:
            logger.error(f"Save State Error: {e}")

    def get_host_port(self, link):
        """Извлечение хоста и порта из ссылки."""
        try:
            match = self.ip_pattern.search(link)
            if match: return match.group(1), match.group(2)
        except: pass
        return None, None

    async def fetch_subscription(self, session, url):
        """Загрузка данных по ссылке-подписке."""
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
        """Проверка ноды: DNS резолв и TCP коннект."""
        if not host or not port: return None, 9999
        
        key = f"{host}:{port}"
        if key in ip_cache: return ip_cache[key]
        
        async with self.semaphore:
            start = time.time()
            try:
                # DNS RESOLVE - Ключевой этап
                ip = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(None, socket.gethostbyname, host),
                    timeout=TIMEOUT
                )
                # TCP CONNECT
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
        """Определение страны по IP."""
        if not self.geo_reader or not ip: return None
        try:
            return self.geo_reader.country(ip).country.iso_code
        except: return None

    def apply_fragmentation(self, link):
        """Добавление фрагментации для VLESS/VMESS/Trojan (обход блокировок RU)."""
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
        """Обновление файла со ссылками для клиентов."""
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
                "Note: Links are static. Distribution is updated every 20m.",
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
        """Основной цикл движка."""
        if os.path.exists(LOCK_FILE):
            if (time.time() - os.path.getmtime(LOCK_FILE)) < 1200:
                logger.warning("Engine is locked. Parallel run prevented.")
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

                # Расчет батча
                batch_size = max(500, int(total / ((CYCLE_HOURS * 60) / BATCH_INTERVAL_MIN)))
                pool_list = list(unique_pool)
                # Сортировка: приоритетные регионы вперед
                pool_list.sort(key=lambda x: any(reg in x.upper() for reg in PRIORITY_REGIONS), reverse=True)
                
                idx = self.state.get("last_index", 0)
                if idx >= total: idx = 0
                batch = pool_list[idx : idx + batch_size]
                
                ip_cache = {}
                results = await asyncio.gather(*[self.check_node(session, *self.get_host_port(l), ip_cache) for l in batch])
                
                valid_nodes = [] # Узлы с метаданными
                dead_set = set()
                
                for i, (ip, ping) in enumerate(results):
                    link = batch[i]
                    if not ip: # DNS Fail = Dead
                        dead_set.add(link)
                        continue
                        
                    cc = self.get_country_code(ip)
                    limit = PING_LIMITS.get(cc, PING_LIMITS['DEFAULT'])
                    
                    if ping <= limit:
                        # Хорошая нода
                        final_link = self.apply_fragmentation(link) if cc == 'RU' else link
                        valid_nodes.append({"link": final_link, "cc": cc, "type": "good"})
                    elif cc in INVALID_REGIONS and INVALID_THRESHOLD_MIN <= ping <= INVALID_THRESHOLD_MAX:
                        # Инвалидная нода (медленная, но из BY/KZ/US)
                        valid_nodes.append({"link": link, "cc": cc, "type": "invalid"})
                    else:
                        # Пинг выше всех норм
                        dead_set.add(link)

            # Чистка мусора в папке прокси
            for f in os.listdir(OUTPUT_DIR):
                if f.endswith('.txt') and f not in self.mandatory_files and f != os.path.basename(LINKS_INFO_FILE):
                    try: os.remove(os.path.join(OUTPUT_DIR, f))
                    except: pass

            # Глобальное распределение по файлам
            for filename in self.mandatory_files:
                target_path = os.path.join(OUTPUT_DIR, filename)
                file_content = {}
                
                # 1. Берем выжившее старое
                if os.path.exists(target_path):
                    with open(target_path, 'r', encoding='utf-8') as f:
                        for line in f:
                            node = line.strip()
                            if node in unique_pool and node not in dead_set:
                                file_content[node] = True
                
                # 2. Добавляем новое по жесткой логике
                for node_data in valid_nodes:
                    node_cc = node_data['cc']
                    node_link = node_data['link']
                    
                    if node_data['type'] == "invalid":
                        # Только в invalid.txt
                        if filename == INVALID_FILE:
                            file_content[node_link] = True
                    else:
                        # Тип "good" - распределяем по странам
                        assigned_file = COUNTRY_MAP.get(node_cc, DEFAULT_MIX)
                        if assigned_file == filename:
                            file_content[node_link] = True

                # 3. Финальная запись (Strict Touch)
                with open(target_path, 'w', encoding='utf-8') as f:
                    nodes_list = list(file_content.keys())[:MAX_NODES_PER_FILE]
                    if nodes_list:
                        f.write('\n'.join(nodes_list) + '\n')
                    else:
                        # Сохраняем активность файла для GitHub
                        f.write(f"# Monster Update: {datetime.now().strftime('%H:%M:%S')} | Total Nodes: 0\n")
                
                os.utime(target_path, None)

            # Обновление all_sources
            updated_sources = [s for s in raw if s.startswith('http') or (s in unique_pool and s not in dead_set)]
            with open(SOURCE_FILE, 'w', encoding='utf-8') as f:
                f.write('\n'.join(updated_sources) + '\n')

            # Апдейт стейта
            self.state.update({
                "last_index": idx + batch_size,
                "processed_total": self.state.get("processed_total", 0) + len(batch),
                "last_run": datetime.now().isoformat()
            })
            self.save_state()
            logger.info("✅ Engine Cycle Finished. Distribution verified.")

        except Exception as e:
            logger.error(f"Critical System Error: {e}", exc_info=True)
        finally:
            if os.path.exists(LOCK_FILE):
                try: os.remove(LOCK_FILE)
                except: pass

if __name__ == "__main__":
    asyncio.run(MonsterParser().run())

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

# --- ENGINE CONSTANTS (MAX PERFORMANCE MODE) ---
TIMEOUT = 3              
MAX_CONCURRENCY = 1000   # Максимальный поток для быстрой обработки 20к+ ссылок
REST_INTERVAL_MIN = 10    # Время отдыха между кругами логики (минуты)
BATCH_SIZE = 100000       # Берем абсолютно все ссылки из базы за один проход

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

# Пороги для "инвалидных" серверов (медленные, но из приоритетных регионов)
INVALID_THRESHOLD_MIN = 250
INVALID_THRESHOLD_MAX = 450 
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
MAX_NODES_PER_FILE = 2500 # Лимит нод в одном файле для выдачи клиентам

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
        """Перенос данных из кириллических папок в латиницу (те самые 15 строк миграции)."""
        old_folders = {'база данных': DB_DIR, 'прокси': OUTPUT_DIR}
        for old, new in old_folders.items():
            if os.path.exists(old) and os.path.isdir(old):
                logger.info(f"🔄 Migration: '{old}' -> '{new}'")
                for item in os.listdir(old):
                    src = os.path.join(old, item)
                    dst = os.path.join(new, item)
                    try:
                        if os.path.exists(dst): 
                            os.remove(src)
                        else: 
                            shutil.move(src, dst)
                    except Exception as e:
                        logger.debug(f"Migration error: {e}")
                try: 
                    os.rmdir(old)
                except: 
                    pass

    def init_geo(self):
        """Инициализация базы GeoIP2 с защитой от битых файлов."""
        if not os.path.exists(GEOIP_DB):
            logger.warning(f"⚠️ GeoIP Database NOT FOUND at {GEOIP_DB}. Sorting will be limited.")
            return None
            
        try:
            file_size = os.path.getsize(GEOIP_DB)
            if file_size < 1048576: 
                logger.error(f"❌ GeoIP file is too small ({file_size} bytes). Likely corrupted or 404. Skipping.")
                return None

            reader = geoip2.database.Reader(GEOIP_DB)
            reader.country('8.8.8.8') 
            logger.info(f"✅ GeoIP Engine ready (Size: {file_size/1024/1024:.2f} MB)")
            return reader
        except Exception as e:
            logger.error(f"❌ GeoIP Init Error: {e}")
            try:
                if "valid MaxMind" in str(e) or "not a valid" in str(e).lower():
                    os.remove(GEOIP_DB)
                    logger.info("🗑️ Corrupted GeoIP file removed for re-download.")
            except: pass
            return None

    def load_state(self):
        """Загрузка состояния парсера для отслеживания прогресса."""
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
        """Извлечение хоста и порта из ссылки для сетевой проверки."""
        try:
            match = self.ip_pattern.search(link)
            if match: return match.group(1), match.group(2)
        except: pass
        return None, None

    async def fetch_subscription(self, session, url):
        """Загрузка данных по ссылке-подписке (поддержка Base64)."""
        try:
            async with session.get(url, timeout=20) as response:
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
        """Проверка ноды: DNS резолв и TCP коннект с ограничением семафором."""
        if not host or not port: return None, 9999
        
        key = f"{host}:{port}"
        if key in ip_cache: return ip_cache[key]
        
        async with self.semaphore:
            start = time.time()
            try:
                # DNS RESOLVE
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
        """Определение страны по IP через GeoIP2 базу."""
        if not self.geo_reader or not ip: return None
        try:
            return self.geo_reader.country(ip).country.iso_code
        except: return None

    def apply_fragmentation(self, link):
        """Добавление фрагментации для VLESS/VMESS/Trojan (обход ТСПУ РФ)."""
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
        """Обновление манифеста ссылок для клиентов (поддержка GitHub Raw)."""
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
                "Note: Automatic high-speed distribution is active.",
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

    async def execute_cycle(self):
        """Одиночный полный цикл обработки базы."""
        if os.path.exists(LOCK_FILE):
            if (time.time() - os.path.getmtime(LOCK_FILE)) < 300:
                logger.warning("Engine cycle skipped: lock file is too fresh.")
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
                logger.info(f"📥 Fetching {len(subs)} subscriptions...")
                sub_data = await asyncio.gather(*[self.fetch_subscription(session, u) for u in subs])
                for links in sub_data: all_links.extend(links)
                
                unique_pool = set(all_links)
                self.update_links_manifest()
                
                total = len(unique_pool)
                logger.info(f"🔎 Total unique nodes discovered: {total}")
                
                if total == 0:
                    for filename in self.mandatory_files:
                        p = os.path.join(OUTPUT_DIR, filename)
                        with open(p, 'w', encoding='utf-8') as f: f.write('')
                    return

                # ПРИОРИТЕТНАЯ СОРТИРОВКА
                pool_list = list(unique_pool)
                pool_list.sort(key=lambda x: any(reg in x.upper() for reg in PRIORITY_REGIONS), reverse=True)
                
                # РЕЖИМ МОНСТРА: Обрабатываем всё сразу
                batch = pool_list[:BATCH_SIZE]
                logger.info(f"⚡ Checking {len(batch)} nodes with concurrency {MAX_CONCURRENCY}...")
                
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

            # --- РАСПРЕДЕЛЕНИЕ ПО ФАЙЛАМ ---
            for filename in self.mandatory_files:
                target_path = os.path.join(OUTPUT_DIR, filename)
                file_content = {}
                fallback_candidate = None

                # 1. Читаем текущее содержимое файла (спасаем "выживших")
                if os.path.exists(target_path):
                    try:
                        with open(target_path, 'r', encoding='utf-8') as f:
                            for line in f:
                                node = line.strip()
                                if "://" in node:
                                    if not fallback_candidate and node not in dead_set:
                                        fallback_candidate = node
                                    if node in unique_pool and node not in dead_set:
                                        file_content[node] = True
                    except: pass
                
                # 2. Добавляем новые валидные ноды
                for node_data in valid_nodes:
                    node_cc, node_link = node_data['cc'], node_data['link']
                    if node_data['type'] == "invalid" and filename == INVALID_FILE:
                        file_content[node_link] = True
                    elif node_data['type'] == "good":
                        if COUNTRY_MAP.get(node_cc, DEFAULT_MIX) == filename:
                            file_content[node_link] = True

                # 3. Финализация списка (не более MAX_NODES_PER_FILE)
                final_list = list(file_content.keys())[:MAX_NODES_PER_FILE]
                
                # ЗАЩИТА: Если файл пуст, но у нас есть хоть одна живая нода - втыкаем её как щит
                if not final_list and fallback_candidate:
                    final_list = [fallback_candidate]

                with open(target_path, 'w', encoding='utf-8') as f:
                    if final_list:
                        f.write('\n'.join(final_list) + '\n')
                    else:
                        f.write(f"# Monster Update: {datetime.now().strftime('%H:%M:%S')} | No active nodes\n")
                
                os.utime(target_path, None)

            # Очистка all_sources.txt от мертвецов (кроме подписок)
            updated_sources = [s for s in raw if s.startswith('http') or (s in unique_pool and s not in dead_set)]
            with open(SOURCE_FILE, 'w', encoding='utf-8') as f:
                f.write('\n'.join(updated_sources) + '\n')

            self.state.update({
                "processed_total": self.state.get("processed_total", 0) + len(batch),
                "last_run": datetime.now().isoformat()
            })
            self.save_state()
            logger.info("✅ Cycle finished successfully.")

        except Exception as e:
            logger.error(f"🚨 Engine Error: {e}", exc_info=True)
        finally:
            if os.path.exists(LOCK_FILE):
                try: os.remove(LOCK_FILE)
                except: pass

    async def main_loop(self):
        """Бесконечный режим: цикл -> отдых -> цикл."""
        while True:
            logger.info("🌀 Starting new Monster Cycle...")
            await self.execute_cycle()
            logger.info(f"💤 Sleeping for {REST_INTERVAL_MIN} minutes before next run...")
            await asyncio.sleep(REST_INTERVAL_MIN * 60)

if __name__ == "__main__":
    # Выбор режима: один проход или бесконечный демон
    engine = MonsterParser()
    try:
        # Для GitHub Actions лучше использовать engine.execute_cycle()
        # Для локального сервера/VPS лучше engine.main_loop()
        asyncio.run(engine.execute_cycle()) 
    except KeyboardInterrupt:
        logger.info("Engine stopped by user.")

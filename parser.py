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
SOURCE_FILE = 'all_sources.txt' 
STATE_FILE = os.path.join(DB_DIR, 'monster_state.json')
GEOIP_DB = os.path.join(DB_DIR, 'GeoLite2-Country.mmdb')
LOCK_FILE = os.path.join(DB_DIR, '.monster.lock')

# --- ENGINE CONSTANTS (STABLE PERFORMANCE MODE) ---
TIMEOUT = 5              
MAX_CONCURRENCY = 800    
BATCH_SIZE = 150000      
LOG_STEP = 1000          

# --- NETWORK THRESHOLDS (MS) ---
PING_LIMITS = {
    'DEFAULT': 300,
    'US': 450, 
    'HK': 400, 
    'SG': 400, 
    'JP': 400,
    'BY': 250, 
    'KZ': 250, 
    'RU': 300
}

INVALID_THRESHOLD_MAX = 600 
INVALID_REGIONS = {'BY', 'KZ', 'US', 'RU'}

# --- COUNTRY TO FILE MAPPING ---
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
MAX_NODES_PER_FILE = 5000 

class MonsterParser:
    def __init__(self):
        self.start_time = time.time()
        self.ensure_structure()
        self.clean_cyrillic_files()
        self.migrate_old_data()
        self.state = self.load_state()
        self.geo_reader = self.init_geo()
        
        self.proxy_pattern = re.compile(r'(vless|vmess|trojan|ss|ssr)://[^\s"\'<>()]+')
        self.ip_pattern = re.compile(r'@?([\w\.-]+):(\d+)')
        
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
        self.mandatory_files = set(COUNTRY_MAP.values()) | {DEFAULT_MIX, INVALID_FILE}
        
        self.counter_checked = 0
        self.counter_alive = 0
        self.counter_dead = 0

    def ensure_structure(self):
        """Создает папки если они отсутствуют."""
        for d in [DB_DIR, OUTPUT_DIR]:
            if not os.path.exists(d):
                os.makedirs(d, exist_ok=True)
        if not os.path.exists(SOURCE_FILE):
            with open(SOURCE_FILE, 'w', encoding='utf-8') as f:
                f.write("# Monster Engine Source List\n")

    def clean_cyrillic_files(self):
        """Удаляет файлы с некорректными именами."""
        if os.path.exists(OUTPUT_DIR):
            for file in os.listdir(OUTPUT_DIR):
                if re.search(r'[А-Яа-я]', file):
                    try: os.remove(os.path.join(OUTPUT_DIR, file))
                    except: pass

    def migrate_old_data(self):
        """Миграция данных из устаревших директорий."""
        old_folders = {'база данных': DB_DIR, 'прокси': OUTPUT_DIR}
        for old, new in old_folders.items():
            if os.path.exists(old) and os.path.isdir(old):
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
        """Загрузка базы GeoIP для определения стран."""
        if not os.path.exists(GEOIP_DB):
            logger.error(f"❌ База GeoIP не найдена: {GEOIP_DB}")
            return None
        try:
            reader = geoip2.database.Reader(GEOIP_DB)
            logger.info(f"🌍 База GeoIP успешно подключена: {GEOIP_DB}")
            return reader
        except Exception as e:
            logger.error(f"❌ Ошибка инициализации GeoIP: {e}")
            return None

    def load_state(self):
        """Загрузка сохраненного состояния."""
        default = {"processed_total": 0}
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, 'r', encoding='utf-8') as f:
                    return {**default, **json.load(f)}
            except: pass
        return default

    def save_state(self):
        """Сохранение состояния."""
        try:
            with open(STATE_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.state, f, indent=4, ensure_ascii=False)
        except: pass

    def get_host_port(self, link):
        """Извлечение адреса сервера и порта."""
        try:
            match = self.ip_pattern.search(link)
            if match: return match.group(1), match.group(2)
        except: pass
        return None, None

    async def fetch_subscription(self, session, url):
        """Получение списка нод из подписки."""
        try:
            async with session.get(url, timeout=25) as response:
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
        """Проверка доступности ноды (Double-check TCP)."""
        if not host or not port: 
            self.counter_checked += 1
            self.counter_dead += 1
            return None, 9999
            
        key = f"{host}:{port}"
        if key in ip_cache: 
            res = ip_cache[key]
            self.counter_checked += 1
            if res[0]: self.counter_alive += 1
            else: self.counter_dead += 1
            return res
        
        async with self.semaphore:
            # Двойная попытка для исключения случайных сбоев сети
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
                    self.counter_checked += 1
                    self.counter_alive += 1
                    
                    if self.counter_checked % LOG_STEP == 0:
                        logger.info(f"⚡ Проверено: {self.counter_checked} | Найдено живых: {self.counter_alive}")
                    return res
                except:
                    if attempt == 0: 
                        await asyncio.sleep(0.3)
                        continue
                    ip_cache[key] = (None, 9999)
                    self.counter_checked += 1
                    self.counter_dead += 1
                    return None, 9999

    def get_country_code(self, ip):
        """Определение кода страны по IP."""
        if not self.geo_reader or not ip: return "UNKNOWN"
        try: 
            code = self.geo_reader.country(ip).country.iso_code
            return code if code else "UNKNOWN"
        except: 
            return "UNKNOWN"

    def apply_fragmentation(self, link):
        """Добавление параметров фрагментации для РФ."""
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

    async def run(self):
        """Основной цикл парсера."""
        if os.path.exists(LOCK_FILE):
            if (time.time() - os.path.getmtime(LOCK_FILE)) < 900:
                logger.warning("Процесс уже запущен. Завершение работы.")
                return
        
        try:
            with open(LOCK_FILE, 'w') as f: f.write(str(time.time()))
            
            with open(SOURCE_FILE, 'r', encoding='utf-8', errors='ignore') as f:
                raw = [l.strip() for l in f if len(l.strip()) > 10 or l.startswith('#')]
            
            subs = [u for u in raw if u.startswith('http') and not u.startswith('#')]
            configs = [c for c in raw if '://' in c and not c.startswith('http') and not c.startswith('#')]
            
            logger.info(f"📥 Загружено источников: {len(subs)} подписок, {len(configs)} конфигов")

            all_links = configs.copy()
            async with aiohttp.ClientSession() as session:
                sub_data = await asyncio.gather(*[self.fetch_subscription(session, u) for u in subs])
                for links in sub_data: all_links.extend(links)
                
                unique_pool = list(set(all_links))
                total = len(unique_pool)
                logger.info(f"🔍 Уникальных нод после фильтрации: {total}")
                
                if total == 0: return

                batch = unique_pool[:BATCH_SIZE]
                logger.info(f"⚡ Начало массовой проверки {len(batch)} нод...")
                
                ip_cache = {}
                tasks = [self.check_node(session, *self.get_host_port(l), ip_cache) for l in batch]
                results = await asyncio.gather(*tasks)
                
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
                        valid_nodes.append({"link": final_link, "cc": cc, "type": "good", "ping": ping})
                    elif cc in INVALID_REGIONS and ping <= INVALID_THRESHOLD_MAX:
                        valid_nodes.append({"link": link, "cc": cc, "type": "invalid", "ping": ping})
                    else:
                        dead_set.add(link)

            # --- Распределение по файлам ---
            file_stats = {f: 0 for f in self.mandatory_files}
            nodes_by_file = {f: [] for f in self.mandatory_files}

            for node in valid_nodes:
                if node['type'] == "invalid":
                    nodes_by_file[INVALID_FILE].append(node['link'])
                else:
                    target_file = COUNTRY_MAP.get(node['cc'], DEFAULT_MIX)
                    nodes_by_file[target_file].append(node['link'])

            for filename, links in nodes_by_file.items():
                target_path = os.path.join(OUTPUT_DIR, filename)
                final_list = list(set(links))[:MAX_NODES_PER_FILE]
                file_stats[filename] = len(final_list)

                with open(target_path, 'w', encoding='utf-8') as f:
                    if final_list: 
                        f.write('\n'.join(final_list) + '\n')
                    else:
                        f.write(f"# Monster Engine: Update {datetime.now().strftime('%H:%M:%S')} | Offline\n")

            # Обновление списка источников
            updated_sources = [s for s in raw if s.startswith('http') or s.startswith('#') or (s in configs and s not in dead_set)]
            with open(SOURCE_FILE, 'w', encoding='utf-8') as f:
                f.write('\n'.join(updated_sources) + '\n')

            # Итоговый отчет
            logger.info("="*50)
            logger.info(f"📊 ОТЧЕТ | Проверено: {len(batch)} | Живых: {len(valid_nodes)} | Мертвых: {len(dead_set)}")
            logger.info("-" * 50)
            for f, c in sorted(file_stats.items(), key=lambda x: x[1], reverse=True):
                icon = "🟢" if c > 0 else "🔴"
                logger.info(f"   {icon} {f.ljust(18)} : {c} нод")
            
            unknown_count = len([n for n in valid_nodes if n['cc'] == "UNKNOWN"])
            if unknown_count > 0:
                logger.warning(f"⚠️ Неопознанные страны: {unknown_count} (направлены в mix.txt)")
            logger.info("="*50)

            self.state["processed_total"] += len(batch)
            self.save_state()

        except Exception as e:
            logger.error(f"Критическая ошибка: {e}", exc_info=True)
        finally:
            if os.path.exists(LOCK_FILE): 
                try: os.remove(LOCK_FILE)
                except: pass

if __name__ == "__main__":
    asyncio.run(MonsterParser().run())

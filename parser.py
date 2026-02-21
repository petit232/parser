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
DB_DIR = 'database'   # Системная папка
OUTPUT_DIR = 'proxy'  # Папка с результатами (публичная)

# Files inside "database"
SOURCE_FILE = 'all_sources.txt'
STATE_FILE = os.path.join(DB_DIR, 'monster_state.json')
GEOIP_DB = os.path.join(DB_DIR, 'GeoLite2-Country.mmdb')
LOCK_FILE = os.path.join(DB_DIR, '.monster.lock')

# Files inside "proxy"
LINKS_INFO_FILE = os.path.join(OUTPUT_DIR, 'LINKS_FOR_CLIENTS.txt')

# --- ENGINE CONSTANTS ---
TIMEOUT = 3              # Таймаут соединения
MAX_CONCURRENCY = 150    # Кол-во одновременных проверок
CYCLE_HOURS = 3          # Время полного круга обхода базы
BATCH_INTERVAL_MIN = 20  # Интервал запуска (минуты)

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

# Приоритетные регионы для сортировки
PRIORITY_REGIONS = {'BY', 'KZ', 'DE', 'FI', 'SE', 'LV', 'RU', 'US', 'CH', 'FR'}

# --- COUNTRY TO FILE MAPPING ---
# Только эти страны получают свои файлы. Все остальные идут в mix.txt.
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
MAX_NODES_PER_FILE = 500

class MonsterParser:
    """
    Основной движок парсинга, проверки и распределения прокси-конфигураций.
    """
    
    def __init__(self):
        self.start_time = time.time()
        self.ensure_structure()
        self.migrate_old_data()
        self.state = self.load_state()
        self.geo_reader = self.init_geo()
        
        # Регулярки для быстрого поиска
        self.proxy_pattern = re.compile(r'(vless|vmess|trojan|ss|ssr)://[^\s"\'<>()]+')
        self.ip_pattern = re.compile(r'@?([\w\.-]+):(\d+)')
        
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
        self.active_files = set(COUNTRY_MAP.values()) | {DEFAULT_MIX}

    def ensure_structure(self):
        """Создает структуру папок."""
        os.makedirs(DB_DIR, exist_ok=True)
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        if not os.path.exists(SOURCE_FILE):
            with open(SOURCE_FILE, 'w', encoding='utf-8') as f:
                f.write("# Вставьте сюда ссылки на подписки или конфиги\n")

    def migrate_old_data(self):
        """Переносит данные из старых папок на русском в новые латинские."""
        old_folders = {'база данных': DB_DIR, 'прокси': OUTPUT_DIR}
        for old, new in old_folders.items():
            if os.path.exists(old) and os.path.isdir(old):
                logger.info(f"🔄 Миграция данных из '{old}' в '{new}'...")
                for item in os.listdir(old):
                    s = os.path.join(old, item)
                    d = os.path.join(new, item)
                    try:
                        if os.path.exists(d): os.remove(s)
                        else: shutil.move(s, d)
                    except: pass
                try: os.rmdir(old)
                except: pass

    def init_geo(self):
        """Инициализация базы GeoIP."""
        if os.path.exists(GEOIP_DB):
            try: return geoip2.database.Reader(GEOIP_DB)
            except: pass
        return None

    def load_state(self):
        """Загрузка состояния движка."""
        default_state = {"last_index": 0, "processed_total": 0, "dead_total": 0}
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, 'r', encoding='utf-8') as f:
                    return {**default_state, **json.load(f)}
            except: pass
        return default_state

    def save_state(self):
        """Сохранение состояния."""
        try:
            with open(STATE_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.state, f, indent=4, ensure_ascii=False)
        except: pass

    def get_host_port(self, link):
        """Извлечение хоста и порта из ссылки."""
        try:
            match = self.ip_pattern.search(link)
            if match: return match.group(1), match.group(2)
        except: pass
        return None, None

    async def fetch_subscription(self, session, url):
        """Загрузка подписки и извлечение ссылок."""
        try:
            async with session.get(url, timeout=15) as response:
                if response.status == 200:
                    raw = await response.text()
                    try:
                        # Проверка на base64
                        decoded = base64.b64decode(raw.strip()).decode('utf-8', errors='ignore')
                        raw = decoded if '://' in decoded else raw
                    except: pass
                    return [m.group(0) for m in self.proxy_pattern.finditer(raw)]
        except Exception as e:
            logger.debug(f"Fetch error {url}: {e}")
        return []

    async def check_node(self, session, host, port, ip_cache):
        """Проверка доступности узла."""
        if not host or not port: return None, 9999
        cache_key = f"{host}:{port}"
        if cache_key in ip_cache: return ip_cache[cache_key]
        
        async with self.semaphore:
            start = time.time()
            try:
                # Резолв DNS
                ip = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(None, socket.gethostbyname, host), 
                    timeout=TIMEOUT
                )
                # Попытка TCP соединения
                _, writer = await asyncio.wait_for(
                    asyncio.open_connection(ip, int(port)), 
                    timeout=TIMEOUT
                )
                writer.close()
                await writer.wait_closed()
                
                ping = int((time.time() - start) * 1000)
                ip_cache[cache_key] = (ip, ping)
                return ip, ping
            except:
                ip_cache[cache_key] = (None, 9999)
                return None, 9999

    def get_country(self, ip):
        """Определение страны по IP."""
        if not self.geo_reader or not ip: return None
        try: return self.geo_reader.country(ip).country.iso_code
        except: return None

    def wrap_for_russia(self, link):
        """Оптимизация конфига для РФ (fragment/mux)."""
        try:
            parsed = urlparse(link)
            if parsed.scheme in ['vless', 'vmess', 'trojan']:
                query = parse_qs(parsed.query)
                if 'reality' in str(query).lower(): return link
                query['fragment'] = ['10-20,30-50']
                query['mux'] = ['enable=true&concurrency=8']
                new_parts = list(parsed)
                new_parts[4] = urlencode(query, doseq=True)
                return urlunparse(new_parts)
        except: pass
        return link

    def cleanup_obsolete_files(self):
        """Удаление старых .txt файлов, которых нет в мапе."""
        try:
            allowed = self.active_files | {os.path.basename(LINKS_INFO_FILE)}
            for f in os.listdir(OUTPUT_DIR):
                if f.endswith('.txt') and f not in allowed:
                    os.remove(os.path.join(OUTPUT_DIR, f))
        except: pass

    def update_links_for_clients(self):
        """Генерация статического файла со списком ссылок (без кириллицы)."""
        try:
            repo_url = ""
            try:
                remote = subprocess.check_output(["git", "config", "--get", "remote.origin.url"]).decode().strip()
                path = remote.replace("git@github.com:", "").replace("https://github.com/", "").replace(".git", "")
                repo_url = f"https://raw.githubusercontent.com/{path}/main/{OUTPUT_DIR}"
            except:
                repo_url = f"https://raw.githubusercontent.com/USER/REPO/main/{OUTPUT_DIR}"

            content = [
                "🚀 MONSTER ENGINE SUBSCRIPTIONS",
                "-"*40,
                f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
                ""
            ]
            
            for filename in sorted(list(self.active_files)):
                display_name = filename.replace('.txt','').upper()
                content.append(f"📍 {display_name}:")
                content.append(f"{repo_url}/{filename}")
                content.append("")
            
            with open(LINKS_INFO_FILE, 'w', encoding='utf-8') as f:
                f.write('\n'.join(content))
            logger.info("📄 LINKS_FOR_CLIENTS.txt updated.")
        except Exception as e:
            logger.error(f"Links file update error: {e}")

    async def run(self):
        """Главный цикл работы."""
        # Проверка блокировки
        if os.path.exists(LOCK_FILE) and (time.time() - os.path.getmtime(LOCK_FILE)) < 1200:
            return
            
        try:
            with open(LOCK_FILE, 'w') as f: f.write(str(time.time()))
            if not os.path.exists(SOURCE_FILE): return

            # Чтение источников
            with open(SOURCE_FILE, 'r', encoding='utf-8', errors='ignore') as f:
                raw_entries = list(dict.fromkeys([l.strip() for l in f if len(l.strip()) > 5 and not l.startswith('#')]))
            
            subscriptions = [e for e in raw_entries if e.startswith('http')]
            direct_configs = [e for e in raw_entries if '://' in e and not e.startswith('http')]

            all_links = direct_configs.copy()
            
            async with aiohttp.ClientSession() as session:
                # Сбор ссылок из подписок
                sub_results = await asyncio.gather(*[self.fetch_subscription(session, url) for url in subscriptions])
                for res in sub_results: all_links.extend(res)

                total_pool = set(all_links)
                
                # Обновляем инфо-файл
                self.update_links_for_clients()
                
                # Если нод нет, "трогаем" файлы и выходим
                if not total_pool:
                    for filename in self.active_files:
                        path = os.path.join(OUTPUT_DIR, filename)
                        with open(path, 'w') as f: f.write('')
                        os.utime(path, None)
                    return

                # Расчет батча
                batch_size = max(500, int(len(total_pool) / ((CYCLE_HOURS * 60) / BATCH_INTERVAL_MIN)))
                sorted_pool = list(total_pool)
                sorted_pool.sort(key=lambda x: any(p in x.upper() for p in PRIORITY_REGIONS), reverse=True)
                
                start_idx = self.state.get("last_index", 0)
                if start_idx >= len(sorted_pool): start_idx = 0
                current_batch = sorted_pool[start_idx : start_idx + batch_size]
                
                # Проверка батча
                ip_cache = {}
                tasks = [self.check_node(session, *self.get_host_port(link), ip_cache) for link in current_batch]
                checked = await asyncio.gather(*tasks)
                
                live_results = []
                dead_links = set()
                
                for idx, (ip, ping) in enumerate(checked):
                    link = current_batch[idx]
                    country = self.get_country(ip)
                    limit = PING_LIMITS.get(country, PING_LIMITS['DEFAULT'])
                    
                    if ip and ping <= limit:
                        node_link = self.wrap_for_russia(link) if country == 'RU' else link
                        live_results.append({"link": node_link, "country": country})
                    else:
                        dead_links.add(link)

            # Чистка и обновление файлов
            self.cleanup_obsolete_files()
            
            for filename in self.active_files:
                path = os.path.join(OUTPUT_DIR, filename)
                nodes = {}
                
                # Читаем старые (если они еще живы)
                if os.path.exists(path):
                    with open(path, 'r', encoding='utf-8') as f:
                        for l in f:
                            n = l.strip()
                            if n in total_pool and n not in dead_links: nodes[n] = True
                
                # Добавляем новые
                for res in live_results:
                    target = COUNTRY_MAP.get(res['country'], DEFAULT_MIX)
                    if target == filename:
                        nodes[res['link']] = True
                
                # Сохраняем и обновляем дату файла (mtime)
                with open(path, 'w', encoding='utf-8') as f:
                    if nodes:
                        f.write('\n'.join(list(nodes.keys())[:MAX_NODES_PER_FILE]) + '\n')
                    else:
                        f.write('')
                os.utime(path, None) # Принудительное обновление даты для GitHub

            # Синхронизация all_sources.txt
            final_sources = [e for e in raw_entries if e.startswith('http') or (e in total_pool and e not in dead_links)]
            with open(SOURCE_FILE, 'w', encoding='utf-8') as f:
                f.write('\n'.join(final_sources) + '\n')

            # Сохранение состояния
            self.state.update({
                "last_index": start_idx + batch_size, 
                "last_run": datetime.now().isoformat(),
                "processed_total": self.state.get("processed_total", 0) + len(current_batch)
            })
            self.save_state()
            logger.info("✅ Cycle finished successfully.")

        except Exception as e:
            logger.error(f"💥 Critical error in run: {e}", exc_info=True)
        finally:
            if os.path.exists(LOCK_FILE):
                try: os.remove(LOCK_FILE)
                except: pass

if __name__ == "__main__":
    asyncio.run(MonsterParser().run())

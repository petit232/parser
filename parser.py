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
from datetime import datetime
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("MonsterEngine")

# --- CONFIGURATION ---
SOURCE_FILE = 'all_sources.txt'
STATE_FILE = 'monster_state.json'
GEOIP_DB = 'GeoLite2-Country.mmdb'
LINKS_INFO_FILE = 'LINKS_FOR_CLIENTS.txt'
LOCK_FILE = '.monster.lock'

# Performance constants
TIMEOUT = 3            # Connection timeout
MAX_CONCURRENCY = 150  # Parallel checks
CYCLE_HOURS = 3        # Full database refresh cycle
BATCH_INTERVAL_MIN = 20 # GitHub Actions cron interval

# Ping thresholds (ms)
PING_LIMITS = {
    'DEFAULT': 250,
    'US': 300, 'HK': 300, 'SG': 300, 'JP': 300,
    'BY': 200, 'KZ': 200, 'RU': 250
}

# Priority regions
PRIORITY_REGIONS = {'BY', 'KZ', 'PL', 'DE', 'FI', 'SE', 'LT', 'LV', 'EE', 'RU'}

COUNTRY_MAP = {
    'RU': 'russia.txt', 'BY': 'belarus.txt', 'FI': 'finland.txt',
    'FR': 'france.txt', 'DE': 'germany.txt', 'HK': 'hongkong.txt',
    'KZ': 'kazakhstan.txt', 'NL': 'netherlands.txt', 'PL': 'poland.txt',
    'SG': 'singapore.txt', 'SE': 'sweden.txt', 'GB': 'uk.txt', 'US': 'usa.txt',
}
DEFAULT_MIX = 'mix.txt'
MAX_NODES_PER_COUNTRY = 500

class MonsterParser:
    def __init__(self):
        self.state = self.load_state()
        self.geo_reader = None
        try:
            if os.path.exists(GEOIP_DB):
                self.geo_reader = geoip2.database.Reader(GEOIP_DB)
        except Exception as e:
            logger.error(f"GeoIP Database error: {e}")
        
        # Регулярка для поиска прокси-ссылок в любом мусоре
        self.proxy_pattern = re.compile(r'(vless|vmess|trojan|ss|ssr)://[^\s"\'<>()]+')
        self.ip_pattern = re.compile(r'@?([\w\.-]+):(\d+)')
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

    def load_state(self):
        default_state = {"last_index": 0, "processed_total": 0, "dead_total": 0, "history": []}
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, 'r') as f:
                    data = json.load(f)
                    return {**default_state, **data}
            except Exception as e:
                logger.warning(f"Failed to load state: {e}")
        return default_state

    def save_state(self):
        try:
            with open(STATE_FILE, 'w') as f:
                json.dump(self.state, f, indent=4)
        except Exception as e:
            logger.error(f"Failed to save state: {e}")

    def get_host_port(self, link):
        try:
            match = self.ip_pattern.search(link)
            if match:
                return match.group(1), match.group(2)
        except Exception: pass
        return None, None

    def decode_content(self, content):
        """Декодирует Base64 содержимое подписок или возвращает текст как есть"""
        try:
            # Пытаемся декодировать как Base64
            decoded = base64.b64decode(content).decode('utf-8', errors='ignore')
            if '://' in decoded:
                return decoded
        except Exception:
            pass
        return content

    async def fetch_subscription(self, session, url):
        """Скачивает подписку и извлекает из неё ссылки"""
        try:
            async with session.get(url, timeout=10) as response:
                if response.status == 200:
                    raw_text = await response.text()
                    decoded_text = self.decode_content(raw_text.strip())
                    found = self.proxy_pattern.findall(decoded_text)
                    # findall вернет список протоколов, нам нужны полные ссылки
                    # Используем finditer для получения полных совпадений
                    return [m.group(0) for m in self.proxy_pattern.finditer(decoded_text)]
        except Exception as e:
            logger.error(f"Failed to fetch sub {url}: {e}")
        return []

    async def check_node(self, session, host, port, ip_cache):
        cache_key = f"{host}:{port}"
        if cache_key in ip_cache:
            return ip_cache[cache_key]
        
        async with self.semaphore:
            start_time = time.time()
            try:
                ip_addr = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(None, socket.gethostbyname, host),
                    timeout=TIMEOUT
                )
                
                target_port = int(port) if port else 443
                conn = asyncio.open_connection(ip_addr, target_port)
                reader, writer = await asyncio.wait_for(conn, timeout=TIMEOUT)
                
                writer.close()
                await writer.wait_closed()
                
                ping_ms = int((time.time() - start_time) * 1000)
                res = (ip_addr, ping_ms)
                ip_cache[cache_key] = res
                return res
            except Exception:
                ip_cache[cache_key] = (None, 9999)
                return None, 9999

    def get_country(self, ip):
        if not self.geo_reader or not ip: return None
        try:
            return self.geo_reader.country(ip).country.iso_code
        except Exception: return None

    def wrap_for_russia(self, link):
        try:
            parsed = urlparse(link)
            if not parsed.scheme or (not parsed.netloc and '@' not in link): return link
            query = parse_qs(parsed.query)
            
            if 'reality' in str(query.get('security', [])).lower(): return link
            
            if parsed.scheme in ['vless', 'vmess', 'trojan']:
                query['fragment'] = ['10-20,30-50']
                query['mux'] = ['enable=true&concurrency=8']
                if 'security' not in query: query['security'] = ['tls']
                
                new_parts = list(parsed)
                new_parts[4] = urlencode(query, doseq=True)
                return urlunparse(new_parts)
            return link
        except Exception: return link

    async def run(self):
        if os.path.exists(LOCK_FILE):
            lock_age = time.time() - os.path.getmtime(LOCK_FILE)
            if lock_age < 1200:
                logger.warning(f"Process already running. Aborting.")
                return
            else:
                try: os.remove(LOCK_FILE)
                except: pass
        
        try:
            with open(LOCK_FILE, 'w') as f: f.write(str(time.time()))

            if not os.path.exists(SOURCE_FILE):
                logger.error("Source file missing!")
                return

            # 1. Читаем сырые источники и разделяем их
            with open(SOURCE_FILE, 'r', encoding='utf-8', errors='ignore') as f:
                raw_entries = [l.strip() for l in f if len(l.strip()) > 5]
            
            raw_entries = list(dict.fromkeys(raw_entries))
            
            subscriptions = []
            direct_configs = []
            
            for entry in raw_entries:
                if entry.startswith('http'):
                    subscriptions.append(entry)
                else:
                    # Извлекаем ссылки из текста, даже если там кавычки
                    found = [m.group(0) for m in self.proxy_pattern.finditer(entry)]
                    direct_configs.extend(found)

            # 2. Распаковка подписок
            all_expanded_links = direct_configs
            logger.info(f"🌐 Fetching {len(subscriptions)} subscriptions...")
            
            async with aiohttp.ClientSession() as session:
                sub_tasks = [self.fetch_subscription(session, url) for url in subscriptions]
                sub_results = await asyncio.gather(*sub_tasks)
                for sub_links in sub_results:
                    all_expanded_links.extend(sub_links)

                # Убираем дубликаты после распаковки
                all_expanded_links = list(dict.fromkeys(all_expanded_links))
                total_count = len(all_expanded_links)
                
                if total_count == 0:
                    logger.warning("No links found in any source.")
                    return

                # 3. Батчинг
                runs_per_cycle = (CYCLE_HOURS * 60) / BATCH_INTERVAL_MIN
                batch_size = max(500, int(total_count / runs_per_cycle))
                
                # Приоритезация
                all_expanded_links.sort(key=lambda x: any(p in x.upper() for p in PRIORITY_REGIONS), reverse=True)
                
                start_idx = self.state.get("last_index", 0)
                if start_idx >= total_count: start_idx = 0
                end_idx = min(start_idx + batch_size, total_count)
                
                current_batch = all_expanded_links[start_idx:end_idx]
                logger.info(f"📊 Engine Stats: Total Found={total_count}, Batch={len(current_batch)}")
                
                ip_cache = {}
                results = []
                dead_links = set()
                
                # 4. Проверка нод
                tasks = []
                for link in current_batch:
                    h, p = self.get_host_port(link)
                    tasks.append(self.check_node(session, h, p, ip_cache))
                
                checked_data = await asyncio.gather(*tasks)
                
                for idx, (ip, ping) in enumerate(checked_data):
                    link = current_batch[idx]
                    country = self.get_country(ip) if ip else None
                    limit = PING_LIMITS.get(country, PING_LIMITS['DEFAULT'])
                    
                    if ip and ping <= limit:
                        results.append({
                            "link": self.wrap_for_russia(link),
                            "country": country,
                            "ping": ping
                        })
                    else:
                        dead_links.add(link)

            # 5. Обновление файлов по странам
            files_updated_stats = {}
            for filename in set(COUNTRY_MAP.values()) | {DEFAULT_MIX}:
                current_nodes = {}
                if os.path.exists(filename):
                    with open(filename, 'r', encoding='utf-8', errors='ignore') as f:
                        for l in f:
                            node = l.strip()
                            if node and node not in dead_links: 
                                current_nodes[node] = True
                
                for res in results:
                    target_file = COUNTRY_MAP.get(res['country'], DEFAULT_MIX)
                    if target_file == filename:
                        current_nodes[res['link']] = True
                
                nodes_to_save = list(current_nodes.keys())[:MAX_NODES_PER_COUNTRY]
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(nodes_to_save) + '\n')
                files_updated_stats[filename] = len(nodes_to_save)

            # 6. Глобальная чистка мастер-базы
            # Мы удаляем только те "прямые" конфиги, которые сдохли.
            # Ссылки на подписки (http) мы не трогаем, так как они - вечные источники.
            final_sources = []
            for entry in raw_entries:
                if entry.startswith('http'):
                    final_sources.append(entry)
                elif entry not in dead_links:
                    final_sources.append(entry)

            with open(SOURCE_FILE, 'w', encoding='utf-8') as f:
                f.write('\n'.join(final_sources) + '\n')

            # Сохранение состояния
            self.state["last_index"] = end_idx if end_idx < total_count else 0
            self.state["processed_total"] = self.state.get("processed_total", 0) + len(current_batch)
            self.state["dead_total"] = self.state.get("dead_total", 0) + len(dead_links)
            self.state["last_run_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.save_state()
            
            print("\n" + "="*50)
            print(f"🚀 MONSTER ENGINE REPORT | {self.state['last_run_time']}")
            print("="*50)
            print(f"📦 Sources: {len(subscriptions)} subs, {len(direct_configs)} direct")
            print(f"🔍 Total nodes found: {total_count}")
            print(f"✅ Live in batch: {len(results)} | 💀 Dead: {len(dead_links)}")
            print(f"📈 Active in files: {sum(files_updated_stats.values())}")
            print("="*50 + "\n")

        except Exception as e:
            logger.critical(f"FATAL ERROR: {e}", exc_info=True)
        finally:
            if os.path.exists(LOCK_FILE):
                try: os.remove(LOCK_FILE)
                except: pass

if __name__ == "__main__":
    parser = MonsterParser()
    asyncio.run(parser.run())

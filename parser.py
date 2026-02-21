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

    def update_links_for_clients(self, files_stats):
        """Создает файл со ссылками для клиентов на основе обновленных файлов"""
        try:
            # Пытаемся получить URL репозитория для формирования RAW ссылок
            repo_url = ""
            try:
                remote = subprocess.check_output(["git", "config", "--get", "remote.origin.url"]).decode().strip()
                if "github.com" in remote:
                    # Превращаем git@github.com:user/repo.git или https://github.com/user/repo.git
                    # в https://raw.githubusercontent.com/user/repo/main/
                    path = remote.replace("git@github.com:", "").replace("https://github.com/", "").replace(".git", "")
                    repo_url = f"https://raw.githubusercontent.com/{path}/main"
            except:
                logger.warning("Could not determine git remote URL, using placeholders.")
                repo_url = "https://raw.githubusercontent.com/YOUR_USERNAME/YOUR_REPO/main"

            content = [
                "🚀 MONSTER ENGINE - АКТУАЛЬНЫЕ ПОДПИСКИ",
                f"Обновлено: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}",
                "------------------------------------------",
                ""
            ]

            for filename, count in files_stats.items():
                if count > 0:
                    display_name = filename.replace(".txt", "").capitalize()
                    content.append(f"📍 {display_name} ({count} nodes):")
                    content.append(f"{repo_url}/{filename}")
                    content.append("")

            with open(LINKS_INFO_FILE, 'w', encoding='utf-8') as f:
                f.write('\n'.join(content))
            logger.info(f"✅ {LINKS_INFO_FILE} updated.")
        except Exception as e:
            logger.error(f"Failed to update client links file: {e}")

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

            with open(SOURCE_FILE, 'r', encoding='utf-8', errors='ignore') as f:
                raw_entries = [l.strip() for l in f if len(l.strip()) > 5]
            
            raw_entries = list(dict.fromkeys(raw_entries))
            subscriptions = []
            direct_configs = []
            
            for entry in raw_entries:
                if entry.startswith('http'):
                    subscriptions.append(entry)
                else:
                    found = [m.group(0) for m in self.proxy_pattern.finditer(entry)]
                    direct_configs.extend(found)

            all_expanded_links = direct_configs
            logger.info(f"🌐 Processing sources: {len(subscriptions)} subs, {len(direct_configs)} direct configs")
            
            async with aiohttp.ClientSession() as session:
                sub_tasks = [self.fetch_subscription(session, url) for url in subscriptions]
                sub_results = await asyncio.gather(*sub_tasks)
                for sub_links in sub_results:
                    all_expanded_links.extend(sub_links)

                initial_count = len(all_expanded_links)
                all_expanded_links = list(dict.fromkeys(all_expanded_links))
                duplicates_removed = initial_count - len(all_expanded_links)
                total_count = len(all_expanded_links)
                
                if total_count == 0:
                    logger.warning("No links found in any source.")
                    return

                runs_per_cycle = (CYCLE_HOURS * 60) / BATCH_INTERVAL_MIN
                batch_size = max(500, int(total_count / runs_per_cycle))
                
                all_expanded_links.sort(key=lambda x: any(p in x.upper() for p in PRIORITY_REGIONS), reverse=True)
                
                start_idx = self.state.get("last_index", 0)
                if start_idx >= total_count: start_idx = 0
                end_idx = min(start_idx + batch_size, total_count)
                
                current_batch = all_expanded_links[start_idx:end_idx]
                logger.info(f"📊 Engine Stats: Total Found={total_count}, Batch={len(current_batch)}")
                
                ip_cache = {}
                results = []
                dead_links = set()
                
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
            for filename in sorted(set(COUNTRY_MAP.values()) | {DEFAULT_MIX}):
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

            # 6. Обновление LINKS_FOR_CLIENTS.txt
            self.update_links_for_clients(files_updated_stats)

            # 7. Глобальная чистка мастер-базы
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
            print(f"🔍 Total unique nodes: {total_count}")
            print(f"✅ Live in batch: {len(results)} | 💀 Dead: {len(dead_links)}")
            print(f"📈 Total Active: {sum(files_updated_stats.values())}")
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

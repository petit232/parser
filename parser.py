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

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger("MonsterEngine")

# --- ARCHITECTURE (Russian Folders) ---
DB_DIR = 'база данных'
OUTPUT_DIR = 'прокси'

# Files in "база данных"
SOURCE_FILE = 'all_sources.txt' # Остается в корне
STATE_FILE = os.path.join(DB_DIR, 'monster_state.json')
GEOIP_DB = os.path.join(DB_DIR, 'GeoLite2-Country.mmdb')
LOCK_FILE = os.path.join(DB_DIR, '.monster.lock')

# Files in "прокси"
LINKS_INFO_FILE = os.path.join(OUTPUT_DIR, 'LINKS_FOR_CLIENTS.txt')

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

# Priority regions for sorting
PRIORITY_REGIONS = {'BY', 'KZ', 'PL', 'DE', 'FI', 'SE', 'LT', 'LV', 'EE', 'RU', 'US'}

# Mapping countries to filenames inside OUTPUT_DIR
COUNTRY_MAP = {
    'RU': 'russia.txt', 'BY': 'belarus.txt', 'FI': 'finland.txt',
    'FR': 'france.txt', 'DE': 'germany.txt', 'HK': 'hongkong.txt',
    'KZ': 'kazakhstan.txt', 'NL': 'netherlands.txt', 'PL': 'poland.txt',
    'SG': 'singapore.txt', 'SE': 'sweden.txt', 'GB': 'uk.txt', 'US': 'usa.txt',
    'LT': 'lithuania.txt', 'LV': 'latvia.txt', 'EE': 'estonia.txt', 'CH': 'switzerland.txt'
}
DEFAULT_MIX = 'mix.txt'
MAX_NODES_PER_FILE = 500

class MonsterParser:
    def __init__(self):
        self.ensure_structure()
        self.migrate_old_data()
        self.state = self.load_state()
        self.geo_reader = self.init_geo()
        
        # Pattern to find proxy links in any "trash" text
        self.proxy_pattern = re.compile(r'(vless|vmess|trojan|ss|ssr)://[^\s"\'<>()]+')
        self.ip_pattern = re.compile(r'@?([\w\.-]+):(\d+)')
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

    def ensure_structure(self):
        """Creates required Russian folders"""
        os.makedirs(DB_DIR, exist_ok=True)
        os.makedirs(OUTPUT_DIR, exist_ok=True)

    def migrate_old_data(self):
        """Moves files from root to 'база данных' if they exist in root"""
        to_migrate = ['monster_state.json', 'GeoLite2-Country.mmdb', 'persistent_blacklist.txt', 'processed_sources.dat']
        for filename in to_migrate:
            if os.path.exists(filename) and not os.path.islink(filename):
                try:
                    shutil.move(filename, os.path.join(DB_DIR, filename))
                    logger.info(f"🚚 Moved {filename} to '{DB_DIR}/'")
                except Exception as e:
                    logger.error(f"Migration error for {filename}: {e}")

    def init_geo(self):
        if os.path.exists(GEOIP_DB):
            try:
                return geoip2.database.Reader(GEOIP_DB)
            except Exception as e:
                logger.error(f"GeoIP Database error: {e}")
        return None

    def load_state(self):
        default_state = {"last_index": 0, "processed_total": 0, "dead_total": 0}
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, 'r', encoding='utf-8') as f:
                    return {**default_state, **json.load(f)}
            except Exception as e:
                logger.warning(f"Failed to load state: {e}")
        return default_state

    def save_state(self):
        try:
            with open(STATE_FILE, 'w', encoding='utf-8') as f:
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
        """Decodes Base64 or returns plain text"""
        try:
            decoded = base64.b64decode(content).decode('utf-8', errors='ignore')
            if '://' in decoded:
                return decoded
        except Exception:
            pass
        return content

    async def fetch_subscription(self, session, url):
        """Downloads sub and extracts links"""
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
        """Applies fragment/mux for Russian optimization"""
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

    def cleanup_root(self):
        """Deletes old country .txt files from root to keep it clean as requested"""
        protected = [SOURCE_FILE, 'requirements.txt', 'README.md', 'parser.py']
        for f in os.listdir('.'):
            if f.endswith('.txt') and f not in protected:
                try:
                    os.remove(f)
                    logger.info(f"🧹 Cleaned up old file from root: {f}")
                except: pass

    def update_links_for_clients(self, files_stats):
        """Updates LINKS_FOR_CLIENTS.txt in the 'прокси' folder"""
        try:
            repo_url = ""
            try:
                remote = subprocess.check_output(["git", "config", "--get", "remote.origin.url"]).decode().strip()
                path = remote.replace("git@github.com:", "").replace("https://github.com/", "").replace(".git", "")
                repo_url = f"https://raw.githubusercontent.com/{path}/main/%D0%BF%D1%80%D0%BE%D0%BA%D1%81%D0%B8"
            except:
                repo_url = "https://raw.githubusercontent.com/USER/REPO/main/%D0%BF%D1%80%D0%BE%D0%BA%D1%81%D0%B8"

            content = [
                "🚀 MONSTER ENGINE - АКТУАЛЬНЫЕ ПОДПИСКИ",
                f"Обновлено: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}",
                "------------------------------------------",
                ""
            ]

            # Sort files by importance
            sorted_files = sorted(files_stats.items(), key=lambda x: x[1], reverse=True)
            for filename, count in sorted_files:
                if count > 0:
                    display_name = filename.replace(".txt", "").upper()
                    content.append(f"📍 {display_name} ({count} nodes):")
                    content.append(f"{repo_url}/{filename}")
                    content.append("")

            with open(LINKS_INFO_FILE, 'w', encoding='utf-8') as f:
                f.write('\n'.join(content))
        except Exception as e:
            logger.error(f"Failed to update client links file: {e}")

    async def run(self):
        self.cleanup_root()
        
        if os.path.exists(LOCK_FILE):
            lock_age = time.time() - os.path.getmtime(LOCK_FILE)
            if lock_age < 1200:
                logger.warning("Process already running. Aborting.")
                return
            else:
                try: os.remove(LOCK_FILE)
                except: pass
        
        try:
            with open(LOCK_FILE, 'w') as f: f.write(str(time.time()))

            if not os.path.exists(SOURCE_FILE):
                logger.error(f"Source file {SOURCE_FILE} missing!")
                return

            with open(SOURCE_FILE, 'r', encoding='utf-8', errors='ignore') as f:
                raw_entries = list(dict.fromkeys([l.strip() for l in f if len(l.strip()) > 5]))
            
            subscriptions = [e for e in raw_entries if e.startswith('http')]
            direct_configs = []
            for entry in [e for e in raw_entries if not e.startswith('http')]:
                direct_configs.extend([m.group(0) for m in self.proxy_pattern.finditer(entry)])

            all_expanded_links = direct_configs
            logger.info(f"🌐 Sources: {len(subscriptions)} subs, {len(direct_configs)} direct")
            
            async with aiohttp.ClientSession() as session:
                sub_results = await asyncio.gather(*[self.fetch_subscription(session, url) for url in subscriptions])
                for sub_links in sub_results:
                    all_expanded_links.extend(sub_links)

                all_expanded_links = list(dict.fromkeys(all_expanded_links))
                total_count = len(all_expanded_links)
                
                if total_count == 0:
                    logger.warning("No links found.")
                    # Даже если ничего не нашли, обновим дату в главном файле ссылок
                    self.update_links_for_clients({})
                    return

                # Batching logic
                runs_per_cycle = (CYCLE_HOURS * 60) / BATCH_INTERVAL_MIN
                batch_size = max(500, int(total_count / runs_per_cycle))
                
                # Sort by priority regions
                all_expanded_links.sort(key=lambda x: any(p in x.upper() for p in PRIORITY_REGIONS), reverse=True)
                
                start_idx = self.state.get("last_index", 0)
                if start_idx >= total_count: start_idx = 0
                end_idx = min(start_idx + batch_size, total_count)
                
                current_batch = all_expanded_links[start_idx:end_idx]
                logger.info(f"📊 Engine Batch: {len(current_batch)} nodes from {total_count}")
                
                ip_cache = {}
                tasks = [self.check_node(session, *self.get_host_port(link), ip_cache) for link in current_batch]
                checked_data = await asyncio.gather(*tasks)
                
                live_results = []
                dead_links = set()
                
                for idx, (ip, ping) in enumerate(checked_data):
                    link = current_batch[idx]
                    country = self.get_country(ip) if ip else None
                    limit = PING_LIMITS.get(country, PING_LIMITS['DEFAULT'])
                    
                    if ip and ping <= limit:
                        final_link = self.wrap_for_russia(link) if country == 'RU' else link
                        live_results.append({"link": final_link, "country": country})
                    else:
                        dead_links.add(link)

            # Update files in "прокси" folder
            files_updated_stats = {}
            target_filenames = set(COUNTRY_MAP.values()) | {DEFAULT_MIX}
            
            for filename in target_filenames:
                path = os.path.join(OUTPUT_DIR, filename)
                current_nodes = {}
                
                # Load existing nodes to keep them until they fail a check
                if os.path.exists(path):
                    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                        for l in f:
                            node = l.strip()
                            if node and node not in dead_links:
                                current_nodes[node] = True
                
                # Add new live results
                for res in live_results:
                    target = COUNTRY_MAP.get(res['country'], DEFAULT_MIX)
                    if target == filename:
                        current_nodes[res['link']] = True
                
                nodes_to_save = list(current_nodes.keys())[:MAX_NODES_PER_FILE]
                
                # FORCE UPDATE: Even if nodes_to_save is empty or unchanged,
                # we rewrite the file to update the Git timestamp.
                try:
                    with open(path, 'w', encoding='utf-8') as f:
                        if nodes_to_save:
                            f.write('\n'.join(nodes_to_save) + '\n')
                        else:
                            # If no nodes, leave a comment or empty line to ensure file exists and is "touched"
                            f.write('') 
                    
                    # Update modification time explicitly for insurance
                    os.utime(path, None)
                except Exception as e:
                    logger.error(f"Error updating file {filename}: {e}")
                    
                files_updated_stats[filename] = len(nodes_to_save)

            self.update_links_for_clients(files_updated_stats)

            # Update source file (clean dead nodes)
            final_sources = [e for e in raw_entries if e.startswith('http') or e not in dead_links]
            with open(SOURCE_FILE, 'w', encoding='utf-8') as f:
                f.write('\n'.join(final_sources) + '\n')

            # Save state
            self.state.update({
                "last_index": end_idx if end_idx < total_count else 0,
                "processed_total": self.state.get("processed_total", 0) + len(current_batch),
                "dead_total": self.state.get("dead_total", 0) + len(dead_links),
                "last_run_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
            self.save_state()
            logger.info(f"✅ Cycle complete. Active nodes: {sum(files_updated_stats.values())}")

        except Exception as e:
            logger.critical(f"FATAL ERROR: {e}", exc_info=True)
        finally:
            if os.path.exists(LOCK_FILE):
                try: os.remove(LOCK_FILE)
                except: pass

if __name__ == "__main__":
    asyncio.run(MonsterParser().run())

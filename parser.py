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

# --- DIRECTORY STRUCTURE (Russian Names) ---
DB_DIR = 'база данных'
OUTPUT_DIR = 'прокси'

# Files inside "база данных" (Database Folder)
SOURCE_FILE = 'all_sources.txt' # Main source list (root)
STATE_FILE = os.path.join(DB_DIR, 'monster_state.json')
GEOIP_DB = os.path.join(DB_DIR, 'GeoLite2-Country.mmdb')
LOCK_FILE = os.path.join(DB_DIR, '.monster.lock')

# Files inside "прокси" (Output Folder)
LINKS_INFO_FILE = os.path.join(OUTPUT_DIR, 'LINKS_FOR_CLIENTS.txt')

# --- ENGINE CONSTANTS ---
TIMEOUT = 3              # Max seconds for connection attempt
MAX_CONCURRENCY = 150    # Concurrent connection workers
CYCLE_HOURS = 3          # Time to rotate through entire database
BATCH_INTERVAL_MIN = 20  # How often the script triggers (Cron)

# --- NETWORK THRESHOLDS ---
# Ping limits in milliseconds per region
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

# Sorting priority: these regions appear first in combined lists
PRIORITY_REGIONS = {'BY', 'KZ', 'DE', 'FI', 'SE', 'LV', 'RU', 'US', 'CH', 'FR'}

# --- COUNTRY TO FILE MAPPING ---
# ONLY these countries get dedicated files. Others go to mix.txt.
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
    Main Engine for parsing, checking, and distributing proxy configurations.
    Supports VLESS, VMESS, TROJAN, SS, SSR.
    """
    
    def __init__(self):
        self.start_time = time.time()
        self.ensure_structure()
        self.migrate_old_data()
        self.state = self.load_state()
        self.geo_reader = self.init_geo()
        
        # Regex patterns for high-speed parsing
        self.proxy_pattern = re.compile(r'(vless|vmess|trojan|ss|ssr)://[^\s"\'<>()]+')
        self.ip_pattern = re.compile(r'@?([\w\.-]+):(\d+)')
        
        # Control flow
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENCY)
        self.active_files = set(COUNTRY_MAP.values()) | {DEFAULT_MIX}

    def ensure_structure(self):
        """Creates the necessary Russian-named folders if they don't exist."""
        os.makedirs(DB_DIR, exist_ok=True)
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        if not os.path.exists(SOURCE_FILE):
            with open(SOURCE_FILE, 'w', encoding='utf-8') as f:
                f.write("# Добавьте сюда ваши ссылки (http подписки или прямые конфиги)\n")

    def migrate_old_data(self):
        """Automatically moves system files from root to the database folder."""
        to_migrate = [
            'monster_state.json', 
            'GeoLite2-Country.mmdb', 
            'persistent_blacklist.txt', 
            'processed_sources.dat',
            '.monster.lock'
        ]
        for filename in to_migrate:
            if os.path.exists(filename) and not os.path.islink(filename):
                try:
                    target_path = os.path.join(DB_DIR, filename)
                    if os.path.exists(target_path):
                        os.remove(filename)
                    else:
                        shutil.move(filename, target_path)
                    logger.info(f"🚚 Migrated {filename} to '{DB_DIR}/'")
                except Exception as e:
                    logger.debug(f"Migration skip for {filename}: {e}")

    def init_geo(self):
        """Initializes the GeoIP2 database reader."""
        if os.path.exists(GEOIP_DB):
            try:
                return geoip2.database.Reader(GEOIP_DB)
            except Exception as e:
                logger.error(f"GeoIP DB Initialization error: {e}")
        else:
            logger.warning(f"GeoIP Database not found at {GEOIP_DB}")
        return None

    def load_state(self):
        """Loads engine state from JSON."""
        default_state = {
            "last_index": 0, 
            "processed_total": 0, 
            "dead_total": 0,
            "history": []
        }
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, 'r', encoding='utf-8') as f:
                    content = f.read()
                    if content:
                        return {**default_state, **json.loads(content)}
            except Exception as e:
                logger.warning(f"State load failed, using defaults: {e}")
        return default_state

    def save_state(self):
        """Persists engine state to JSON."""
        try:
            with open(STATE_FILE, 'w', encoding='utf-8') as f:
                json.dump(self.state, f, indent=4, ensure_ascii=False)
        except Exception as e:
            logger.error(f"State save failed: {e}")

    def get_host_port(self, link):
        """Extracts hostname and port from a proxy link string."""
        try:
            # Handle standard formats and IPv6
            match = self.ip_pattern.search(link)
            if match:
                return match.group(1), match.group(2)
        except Exception:
            pass
        return None, None

    def decode_content(self, content):
        """Attempts to decode base64 subscription content."""
        try:
            # Check if it looks like base64
            decoded = base64.b64decode(content).decode('utf-8', errors='ignore')
            if '://' in decoded:
                return decoded
        except Exception:
            pass
        return content

    async def fetch_subscription(self, session, url):
        """Downloads a subscription link and extracts all valid proxy configs."""
        try:
            async with session.get(url, timeout=15) as response:
                if response.status == 200:
                    raw_text = await response.text()
                    decoded_text = self.decode_content(raw_text.strip())
                    links = [m.group(0) for m in self.proxy_pattern.finditer(decoded_text)]
                    logger.info(f"📥 Sub {url[:30]}... found {len(links)} links")
                    return links
                else:
                    logger.warning(f"⚠️ Sub {url[:30]}... returned status {response.status}")
        except Exception as e:
            logger.error(f"❌ Failed to fetch sub {url[:30]}: {e}")
        return []

    async def check_node(self, session, host, port, ip_cache):
        """Performs a TCP check and measures latency."""
        if not host or not port:
            return None, 9999
            
        cache_key = f"{host}:{port}"
        if cache_key in ip_cache:
            return ip_cache[cache_key]
        
        async with self.semaphore:
            start_check = time.time()
            try:
                # Resolve DNS with timeout
                ip_addr = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(None, socket.gethostbyname, host),
                    timeout=TIMEOUT
                )
                
                # Try TCP connection
                conn = asyncio.open_connection(ip_addr, int(port))
                reader, writer = await asyncio.wait_for(conn, timeout=TIMEOUT)
                
                writer.close()
                await writer.wait_closed()
                
                ping_ms = int((time.time() - start_check) * 1000)
                res = (ip_addr, ping_ms)
                ip_cache[cache_key] = res
                return res
            except Exception:
                ip_cache[cache_key] = (None, 9999)
                return None, 9999

    def get_country(self, ip):
        """Determines ISO country code for an IP address."""
        if not self.geo_reader or not ip:
            return None
        try:
            return self.geo_reader.country(ip).country.iso_code
        except Exception:
            return None

    def wrap_for_russia(self, link):
        """Enhances VLESS/VMESS/Trojan for Russian network conditions (fragment/mux)."""
        try:
            parsed = urlparse(link)
            if not parsed.scheme or (not parsed.netloc and '@' not in link):
                return link
                
            query = parse_qs(parsed.query)
            # Skip reality nodes as they don't support standard fragment
            if 'reality' in str(query.get('security', [])).lower():
                return link
            
            if parsed.scheme in ['vless', 'vmess', 'trojan']:
                # Adding fragment and mux parameters
                query['fragment'] = ['10-20,30-50']
                query['mux'] = ['enable=true&concurrency=8']
                if 'security' not in query:
                    query['security'] = ['tls']
                
                new_parts = list(parsed)
                new_parts[4] = urlencode(query, doseq=True)
                return urlunparse(new_parts)
            return link
        except Exception:
            return link

    def cleanup_obsolete_files(self):
        """Deletes any .txt files in the output directory that are not in the current config."""
        try:
            # We want to keep only files from COUNTRY_MAP + mix.txt + LINKS_INFO_FILE
            allowed = self.active_files | {os.path.basename(LINKS_INFO_FILE)}
            
            for f in os.listdir(OUTPUT_DIR):
                if f.endswith('.txt') and f not in allowed:
                    try:
                        os.remove(os.path.join(OUTPUT_DIR, f))
                        logger.info(f"🧹 Removed obsolete file: {f}")
                    except Exception as e:
                        logger.error(f"Error removing {f}: {e}")
        except Exception as e:
            logger.error(f"Cleanup error: {e}")

    def update_links_for_clients(self, files_stats):
        """Generates a summary file with direct links to all proxy lists."""
        try:
            # Get current repo URL for raw links
            repo_url = ""
            try:
                remote = subprocess.check_output(["git", "config", "--get", "remote.origin.url"]).decode().strip()
                path = remote.replace("git@github.com:", "").replace("https://github.com/", "").replace(".git", "")
                repo_url = f"https://raw.githubusercontent.com/{path}/main/%D0%BF%D1%80%D0%BE%D0%BA%D1%81%D0%B8"
            except:
                repo_url = "https://raw.githubusercontent.com/USER/REPO/main/%D0%BF%D1%80%D0%BE%D0%BA%D1%81%D0%B8"

            now_str = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
            content = [
                "🚀 MONSTER ENGINE - АКТУАЛЬНЫЕ ПОДПИСКИ",
                f"⚡ Последнее обновление: {now_str}",
                "------------------------------------------",
                ""
            ]

            # Sort files by node count (descending)
            sorted_files = sorted(files_stats.items(), key=lambda x: x[1], reverse=True)
            for filename, count in sorted_files:
                display_name = filename.replace(".txt", "").replace("_", " ").upper()
                content.append(f"📍 {display_name} ({count} нод):")
                content.append(f"{repo_url}/{filename}")
                content.append("")

            with open(LINKS_INFO_FILE, 'w', encoding='utf-8') as f:
                f.write('\n'.join(content))
        except Exception as e:
            logger.error(f"Failed to update client links file: {e}")

    async def run(self):
        """Main execution loop."""
        # --- LOCK CHECK ---
        if os.path.exists(LOCK_FILE):
            lock_age = time.time() - os.path.getmtime(LOCK_FILE)
            if lock_age < 1200: # 20 mins
                logger.warning("Engine is already running (locked). Skipping.")
                return
            else:
                try: os.remove(LOCK_FILE)
                except: pass
        
        try:
            with open(LOCK_FILE, 'w') as f:
                f.write(str(time.time()))

            # --- LOAD SOURCES ---
            if not os.path.exists(SOURCE_FILE):
                logger.error("No source file found. Exiting.")
                return

            with open(SOURCE_FILE, 'r', encoding='utf-8', errors='ignore') as f:
                raw_entries = list(dict.fromkeys([l.strip() for l in f if len(l.strip()) > 5 and not l.startswith('#')]))
            
            subscriptions = [e for e in raw_entries if e.startswith('http')]
            direct_configs = [e for e in raw_entries if not e.startswith('http') and '://' in e]

            all_expanded_links = direct_configs.copy()
            logger.info(f"🌐 Processing {len(subscriptions)} subs and {len(direct_configs)} direct links.")
            
            # --- FETCHING ---
            async with aiohttp.ClientSession() as session:
                sub_results = await asyncio.gather(*[self.fetch_subscription(session, url) for url in subscriptions])
                for sub_links in sub_results:
                    if sub_links:
                        all_expanded_links.extend(sub_links)

                # Use set for unique pool
                total_pool = set(all_expanded_links)
                total_count = len(total_pool)
                logger.info(f"📦 Total unique nodes discovered: {total_count}")
                
                if total_count == 0:
                    # Update date even if no nodes
                    self.update_links_for_clients({f: 0 for f in self.active_files})
                    return

                # --- BATCHING ---
                runs_per_cycle = (CYCLE_HOURS * 60) / BATCH_INTERVAL_MIN
                batch_size = max(500, int(total_count / runs_per_cycle))
                
                sorted_pool = list(total_pool)
                # Sort by region priority
                sorted_pool.sort(key=lambda x: any(p in x.upper() for p in PRIORITY_REGIONS), reverse=True)
                
                start_idx = self.state.get("last_index", 0)
                if start_idx >= total_count: start_idx = 0
                end_idx = min(start_idx + batch_size, total_count)
                
                current_batch = sorted_pool[start_idx:end_idx]
                logger.info(f"📊 Checking batch: [{start_idx}:{end_idx}] ({len(current_batch)} nodes)")
                
                # --- CHECKING ---
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

            # --- FILE DISTRIBUTION & SYNC ---
            files_updated_stats = {}
            
            # 1. Clean up old files like estonia.txt if they are not in COUNTRY_MAP
            self.cleanup_obsolete_files()
            
            # 2. Update each active file
            for filename in self.active_files:
                path = os.path.join(OUTPUT_DIR, filename)
                current_nodes = {}
                
                # Load existing nodes from file
                if os.path.exists(path):
                    with open(path, 'r', encoding='utf-8', errors='ignore') as f:
                        for l in f:
                            node = l.strip()
                            # SYNC: Keep only if node still exists in sources AND is not dead
                            if node and node in total_pool and node not in dead_links:
                                current_nodes[node] = True
                
                # Add newly found live nodes for this country/category
                for res in live_results:
                    target = COUNTRY_MAP.get(res['country'], DEFAULT_MIX)
                    if target == filename:
                        current_nodes[res['link']] = True
                
                # Limit size and prepare for writing
                nodes_to_save = list(current_nodes.keys())[:MAX_NODES_PER_FILE]
                
                # MANDATORY: Open and Write to ensure file exists and mtime updates
                try:
                    with open(path, 'w', encoding='utf-8') as f:
                        if nodes_to_save:
                            f.write('\n'.join(nodes_to_save) + '\n')
                        else:
                            f.write('') # Keep file empty but "touched"
                    
                    # Force update timestamp even if content is same
                    os.utime(path, None)
                except Exception as e:
                    logger.error(f"Failed to write/touch {filename}: {e}")
                    
                files_updated_stats[filename] = len(nodes_to_save)

            # 3. Update Client Links File
            self.update_links_for_clients(files_updated_stats)

            # --- SOURCE FILE CLEANUP ---
            # Remove direct configs that were found to be dead
            # We preserve http subscriptions (as they are managed externally)
            final_sources = [e for e in raw_entries if e.startswith('http') or (e in total_pool and e not in dead_links)]
            
            with open(SOURCE_FILE, 'w', encoding='utf-8') as f:
                f.write('\n'.join(final_sources) + '\n')

            # --- STATE UPDATE ---
            self.state.update({
                "last_index": end_idx if end_idx < total_count else 0,
                "processed_total": self.state.get("processed_total", 0) + len(current_batch),
                "dead_total": self.state.get("dead_total", 0) + len(dead_links),
                "last_run_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            })
            self.save_state()
            
            logger.info(f"✅ Finished cycle. Live: {sum(files_updated_stats.values())}. Removed dead: {len(dead_links)}")

        except Exception as e:
            logger.critical(f"💥 CRITICAL ENGINE FAILURE: {e}", exc_info=True)
        finally:
            # Always ensure the lock is released
            if os.path.exists(LOCK_FILE):
                try: os.remove(LOCK_FILE)
                except: pass

if __name__ == "__main__":
    # Start the async event loop
    asyncio.run(MonsterParser().run())

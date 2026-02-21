import os
import json
import asyncio
import aiohttp
import time
import re
import socket
import geoip2.database
import logging
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

# Константы производительности
TIMEOUT = 3            # Максимальное время ожидания ответа (DNS + TCP)
MAX_CONCURRENCY = 150  # Количество параллельных проверок
CYCLE_HOURS = 3        # За сколько часов нужно обновить всю базу полностью
BATCH_INTERVAL_MIN = 20 # Как часто запускается скрипт (через GitHub Actions cron)

# Пороги пинга для фильтрации (в миллисекундах)
PING_LIMITS = {
    'DEFAULT': 200,
    'US': 220, 'HK': 220, 'SG': 220, 'JP': 220,
    'BY': 200, 'KZ': 200, 'RU': 200
}

# Регионы с наивысшим приоритетом (Беларусь, Казахстан, Европа)
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
        
        # Регулярка для извлечения хоста и порта
        self.ip_pattern = re.compile(r'@?([\w\.-]+):(\d+)')
        self.semaphore = asyncio.Semaphore(MAX_CONCURRENCY)

    def load_state(self):
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, 'r') as f:
                    return json.load(f)
            except Exception: pass
        return {"last_index": 0, "processed_total": 0, "history": []}

    def save_state(self):
        try:
            with open(STATE_FILE, 'w') as f:
                json.dump(self.state, f, indent=4)
        except Exception: pass

    def get_host_port(self, link):
        """Извлекает хост и порт из прокси-ссылки"""
        try:
            match = self.ip_pattern.search(link)
            if match:
                return match.group(1), match.group(2)
        except Exception: pass
        return None, None

    async def check_node(self, session, host, port, ip_cache):
        """
        Ультра-быстрый асинхронный TCP-пинг с неблокирующим DNS.
        """
        cache_key = f"{host}:{port}"
        if cache_key in ip_cache:
            return ip_cache[cache_key]
        
        async with self.semaphore:
            start_time = time.time()
            try:
                # DNS Резолвинг в отдельном потоке (чтобы не блокировать asyncio)
                ip_addr = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(None, socket.gethostbyname, host),
                    timeout=TIMEOUT
                )
                
                # Попытка TCP соединения
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
        """Добавляет параметры фрагментации для обхода DPI в РФ"""
        try:
            parsed = urlparse(link)
            if not parsed.scheme or (not parsed.netloc and '@' not in link): return link
            query = parse_qs(parsed.query)
            
            # Не трогаем Reality, они и так работают
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
        # 1. Защита от наслоения (Lock File)
        if os.path.exists(LOCK_FILE):
            lock_age = time.time() - os.path.getmtime(LOCK_FILE)
            if lock_age < 1200: # 20 минут лимит на один прогон
                logger.warning(f"Process already running (Age: {int(lock_age)}s). Aborting.")
                return
            else:
                os.remove(LOCK_FILE)
        
        try:
            with open(LOCK_FILE, 'w') as f: f.write(str(time.time()))

            if not os.path.exists(SOURCE_FILE):
                logger.error("Source file missing!")
                return

            # Читаем и чистим базу от дублей
            with open(SOURCE_FILE, 'r', encoding='utf-8', errors='ignore') as f:
                links = [l.strip() for l in f if len(l.strip()) > 10]
            
            links = list(dict.fromkeys(links))
            total_count = len(links)
            
            # 2. Динамический расчет размера батча (3-часовой цикл)
            runs_per_cycle = (CYCLE_HOURS * 60) / BATCH_INTERVAL_MIN
            batch_size = max(500, int(total_count / runs_per_cycle))
            
            # 3. Умная приоритезация
            # Сначала проверяем BY, KZ и те, что содержат приоритетные регионы в ссылке
            links.sort(key=lambda x: any(p in x.upper() for p in PRIORITY_REGIONS), reverse=True)
            
            # Определяем диапазон для текущего прогона
            start_idx = self.state.get("last_index", 0)
            if start_idx >= total_count: start_idx = 0
            end_idx = min(start_idx + batch_size, total_count)
            
            current_batch = links[start_idx:end_idx]

            logger.info(f"📊 Engine Stats: Total Links={total_count}, Current Batch={len(current_batch)}")
            logger.info(f"🎯 Cycle Target: Complete update every {CYCLE_HOURS} hours")
            
            ip_cache = {}
            results = []
            dead_links = set()
            
            async with aiohttp.ClientSession() as session:
                tasks = []
                for link in current_batch:
                    h, p = self.get_host_port(link)
                    tasks.append(self.check_node(session, h, p, ip_cache))
                
                checked_data = await asyncio.gather(*tasks)
                
                for idx, (ip, ping) in enumerate(checked_data):
                    link = current_batch[idx]
                    country = self.get_country(ip) if ip else None
                    
                    # Получаем лимит пинга для страны (или дефолт 200)
                    limit = PING_LIMITS.get(country, PING_LIMITS['DEFAULT'])
                    
                    if ip and ping <= limit:
                        results.append({
                            "link": self.wrap_for_russia(link),
                            "country": country,
                            "ping": ping,
                            "score": 1000 if country in PRIORITY_REGIONS else 0
                        })
                    else:
                        # Если не ответил или пинг > лимита — в бан
                        dead_links.add(link)

            # 4. Атомарное обновление файлов по странам
            for filename in set(COUNTRY_MAP.values()) | {DEFAULT_MIX}:
                current_nodes = {}
                # Читаем старые, если они не мертвы
                if os.path.exists(filename):
                    with open(filename, 'r', encoding='utf-8', errors='ignore') as f:
                        for l in f:
                            node = l.strip()
                            if node and node not in dead_links: current_nodes[node] = True
                
                # Добавляем новые живые
                for res in results:
                    if COUNTRY_MAP.get(res['country'], DEFAULT_MIX) == filename:
                        current_nodes[res['link']] = True
                
                # Сохраняем топ-500
                with open(filename, 'w', encoding='utf-8') as f:
                    f.write('\n'.join(list(current_nodes.keys())[:MAX_NODES_PER_COUNTRY]) + '\n')

            # 5. Глобальная чистка мастер-базы (all_sources.txt)
            # Убираем все мертвые ссылки навсегда
            remaining_master = [l for l in links if l not in dead_links]
            with open(SOURCE_FILE, 'w', encoding='utf-8') as f:
                f.write('\n'.join(remaining_master) + '\n')

            # Сохраняем прогресс
            self.state["last_index"] = end_idx if end_idx < total_count else 0
            self.state["processed_total"] += len(current_batch)
            self.state["last_run_time"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.save_state()
            
            logger.info(f"✅ Batch Completed. Live: {len(results)}, Removed: {len(dead_links)}")
            logger.info(f"📍 Next check will start from index: {self.state['last_index']}")

        except Exception as e:
            logger.critical(f"FATAL ERROR: {e}", exc_info=True)
        finally:
            if os.path.exists(LOCK_FILE):
                os.remove(LOCK_FILE)

if __name__ == "__main__":
    parser = MonsterParser()
    asyncio.run(parser.run())

import os
import re
import requests
import base64
import json
import threading
import time
import random
import subprocess
import signal
import sys
import gc
import socket
import uuid
import hashlib
from datetime import datetime, timedelta
from urllib.parse import quote, unquote
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- КОНФИГУРАЦИЯ СТРАН (PREMIUM MIRROR DESIGN) ---
# Формат оформления: ❤️ 🇧🇾 Belarus | BY 🇧🇾 ❤️
COUNTRIES = {
    "belarus": {"flag": "🇧🇾", "code": "BY", "name": "Belarus"},
    "kazakhstan": {"flag": "🇰🇿", "code": "KZ", "name": "Kazakhstan"},
    "germany": {"flag": "🇩🇪", "code": "DE", "name": "Germany"},
    "poland": {"flag": "🇵🇱", "code": "PL", "name": "Poland"},
    "usa": {"flag": "🇺🇸", "code": "US", "name": "USA"},
    "sweden": {"flag": "🇸🇪", "code": "SE", "name": "Sweden"},
    "netherlands": {"flag": "🇳🇱", "code": "NL", "name": "Netherlands"},
    "latvia_lithuania": {"flag": "🇱🇻", "code": "LV", "alt_code": "LT", "name": "Latvia/Lithuania"},
    "russia": {"flag": "🇷🇺", "code": "RU", "name": "Russia"},
    "singapore": {"flag": "🇸🇬", "code": "SG", "name": "Singapore"},
    "uk": {"flag": "🇬🇧", "code": "GB", "extra": "UK", "name": "United Kingdom"},
    "hongkong": {"flag": "🇭🇰", "code": "HK", "name": "Hong Kong"},
    "finland": {"flag": "🇫🇮", "code": "FI", "name": "Finland"},
    "france": {"flag": "🇫🇷", "code": "FR", "name": "France"}
}

# Строгий отбор протоколов для обхода блокировок (DPI)
ALLOWED_PROTOCOLS = ["vless://", "vmess://", "trojan://", "ss://", "hysteria2://", "tuic://"]

# Глобальное состояние системы
IP_CACHE = {} 
CACHE_LOCK = threading.Lock()
PROCESSED_COUNT = 0
SHOULD_EXIT = False 

# Экстремальные настройки производительности и защиты
BLACKLIST_BAIL_HOURS = 12    # Время бана за мертвый порт (увеличено для чистоты)
MAX_BLACKLIST_SIZE = 100000  # Максимальный размер черного списка
GEOIP_LIMIT_PER_RUN = 5000   # Лимит проверок через API за один цикл
THREAD_COUNT = 100           # Максимальное количество потоков для TCP Ping
GEOIP_PARALLEL_LEVEL = 20    # Уровень параллельности запросов к GeoIP API
PORT_TIMEOUT = 4.0           # Таймаут ожидания (увеличен для Hysteria2/Reality)
LOCK_FILE = "monster_engine.lock"
PROCESSED_SOURCES_FILE = "processed_sources.dat"

def signal_handler(sig, frame):
    """Корректный выход из программы при получении системных сигналов."""
    global SHOULD_EXIT
    print("\n[!] ВНИМАНИЕ: Получен сигнал остановки. Завершаем потоки и чистим Lock...", flush=True)
    SHOULD_EXIT = True
    if os.path.exists(LOCK_FILE):
        try: os.remove(LOCK_FILE)
        except: pass

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def get_random_ua():
    """Генерация случайного User-Agent для обхода защиты Cloudflare."""
    uas = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Mobile/15E148 Safari/604.1"
    ]
    return random.choice(uas)

def decode_base64(data):
    """Безопасное декодирование Base64 с коррекцией паддинга."""
    try:
        data = re.sub(r'[^a-zA-Z0-9+/=]', '', data)
        if not data: return ""
        missing_padding = len(data) % 4
        if missing_padding: data += '=' * (4 - missing_padding)
        return base64.b64decode(data).decode('utf-8', errors='ignore')
    except Exception: return ""

def encode_base64(data):
    """Кодирование в Base64 без переносов строк."""
    try:
        return base64.b64encode(data.encode('utf-8')).decode('utf-8')
    except Exception: return ""

def get_server_info(config):
    """Извлечение хоста и порта. Поддержка Vmess (JSON) и URI протоколов."""
    try:
        clean_config = config.split('#')[0]
        if clean_config.startswith("vmess://"):
            decoded = decode_base64(clean_config[8:])
            if decoded:
                v_data = json.loads(decoded)
                return str(v_data.get('add', '')).strip(), str(v_data.get('port', '')).strip()
        match = re.search(r'://(?:[^@]+@)?([^:/#\?]+):(\d+)', clean_config)
        if match:
            return match.group(1).strip(), match.group(2).strip()
    except Exception: pass
    return None, None

def is_node_alive(host, port, timeout=PORT_TIMEOUT):
    """TCP-проверка доступности порта (фильтр мертвых узлов)."""
    if not host or not port: return False
    # Игнорируем мусорные и локальные адреса
    if host.startswith(('127.', '192.168.', '10.', '172.16.', '0.')) or host == 'localhost':
        return False
    try:
        port_int = int(port)
        with socket.create_connection((host, port_int), timeout=timeout):
            return True
    except: return False

def beautify_config(config, country_key=None, fallback_code="UN"):
    """Премиальный стайлинг названий конфигураций."""
    try:
        if country_key and country_key in COUNTRIES:
            info = COUNTRIES[country_key]
            label = f"❤️ {info['flag']} {info['name']} | {info['code']} {info['flag']} ❤️"
        else:
            code = fallback_code if fallback_code else "UN"
            label = f"❤️ 🌍 Global | {code} 🌍 ❤️"
        
        if config.startswith("vmess://"):
            clean_config = config.split('#')[0]
            decoded = decode_base64(clean_config[8:])
            if decoded:
                data = json.loads(decoded)
                data['ps'] = label
                return "vmess://" + encode_base64(json.dumps(data))
        else:
            base_part = config.split('#')[0]
            return f"{base_part}#{quote(label)}"
    except Exception: return config

def pre_populate_ip_cache():
    """Анализ локальных файлов для минимизации запросов к GeoIP API."""
    print("🧠 Загрузка базы знаний (IP Cache)...", flush=True)
    files = [f"{c}.txt" for c in COUNTRIES] + ["mix.txt"]
    loaded_count = 0
    with CACHE_LOCK:
        IP_CACHE.clear()
    for f_name in files:
        if os.path.exists(f_name):
            country_code = None
            for c_key, c_info in COUNTRIES.items():
                if f"{c_key}.txt" == f_name:
                    country_code = c_info["code"]
                    break
            try:
                with open(f_name, 'r', encoding='utf-8') as f:
                    for line in f:
                        cfg = line.strip()
                        if cfg and not cfg.startswith('#'):
                            host, _ = get_server_info(cfg)
                            if host:
                                with CACHE_LOCK:
                                    if country_code and host not in IP_CACHE:
                                        IP_CACHE[host] = country_code
                                        loaded_count += 1
                                    elif f_name == "mix.txt" and host not in IP_CACHE:
                                        match = re.search(r'\|\s*([A-Z]{2})\s*', unquote(cfg))
                                        if match:
                                            IP_CACHE[host] = match.group(1)
                                            loaded_count += 1
            except Exception: pass
    print(f"✅ В память загружено {loaded_count} известных IP.")

# --- ТУРБО-ДВИЖОК GEOIP (10 ЗЕРКАЛЬНЫХ ПРОВАЙДЕРОВ) ---
def api_01(h): return requests.get(f"http://ip-api.com/json/{h}?fields=status,countryCode", timeout=3).json().get("countryCode")
def api_02(h): return requests.get(f"https://ipwho.is/{h}", timeout=3).json().get("country_code")
def api_03(h): 
    r = requests.get(f"https://ip2c.org/{h}", timeout=3)
    return r.text.split(';')[1] if "1;" in r.text else None
def api_04(h): return requests.get(f"https://freeipapi.com/api/json/{h}", timeout=3).json().get("countryCode")
def api_05(h): return requests.get(f"https://ipapi.co/{h}/json/", timeout=3, headers={'User-Agent': get_random_ua()}).json().get("country_code")
def api_06(h): return requests.get(f"https://ip-json.com/json/{h}", timeout=3).json().get("country_code")
def api_07(h): return requests.get(f"https://ipapi.is/json/{h}", timeout=3).json().get("location", {}).get("country_code")
def api_08(h): return requests.get(f"http://www.geoplugin.net/json.gp?ip={h}", timeout=3).json().get("geoplugin_countryCode")
def api_09(h): return requests.get(f"https://api.scamalytics.com/ip/{h}", timeout=3).json().get("country_code")
def api_10(h): return requests.get(f"https://extreme-ip-lookup.com/json/{h}?key=demo", timeout=3).json().get("countryCode")

def check_ip_location_smart(host):
    """Многопоточное определение страны с защитой от лимитов."""
    if SHOULD_EXIT: return None
    with CACHE_LOCK:
        if host in IP_CACHE: return IP_CACHE[host]
    
    time.sleep(random.uniform(0.1, 0.4))
    providers = [api_01, api_02, api_03, api_04, api_05, api_06, api_07, api_08, api_09, api_10]
    random.shuffle(providers)
    
    for provider in providers:
        if SHOULD_EXIT: break
        try:
            code = provider(host)
            if code and len(str(code)) == 2:
                code = str(code).upper()
                with CACHE_LOCK: IP_CACHE[host] = code
                return code
        except: continue
    
    with CACHE_LOCK: IP_CACHE[host] = "UN"
    return "UN"

def load_processed_sources():
    """Загрузка хешей уже обработанных ссылок."""
    if os.path.exists(PROCESSED_SOURCES_FILE):
        try:
            with open(PROCESSED_SOURCES_FILE, 'r') as f:
                return set([line.strip() for line in f if line.strip()])
        except: return set()
    return set()

def save_processed_source_hash(url):
    """Сохранение хеша источника."""
    h = hashlib.sha256(url.encode()).hexdigest()
    try:
        with open(PROCESSED_SOURCES_FILE, 'a') as f: f.write(h + "\n")
    except: pass

def load_blacklist():
    """Загрузка черного списка узлов."""
    bl = {}
    if os.path.exists('blacklist.txt'):
        try:
            with open('blacklist.txt', 'r', encoding='utf-8') as f:
                for line in f:
                    if '|' in line:
                        p = line.strip().split('|')
                        if len(p) >= 2: bl[p[0]] = datetime.fromisoformat(p[1])
        except: pass
    return bl

def save_blacklist(bl):
    """Сохранение и очистка черного списка."""
    now = datetime.now()
    active = {n: ts for n, ts in bl.items() if now - ts < timedelta(hours=BLACKLIST_BAIL_HOURS)}
    sorted_bl = sorted(active.items(), key=lambda x: x[1], reverse=True)[:MAX_BLACKLIST_SIZE]
    try:
        with open('blacklist.txt', 'w', encoding='utf-8') as f:
            for node, ts in sorted_bl: f.write(f"{node}|{ts.isoformat()}\n")
    except: pass

def generate_static_links():
    """Автогенерация LINKS_FOR_CLIENTS.txt на основе данных репозитория."""
    print("\n🔗 Генерация актуальных ссылок для клиентов...", flush=True)
    try:
        remote_url = subprocess.run(["git", "config", "--get", "remote.origin.url"], 
                                   capture_output=True, text=True).stdout.strip()
        
        if not remote_url:
            print("[!] URL репозитория не найден. Пропуск.")
            return

        raw_base = remote_url.replace("github.com", "raw.githubusercontent.com").replace(".git", "")
        if "raw.githubusercontent.com" in raw_base:
            raw_base += "/main/"
        
        links = []
        links.append(f"🚀 MONSTER VPN PRO SUBSCRIPTIONS 🚀\n")
        links.append(f"MIX (Text): {raw_base}mix.txt")
        links.append(f"MIX (Base64): {raw_base}sub_monster.txt\n")
        links.append("--- BY COUNTRIES ---")
        for c in COUNTRIES:
            links.append(f"{c.upper()}: {raw_base}{c}.txt")
        
        with open("LINKS_FOR_CLIENTS.txt", "w", encoding="utf-8") as f:
            f.write("\n".join(links))
        print("✅ Файл LINKS_FOR_CLIENTS.txt обновлен.")
    except Exception as e:
        print(f"[!] Ошибка генератора ссылок: {e}")

def save_and_organize(structured, final_mix_list, failed_list):
    """Сохранение финальных результатов в файлы."""
    for country in COUNTRIES:
        file_name = f"{country}.txt"
        configs = structured.get(country, [])
        valid = sorted(list(set(configs)))
        try:
            with open(file_name, 'w', encoding='utf-8') as f:
                if valid: f.write("\n".join(valid))
                else: f.write(f"# No active nodes for {country}\n")
        except Exception: pass

    valid_mix = sorted(list(set(final_mix_list)))
    try:
        with open("mix.txt", 'w', encoding='utf-8') as f:
            if valid_mix: f.write("\n".join(valid_mix))
            else: f.write("# No active nodes found\n")
        
        with open("sub_monster.txt", 'w', encoding='utf-8') as f:
            f.write(encode_base64("\n".join(valid_mix)) if valid_mix else "")
            
        valid_failed = sorted(list(set(failed_list)))
        with open("failed_nodes.txt", 'w', encoding='utf-8') as f:
            if valid_failed: f.write("\n".join(valid_failed))
            else: f.write("# No failed nodes detected\n")
                
        with open("sub_failed.txt", 'w', encoding='utf-8') as f:
            f.write(encode_base64("\n".join(valid_failed)) if valid_failed else "")
    except Exception: pass

def git_commit_push():
    """Синхронизация с GitHub."""
    print("\n[Git] Пуш в репозиторий...", flush=True)
    try:
        subprocess.run(["git", "config", "--local", "user.name", "VPN-Monster-Bot"], check=True)
        subprocess.run(["git", "config", "--local", "user.email", "bot@vpn-monster.com"], check=True)
        subprocess.run(["git", "add", "."], check=True)
        
        status = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True).stdout.strip()
        if not status:
            print("[Git] Изменений нет. Отмена пуша.")
            return

        timestamp = datetime.now().strftime('%d/%m %H:%M')
        subprocess.run(["git", "commit", "-m", f"🚀 Monster Sync {timestamp}"], check=True)
        
        res = subprocess.run(["git", "push", "origin", "main"], capture_output=True, text=True)
        if res.returncode != 0:
            subprocess.run(["git", "push", "origin", "main", "--force"], check=True)
        print("[Git] Успешно синхронизировано.")
    except Exception as e: print(f"[Git] Ошибка: {e}")

# --- WORKERS ---

def check_worker(config, blacklist, lock, seen):
    """Потоковый воркер проверки узла."""
    h, p = get_server_info(config)
    if not h or not p: return None
    nid = f"{h}:{p}"
    if nid in blacklist: return None
    with lock:
        if nid in seen: return None
        seen.add(nid)
    if is_node_alive(h, p): return config
    else: return ("FAIL", nid, config)

def geoip_parallel_worker(cfg):
    """Потоковый воркер определения страны."""
    host, _ = get_server_info(cfg)
    code = check_ip_location_smart(host)
    return (cfg, code)

# --- ГЛАВНЫЙ ДВИЖОК ---

def process_monster_engine():
    """Основной цикл работы VPN Monster Engine."""
    if os.path.exists(LOCK_FILE):
        print(f"[!] КРИТ: Процесс уже запущен (Lock найден).")
        return
    
    with open(LOCK_FILE, 'w') as f: f.write(str(os.getpid()))
    
    try:
        start_time = datetime.now()
        print(f"\n{'='*60}\n🚀 VPN MONSTER ENGINE PRO START: {start_time.strftime('%H:%M:%S')}\n{'='*60}", flush=True)
        
        pre_populate_ip_cache()
        processed_hashes = load_processed_sources()
        
        # Сбор источников
        new_sources = []
        if os.path.exists('all_sources.txt'):
            with open('all_sources.txt', 'r', encoding='utf-8') as f:
                all_raw = list(set([l.strip() for l in f if l.strip() and l.startswith('http')]))
                for url in all_raw:
                    h = hashlib.sha256(url.encode()).hexdigest()
                    if h not in processed_hashes:
                        new_sources.append(url)
        
        blacklist = load_blacklist()
        raw_configs = []
        
        # Добавляем уже имеющиеся узлы для перепроверки
        for c in COUNTRIES:
            fn = f"{c}.txt"
            if os.path.exists(fn):
                with open(fn, 'r', encoding='utf-8') as f:
                    raw_configs.extend([l.strip() for l in f if l.strip() and not l.startswith('#')])

        # Сбор новых конфигов
        if new_sources:
            print(f"📡 Опрос {len(new_sources)} новых источников...", flush=True)
            for url in new_sources:
                try:
                    if any(x in url for x in ["sub_monster.txt", "mix.txt", "failed_nodes.txt"]): continue
                    r = requests.get(url, timeout=15, headers={'User-Agent': get_random_ua()})
                    text = r.text
                    if not any(p in text for p in ALLOWED_PROTOCOLS):
                        decoded = decode_base64(text)
                        if decoded: text = decoded
                    
                    pattern = r'(?:' + '|'.join(ALLOWED_PROTOCOLS).replace('://', '') + r')://[^\s#"\'<>,]+'
                    found = re.findall(pattern, text)
                    raw_configs.extend(found)
                    save_processed_source_hash(url)
                except: continue

        total_configs = list(set(raw_configs))
        print(f"🔍 Всего уникальных ссылок на входе: {len(total_configs)}")

        valid_nodes = []
        failed_nodes = []
        global_seen = set()
        seen_lock = threading.Lock()
        
        # Фаза проверки портов
        if total_configs:
            print(f"⚡ TCP Ping ({THREAD_COUNT} потоков)...", flush=True)
            with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
                futures = [executor.submit(check_worker, c, blacklist, seen_lock, global_seen) for c in total_configs]
                for future in as_completed(futures):
                    if SHOULD_EXIT: break
                    try:
                        res = future.result()
                        if res:
                            if isinstance(res, tuple): 
                                blacklist[res[1]] = datetime.now()
                                failed_nodes.append(res[2])
                            else: valid_nodes.append(res)
                    except: continue

        print(f"✅ Живых узлов найдено: {len(valid_nodes)}")

        # Фаза Геолокации
        structured_data = {c: [] for c in COUNTRIES}
        final_mix = []
        
        if valid_nodes:
            print(f"🌍 GeoIP Classification ({GEOIP_PARALLEL_LEVEL} потоков)...", flush=True)
            random.shuffle(valid_nodes)
            queue = valid_nodes[:GEOIP_LIMIT_PER_RUN]
            
            with ThreadPoolExecutor(max_workers=GEOIP_PARALLEL_LEVEL) as geo_executor:
                geo_futures = [geo_executor.submit(geoip_parallel_worker, cfg) for cfg in queue]
                for i, f in enumerate(as_completed(geo_futures)):
                    if SHOULD_EXIT: break
                    try:
                        cfg, code = f.result()
                        matched = False
                        if code and code != "UN":
                            for c_name, c_info in COUNTRIES.items():
                                if code in [c_info["code"], c_info.get("alt_code"), c_info.get("extra")]:
                                    b_cfg = beautify_config(cfg, c_name)
                                    structured_data[c_name].append(b_cfg)
                                    final_mix.append(b_cfg)
                                    matched = True
                                    break
                        if not matched:
                            final_mix.append(beautify_config(cfg, None, fallback_code=code))
                        
                        if i > 0 and i % 200 == 0: print(f"   > Сортировка: {i}/{len(queue)}...")
                    except: continue
        
        # Финализация
        save_and_organize(structured_data, final_mix, failed_nodes)
        save_blacklist(blacklist)
        generate_static_links()
        
        # Чистка
        global_seen.clear()
        gc.collect()
        
        git_commit_push()
        print(f"\n🏁 ЦИКЛ ЗАВЕРШЕН ЗА {datetime.now() - start_time}.")
        
    finally:
        if os.path.exists(LOCK_FILE):
            try: os.remove(LOCK_FILE)
            except: pass

if __name__ == "__main__":
    try:
        socket.setdefaulttimeout(PORT_TIMEOUT)
        process_monster_engine()
    except Exception as e:
        print(f"\n[FATAL ERROR]: {e}")
        if os.path.exists(LOCK_FILE):
            try: os.remove(LOCK_FILE)
            except: pass
        sys.exit(1)

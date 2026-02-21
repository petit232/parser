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
from datetime import datetime, timedelta
from urllib.parse import urlparse, parse_qs, quote, unquote
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- КОНФИГУРАЦИЯ СТРАН (ФОРМАТ: 🏁 Страна 🏁) ---
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

# Поддерживаемые протоколы
PROTOCOLS = ["vless://", "vmess://", "trojan://", "ss://", "hysteria2://", "tuic://"]

# Глобальное состояние
IP_CACHE = {} 
CACHE_LOCK = threading.Lock()
PROCESSED_COUNT = 0
SHOULD_EXIT = False 

# Настройки производительности и лимитов (Максимально агрессивные)
MAX_AGE_HOURS = 48          # Время жизни конфига (удаление старья)
BLACKLIST_BAIL_HOURS = 24   # Время бана за мертвый порт
MAX_BLACKLIST_SIZE = 30000  # Максимальное количество забаненных IP
GEOIP_LIMIT_PER_RUN = 2000  # Лимит новых GeoIP проверок за цикл
THREAD_COUNT = 75           # Количество потоков для проверки портов (ультра-скорость)
GEOIP_PARALLEL_LEVEL = 5    # Сколько API опрашивать ОДНОВРЕМЕННО для одного IP

def signal_handler(sig, frame):
    """Корректный выход при прерывании."""
    global SHOULD_EXIT
    print("\n[!] Остановка процесса пользователем...", flush=True)
    SHOULD_EXIT = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def get_random_ua():
    """Случайный User-Agent для мимикрии под браузер."""
    uas = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:123.0) Gecko/20100101 Firefox/123.0"
    ]
    return random.choice(uas)

def decode_base64(data):
    """Безопасное декодирование Base64 с фиксом паддинга."""
    try:
        data = re.sub(r'[^a-zA-Z0-9+/=]', '', data)
        if not data: return ""
        missing_padding = len(data) % 4
        if missing_padding: data += '=' * (4 - missing_padding)
        return base64.b64decode(data).decode('utf-8', errors='ignore')
    except Exception: return ""

def encode_base64(data):
    """Кодирование строки в чистый Base64."""
    try:
        return base64.b64encode(data.encode('utf-8')).decode('utf-8')
    except Exception: return ""

def get_server_info(config):
    """Извлечение IP/Хоста и Порта из ссылки любого типа."""
    try:
        if config.startswith("vmess://"):
            decoded = decode_base64(config[8:])
            if decoded:
                v_data = json.loads(decoded)
                return str(v_data.get('add', '')).strip(), str(v_data.get('port', '')).strip()
        match = re.search(r'://(?:[^@]+@)?([^:/#\?]+):(\d+)', config)
        if match:
            return match.group(1).strip(), match.group(2).strip()
    except Exception: pass
    return None, None

def is_node_alive(host, port, timeout=1.2):
    """Мгновенная проверка доступности TCP порта."""
    if not host or not port: return False
    # Игнорируем локальные адреса
    if host.startswith(('127.', '192.168.', '10.', '172.16.')) or host == 'localhost':
        return False
    try:
        port_int = int(port)
        with socket.create_connection((host, port_int), timeout=timeout):
            return True
    except: return False

# --- МОДУЛЬ ДИЗАЙНА (ФЛАГИ ПО БОКАМ) ---

def beautify_config(config, country_key):
    """Применяет визуальное оформление: 🏁 Название Страны 🏁."""
    try:
        info = COUNTRIES.get(country_key)
        if not info: return config
        label = f"{info['flag']} {info['name']} {info['flag']}"
        
        if config.startswith("vmess://"):
            decoded = decode_base64(config[8:])
            if decoded:
                data = json.loads(decoded)
                data['ps'] = label
                return "vmess://" + encode_base64(json.dumps(data))
        elif "#" in config:
            base, _ = config.split("#", 1)
            return f"{base}#{quote(label)}"
        else:
            return f"{config}#{quote(label)}"
    except Exception: return config

# --- ТУРБО-ДВИЖОК GEOIP (10 ИСТОЧНИКОВ API) ---

def api_01(host):
    try:
        r = requests.get(f"http://ip-api.com/json/{host}?fields=status,countryCode", timeout=2)
        if r.status_code == 200:
            d = r.json()
            if d.get("status") == "success": return d.get("countryCode")
    except: pass
    return None

def api_02(host):
    try:
        r = requests.get(f"https://ipwho.is/{host}", timeout=2)
        if r.status_code == 200:
            d = r.json()
            if d.get("success"): return d.get("country_code")
    except: pass
    return None

def api_03(host):
    try:
        r = requests.get(f"https://ip2c.org/{host}", timeout=2)
        if r.status_code == 200 and r.text.startswith("1;"):
            p = r.text.split(';')
            if len(p) > 1: return p[1]
    except: pass
    return None

def api_04(host):
    try:
        r = requests.get(f"https://freeipapi.com/api/json/{host}", timeout=2)
        if r.status_code == 200: return r.json().get("countryCode")
    except: pass
    return None

def api_05(host):
    try:
        r = requests.get(f"https://ipapi.co/{host}/json/", timeout=2, headers={'User-Agent': get_random_ua()})
        if r.status_code == 200: return r.json().get("country_code")
    except: pass
    return None

def api_06(host):
    try:
        r = requests.get(f"https://ip-json.com/json/{host}", timeout=2)
        if r.status_code == 200: return r.json().get("country_code")
    except: pass
    return None

def api_07(host):
    try:
        r = requests.get(f"https://ipapi.is/json/{host}", timeout=2)
        if r.status_code == 200: return r.json().get("location", {}).get("country_code")
    except: pass
    return None

def api_08(host):
    try:
        r = requests.get(f"http://www.geoplugin.net/json.gp?ip={host}", timeout=2)
        if r.status_code == 200: return r.json().get("geoplugin_countryCode")
    except: pass
    return None

def api_09(host):
    try:
        r = requests.get(f"https://api.scamalytics.com/ip/{host}", timeout=2)
        if r.status_code == 200: return r.json().get("country_code")
    except: pass
    return None

def api_10(host):
    try:
        r = requests.get(f"https://extreme-ip-lookup.com/json/{host}?key=demo", timeout=2)
        if r.status_code == 200: return r.json().get("countryCode")
    except: pass
    return None

def check_ip_location_smart(host):
    """Умное ПАРАЛЛЕЛЬНОЕ определение страны. Опрашивает 5 API одновременно."""
    global PROCESSED_COUNT
    if SHOULD_EXIT: return None
    
    with CACHE_LOCK:
        if host in IP_CACHE: return IP_CACHE[host]

    providers = [api_01, api_02, api_03, api_04, api_05, api_06, api_07, api_08, api_09, api_10]
    random.shuffle(providers)

    # Параллельный опрос пачки API
    with ThreadPoolExecutor(max_workers=GEOIP_PARALLEL_LEVEL) as api_executor:
        future_to_api = {api_executor.submit(p, host): p for p in providers[:GEOIP_PARALLEL_LEVEL]}
        for future in as_completed(future_to_api):
            if SHOULD_EXIT: break
            code = future.result()
            if code and len(str(code)) == 2:
                code = str(code).upper()
                with CACHE_LOCK:
                    IP_CACHE[host] = code
                    PROCESSED_COUNT += 1
                return code

    # Фолбэк на оставшиеся API если первая пачка не дала результат
    for provider in providers[GEOIP_PARALLEL_LEVEL:]:
        if SHOULD_EXIT: break
        code = provider(host)
        if code and len(str(code)) == 2:
            code = str(code).upper()
            with CACHE_LOCK:
                IP_CACHE[host] = code
                PROCESSED_COUNT += 1
            return code

    with CACHE_LOCK: IP_CACHE[host] = None
    return None

# --- СИСТЕМА СОХРАНЕНИЯ И ЧИСТКИ ---

def load_blacklist():
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
    now = datetime.now()
    active = {n: ts for n, ts in bl.items() if now - ts < timedelta(hours=BLACKLIST_BAIL_HOURS)}
    sorted_bl = sorted(active.items(), key=lambda x: x[1], reverse=True)[:MAX_BLACKLIST_SIZE]
    try:
        with open('blacklist.txt', 'w', encoding='utf-8') as f:
            for node, ts in sorted_bl: f.write(f"{node}|{ts.isoformat()}\n")
    except: pass

def load_current_database():
    """Загрузка всей текущей базы узлов для предотвращения дублей."""
    db, nodes = {}, set()
    files = [f"{c}.txt" for c in COUNTRIES] + ["mix.txt"]
    now = datetime.now()
    for f_name in files:
        if os.path.exists(f_name):
            try:
                with open(f_name, 'r', encoding='utf-8') as f:
                    content = f.read()
                dm = re.search(r'# Updated: ([\d\-\s:]+)', content)
                f_dt = datetime.strptime(dm.group(1), "%Y-%m-%d %H:%M:%S") if dm else now
                for line in content.splitlines():
                    cfg = line.strip()
                    if cfg and not cfg.startswith('#'):
                        db[cfg] = f_dt
                        h, p = get_server_info(cfg)
                        if h and p: nodes.add(f"{h}:{p}")
            except: pass
    return db, nodes

def save_and_organize(structured, failed_list):
    """Сборка всех файлов, микса и подписки ошибок."""
    now = datetime.now()
    threshold = now - timedelta(hours=MAX_AGE_HOURS)
    all_configs_to_mix = []

    for country, configs in structured.items():
        file_name = f"{country}.txt"
        current_data = {}
        # Загружаем старое из файла для слияния
        if os.path.exists(file_name):
            try:
                with open(file_name, 'r', encoding='utf-8') as f:
                    for line in f:
                        c = line.strip()
                        if c and not c.startswith('#'): current_data[c] = now
            except: pass
        
        # Добавляем новые с оформлением
        for nc in configs:
            current_data[beautify_config(nc, country)] = now
            
        # Фильтруем по времени
        valid = [c for c, ts in current_data.items() if ts > threshold]
        all_configs_to_mix.extend(valid)
        
        try:
            with open(file_name, 'w', encoding='utf-8') as f:
                if valid: f.write("\n".join(sorted(list(set(valid)))))
                f.write(f"\n\n# Total: {len(valid)}\n# Updated: {now.strftime('%Y-%m-%d %H:%M:%S')}")
        except: pass

    # Создание MIX
    unique_mix = sorted(list(set(all_configs_to_mix)))
    try:
        with open("mix.txt", 'w', encoding='utf-8') as f:
            if unique_mix: f.write("\n".join(unique_mix))
            f.write(f"\n\n# Total: {len(unique_mix)}\n# Updated: {now.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Основная подписка Monster
        with open("sub_monster.txt", 'w', encoding='utf-8') as f:
            f.write(encode_base64("\n".join(unique_mix)))
            
        # Подписка на ошибки (FAILED NODES)
        with open("failed_nodes.txt", 'w', encoding='utf-8') as f:
            if failed_list: f.write("\n".join(list(set(failed_list))))
            f.write(f"\n\n# Failed Nodes Count: {len(failed_list)}\n# Log: {now.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Подписка на ошибки в Base64
        with open("sub_failed.txt", 'w', encoding='utf-8') as f:
            f.write(encode_base64("\n".join(list(set(failed_list)))))
    except: pass

def git_commit_push():
    """Автоматическая отправка в GitHub."""
    print("\n[Git] Отправка обновлений в репозиторий...", flush=True)
    try:
        subprocess.run(["git", "config", "--local", "user.name", "VPN-Monster-Bot"], check=True)
        subprocess.run(["git", "config", "--local", "user.email", "bot@vpn-monster.com"], check=True)
        subprocess.run(["git", "add", "*.txt"], check=True)
        if subprocess.run(["git", "diff", "--cached", "--quiet"]).returncode == 0:
            print("[Git] Нет изменений для коммита.")
            return
        msg = f"Ultra-Update {datetime.now().strftime('%d/%m %H:%M')} | Speed Mode"
        subprocess.run(["git", "commit", "-m", msg], check=True)
        subprocess.run(["git", "push", "origin", "main"], check=True)
    except Exception as e:
        print(f"[Git] Ошибка при пуше: {e}")

# --- ФУНКЦИИ ВОРКЕРЫ (ПОТОКИ) ---

def check_worker(config, blacklist, db_now, known_nodes, lock, seen):
    """Задача для проверки одного узла."""
    h, p = get_server_info(config)
    if not h or not p: return None
    nid = f"{h}:{p}"
    
    # Отсеиваем дубли и забаненных сразу
    if config in db_now or nid in known_nodes or nid in blacklist: return None
    
    with lock:
        if nid in seen: return None
        seen.add(nid)
        
    if is_node_alive(h, p):
        return config
    else:
        return ("FAIL", nid, config)

def process_monster_engine():
    """Главный двигатель парсера."""
    start_time = datetime.now()
    print(f"--- MONSTER ENGINE ULTIMATE START: {start_time.strftime('%H:%M:%S')} ---", flush=True)
    
    if not os.path.exists('all_sources.txt'):
        print("[!] Файл all_sources.txt не найден!")
        return

    with open('all_sources.txt', 'r', encoding='utf-8') as f:
        sources = list(set([l.strip() for l in f if l.strip()]))

    blacklist = load_blacklist()
    db_now, known_nodes = load_current_database()
    raw_configs = []
    
    # 1. Сбор ссылок со всех источников
    print(f"Сбор данных из {len(sources)} источников...", flush=True)
    for url in sources:
        if SHOULD_EXIT: break
        try:
            # Не парсим свои же файлы
            if any(x in url for x in ["sub_monster.txt", "mix.txt", "failed_nodes.txt", "sub_failed.txt"]):
                continue
            r = requests.get(url, timeout=12, headers={'User-Agent': get_random_ua()})
            text = r.text
            # Авто-декодирование если источник в base64
            if not any(p in text for p in PROTOCOLS):
                decoded = decode_base64(text)
                if decoded: text = decoded
            
            # Извлекаем все подходящие ссылки
            found = re.findall(r'(?:vless|vmess|trojan|ss|hysteria2|tuic)://[^\s#"\'<>,]+', text)
            raw_configs.extend(found)
            gc.collect()
        except:
            print(f"[-] Пропуск битого источника: {url}")

    # 2. Массовая проверка портов (Multi-threading)
    valid_new = []
    failed_new = []
    global_seen = set()
    seen_lock = threading.Lock()
    
    print(f"Проверка {len(raw_configs)} узлов на порт в {THREAD_COUNT} потоков...", flush=True)
    with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
        futures = [executor.submit(check_worker, c, blacklist, db_now, known_nodes, seen_lock, global_seen) for c in raw_configs]
        for future in as_completed(futures):
            if SHOULD_EXIT: break
            res = future.result()
            if res:
                if isinstance(res, tuple): # Это фейл
                    blacklist[res[1]] = datetime.now()
                    failed_new.append(res[2])
                else:
                    valid_new.append(res)

    # 3. GeoIP Распределение (Turbo)
    random.shuffle(valid_new)
    # Берем пачку самых свежих
    queue = valid_new[:GEOIP_LIMIT_PER_RUN]
    structured_data = {c: [] for c in COUNTRIES}
    
    print(f"Параллельный GeoIP (x{GEOIP_PARALLEL_LEVEL}) для {len(queue)} узлов...", flush=True)
    for cfg in queue:
        if SHOULD_EXIT: break
        host, _ = get_server_info(cfg)
        code = check_ip_location_smart(host)
        if code:
            matched = False
            for c_name, c_info in COUNTRIES.items():
                if code in [c_info["code"], c_info.get("alt_code"), c_info.get("extra")]:
                    structured_data[c_name].append(cfg)
                    matched = True
                    break
            if not matched: failed_new.append(cfg) # Страна не в нашем списке
        else:
            failed_new.append(cfg) # Вообще не определился
            
    # 4. Сохранение результатов и пуш
    save_and_organize(structured_data, failed_new)
    save_blacklist(blacklist)
    git_commit_push()
    
    end_time = datetime.now()
    print(f"--- ЦИКЛ ЗАВЕРШЕН ЗА {end_time - start_time} ---", flush=True)

if __name__ == "__main__":
    try:
        process_monster_engine()
    except Exception as fatal_error:
        print(f"\n[КРИТИЧЕСКАЯ ОШИБКА]: {fatal_error}")
        sys.exit(1)

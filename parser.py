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
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- КОНФИГУРАЦИЯ СТРАН ---
# Карта стран: Код API -> Название файла.
COUNTRIES = {
    "belarus": {"flag": "🇧🇾", "code": "BY"},
    "kazakhstan": {"flag": "🇰🇿", "code": "KZ"},
    "germany": {"flag": "🇩🇪", "code": "DE"},
    "poland": {"flag": "🇵🇱", "code": "PL"},
    "usa": {"flag": "🇺🇸", "code": "US"},
    "sweden": {"flag": "🇸🇪", "code": "SE"},
    "netherlands": {"flag": "🇳🇱", "code": "NL"},
    "latvia_lithuania": {"flag": "🇱🇻", "code": "LV", "alt_code": "LT"},
    "russia": {"flag": "🇷🇺", "code": "RU"},
    "singapore": {"flag": "🇸🇬", "code": "SG"},
    "uk": {"flag": "🇬🇧", "code": "GB", "extra": "UK"},
    "hongkong": {"flag": "🇭🇰", "code": "HK"},
    "finland": {"flag": "🇫🇮", "code": "FI"},
    "france": {"flag": "🇫🇷", "code": "FR"}
}

# Поддерживаемые протоколы
PROTOCOLS = ["vless://", "vmess://", "trojan://", "ss://", "hysteria2://", "tuic://"]

# Глобальное состояние и синхронизация
IP_CACHE = {} 
CACHE_LOCK = threading.Lock()
PROCESSED_COUNT = 0
SHOULD_EXIT = False 

# Настройки времени и лимитов
MAX_AGE_HOURS = 48          # Срок хранения живого конфига
BLACKLIST_BAIL_HOURS = 24   # Время блокировки мертвого узла
MAX_BLACKLIST_SIZE = 3000   # Лимит записей в черном списке
GEOIP_LIMIT_PER_RUN = 450   # Лимит новых GeoIP проверок за запуск
THREAD_COUNT = 25           # Оптимальное количество потоков

def signal_handler(sig, frame):
    """Корректное прерывание работы скрипта."""
    global SHOULD_EXIT
    print("\n[!] Сигнал остановки. Завершаем текущие операции...", flush=True)
    SHOULD_EXIT = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def get_random_ua():
    """Генерация случайного User-Agent для обхода защиты API."""
    uas = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Mobile/15E148 Safari/604.1"
    ]
    return random.choice(uas)

def decode_base64(data):
    """Безопасное декодирование Base64."""
    try:
        data = re.sub(r'[^a-zA-Z0-9+/=]', '', data)
        if not data: return ""
        missing_padding = len(data) % 4
        if missing_padding: data += '=' * (4 - missing_padding)
        return base64.b64decode(data).decode('utf-8', errors='ignore')
    except Exception: return ""

def encode_base64(data):
    """Кодирование строки в Base64."""
    try:
        return base64.b64encode(data.encode('utf-8')).decode('utf-8')
    except Exception: return ""

def get_server_info(config):
    """Извлечение хоста и порта из конфига."""
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

def is_node_alive(host, port, timeout=4):
    """Проверка доступности TCP порта."""
    if not host or not port: return False
    if host.startswith(('127.', '192.168.', '10.', '172.16.')) or host == 'localhost':
        return False
    try:
        port_int = int(port)
        with socket.create_connection((host, port_int), timeout=timeout):
            return True
    except (socket.timeout, socket.error, ValueError):
        return False

def load_blacklist():
    """Загрузка черного списка."""
    blacklist = {}
    if os.path.exists('blacklist.txt'):
        try:
            with open('blacklist.txt', 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if '|' in line:
                        parts = line.split('|')
                        if len(parts) >= 2:
                            node, timestamp = parts[0], parts[1]
                            blacklist[node] = datetime.fromisoformat(timestamp)
        except Exception: pass
    return blacklist

def save_blacklist(blacklist):
    """Сохранение черного списка."""
    now = datetime.now()
    active = {n: ts for n, ts in blacklist.items() if now - ts < timedelta(hours=BLACKLIST_BAIL_HOURS)}
    sorted_items = sorted(active.items(), key=lambda x: x[1], reverse=True)[:MAX_BLACKLIST_SIZE]
    try:
        with open('blacklist.txt', 'w', encoding='utf-8') as f:
            for node, ts in sorted_items:
                f.write(f"{node}|{ts.isoformat()}\n")
    except Exception: pass

# --- ПРОВАЙДЕРЫ GEOIP API ---

def api_ip_api_com(host):
    try:
        r = requests.get(f"http://ip-api.com/json/{host}?fields=status,countryCode", timeout=7, headers={'User-Agent': get_random_ua()})
        if r.status_code == 200:
            d = r.json()
            if d.get("status") == "success": return d.get("countryCode")
    except: pass
    return None

def api_ipwhois_io(host):
    try:
        r = requests.get(f"https://ipwho.is/{host}", timeout=7, headers={'User-Agent': get_random_ua()})
        if r.status_code == 200:
            d = r.json()
            if d.get("success"): return d.get("country_code")
    except: pass
    return None

def api_ip2c_org(host):
    try:
        r = requests.get(f"https://ip2c.org/{host}", timeout=7)
        if r.status_code == 200 and r.text.startswith("1;"):
            p = r.text.split(';')
            if len(p) > 1: return p[1]
    except: pass
    return None

def api_freeipapi_com(host):
    try:
        r = requests.get(f"https://freeipapi.com/api/json/{host}", timeout=7, headers={'User-Agent': get_random_ua()})
        if r.status_code == 200: return r.json().get("countryCode")
    except: pass
    return None

def api_ipapi_co(host):
    try:
        r = requests.get(f"https://ipapi.co/{host}/json/", timeout=7, headers={'User-Agent': get_random_ua()})
        if r.status_code == 200: return r.json().get("country_code")
    except: pass
    return None

def api_ip_json_com(host):
    try:
        r = requests.get(f"https://ip-json.com/json/{host}", timeout=7, headers={'User-Agent': get_random_ua()})
        if r.status_code == 200: return r.json().get("country_code")
    except: pass
    return None

def api_ipapi_is(host):
    try:
        r = requests.get(f"https://ipapi.is/json/{host}", timeout=7, headers={'User-Agent': get_random_ua()})
        if r.status_code == 200: return r.json().get("location", {}).get("country_code")
    except: pass
    return None

def check_ip_location_smart(host):
    """Распределенная проверка GeoIP с автоматическим переключением."""
    global PROCESSED_COUNT
    if SHOULD_EXIT: return None

    with CACHE_LOCK:
        if host in IP_CACHE: return IP_CACHE[host]

    providers = [
        api_ip_api_com, api_ipwhois_io, api_ip2c_org, 
        api_freeipapi_com, api_ipapi_co, api_ip_json_com, api_ipapi_is
    ]
    
    # Ротация: каждый хост пробует API в случайном порядке
    random.shuffle(providers)

    for provider in providers:
        if SHOULD_EXIT: break
        code = provider(host)
        if code and len(str(code)) == 2:
            code = str(code).upper()
            with CACHE_LOCK:
                IP_CACHE[host] = code
                PROCESSED_COUNT += 1
                if PROCESSED_COUNT % 10 == 0:
                    print(f"  [GeoIP] Найдено: {PROCESSED_COUNT} новых локаций.", flush=True)
            return code
        # Если API не ответило, ждем немного перед следующим
        time.sleep(random.uniform(1.0, 2.0))

    with CACHE_LOCK:
        IP_CACHE[host] = None
    return None

def load_current_database():
    """Загрузка существующей базы для исключения дублей."""
    db = {} 
    known_nodes = set() 
    files = [f"{c}.txt" for c in COUNTRIES] + ["mix.txt"]
    now = datetime.now()
    
    for f_name in files:
        if os.path.exists(f_name):
            try:
                with open(f_name, 'r', encoding='utf-8') as f:
                    content = f.read()
                date_match = re.search(r'# Updated: ([\d\-\s:]+)', content)
                file_dt = now
                if date_match:
                    try: file_dt = datetime.strptime(date_match.group(1), "%Y-%m-%d %H:%M:%S")
                    except: pass
                for line in content.splitlines():
                    cfg = line.strip()
                    if cfg and not cfg.startswith('#'):
                        db[cfg] = file_dt
                        h, p = get_server_info(cfg)
                        if h and p:
                            known_nodes.add(f"{h}:{p}")
            except Exception: pass
    return db, known_nodes

def save_and_cleanup(structured_data):
    """Сохранение результатов и очистка старых данных."""
    now = datetime.now()
    threshold = now - timedelta(hours=MAX_AGE_HOURS)
    all_valid_configs = []

    for country, info in COUNTRIES.items():
        file_name = f"{country}.txt"
        current_file_configs = load_existing_configs_internal(file_name)
        
        # Добавляем новые проверенные конфиги
        for nc in structured_data.get(country, []):
            current_file_configs[nc] = now
            
        # Удаляем просроченные
        valid_list = [c for c, ts in current_file_configs.items() if ts > threshold]
        all_valid_configs.extend(valid_list)
        
        try:
            time.sleep(0.1)
            with open(file_name, 'w', encoding='utf-8') as f:
                if valid_list:
                    f.write("\n".join(sorted(list(set(valid_list)))))
                f.write(f"\n\n# Total: {len(valid_list)}\n# Updated: {now.strftime('%Y-%m-%d %H:%M:%S')}")
        except Exception: pass

    # Обновление общего файла mix.txt
    unique_mix = list(set(all_valid_configs))
    try:
        with open("mix.txt", 'w', encoding='utf-8') as f:
            if unique_mix:
                f.write("\n".join(sorted(unique_mix)))
            f.write(f"\n\n# Total: {len(unique_mix)}\n# Updated: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception: pass

    # Создание подписки Base64
    sub_payload = "\n".join(unique_mix)
    encoded_payload = encode_base64(sub_payload)
    try:
        with open("sub_monster.txt", 'w', encoding='utf-8') as f:
            f.write(encoded_payload)
        print(f"[Monster] Файл sub_monster.txt обновлен. Всего узлов: {len(unique_mix)}", flush=True)
    except Exception: pass

def load_existing_configs_internal(file_name):
    """Чтение существующих конфигов из файла."""
    res = {}
    if os.path.exists(file_name):
        try:
            with open(file_name, 'r', encoding='utf-8') as f:
                content = f.read()
            date_match = re.search(r'# Updated: ([\d\-\s:]+)', content)
            file_date = datetime.now()
            if date_match:
                try: file_date = datetime.strptime(date_match.group(1), "%Y-%m-%d %H:%M:%S")
                except: pass
            for line in content.splitlines():
                cfg = line.strip()
                if cfg and not cfg.startswith('#'):
                    res[cfg] = file_date
        except Exception: pass
    return res

def git_commit_and_push():
    """Автоматический пуш в репозиторий."""
    print("\n[Git] Начало синхронизации...", flush=True)
    try:
        subprocess.run(["git", "config", "--local", "user.name", "VPN-Monster-Bot"], check=True)
        subprocess.run(["git", "config", "--local", "user.email", "bot@vpn-monster.com"], check=True)
        subprocess.run(["git", "add", "*.txt"], check=True)
        
        status = subprocess.run(["git", "diff", "--cached", "--quiet"])
        if status.returncode == 0:
            print("[Git] Нет новых данных для коммита.", flush=True)
            return

        msg = f"Update {datetime.now().strftime('%d.%m %H:%M')} | Verified & GeoFiltered"
        subprocess.run(["git", "commit", "-m", msg], check=True)
        subprocess.run(["git", "pull", "--rebase", "origin", "main"], check=False)
        subprocess.run(["git", "push", "origin", "main"], check=True)
        print("[Git] Изменения успешно опубликованы.", flush=True)
    except Exception as e:
        print(f"[Git] Ошибка синхронизации: {e}", flush=True)

def thread_check_worker(config, blacklist, db_knowledge, known_nodes, seen_lock, global_seen):
    """Логика проверки отдельного узла."""
    host, port = get_server_info(config)
    if not host or not port: return None
    
    node_id = f"{host}:{port}"
    
    # 1. Проверка на дубликат (текстовый и сетевой)
    if config in db_knowledge: return None
    if node_id in known_nodes: return None
    
    # 2. Проверка черного списка
    if node_id in blacklist: return None
    
    with seen_lock:
        if node_id in global_seen: return None
        global_seen.add(node_id)

    # 3. Проверка порта
    if is_node_alive(host, port):
        return config
    else:
        # Узел мертв — вносим в черный список текущей сессии
        return ("FAIL", node_id)

def process():
    """Основной цикл парсинга."""
    start_run = datetime.now()
    print(f"--- ЗАПУСК ПАРСЕРА MONSTER (MULTI-GEO MODE): {start_run.strftime('%H:%M:%S')} ---", flush=True)
    
    if not os.path.exists('all_sources.txt'):
        print("[!] Ошибка: Файл all_sources.txt отсутствует!", flush=True)
        return

    with open('all_sources.txt', 'r', encoding='utf-8') as f:
        sources = list(set([l.strip() for l in f if l.strip()]))

    blacklist = load_blacklist()
    db_knowledge, known_nodes = load_current_database()
    
    raw_configs = []
    print(f"Этап 1: Сбор данных из {len(sources)} источников...", flush=True)
    
    for url in sources:
        if SHOULD_EXIT: break
        try:
            if "sub_monster.txt" in url or "mix.txt" in url: continue
            
            r = requests.get(url, timeout=25, headers={'User-Agent': get_random_ua()})
            r.raise_for_status()
            text = r.text
            
            if not any(p in text for p in PROTOCOLS):
                decoded = decode_base64(text)
                if decoded: text = decoded
                
            matches = re.findall(r'(?:vless|vmess|trojan|ss|hysteria2|tuic)://[^\s#"\'<>,]+', text)
            raw_configs.extend(matches)
            print(f"  + {url[:30]}... : Найдено {len(matches)} узлов", flush=True)
            gc.collect()
        except Exception as e:
            print(f"  [!] Пропуск источника {url[:30]}: {e}", flush=True)

    print(f"\nЭтап 2: Проверка доступности портов (Threads: {THREAD_COUNT})...", flush=True)
    valid_new_configs = []
    global_seen = set()
    seen_lock = threading.Lock()
    
    with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
        futures = [executor.submit(thread_check_worker, c, blacklist, db_knowledge, known_nodes, seen_lock, global_seen) for c in raw_configs]
        for future in as_completed(futures):
            if SHOULD_EXIT: break
            result = future.result()
            if result:
                if isinstance(result, tuple) and result[0] == "FAIL":
                    blacklist[result[1]] = datetime.now()
                else:
                    valid_new_configs.append(result)

    # Ограничиваем количество GeoIP проверок за раз, чтобы не ловить баны
    random.shuffle(valid_new_configs)
    queue = valid_new_configs[:GEOIP_LIMIT_PER_RUN]
    
    structured = {c: [] for c in COUNTRIES}
    print(f"\nЭтап 3: Распределенное определение стран для {len(queue)} живых узлов...", flush=True)
    
    for cfg in queue:
        if SHOULD_EXIT: break
        host, _ = get_server_info(cfg)
        code = check_ip_location_smart(host)
        
        if code:
            matched = False
            for c_name, c_info in COUNTRIES.items():
                if code == c_info["code"] or code == c_info.get("alt_code") or code == c_info.get("extra"):
                    structured[c_name].append(cfg)
                    matched = True
                    break
    
    print("\nЭтап 4: Финализация файлов и очистка памяти...", flush=True)
    save_and_cleanup(structured)
    save_blacklist(blacklist)
    git_commit_and_push()
    
    print(f"--- ПАРСИНГ ЗАВЕРШЕН. ВРЕМЯ РАБОТЫ: {datetime.now() - start_run} ---", flush=True)

if __name__ == "__main__":
    try:
        process()
    except Exception as e:
        print(f"КРИТИЧЕСКИЙ СБОЙ ПРИЛОЖЕНИЯ: {e}", flush=True)
        sys.exit(1)

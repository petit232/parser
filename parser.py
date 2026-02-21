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
from urllib.parse import quote, unquote
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- КОНФИГУРАЦИЯ СТРАН ---
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

# Глобальное состояние
IP_CACHE = {} 
CACHE_LOCK = threading.Lock()
PROCESSED_COUNT = 0
SHOULD_EXIT = False 

# Экстремальные настройки производительности и защиты
BLACKLIST_BAIL_HOURS = 6    # Время бана за мертвый порт (6 часов для синхронности)
MAX_BLACKLIST_SIZE = 50000  # Максимальный размер черного списка
GEOIP_LIMIT_PER_RUN = 3000  # Лимит проверок API
THREAD_COUNT = 60           # Оптимально для GitHub Actions (стабильность сети)
GEOIP_PARALLEL_LEVEL = 10   # Параллельный опрос API
PORT_TIMEOUT = 3.5          # Таймаут для медленных серверов (Reality/Hysteria)

def signal_handler(sig, frame):
    """Корректный выход при прерывании."""
    global SHOULD_EXIT
    print("\n[!] Остановка процесса. Завершаем текущие задачи...", flush=True)
    SHOULD_EXIT = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def get_random_ua():
    """Рандомный User-Agent для мимикрии под браузер."""
    uas = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ]
    return random.choice(uas)

def decode_base64(data):
    """Безопасное декодирование Base64 с исправлением паддинга."""
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
    """Извлечение IP и Порта БЕЗ ПОВРЕЖДЕНИЯ параметров обхода блокировок."""
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
    """Быстрая проверка доступности TCP порта."""
    if not host or not port: return False
    if host.startswith(('127.', '192.168.', '10.', '172.16.')) or host == 'localhost':
        return False
    try:
        port_int = int(port)
        with socket.create_connection((host, port_int), timeout=timeout):
            return True
    except: return False

# --- МОДУЛЬ ДИЗАЙНА (ЗАВОРАЧИВАНИЕ И ФИКС ИМЕН) ---

def beautify_config(config, country_key=None, fallback_code="UN"):
    """
    Создает идеальное оформление: ❤️ 🏁 Страна | Код 🏁 ❤️
    Если страны нет в списке: ❤️ 🌍 Global | Код 🌍 ❤️
    Оставляет параметры шифрования (sni, fp, pbk) нетронутыми!
    """
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

# --- ИНТЕЛЛЕКТУАЛЬНАЯ ПАМЯТЬ ПОДПИСОК (КЭШИРОВАНИЕ IP) ---

def pre_populate_ip_cache():
    """
    Сканирует твои готовые подписки (.txt файлы). 
    Если находит IP, запоминает его страну. 
    Благодаря этому бот НЕ тратит время на GeoIP API для старых узлов!
    """
    print("🧠 Загрузка базы знаний (IP Cache) из существующих подписок...", flush=True)
    files = [f"{c}.txt" for c in COUNTRIES] + ["mix.txt"]
    loaded_count = 0
    
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
            
    print(f"✅ В память загружено {loaded_count} известных IP. Они мгновенно пропустят проверку API.")

# --- ТУРБО-ДВИЖОК GEOIP (10 ИСТОЧНИКОВ API С АНТИ-БАНОМ) ---

def api_01(h):
    try: return requests.get(f"http://ip-api.com/json/{h}?fields=status,countryCode", timeout=3).json().get("countryCode")
    except: return None
def api_02(h):
    try: return requests.get(f"https://ipwho.is/{h}", timeout=3).json().get("country_code")
    except: return None
def api_03(h):
    try: return requests.get(f"https://ip2c.org/{h}", timeout=3).text.split(';')[1] if "1;" in requests.get(f"https://ip2c.org/{h}", timeout=3).text else None
    except: return None
def api_04(h):
    try: return requests.get(f"https://freeipapi.com/api/json/{h}", timeout=3).json().get("countryCode")
    except: return None
def api_05(h):
    try: return requests.get(f"https://ipapi.co/{h}/json/", timeout=3, headers={'User-Agent': get_random_ua()}).json().get("country_code")
    except: return None
def api_06(h):
    try: return requests.get(f"https://ip-json.com/json/{h}", timeout=3).json().get("country_code")
    except: return None
def api_07(h):
    try: return requests.get(f"https://ipapi.is/json/{h}", timeout=3).json().get("location", {}).get("country_code")
    except: return None
def api_08(h):
    try: return requests.get(f"http://www.geoplugin.net/json.gp?ip={h}", timeout=3).json().get("geoplugin_countryCode")
    except: return None
def api_09(h):
    try: return requests.get(f"https://api.scamalytics.com/ip/{h}", timeout=3).json().get("country_code")
    except: return None
def api_10(h):
    try: return requests.get(f"https://extreme-ip-lookup.com/json/{h}?key=demo", timeout=3).json().get("countryCode")
    except: return None

def check_ip_location_smart(host):
    """ПАРАЛЛЕЛЬНОЕ определение страны. Если IP есть в памяти, API не вызывается!"""
    global PROCESSED_COUNT
    if SHOULD_EXIT: return None

    with CACHE_LOCK:
        if host in IP_CACHE: 
            return IP_CACHE[host]

    # Защита от спам-бана со стороны API
    time.sleep(random.uniform(0.1, 0.4))

    providers = [api_01, api_02, api_03, api_04, api_05, api_06, api_07, api_08, api_09, api_10]
    random.shuffle(providers)

    # Попытка опроса API
    for provider in providers:
        if SHOULD_EXIT: break
        try:
            code = provider(host)
            if code and len(str(code)) == 2:
                code = str(code).upper()
                with CACHE_LOCK:
                    IP_CACHE[host] = code
                return code
        except: continue

    with CACHE_LOCK: IP_CACHE[host] = "UN"
    return "UN"

# --- СИСТЕМА ЧЕРНОГО СПИСКА ---

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

# --- СИСТЕМА СОХРАНЕНИЯ (СТРОГОЕ ЗЕРКАЛИРОВАНИЕ) ---

def save_and_organize(structured, final_mix_list, failed_list):
    """
    Режим Strict Mirror Sync: 
    Файлы ПЕРЕЗАПИСЫВАЮТСЯ полностью. Если источника нет или он пуст — файл затирается.
    Обеспечивает 100% синхронизацию между all_sources.txt и конечными подписками.
    """
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Синхронизация по странам
    for country in COUNTRIES:
        file_name = f"{country}.txt"
        configs = structured.get(country, [])
        valid = sorted(list(set(configs)))
        
        try:
            with open(file_name, 'w', encoding='utf-8') as f:
                if valid:
                    f.write("\n".join(valid))
                    f.write(f"\n\n# Total Active: {len(valid)}\n# Synced: {now_str}")
                else:
                    # Если узлов нет — файл становится пустым сервисным сообщением
                    f.write(f"# No active nodes found for {country}\n# Synced: {now_str}")
        except: pass

    # Синхронизация общего микса
    valid_mix = sorted(list(set(final_mix_list)))
    try:
        with open("mix.txt", 'w', encoding='utf-8') as f:
            if valid_mix:
                f.write("\n".join(valid_mix))
                f.write(f"\n\n# Total Active: {len(valid_mix)}\n# Synced: {now_str}")
            else:
                f.write(f"# No active nodes found\n# Synced: {now_str}")
        
        # Base64 подписка
        with open("sub_monster.txt", 'w', encoding='utf-8') as f:
            f.write(encode_base64("\n".join(valid_mix)) if valid_mix else "")
            
        # Список неудачных узлов (failed)
        valid_failed = sorted(list(set(failed_list)))
        with open("failed_nodes.txt", 'w', encoding='utf-8') as f:
            if valid_failed:
                f.write("\n".join(valid_failed))
                f.write(f"\n\n# Failed Nodes Count: {len(valid_failed)}\n# Log: {now_str}")
            else:
                f.write(f"# No failed nodes\n# Log: {now_str}")
                
        with open("sub_failed.txt", 'w', encoding='utf-8') as f:
            f.write(encode_base64("\n".join(valid_failed)) if valid_failed else "")
    except: pass

def git_commit_push():
    """Встроенная отправка в GitHub с принудительной синхронизацией."""
    print("\n[Git] Синхронизация репозитория (Mirror Mode)...", flush=True)
    try:
        subprocess.run(["git", "config", "--local", "user.name", "VPN-Monster-Bot"], check=True)
        subprocess.run(["git", "config", "--local", "user.email", "bot@vpn-monster.com"], check=True)
        
        # Принудительно забираем актуальное состояние, чтобы не было конфликтов
        subprocess.run(["git", "fetch", "origin"], check=True)
        subprocess.run(["git", "reset", "--hard", "origin/main"], check=True)
        
        subprocess.run(["git", "add", "*.txt"], check=True)
        
        if subprocess.run(["git", "diff", "--cached", "--quiet"]).returncode == 0:
            print("[Git] Файлы уже синхронизированы. Изменений нет.")
            return
            
        msg = f"Ultra-Sync {datetime.now().strftime('%d/%m %H:%M')} | Mirror Sync Active"
        subprocess.run(["git", "commit", "-m", msg], check=True)
        
        # Force push гарантирует, что репозиторий будет точной копией локальных данных
        subprocess.run(["git", "push", "origin", "main", "--force"], check=True)
        print("[Git] Зеркало успешно обновлено!")
    except Exception as e:
        print(f"[Git] Ошибка при пуше: {e}")

# --- ФУНКЦИИ ВОРКЕРЫ ---

def check_worker(config, blacklist, lock, seen):
    """Проверка узла на доступность с защитой от дублирования."""
    h, p = get_server_info(config)
    if not h or not p: return None
    nid = f"{h}:{p}"
    
    if nid in blacklist: return None
    
    with lock:
        if nid in seen: return None
        seen.add(nid)
        
    if is_node_alive(h, p):
        return config
    else:
        return ("FAIL", nid, config)

def geoip_parallel_worker(cfg):
    """Воркер для многопоточного GeoIP."""
    host, _ = get_server_info(cfg)
    code = check_ip_location_smart(host)
    return (cfg, code)

# --- ГЛАВНЫЙ ДВИЖОК GITHUB ACTIONS ---

def process_monster_engine():
    start_time = datetime.now()
    print(f"\n{'='*50}\n🚀 MONSTER ENGINE SYNC СТАРТ: {start_time.strftime('%H:%M:%S')}\n{'='*50}", flush=True)
    
    # 1. Загружаем память подписок
    pre_populate_ip_cache()
    
    sources = []
    if os.path.exists('all_sources.txt'):
        with open('all_sources.txt', 'r', encoding='utf-8') as f:
            sources = list(set([l.strip() for l in f if l.strip()]))
    
    if not sources:
        print("[!] ВНИМАНИЕ: all_sources.txt пуст. Будет выполнена полная очистка всех подписок!")

    blacklist = load_blacklist()
    raw_configs = []
    
    print(f"📡 Сбор данных из {len(sources)} источников...", flush=True)
    for url in sources:
        try:
            # Игнорируем собственные результирующие файлы во избежание рекурсии
            if any(x in url for x in ["sub_monster.txt", "mix.txt", "failed_nodes.txt", "sub_failed.txt"]):
                continue
            r = requests.get(url, timeout=15, headers={'User-Agent': get_random_ua()})
            text = r.text
            
            # Попытка декодировать, если это подписка в base64
            if not any(p in text for p in ALLOWED_PROTOCOLS):
                decoded = decode_base64(text)
                if decoded: text = decoded
            
            regex_pattern = r'(?:' + '|'.join(ALLOWED_PROTOCOLS).replace('://', '') + r')://[^\s#"\'<>,]+'
            found = re.findall(regex_pattern, text)
            raw_configs.extend(found)
        except Exception: pass

    raw_configs = list(set(raw_configs))
    print(f"🔍 Найдено уникальных ссылок для проверки: {len(raw_configs)}")

    valid_new = []
    failed_new = []
    global_seen = set()
    seen_lock = threading.Lock()
    
    if raw_configs:
        print(f"⚡ Проверка портов ({THREAD_COUNT} потоков)...", flush=True)
        with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
            futures = [executor.submit(check_worker, c, blacklist, seen_lock, global_seen) for c in raw_configs]
            for future in as_completed(futures):
                if SHOULD_EXIT: break
                try:
                    res = future.result()
                    if res:
                        if isinstance(res, tuple): 
                            blacklist[res[1]] = datetime.now()
                            failed_new.append(res[2])
                        else:
                            valid_new.append(res)
                except: continue

    print(f"✅ Итого ЖИВЫХ узлов: {len(valid_new)}")

    structured_data = {c: [] for c in COUNTRIES}
    final_mix_list = []
    
    if valid_new:
        print(f"🌍 Турбо-GeoIP определение (Синхронный Mirror режим)...", flush=True)
        random.shuffle(valid_new)
        queue = valid_new[:GEOIP_LIMIT_PER_RUN]
        
        # Параллельное определение стран для ускорения
        with ThreadPoolExecutor(max_workers=GEOIP_PARALLEL_LEVEL) as geo_executor:
            geo_futures = [geo_executor.submit(geoip_parallel_worker, cfg) for cfg in queue]
            
            for i, future in enumerate(as_completed(geo_futures)):
                if SHOULD_EXIT: break
                try:
                    cfg, code = future.result()
                    
                    matched = False
                    if code and code != "UN":
                        for c_name, c_info in COUNTRIES.items():
                            if code in [c_info["code"], c_info.get("alt_code"), c_info.get("extra")]:
                                beauty_cfg = beautify_config(cfg, c_name)
                                structured_data[c_name].append(beauty_cfg)
                                final_mix_list.append(beauty_cfg)
                                matched = True
                                break
                                
                    if not matched:
                        beauty_cfg = beautify_config(cfg, None, fallback_code=code)
                        final_mix_list.append(beauty_cfg)
                        
                    if i % 100 == 0:
                        print(f"   > Обработано {i}/{len(queue)}...", flush=True)
                except: continue
            
    print("💾 Прямая синхронизация файлов (Режим Зеркала)...", flush=True)
    save_and_organize(structured_data, final_mix_list, failed_new)
    save_blacklist(blacklist)
    
    git_commit_push()
    
    end_time = datetime.now()
    print(f"\n🏁 ЦИКЛ ЗАВЕРШЕН ЗА {end_time - start_time}.", flush=True)

if __name__ == "__main__":
    try:
        process_monster_engine()
    except Exception as fatal_error:
        print(f"\n[КРИТИЧЕСКАЯ ОШИБКА]: {fatal_error}")
        sys.exit(1)

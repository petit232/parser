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
# Формат оформления будет генерироваться так: ❤️ 🇧🇾 Belarus | BY 🇧🇾 ❤️
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

# Поддерживаемые протоколы (Акцент на обход DPI в РФ)
ALLOWED_PROTOCOLS = ["vless://", "vmess://", "trojan://", "ss://", "hysteria2://", "tuic://"]

# Глобальное состояние
IP_CACHE = {} 
CACHE_LOCK = threading.Lock()
PROCESSED_COUNT = 0
SHOULD_EXIT = False 

# Агрессивные настройки производительности
BLACKLIST_BAIL_HOURS = 24   # Время бана за мертвый порт
MAX_BLACKLIST_SIZE = 50000  # Лимит черного списка
GEOIP_LIMIT_PER_RUN = 3000  # Макс. узлов для GeoIP за раз
THREAD_COUNT = 100          # Потоки для проверки портов (Максимальная скорость)
GEOIP_PARALLEL_LEVEL = 5    # Сколько API опрашивать ОДНОВРЕМЕННО для одного IP

def signal_handler(sig, frame):
    """Корректный выход при прерывании."""
    global SHOULD_EXIT
    print("\n[!] Получен сигнал остановки. Завершаем процессы...", flush=True)
    SHOULD_EXIT = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def get_random_ua():
    """Случайный User-Agent для обхода анти-бот систем."""
    uas = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"
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
    """Жесткое извлечение IP/Хоста и Порта из ссылки любого типа."""
    try:
        # Очищаем конфиг от старых имен (#name), чтобы не сломать парсинг
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

def is_node_alive(host, port, timeout=1.5):
    """Мгновенная проверка доступности TCP порта."""
    if not host or not port: return False
    # Игнорируем локальные и приватные адреса
    if host.startswith(('127.', '192.168.', '10.', '172.16.')) or host == 'localhost':
        return False
    try:
        port_int = int(port)
        with socket.create_connection((host, port_int), timeout=timeout):
            return True
    except: return False

# --- МОДУЛЬ ДИЗАЙНА (СЕРДЕЧКИ И ФЛАГИ ПО БОКАМ) ---

def beautify_config(config, country_key):
    """
    Создает идеальное оформление: ❤️ 🇧🇾 Belarus | BY 🇧🇾 ❤️
    Автоматически удаляет старое имя и заменяет на новое.
    """
    try:
        info = COUNTRIES.get(country_key)
        if not info: return config
        
        # Формируем нужный дизайн
        label = f"❤️ {info['flag']} {info['name']} | {info['code']} {info['flag']} ❤️"
        
        if config.startswith("vmess://"):
            # Для Vmess имя хранится в JSON (ключ 'ps')
            clean_config = config.split('#')[0]
            decoded = decode_base64(clean_config[8:])
            if decoded:
                data = json.loads(decoded)
                data['ps'] = label
                return "vmess://" + encode_base64(json.dumps(data))
        else:
            # Для Vless, Trojan, SS, Hysteria обрезаем старое имя после # и добавляем новое
            base_part = config.split('#')[0]
            return f"{base_part}#{quote(label)}"
            
    except Exception: return config

# --- ТУРБО-ДВИЖОК GEOIP (10 ИСТОЧНИКОВ API) ---

def api_01(h):
    try: return requests.get(f"http://ip-api.com/json/{h}?fields=status,countryCode", timeout=2).json().get("countryCode")
    except: return None
def api_02(h):
    try: return requests.get(f"https://ipwho.is/{h}", timeout=2).json().get("country_code")
    except: return None
def api_03(h):
    try: return requests.get(f"https://ip2c.org/{h}", timeout=2).text.split(';')[1] if "1;" in requests.get(f"https://ip2c.org/{h}", timeout=2).text else None
    except: return None
def api_04(h):
    try: return requests.get(f"https://freeipapi.com/api/json/{h}", timeout=2).json().get("countryCode")
    except: return None
def api_05(h):
    try: return requests.get(f"https://ipapi.co/{h}/json/", timeout=2, headers={'User-Agent': get_random_ua()}).json().get("country_code")
    except: return None
def api_06(h):
    try: return requests.get(f"https://ip-json.com/json/{h}", timeout=2).json().get("country_code")
    except: return None
def api_07(h):
    try: return requests.get(f"https://ipapi.is/json/{h}", timeout=2).json().get("location", {}).get("country_code")
    except: return None
def api_08(h):
    try: return requests.get(f"http://www.geoplugin.net/json.gp?ip={h}", timeout=2).json().get("geoplugin_countryCode")
    except: return None
def api_09(h):
    try: return requests.get(f"https://api.scamalytics.com/ip/{h}", timeout=2).json().get("country_code")
    except: return None
def api_10(h):
    try: return requests.get(f"https://extreme-ip-lookup.com/json/{h}?key=demo", timeout=2).json().get("countryCode")
    except: return None

def check_ip_location_smart(host):
    """
    Параллельное определение страны. Опрашивает 5 случайных API одновременно.
    Кто ответил быстрее - того и берем.
    """
    global PROCESSED_COUNT
    if SHOULD_EXIT: return None
    
    with CACHE_LOCK:
        if host in IP_CACHE: return IP_CACHE[host]

    providers = [api_01, api_02, api_03, api_04, api_05, api_06, api_07, api_08, api_09, api_10]
    random.shuffle(providers)

    # Запускаем сразу несколько API (Стая хищников)
    with ThreadPoolExecutor(max_workers=GEOIP_PARALLEL_LEVEL) as api_executor:
        future_to_api = {api_executor.submit(p, host): p for p in providers[:GEOIP_PARALLEL_LEVEL]}
        for future in as_completed(future_to_api):
            if SHOULD_EXIT: break
            try:
                code = future.result()
                if code and len(str(code)) == 2:
                    code = str(code).upper()
                    with CACHE_LOCK:
                        IP_CACHE[host] = code
                        PROCESSED_COUNT += 1
                    return code
            except: continue

    # Фолбэк на оставшиеся API, если первые не ответили
    for provider in providers[GEOIP_PARALLEL_LEVEL:]:
        if SHOULD_EXIT: break
        try:
            code = provider(host)
            if code and len(str(code)) == 2:
                code = str(code).upper()
                with CACHE_LOCK:
                    IP_CACHE[host] = code
                    PROCESSED_COUNT += 1
                return code
        except: continue

    with CACHE_LOCK: IP_CACHE[host] = None
    return None

# --- СИСТЕМА СОХРАНЕНИЯ И СИНХРОНИЗАЦИИ ---

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

def git_commit_push():
    """Автоматическая отправка в GitHub."""
    print("\n[Git] Синхронизация с репозиторием...", flush=True)
    try:
        subprocess.run(["git", "config", "--local", "user.name", "VPN-Monster-Bot"], check=True)
        subprocess.run(["git", "config", "--local", "user.email", "bot@vpn-monster.com"], check=True)
        subprocess.run(["git", "add", "*.txt"], check=True)
        
        if subprocess.run(["git", "diff", "--cached", "--quiet"]).returncode == 0:
            print("[Git] Нет изменений для коммита.")
            return
            
        msg = f"Ultra-Update {datetime.now().strftime('%d/%m %H:%M')} | Auto-Sync Mode"
        subprocess.run(["git", "commit", "-m", msg], check=True)
        subprocess.run(["git", "push", "origin", "main"], check=True)
        print("[Git] Успешно отправлено!")
    except Exception as e:
        print(f"[Git] Ошибка при пуше: {e}")

# --- ФУНКЦИИ ВОРКЕРЫ ---

def port_check_worker(config, blacklist, lock, seen):
    """Проверка одного узла с жестким отсеиванием дубликатов."""
    h, p = get_server_info(config)
    if not h or not p: return None
    nid = f"{h}:{p}"
    
    # Защита от проверок одного и того же IP:PORT
    if nid in blacklist: return None
    
    with lock:
        if nid in seen: return None
        seen.add(nid)
        
    if is_node_alive(h, p):
        return config
    else:
        return ("FAIL", nid, config)

# --- ГЛАВНЫЙ ДВИЖОК ---

def process_monster_engine():
    start_time = datetime.now()
    print(f"--- MONSTER ENGINE ULTIMATE START: {start_time.strftime('%H:%M:%S')} ---", flush=True)
    
    if not os.path.exists('all_sources.txt'):
        print("[!] Файл all_sources.txt не найден!")
        return

    with open('all_sources.txt', 'r', encoding='utf-8') as f:
        sources = list(set([l.strip() for l in f if l.strip()]))

    blacklist = load_blacklist()
    raw_configs = []
    
    # 1. Жесткий парсинг источников
    print(f"Сбор данных из {len(sources)} источников (Только разрешенные протоколы)...", flush=True)
    for url in sources:
        if SHOULD_EXIT: break
        try:
            # Игнорируем собственные файлы, чтобы не парсить старье
            if any(x in url for x in ["sub_monster.txt", "mix.txt", "failed_nodes.txt", "sub_failed.txt"]):
                continue
            r = requests.get(url, timeout=12, headers={'User-Agent': get_random_ua()})
            text = r.text
            
            # Декодируем Base64, если источник зашифрован
            if not any(p in text for p in ALLOWED_PROTOCOLS):
                decoded = decode_base64(text)
                if decoded: text = decoded
            
            # Строгий Regex только под нужные протоколы
            regex_pattern = r'(?:' + '|'.join(ALLOWED_PROTOCOLS).replace('://', '') + r')://[^\s#"\'<>,]+'
            found = re.findall(regex_pattern, text)
            raw_configs.extend(found)
            gc.collect()
        except: pass

    # Удаляем текстовые дубликаты ссылок сразу
    raw_configs = list(set(raw_configs))
    print(f"Найдено уникальных ссылок: {len(raw_configs)}")

    # 2. Массовая проверка портов (Многопоточность)
    valid_configs = []
    failed_configs = []
    global_seen_ips = set()
    seen_lock = threading.Lock()
    
    print(f"Проверка узлов на доступность порта в {THREAD_COUNT} потоков...", flush=True)
    with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
        futures = [executor.submit(port_check_worker, c, blacklist, seen_lock, global_seen_ips) for c in raw_configs]
        for future in as_completed(futures):
            if SHOULD_EXIT: break
            try:
                res = future.result()
                if res:
                    if isinstance(res, tuple): # Узел мертв -> В черный список
                        blacklist[res[1]] = datetime.now()
                        failed_configs.append(res[2])
                    else:
                        valid_configs.append(res)
            except: continue

    # 3. GeoIP Распределение
    random.shuffle(valid_configs)
    queue = valid_configs[:GEOIP_LIMIT_PER_RUN]
    structured_data = {c: [] for c in COUNTRIES}
    final_mix_list = []
    
    print(f"GeoIP анализ и применение дизайна для {len(queue)} живых узлов...", flush=True)
    for cfg in queue:
        if SHOULD_EXIT: break
        host, _ = get_server_info(cfg)
        code = check_ip_location_smart(host)
        
        if code:
            matched = False
            for c_name, c_info in COUNTRIES.items():
                if code in [c_info["code"], c_info.get("alt_code"), c_info.get("extra")]:
                    # Применяем красоту: ❤️ Флаг Имя | Код Флаг ❤️
                    beautiful_cfg = beautify_config(cfg, c_name)
                    structured_data[c_name].append(beautiful_cfg)
                    final_mix_list.append(beautiful_cfg)
                    matched = True
                    break
            if not matched: failed_configs.append(cfg) # Страна не из списка
        else:
            failed_configs.append(cfg) # Страна не определилась
            
    # 4. ИДЕАЛЬНАЯ СИНХРОНИЗАЦИЯ (Авто-удаление старья)
    # Мы перезаписываем файлы ТОЛЬКО свежими данными текущего цикла.
    print("Сохранение файлов (Синхронизация и удаление неактивных)...", flush=True)
    now_str = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    for country, configs in structured_data.items():
        with open(f"{country}.txt", 'w', encoding='utf-8') as f:
            if configs:
                f.write("\n".join(sorted(list(set(configs)))))
                f.write(f"\n\n# Total Active: {len(configs)}\n# Synced: {now_str}")
            else:
                f.write(f"# No active nodes found\n# Synced: {now_str}")

    # Сохраняем общий микс и подписку
    final_mix_list = sorted(list(set(final_mix_list)))
    
    with open("mix.txt", 'w', encoding='utf-8') as f:
        f.write("\n".join(final_mix_list))
        f.write(f"\n\n# Total Active: {len(final_mix_list)}\n# Synced: {now_str}")
        
    with open("sub_monster.txt", 'w', encoding='utf-8') as f:
        f.write(encode_base64("\n".join(final_mix_list)))
        
    # Сохраняем ошибки (для отладки)
    with open("failed_nodes.txt", 'w', encoding='utf-8') as f:
        f.write("\n".join(list(set(failed_configs))))
        f.write(f"\n\n# Total Failed/Unknown: {len(failed_configs)}\n# Log: {now_str}")
        
    save_blacklist(blacklist)
    git_commit_push()
    
    end_time = datetime.now()
    print(f"--- ЦИКЛ УСПЕШНО ЗАВЕРШЕН ЗА {end_time - start_time} ---", flush=True)

if __name__ == "__main__":
    try:
        process_monster_engine()
    except Exception as fatal_error:
        print(f"\n[КРИТИЧЕСКАЯ ОШИБКА]: {fatal_error}")
        sys.exit(1)

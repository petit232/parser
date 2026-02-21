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
import shutil
from datetime import datetime, timedelta
from urllib.parse import quote, unquote
from concurrent.futures import ThreadPoolExecutor, as_completed

# ==============================================================================
# 🚀 VPN MONSTER ENGINE ULTRA DAEMON - PREMIUM EDITION
# ==============================================================================

# --- НАСТРОЙКИ ДЕМОНА И ТАЙМЕРОВ ---
UPDATE_INTERVAL_HOURS = 1       # ИНТЕРВАЛ ОБНОВЛЕНИЯ: 1 ЧАС (Дробное обновление 1/24 базы)
HOURS_TO_COMPLETE_CYCLE = 24    # За сколько часов база должна быть проверена полностью
WATCHER_INTERVAL_SEC = 2.0      # Как часто проверять локальные файлы на изменения (в секундах)
PORT_TIMEOUT = 4.0              # Таймаут TCP Ping (для Hysteria2/Reality)
BLACKLIST_BAIL_DAYS = 7         # Сколько дней хранить мертвые узлы в блэклисте (глубокая зачистка)

# --- НАСТРОЙКИ ПОТОКОВ И API ---
THREAD_COUNT = 150              # Экстремальная многопоточность для TCP Ping (быстрая отбраковка)
GEOIP_PARALLEL_LEVEL = 10       # Строго 10 потоков для GeoIP (защита от перелимита и банов API)
GEOIP_LIMIT_PER_RUN = 10000     # Жесткий лимит GeoIP (защита на случай резкого всплеска)

# --- ФАЙЛОВАЯ СИСТЕМА ---
LOCK_FILE = "monster_daemon.lock"
PERSISTENT_BLACKLIST = "persistent_blacklist.txt"
PROCESSED_SOURCES_FILE = "processed_sources.dat"
ALL_SOURCES_FILE = "all_sources.txt"
MONSTER_STATE_FILE = "monster_state.json"  # УМНАЯ ПАМЯТЬ: хранит статус и время проверки узлов

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

ALLOWED_PROTOCOLS = ["vless://", "vmess://", "trojan://", "ss://", "hysteria2://", "tuic://"]

# --- ГЛОБАЛЬНЫЕ СОСТОЯНИЯ (THREAD-SAFE) ---
IP_CACHE = {} 
CACHE_LOCK = threading.Lock()
BLACKLIST_CACHE = set()
BLACKLIST_LOCK = threading.Lock()
SHOULD_EXIT = False 

# ==============================================================================
# --- СИСТЕМНЫЕ ФУНКЦИИ И ОБРАБОТЧИКИ ---
# ==============================================================================

def signal_handler(sig, frame):
    """Моментальный и корректный выход из программы (Graceful Shutdown)."""
    global SHOULD_EXIT
    print("\n[!] КРИТИЧЕСКОЕ ПРЕРЫВАНИЕ: Получен сигнал остановки.", flush=True)
    print("[!] Завершаем все активные пулы потоков, сохраняем кэш и чистим Lock-файлы...", flush=True)
    SHOULD_EXIT = True
    if os.path.exists(LOCK_FILE):
        try: os.remove(LOCK_FILE)
        except: pass

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def get_random_ua():
    """Случайный User-Agent для обхода Cloudflare при парсинге."""
    uas = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
        "Mozilla/5.0 (iPhone; CPU iPhone OS 17_4_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4.1 Mobile/15E148 Safari/604.1"
    ]
    return random.choice(uas)

def atomic_save(filepath, content):
    """Атомарное сохранение файла. Гарантирует целостность базы при сбоях."""
    tmp_file = f"{filepath}.tmp"
    try:
        with open(tmp_file, 'w', encoding='utf-8') as f:
            f.write(content)
        os.replace(tmp_file, filepath)
    except Exception as e:
        print(f"[ERROR] Ошибка атомарного сохранения {filepath}: {e}")
        if os.path.exists(tmp_file):
            try: os.remove(tmp_file)
            except: pass

def get_file_mod_time(filepath):
    """Получает время последнего изменения файла для Auto-Trigger системы."""
    try:
        if os.path.exists(filepath):
            return os.path.getmtime(filepath)
    except: pass
    return 0

# ==============================================================================
# --- УМНОЕ КЭШИРОВАНИЕ И ПАМЯТЬ СОСТОЯНИЙ ---
# ==============================================================================

def load_state():
    """Загрузка памяти состояния узлов."""
    if os.path.exists(MONSTER_STATE_FILE):
        try:
            with open(MONSTER_STATE_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        except: return {}
    return {}

def save_state(state):
    """Сохранение памяти состояния узлов."""
    try:
        atomic_save(MONSTER_STATE_FILE, json.dumps(state, indent=2))
    except Exception as e:
        print(f"[ERROR] Не удалось сохранить состояние: {e}")

def sync_caches_with_master(master_set):
    """
    ИДЕАЛЬНОЕ ЗЕРКАЛО: Вычищает кэш IP и файл состояний от мусора.
    Если узел пропал из all_sources.txt, он мгновенно и навсегда удаляется отовсюду.
    """
    print(f"🧹 Синхронизация кэша. Поиск удаленных подписок и призраков...", flush=True)
    
    # Синхронизация STATE_FILE
    state = load_state()
    keys_to_delete = [cfg for cfg in state.keys() if cfg not in master_set]
    for k in keys_to_delete:
        del state[k]
        
    # Синхронизация IP_CACHE
    active_ips = set()
    for cfg in master_set:
        host, _ = get_server_info(cfg)
        if host: active_ips.add(host)
        
    with CACHE_LOCK:
        ips_to_delete = [ip for ip in IP_CACHE.keys() if ip not in active_ips]
        for ip in ips_to_delete:
            del IP_CACHE[ip]

    save_state(state)
    print(f"✨ Кэш очищен! Удалено призраков: {len(keys_to_delete)} конфигов, {len(ips_to_delete)} IP-адресов.")
    return state

# ==============================================================================
# --- ПАРСИНГ И ДЕКОДИРОВАНИЕ ---
# ==============================================================================

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
    """Агрессивный парсер. Извлекает IP/Домен и Порт из любой ссылки."""
    try:
        clean_config = config.split('#')[0].strip()
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

def beautify_config(config, country_key=None, fallback_code="UN"):
    """Зеркальное форматирование 1в1. Премиум дизайн для клиента."""
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

# ==============================================================================
# --- СЕТЬ: TCP PING И GEOIP ДВИЖОК ---
# ==============================================================================

def is_node_alive(host, port, timeout=PORT_TIMEOUT):
    """Жесткий TCP Ping. Отсеивает локальные и мусорные IP-адреса."""
    if not host or not port: return False
    if host.startswith(('127.', '192.168.', '10.', '172.16.', '0.')) or host == 'localhost':
        return False
    try:
        port_int = int(port)
        with socket.create_connection((host, port_int), timeout=timeout):
            return True
    except: return False

# 10 Зеркальных провайдеров GeoIP (Защита от лимитов)
def api_01(h): return requests.get(f"http://ip-api.com/json/{h}?fields=status,countryCode", timeout=4).json().get("countryCode")
def api_02(h): return requests.get(f"https://ipwho.is/{h}", timeout=4).json().get("country_code")
def api_03(h): 
    r = requests.get(f"https://ip2c.org/{h}", timeout=4)
    return r.text.split(';')[1] if "1;" in r.text else None
def api_04(h): return requests.get(f"https://freeipapi.com/api/json/{h}", timeout=4).json().get("countryCode")
def api_05(h): return requests.get(f"https://ipapi.co/{h}/json/", timeout=4, headers={'User-Agent': get_random_ua()}).json().get("country_code")
def api_06(h): return requests.get(f"https://ip-json.com/json/{h}", timeout=4).json().get("country_code")
def api_07(h): return requests.get(f"https://ipapi.is/json/{h}", timeout=4).json().get("location", {}).get("country_code")
def api_08(h): return requests.get(f"http://www.geoplugin.net/json.gp?ip={h}", timeout=4).json().get("geoplugin_countryCode")
def api_09(h): return requests.get(f"https://api.scamalytics.com/ip/{h}", timeout=4).json().get("country_code")
def api_10(h): return requests.get(f"https://extreme-ip-lookup.com/json/{h}?key=demo", timeout=4).json().get("countryCode")

def check_ip_location_smart(host):
    """Умный GeoIP с кэшированием, распределением нагрузки и рандомизацией API."""
    if SHOULD_EXIT: return None
    with CACHE_LOCK:
        if host in IP_CACHE: return IP_CACHE[host]
    
    time.sleep(random.uniform(0.1, 0.5))
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

# ==============================================================================
# --- УПРАВЛЕНИЕ БАЗАМИ И ЧЕРНЫМИ СПИСКАМИ (DEEP PURGE SYSTEM) ---
# ==============================================================================

def load_persistent_blacklist():
    """Загрузка вечного черного списка мертвых узлов."""
    bl = set()
    if os.path.exists(PERSISTENT_BLACKLIST):
        try:
            with open(PERSISTENT_BLACKLIST, 'r', encoding='utf-8') as f:
                for line in f:
                    parts = line.strip().split('|')
                    if len(parts) >= 2:
                        node_id = parts[0]
                        date_str = parts[1]
                        try:
                            date_obj = datetime.fromisoformat(date_str)
                            if datetime.now() - date_obj < timedelta(days=BLACKLIST_BAIL_DAYS):
                                bl.add(node_id)
                        except: pass
        except: pass
    
    with BLACKLIST_LOCK:
        global BLACKLIST_CACHE
        BLACKLIST_CACHE = bl.copy()
    print(f"🛡️ Persistent Blacklist загружен: {len(BLACKLIST_CACHE)} мертвых узлов.")

def save_persistent_blacklist(new_dead_nodes):
    """Обновление вечного черного списка новыми мертвыми узлами."""
    now_str = datetime.now().isoformat()
    with BLACKLIST_LOCK:
        for node in new_dead_nodes:
            BLACKLIST_CACHE.add(f"{node}|{now_str}")
            
    valid_lines = []
    with BLACKLIST_LOCK:
        for item in BLACKLIST_CACHE:
            if '|' in item: valid_lines.append(item)
            else: valid_lines.append(f"{item}|{now_str}")
            
    content = "\n".join(valid_lines) + "\n"
    atomic_save(PERSISTENT_BLACKLIST, content)

def deep_purge_files(dead_configs):
    """
    АБСОЛЮТНАЯ ЗАЧИСТКА: Физически удаляет мертвые конфигурации из текстовых файлов.
    Также мгновенно вычищает их из состояния и кэша.
    """
    if not dead_configs: return
    
    # 1. Удаление из памяти
    dead_set = set([c.strip() for c in dead_configs])
    state = load_state()
    keys_to_delete = [cfg for cfg in state.keys() if cfg in dead_set]
    for k in keys_to_delete: del state[k]
    save_state(state)
    
    # 2. Удаление из файлов
    files_to_purge = [ALL_SOURCES_FILE]
    purged_total = 0
    
    for filepath in files_to_purge:
        if not os.path.exists(filepath): continue
        
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                lines = f.readlines()
            
            clean_lines = []
            file_changed = False
            
            for line in lines:
                l_strip = line.strip()
                if not l_strip or l_strip.startswith('#'):
                    clean_lines.append(line)
                    continue
                
                # Обработка Base64 внутри all_sources.txt
                if not any(p in l_strip for p in ALLOWED_PROTOCOLS):
                    decoded = decode_base64(l_strip)
                    if decoded and any(p in decoded for p in ALLOWED_PROTOCOLS):
                        configs_in_b64 = decoded.splitlines()
                        clean_b64 = [cfg for cfg in configs_in_b64 if cfg.strip() not in dead_set]
                        if len(clean_b64) != len(configs_in_b64):
                            file_changed = True
                            purged_total += (len(configs_in_b64) - len(clean_b64))
                            if clean_b64: clean_lines.append(encode_base64("\n".join(clean_b64)) + "\n")
                        else:
                            clean_lines.append(line)
                        continue

                # Обычный текст
                if l_strip in dead_set:
                    file_changed = True
                    purged_total += 1
                else:
                    clean_lines.append(line)
            
            if file_changed:
                atomic_save(filepath, "".join(clean_lines))
                
        except Exception as e:
            print(f"[ERROR] Ошибка глубокой зачистки файла {filepath}: {e}")
            
    if purged_total > 0:
        print(f"🗑️ DEEP PURGE: Вырезано {purged_total} упоминаний мертвых узлов из источников.")

def load_processed_sources():
    """Хеши уже спарсенных внешних ссылок."""
    if os.path.exists(PROCESSED_SOURCES_FILE):
        try:
            with open(PROCESSED_SOURCES_FILE, 'r') as f:
                return set([line.strip() for line in f if line.strip()])
        except: return set()
    return set()

def save_processed_source_hash(url):
    h = hashlib.sha256(url.encode()).hexdigest()
    try:
        with open(PROCESSED_SOURCES_FILE, 'a') as f: f.write(h + "\n")
    except: pass

# ==============================================================================
# --- ВОРКЕРЫ ДЛЯ МНОГОПОТОЧНОСТИ ---
# ==============================================================================

def check_worker(config, seen_lock, global_seen):
    """Поток TCP проверки."""
    h, p = get_server_info(config)
    if not h or not p: return None
    
    nid = f"{h}:{p}"
    
    with BLACKLIST_LOCK:
        if any(b.startswith(nid) for b in BLACKLIST_CACHE):
            return ("FAIL", nid, config)
            
    with seen_lock:
        if nid in global_seen: return None
        global_seen.add(nid)
        
    if is_node_alive(h, p): 
        return ("OK", nid, config)
    else: 
        return ("FAIL", nid, config)

def geoip_parallel_worker(cfg):
    """Поток определения страны через API."""
    host, _ = get_server_info(cfg)
    code = check_ip_location_smart(host)
    return (cfg, code)

# ==============================================================================
# --- ФИНАЛИЗАЦИЯ И СИНХРОНИЗАЦИЯ ---
# ==============================================================================

def generate_static_links():
    """Абсолютно статические ссылки на подписки (Авто-генератор)."""
    print("\n🔗 Обновление статических ссылок клиента...", flush=True)
    try:
        remote_url = subprocess.run(["git", "config", "--get", "remote.origin.url"], 
                                   capture_output=True, text=True).stdout.strip()
        
        if not remote_url:
            raw_base = "https://raw.githubusercontent.com/USER/REPO/main/"
        else:
            raw_base = remote_url.replace("github.com", "raw.githubusercontent.com").replace(".git", "")
            if "raw.githubusercontent.com" in raw_base:
                raw_base += "/main/"
        
        links = []
        links.append(f"🚀 MONSTER VPN PRO SUBSCRIPTIONS 🚀\n")
        links.append(f"🔥 MIX (Text): {raw_base}mix.txt")
        links.append(f"🔥 MIX (Base64): {raw_base}sub_monster.txt\n")
        links.append("🌍 --- BY COUNTRIES --- 🌍")
        for c in COUNTRIES:
            links.append(f"{c.upper()}: {raw_base}{c}.txt")
        
        atomic_save("LINKS_FOR_CLIENTS.txt", "\n".join(links))
        print("✅ LINKS_FOR_CLIENTS.txt актуализирован.")
    except Exception as e:
        print(f"[!] Ошибка генератора ссылок: {e}")

def git_commit_push():
    """Жесткая синхронизация с GitHub."""
    print("\n[Git Sync] Синхронизация с облаком...", flush=True)
    try:
        subprocess.run(["git", "config", "--local", "user.name", "Monster-Ultra-Daemon"], check=True)
        subprocess.run(["git", "config", "--local", "user.email", "daemon@vpn-monster.com"], check=True)
        subprocess.run(["git", "add", "."], check=True)
        
        status = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True).stdout.strip()
        if not status:
            print("[Git Sync] Изменений в базе нет. Пуш не требуется.")
            return

        timestamp = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
        subprocess.run(["git", "commit", "-m", f"⚡ Auto-Sync Monster Engine: {timestamp}"], check=True)
        
        res = subprocess.run(["git", "push", "origin", "main"], capture_output=True, text=True)
        if res.returncode != 0:
            print("[Git Sync] Конфликт версий. Принудительный пуш (--force)...")
            subprocess.run(["git", "push", "origin", "main", "--force"], check=True)
            
        print(f"[Git Sync] ✅ Успешно. Подписки клиентов обновлены: {timestamp}")
    except Exception as e: 
        print(f"[Git Sync] ❌ Ошибка синхронизации: {e}")

def save_and_organize(master_set, state):
    """
    Генерация итоговых файлов ИСКЛЮЧИТЕЛЬНО на основе 'OK' статуса из State.
    Это гарантирует идеальное зеркало. Если конфиг не 'OK', он в файлы не попадает.
    """
    structured = {c: [] for c in COUNTRIES}
    final_mix = []
    failed_list = []

    for cfg in master_set:
        cfg_state = state.get(cfg, {})
        status = cfg_state.get('status')
        
        if status == 'OK':
            code = cfg_state.get('geoip', 'UN')
            matched = False
            for c_name, c_info in COUNTRIES.items():
                if code in [c_info["code"], c_info.get("alt_code"), c_info.get("extra")]:
                    b_cfg = beautify_config(cfg, c_name)
                    structured[c_name].append(b_cfg)
                    final_mix.append(b_cfg)
                    matched = True
                    break
            if not matched:
                final_mix.append(beautify_config(cfg, None, fallback_code=code))
        elif status == 'FAIL':
            failed_list.append(cfg)

    for country in COUNTRIES:
        file_name = f"{country}.txt"
        valid = sorted(list(set(structured[country])))
        content = "\n".join(valid) if valid else f"# No active nodes for {country}\n"
        atomic_save(file_name, content)

    valid_mix = sorted(list(set(final_mix)))
    atomic_save("mix.txt", "\n".join(valid_mix) if valid_mix else "# No active nodes found\n")
    atomic_save("sub_monster.txt", encode_base64("\n".join(valid_mix)) if valid_mix else "")
    
    valid_failed = sorted(list(set(failed_list)))
    atomic_save("failed_nodes.txt", "\n".join(valid_failed) if valid_failed else "# No failed nodes\n")
    atomic_save("sub_failed.txt", encode_base64("\n".join(valid_failed)) if valid_failed else "")

# ==============================================================================
# --- ГЛАВНЫЙ ПРОЦЕСС ОБНОВЛЕНИЯ (CORE ENGINE) ---
# ==============================================================================

def run_update_cycle(trigger_reason="Таймер"):
    """Полный цикл: Сбор, Синхронизация Кэша, Чанкинг (1/24), Проверка, Пуш."""
    start_time = datetime.now()
    now_ts = start_time.timestamp()
    
    print(f"\n{'='*70}")
    print(f"🔥 ЗАПУСК ЦИКЛА MONSTER ENGINE ULTRA")
    print(f"⏱️ Триггер: {trigger_reason} | Время: {start_time.strftime('%H:%M:%S')}")
    print(f"{'='*70}\n")
    
    load_persistent_blacklist()
    processed_hashes = load_processed_sources()
    
    raw_configs = set()
    new_sources = []
    
    # 1. Читаем локальные источники (Сырые данные)
    if os.path.exists(ALL_SOURCES_FILE):
        with open(ALL_SOURCES_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                l_strip = line.strip()
                if not l_strip or l_strip.startswith('#'): continue
                
                if l_strip.startswith('http'):
                    h = hashlib.sha256(l_strip.encode()).hexdigest()
                    if h not in processed_hashes:
                        new_sources.append(l_strip)
                elif any(p in l_strip for p in ALLOWED_PROTOCOLS):
                    raw_configs.add(l_strip)
                    
    # 2. Парсинг внешних ссылок (Дополняем Мастер-Лист)
    if new_sources:
        print(f"📡 Загрузка {len(new_sources)} новых внешних источников...", flush=True)
        for url in new_sources:
            if SHOULD_EXIT: break
            try:
                if any(x in url for x in ["sub_monster.txt", "mix.txt", "failed_nodes.txt"]): continue
                r = requests.get(url, timeout=15, headers={'User-Agent': get_random_ua()})
                text = r.text
                if not any(p in text for p in ALLOWED_PROTOCOLS):
                    decoded = decode_base64(text)
                    if decoded: text = decoded
                
                pattern = r'(?:' + '|'.join(ALLOWED_PROTOCOLS).replace('://', '') + r')://[^\s#"\'<>,]+'
                for cfg in re.findall(pattern, text): raw_configs.add(cfg)
                save_processed_source_hash(url)
            except Exception as e:
                print(f"  [!] Ошибка парсинга {url}: {e}")

    master_set = list(raw_configs)
    print(f"🔍 Формирование Мастер-Листа: Найдено {len(master_set)} уникальных конфигураций.")

    if not master_set:
        print("⚠️ Мастер-Лист пуст. Зачистка базы и выход.")
        sync_caches_with_master(set())
        save_and_organize([], [], [])
        git_commit_push()
        return

    # 3. Идеальное Зеркало: Очистка кэшей и состояний от удаленных конфигов
    state = sync_caches_with_master(set(master_set))

    # 4. Дробная проверка (Chunking): Берем 1/24 часть самых старых узлов
    chunk_size = max(500, len(master_set) // HOURS_TO_COMPLETE_CYCLE)
    
    # Сортируем: сначала те, у кого last_checked меньше (самые старые) или вообще 0 (новые)
    sorted_master = sorted(master_set, key=lambda c: state.get(c, {}).get('last_checked', 0))
    chunk_to_check = sorted_master[:chunk_size]
    
    print(f"⚖️ Чанкинг: Выбрано {len(chunk_to_check)} конфигов для проверки в этом часу.")

    # 5. Фаза TCP Ping для выбранного Чанка
    dead_configs_for_purge = []
    new_dead_nodes = set()
    global_seen = set()
    seen_lock = threading.Lock()
    
    valid_in_chunk = []
    
    print(f"⚡ Старт TCP Ping (Потоков: {THREAD_COUNT})...", flush=True)
    with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
        futures = [executor.submit(check_worker, c, seen_lock, global_seen) for c in chunk_to_check]
        for i, future in enumerate(as_completed(futures)):
            if SHOULD_EXIT: break
            try:
                res = future.result()
                if res:
                    status, nid, config = res
                    # Обновляем состояние конфига в памяти
                    if config not in state: state[config] = {}
                    state[config]['last_checked'] = now_ts
                    state[config]['status'] = status
                    
                    if status == "OK":
                        valid_in_chunk.append(config)
                    elif status == "FAIL":
                        new_dead_nodes.add(nid)
                        dead_configs_for_purge.append(config)
            except: continue
            
            if i > 0 and i % 500 == 0:
                print(f"  > Пинг: проверено {i}/{len(chunk_to_check)}...")

    # 6. Фаза GeoIP для выживших из Чанка
    if valid_in_chunk:
        print(f"🌍 GeoIP для живых узлов чанка ({GEOIP_PARALLEL_LEVEL} потоков)...", flush=True)
        # Ограничиваем на всякий случай
        queue = valid_in_chunk[:GEOIP_LIMIT_PER_RUN]
        
        with ThreadPoolExecutor(max_workers=GEOIP_PARALLEL_LEVEL) as geo_executor:
            geo_futures = [geo_executor.submit(geoip_parallel_worker, cfg) for cfg in queue]
            for i, f in enumerate(as_completed(geo_futures)):
                if SHOULD_EXIT: break
                try:
                    cfg, code = f.result()
                    state[cfg]['geoip'] = code
                except: continue

    # Сохраняем обновленные состояния
    save_state(state)

    # 7. DEEP PURGE: Физическое удаление мертвого мусора (только тех, кто провалился сейчас)
    if dead_configs_for_purge:
        print("🧹 Запуск системы глубокой зачистки (Deep Purge)...")
        save_persistent_blacklist(new_dead_nodes)
        deep_purge_files(dead_configs_for_purge)

    # 8. Атомарное сохранение и Пуш (На основе ВСЕГО Мастер-Листа, кто имеет статус OK)
    save_and_organize(master_set, state)
    generate_static_links()
    git_commit_push()
    
    gc.collect()
    print(f"\n🏁 ЦИКЛ УСПЕШНО ЗАВЕРШЕН ЗА {datetime.now() - start_time}.")

# ==============================================================================
# --- ДЕМОН-ПЕТЛЯ (DAEMON LOOP) С AUTO-TRIGGER ---
# ==============================================================================

def start_daemon():
    """Бесконечный цикл работы демона."""
    if os.path.exists(LOCK_FILE):
        print(f"[КРИТ] Обнаружен файл {LOCK_FILE}. Демон уже запущен или был прерван.")
        print("Удалите lock-файл вручную, если уверены, что процесс не дублируется.")
        return
        
    with open(LOCK_FILE, 'w') as f: f.write(str(os.getpid()))
    
    print(f"\n{'*'*70}")
    print(f"🛡️ VPN MONSTER DAEMON ЗАПУЩЕН И АКТИВЕН 🛡️")
    print(f"Интервал обновлений: {UPDATE_INTERVAL_HOURS} час (Дробное обновление базы)")
    print(f"Мониторинг файла '{ALL_SOURCES_FILE}' включен.")
    print(f"Нажмите Ctrl+C для безопасной остановки.")
    print(f"{'*'*70}\n")
    
    last_run_time = datetime.min
    last_sources_mod_time = get_file_mod_time(ALL_SOURCES_FILE)
    
    try:
        while not SHOULD_EXIT:
            now = datetime.now()
            trigger_reason = None
            
            # Проверка 1: Прошел ли 1 час?
            if now - last_run_time >= timedelta(hours=UPDATE_INTERVAL_HOURS):
                trigger_reason = f"Плановое обновление (Чанкинг {UPDATE_INTERVAL_HOURS}ч)"
            
            # Проверка 2: Изменил ли пользователь файл all_sources.txt?
            current_mod_time = get_file_mod_time(ALL_SOURCES_FILE)
            if current_mod_time > last_sources_mod_time:
                trigger_reason = f"Обнаружены изменения в {ALL_SOURCES_FILE}"
                last_sources_mod_time = current_mod_time
                
            if trigger_reason:
                run_update_cycle(trigger_reason)
                last_run_time = datetime.now()
                # Перечитываем mod_time на случай, если скрипт сам изменил файл (Deep Purge)
                last_sources_mod_time = get_file_mod_time(ALL_SOURCES_FILE)
                
                if SHOULD_EXIT: break
                print(f"\n💤 Демон перешел в режим ожидания. Следующий плановый запуск в {(last_run_time + timedelta(hours=UPDATE_INTERVAL_HOURS)).strftime('%H:%M:%S')}")
                print(f"👀 Мониторинг {ALL_SOURCES_FILE} продолжается...\n")
                
            time.sleep(WATCHER_INTERVAL_SEC)
            
    finally:
        if os.path.exists(LOCK_FILE):
            try: os.remove(LOCK_FILE)
            except: pass
        print("🛑 Демон остановлен корректно. Lock-файл удален.")

if __name__ == "__main__":
    try:
        socket.setdefaulttimeout(PORT_TIMEOUT)
        start_daemon()
    except Exception as e:
        print(f"\n[FATAL DEMON ERROR]: {e}")
        if os.path.exists(LOCK_FILE):
            try: os.remove(LOCK_FILE)
            except: pass
        sys.exit(1)

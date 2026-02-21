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

# ==============================================================================
# 🚀 VPN MONSTER ENGINE - ULTIMATE AUTO-CLEAN EDITION v5.0
# ==============================================================================

# --- НАСТРОЙКИ ТАЙМЕРОВ И ЛИМИТОВ ---
UPDATE_INTERVAL_HOURS = 1       # Интервал запуска
HOURS_TO_COMPLETE_CYCLE = 12    # Чанкинг для защиты GeoIP
WATCHER_INTERVAL_SEC = 2.0      # Проверка локальных изменений
PORT_TIMEOUT = 3.5              # Таймаут TCP Ping

# --- НАСТРОЙКИ ПОТОКОВ ---
THREAD_COUNT = 150              # Жесткая многопоточность для пинга
GEOIP_PARALLEL_LEVEL = 10       # Защита API GeoIP от бана

# --- ФАЙЛЫ ---
LOCK_FILE = "monster_daemon.lock"
ALL_SOURCES_FILE = "all_sources.txt"
MONSTER_STATE_FILE = "monster_state.json"

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
SHOULD_EXIT = False

# ==============================================================================
# --- БАЗОВЫЕ ФУНКЦИИ ---
# ==============================================================================

def signal_handler(sig, frame):
    global SHOULD_EXIT
    print("\n[!] Остановка процесса...", flush=True)
    SHOULD_EXIT = True
    if os.path.exists(LOCK_FILE):
        try: os.remove(LOCK_FILE)
        except: pass

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def get_random_ua():
    return random.choice([
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ])

def atomic_save(filepath, content):
    tmp_file = f"{filepath}.tmp"
    try:
        with open(tmp_file, 'w', encoding='utf-8') as f: f.write(content)
        os.replace(tmp_file, filepath)
    except Exception as e:
        print(f"[ERROR] Ошибка сохранения {filepath}: {e}")

def get_file_mod_time(filepath):
    try: return os.path.getmtime(filepath) if os.path.exists(filepath) else 0
    except: return 0

def decode_base64(data):
    try:
        data = re.sub(r'[^a-zA-Z0-9+/=]', '', data)
        if not data: return ""
        missing_padding = len(data) % 4
        if missing_padding: data += '=' * (4 - missing_padding)
        return base64.b64decode(data).decode('utf-8', errors='ignore')
    except: return ""

def encode_base64(data):
    try: return base64.b64encode(data.encode('utf-8')).decode('utf-8')
    except: return ""

# ==============================================================================
# --- БРОНЕБОЙНЫЙ ПАРСЕР ---
# ==============================================================================

def extract_all_configs(text):
    """Агрессивный экстрактор: достает ссылки из текста, логов, json и Base64."""
    configs = []
    # 1. Ищем прямые ссылки
    pattern = r'(?:' + '|'.join(ALLOWED_PROTOCOLS).replace('://', '') + r')://[^\s#"\'<>,]+'
    found = re.findall(pattern, text)
    if found: configs.extend(found)
    
    # 2. Пробуем раскодировать построчно, если это Base64
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith('http') or '://' in line: continue
        decoded = decode_base64(line)
        if decoded and any(p in decoded for p in ALLOWED_PROTOCOLS):
            configs.extend(re.findall(pattern, decoded))
            
    return list(set(configs))

def get_server_info(config):
    try:
        clean_config = config.split('#')[0].strip()
        if clean_config.startswith("vmess://"):
            decoded = decode_base64(clean_config[8:])
            if decoded:
                v_data = json.loads(decoded)
                return str(v_data.get('add', '')).strip(), str(v_data.get('port', '')).strip()
        match = re.search(r'://(?:[^@]+@)?([^:/#\?]+):(\d+)', clean_config)
        if match: return match.group(1).strip(), match.group(2).strip()
    except: pass
    return None, None

def beautify_config(config, country_key=None, fallback_code="UN"):
    try:
        if country_key and country_key in COUNTRIES:
            info = COUNTRIES[country_key]
            label = f"❤️ {info['flag']} {info['name']} | {info['code']} {info['flag']} ❤️"
        else:
            label = f"❤️ 🌍 Global | {fallback_code if fallback_code else 'UN'} 🌍 ❤️"
            
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
    except: return config

# ==============================================================================
# --- СЕТЬ И GEOIP ---
# ==============================================================================

def is_node_alive(host, port, timeout=PORT_TIMEOUT):
    if not host or not port: return False
    if host.startswith(('127.', '192.168.', '10.', '0.')) or host == 'localhost': return False
    try:
        with socket.create_connection((host, int(port)), timeout=timeout): return True
    except: return False

def check_ip_location_smart(host):
    if SHOULD_EXIT: return "UN"
    time.sleep(random.uniform(0.1, 0.4))
    
    providers = [
        lambda h: requests.get(f"http://ip-api.com/json/{h}", timeout=4).json().get("countryCode"),
        lambda h: requests.get(f"https://ipwho.is/{h}", timeout=4).json().get("country_code"),
        lambda h: requests.get(f"https://freeipapi.com/api/json/{h}", timeout=4).json().get("countryCode"),
        lambda h: requests.get(f"https://ipapi.co/{h}/json/", timeout=4, headers={'User-Agent': get_random_ua()}).json().get("country_code")
    ]
    random.shuffle(providers)
    
    for provider in providers:
        if SHOULD_EXIT: break
        try:
            code = provider(host)
            if code and len(str(code)) == 2: return str(code).upper()
        except: continue
    return "UN"

# ==============================================================================
# --- ПАМЯТЬ СОСТОЯНИЙ ---
# ==============================================================================

def load_state():
    if os.path.exists(MONSTER_STATE_FILE):
        try:
            with open(MONSTER_STATE_FILE, 'r', encoding='utf-8') as f: return json.load(f)
        except: return {}
    return {}

def save_state(state):
    atomic_save(MONSTER_STATE_FILE, json.dumps(state, indent=2))

def check_worker(config, seen_lock, global_seen):
    h, p = get_server_info(config)
    if not h or not p: return None
    nid = f"{h}:{p}"
    
    with seen_lock:
        if nid in global_seen: return None
        global_seen.add(nid)
        
    if is_node_alive(h, p): return ("OK", nid, config)
    else: return ("FAIL", nid, config)

# ==============================================================================
# --- СИНХРОНИЗАЦИЯ И ЗАЧИСТКА ---
# ==============================================================================

def rewrite_all_sources(external_links, alive_configs):
    """
    МАГИЯ АВТО-ОЧИСТКИ: Перезаписывает all_sources.txt.
    Оставляет внешние http-ссылки и ТОЛЬКО те ручные конфиги, которые прошли TCP-пинг.
    """
    lines = ["# 🚀 VPN MONSTER - АВТОМАТИЧЕСКИЙ МАСТЕР-ЛИСТ"]
    lines.append(f"# Последнее обновление: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("# Мертвые узлы удаляются отсюда автоматически.\n")
    
    if external_links:
        lines.append("# --- ВНЕШНИЕ ИСТОЧНИКИ ПОДПИСОК ---")
        lines.extend(external_links)
        lines.append("")
        
    if alive_configs:
        lines.append("# --- АКТИВНЫЕ ЛОКАЛЬНЫЕ КОНФИГИ ---")
        lines.extend(alive_configs)
        
    atomic_save(ALL_SOURCES_FILE, "\n".join(lines) + "\n")
    print("🧹 Файл all_sources.txt очищен от мертвецов и пересобран!")

def save_and_organize(master_set, state):
    structured = {c: [] for c in COUNTRIES}
    final_mix, failed_list = [], []

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
            if not matched: final_mix.append(beautify_config(cfg, None, fallback_code=code))
        elif status == 'FAIL':
            failed_list.append(cfg)

    # Запись по странам
    for country in COUNTRIES:
        valid = sorted(list(set(structured[country])))
        atomic_save(f"{country}.txt", "\n".join(valid) if valid else f"# No active nodes for {country}\n")

    valid_mix = sorted(list(set(final_mix)))
    atomic_save("mix.txt", "\n".join(valid_mix) if valid_mix else "# No active nodes found\n")
    atomic_save("sub_monster.txt", encode_base64("\n".join(valid_mix)) if valid_mix else "")
    
    valid_failed = sorted(list(set(failed_list)))
    atomic_save("failed_nodes.txt", "\n".join(valid_failed) if valid_failed else "# No failed nodes\n")

def git_commit_push():
    print("\n[Git Sync] Синхронизация файлов с GitHub...", flush=True)
    try:
        subprocess.run(["git", "config", "--local", "user.name", "Monster-Ultra-Daemon"], check=True)
        subprocess.run(["git", "config", "--local", "user.email", "daemon@vpn-monster.com"], check=True)
        subprocess.run(["git", "add", "*.txt"], check=True)
        
        if not subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True).stdout.strip():
            print("[Git Sync] Изменений нет.")
            return

        ts = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
        subprocess.run(["git", "commit", "-m", f"⚡ Auto-Sync Monster: {ts}"], check=True)
        subprocess.run(["git", "push", "origin", "main", "--force"], check=True)
        print(f"[Git Sync] ✅ Обновлено!")
    except Exception as e: print(f"[Git Sync] ❌ Ошибка: {e}")

# ==============================================================================
# --- ГЛАВНЫЙ ДВИЖОК ---
# ==============================================================================

def run_update_cycle(trigger_reason="Таймер"):
    start_time = datetime.now()
    now_ts = start_time.timestamp()
    
    print(f"\n{'='*70}")
    print(f"🔥 ЗАПУСК MONSTER ENGINE | Причина: {trigger_reason}")
    print(f"{'='*70}\n")
    
    state = load_state()
    
    local_raw_configs = []
    external_links = []
    downloaded_configs = []
    
    # 1. Читаем all_sources.txt
    if os.path.exists(ALL_SOURCES_FILE):
        with open(ALL_SOURCES_FILE, 'r', encoding='utf-8') as f:
            content = f.read()
            # Достаем ссылки
            for line in content.splitlines():
                l = line.strip()
                if l.startswith('http'): external_links.append(l)
            
            # Достаем локальные конфиги из файла
            local_raw_configs = extract_all_configs(content)
                    
    # 2. Скачиваем конфиги по внешним ссылкам
    if external_links:
        print(f"📡 Загрузка данных из {len(external_links)} внешних ссылок...")
        for url in set(external_links):
            if SHOULD_EXIT: break
            try:
                r = requests.get(url, timeout=15, headers={'User-Agent': get_random_ua()})
                found = extract_all_configs(r.text)
                downloaded_configs.extend(found)
            except Exception as e: 
                print(f"   [!] Ошибка загрузки {url}: {e}")

    # Объединяем всё в Мастер-Лист
    master_set = list(set(local_raw_configs + downloaded_configs))
    print(f"🔍 Собрано конфигураций: Локальных: {len(local_raw_configs)}, Из сети: {len(downloaded_configs)}")
    print(f"🔍 ВСЕГО УНИКАЛЬНЫХ ДЛЯ ПРОВЕРКИ: {len(master_set)}")

    # ЗАЩИТА ОТ УДАЛЕНИЯ ПРИ ОШИБКЕ: Если парсер ничего не нашел - стоп!
    if not master_set and (local_raw_configs or external_links):
        print("⚠️ ВНИМАНИЕ: Источники есть, но конфиги не найдены. Остановка для защиты базы от затирания!")
        return

    # Зеркалирование State (удаляем призраков)
    keys_to_delete = [cfg for cfg in state.keys() if cfg not in master_set]
    for k in keys_to_delete: del state[k]

    # Чанкинг (Очередь)
    new_configs = [c for c in master_set if c not in state]
    old_configs = sorted([c for c in master_set if c in state], key=lambda c: state[c].get('last_checked', 0))
    
    chunk_size = max(500, len(master_set) // HOURS_TO_COMPLETE_CYCLE)
    if os.environ.get("GITHUB_ACTIONS") == "true": chunk_size = len(master_set)
    
    chunk_to_check = list(dict.fromkeys(new_configs + old_configs[:chunk_size]))
    print(f"⚡ Очередь проверки: {len(chunk_to_check)} конфигов (Потоков: {THREAD_COUNT})...")

    # TCP Ping
    valid_in_chunk = []
    global_seen = set()
    seen_lock = threading.Lock()
    
    with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
        futures = [executor.submit(check_worker, c, seen_lock, global_seen) for c in chunk_to_check]
        for future in as_completed(futures):
            if SHOULD_EXIT: break
            try:
                res = future.result()
                if res:
                    status, nid, config = res
                    if config not in state: state[config] = {}
                    state[config]['last_checked'] = now_ts
                    state[config]['status'] = status
                    if status == "OK": valid_in_chunk.append(config)
            except: continue

    # GeoIP (Только для живых без локации)
    nodes_for_geoip = [cfg for cfg in valid_in_chunk if state[cfg].get('geoip', 'UN') == 'UN']
    if nodes_for_geoip:
        print(f"🌍 GeoIP для {len(nodes_for_geoip)} новых узлов ({GEOIP_PARALLEL_LEVEL} потоков)...", flush=True)
        with ThreadPoolExecutor(max_workers=GEOIP_PARALLEL_LEVEL) as geo_executor:
            geo_futures = [geo_executor.submit(lambda cfg: (cfg, check_ip_location_smart(get_server_info(cfg)[0])), cfg) for cfg in nodes_for_geoip]
            for f in as_completed(geo_futures):
                if SHOULD_EXIT: break
                try:
                    cfg, code = f.result()
                    state[cfg]['geoip'] = code
                except: continue

    save_state(state)

    # --- ПЕРЕЗАПИСЬ ALL_SOURCES.TXT (АВТО-УДАЛЕНИЕ) ---
    # Мы фильтруем local_raw_configs, оставляя только те, которые сейчас имеют статус OK
    alive_local_configs = [cfg for cfg in local_raw_configs if state.get(cfg, {}).get('status') == 'OK']
    rewrite_all_sources(list(set(external_links)), list(set(alive_local_configs)))

    # Сохраняем финальные файлы стран и GitHub push
    save_and_organize(master_set, state)
    git_commit_push()
    
    gc.collect()
    print(f"\n🏁 ЦИКЛ ЗАВЕРШЕН ЗА {datetime.now() - start_time}.")

# ==============================================================================
# --- СТАРТ ---
# ==============================================================================

def start_daemon():
    if os.environ.get("GITHUB_ACTIONS") == "true":
        print("\n[GITHUB ACTIONS] Запуск полного цикла...")
        run_update_cycle("Автоматический запуск GitHub Actions")
        return

    if os.path.exists(LOCK_FILE): return
    with open(LOCK_FILE, 'w') as f: f.write(str(os.getpid()))
    
    print(f"\n🛡️ VPN MONSTER DAEMON 5.0 АКТИВЕН 🛡️\n")
    last_run_time, last_sources_mod_time = datetime.min, get_file_mod_time(ALL_SOURCES_FILE)
    
    try:
        while not SHOULD_EXIT:
            now, trigger_reason = datetime.now(), None
            if now - last_run_time >= timedelta(hours=UPDATE_INTERVAL_HOURS): trigger_reason = "Таймер"
            
            curr_mod = get_file_mod_time(ALL_SOURCES_FILE)
            if curr_mod > last_sources_mod_time:
                trigger_reason = f"Изменение {ALL_SOURCES_FILE}"
                last_sources_mod_time = curr_mod
            
            if trigger_reason:
                run_update_cycle(trigger_reason)
                last_run_time = datetime.now()
                last_sources_mod_time = get_file_mod_time(ALL_SOURCES_FILE)
                
            time.sleep(WATCHER_INTERVAL_SEC)
    finally:
        if os.path.exists(LOCK_FILE):
            try: os.remove(LOCK_FILE)
            except: pass

if __name__ == "__main__":
    try:
        socket.setdefaulttimeout(PORT_TIMEOUT)
        start_daemon()
    except Exception as e:
        print(f"\n[FATAL ERROR]: {e}")
        if os.path.exists(LOCK_FILE):
            try: os.remove(LOCK_FILE)
            except: pass
        sys.exit(1)

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
# 🚀 VPN MONSTER ENGINE - ULTIMATE v6.0 (SMART PARSER & AUTO-CLEAN)
# ==============================================================================

# --- НАСТРОЙКИ ---
UPDATE_INTERVAL_HOURS = 1       # Как часто GitHub Actions или Демон запускает цикл
HOURS_TO_COMPLETE_CYCLE = 12    # На сколько частей делить старую базу
PORT_TIMEOUT = 3.5              # Таймаут TCP проверки (в секундах)
THREAD_COUNT = 150              # Жесткая многопоточность для быстрого пинга
GEOIP_PARALLEL_LEVEL = 10       # Строго 10 потоков для GeoIP (чтобы не забанили API)

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

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def get_random_ua():
    return random.choice([
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
    ])

def atomic_save(filepath, content):
    """Безопасное сохранение файлов."""
    tmp_file = f"{filepath}.tmp"
    try:
        with open(tmp_file, 'w', encoding='utf-8') as f: f.write(content)
        os.replace(tmp_file, filepath)
    except Exception as e:
        print(f"[ERROR] Ошибка сохранения {filepath}: {e}")

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
# --- СУПЕР-ПАРСЕР ---
# ==============================================================================

def extract_urls(text):
    """Вытаскивает ссылки http/https из любого мусора (скобки, тире, пробелы)."""
    urls = re.findall(r'https?://[a-zA-Z0-9\-\.\_\~\:\/\?\#\[\]\@\!\$\&\'\(\)\*\+\,\;\=\%]+', text)
    clean_urls = set()
    for u in urls:
        # Убираем случайные закрывающие скобки или кавычки в конце ссылки
        u = u.rstrip('.,;)\'"]')
        clean_urls.add(u)
    return list(clean_urls)

def extract_all_configs(text):
    """Агрессивный экстрактор: достает сырые конфиги даже из зашифрованного Base64."""
    configs = set()
    pattern = r'(?:' + '|'.join(ALLOWED_PROTOCOLS).replace('://', '') + r')://[^\s<>"\'\[\]]+'
    
    # 1. Прямой поиск в тексте
    for f in re.findall(pattern, text): configs.add(f)
    
    # 2. Поиск внутри Base64
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith('http') or '://' in line: continue
        decoded = decode_base64(line)
        if decoded and any(p in decoded for p in ALLOWED_PROTOCOLS):
            for f in re.findall(pattern, decoded): configs.add(f)
            
    return list(configs)

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

def check_worker(config, seen_lock, global_seen):
    h, p = get_server_info(config)
    if not h or not p: return ("INVALID", "invalid", config)
    nid = f"{h}:{p}"
    
    with seen_lock:
        if nid in global_seen: return ("DUPLICATE", nid, config)
        global_seen.add(nid)
        
    if is_node_alive(h, p): return ("OK", nid, config)
    else: return ("FAIL", nid, config)

# ==============================================================================
# --- ПАМЯТЬ И ФАЙЛОВЫЕ ОПЕРАЦИИ ---
# ==============================================================================

def load_state():
    if os.path.exists(MONSTER_STATE_FILE):
        try:
            with open(MONSTER_STATE_FILE, 'r', encoding='utf-8') as f: return json.load(f)
        except: return {}
    return {}

def save_state(state):
    atomic_save(MONSTER_STATE_FILE, json.dumps(state, indent=2))

def rewrite_all_sources(links, pending_and_alive_configs):
    """
    Авто-очистка: Перезаписывает all_sources.txt. Удаляет все дубликаты.
    Оставляет только рабочие конфиги и уникальные ссылки.
    """
    lines = [
        "# 🚀 VPN MONSTER - АВТОМАТИЧЕСКИЙ МАСТЕР-ЛИСТ",
        f"# Последнее обновление: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        "# Сюда можно кидать ссылки и конфиги в любом формате. Бот сам отфильтрует дубликаты и мертвецов.\n"
    ]
    
    if links:
        lines.append("# --- ВНЕШНИЕ ИСТОЧНИКИ ПОДПИСОК ---")
        lines.extend(sorted(list(set(links))))
        lines.append("")
        
    if pending_and_alive_configs:
        lines.append("# --- АКТИВНЫЕ И НОВЫЕ ЛОКАЛЬНЫЕ КОНФИГИ ---")
        lines.extend(sorted(list(set(pending_and_alive_configs))))
        lines.append("")
        
    atomic_save(ALL_SOURCES_FILE, "\n".join(lines))
    print("\n🧹 Файл all_sources.txt очищен от мусора и дубликатов. Идеальный порядок!")

def save_and_organize(master_set, state):
    """Распределяет живые конфиги по странам."""
    structured = {c: [] for c in COUNTRIES}
    final_mix, failed_list = [], []
    
    alive_count = 0

    for cfg in master_set:
        cfg_state = state.get(cfg, {})
        status = cfg_state.get('status')
        
        if status == 'OK':
            alive_count += 1
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

    print(f"\n📁 РАСПРЕДЕЛЕНИЕ ПО СТРАНАМ (Всего активных: {alive_count}):")
    for country in COUNTRIES:
        valid = sorted(list(set(structured[country])))
        atomic_save(f"{country}.txt", "\n".join(valid) if valid else f"# No active nodes for {country}\n")
        if valid: print(f"   > {COUNTRIES[country]['flag']} {country.upper()}: {len(valid)} узлов")

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
    print(f"🔥 ЗАПУСК MONSTER ENGINE v6.0 | Причина: {trigger_reason}")
    print(f"{'='*70}\n")
    
    state = load_state()
    
    local_raw_configs = []
    external_links = []
    downloaded_configs = []
    
    # 1. Читаем all_sources.txt суровым парсером
    print("🔍 Анализ локального файла all_sources.txt...", flush=True)
    if os.path.exists(ALL_SOURCES_FILE):
        with open(ALL_SOURCES_FILE, 'r', encoding='utf-8') as f:
            content = f.read()
            external_links = extract_urls(content)
            local_raw_configs = extract_all_configs(content)
            print(f"   > Найдено ссылок на подписки: {len(external_links)}")
            print(f"   > Найдено локальных конфигов: {len(local_raw_configs)}")
                    
    # 2. Скачиваем конфиги по внешним ссылкам
    if external_links:
        print(f"\n📡 Скачивание баз по {len(external_links)} внешним ссылкам...", flush=True)
        for url in external_links:
            if SHOULD_EXIT: break
            try:
                r = requests.get(url, timeout=15, headers={'User-Agent': get_random_ua()})
                found = extract_all_configs(r.text)
                downloaded_configs.extend(found)
                print(f"   > [OK] Ссылка обработана (найдено {len(found)} конфигов)")
            except Exception as e: 
                print(f"   > [FAIL] Ошибка загрузки {url}")

    # 3. Формируем Мастер-Лист
    master_set = list(set(local_raw_configs + downloaded_configs))
    print(f"\n⚖️ ИТОГО УНИКАЛЬНЫХ УЗЛОВ ВО ВСЕХ ИСТОЧНИКАХ: {len(master_set)}")

    if not master_set:
        print("⚠️ База пуста. Останавливаем работу для защиты от удаления файлов.")
        return

    # Зеркалирование State (удаляем призраков)
    keys_to_delete = [cfg for cfg in state.keys() if cfg not in master_set]
    for k in keys_to_delete: del state[k]

    # 4. Чанкинг: Все новые проверяем СРАЗУ, старые делим на части
    new_configs = [c for c in master_set if c not in state]
    old_configs = sorted([c for c in master_set if c in state], key=lambda c: state[c].get('last_checked', 0))
    
    chunk_size = max(500, len(master_set) // HOURS_TO_COMPLETE_CYCLE)
    if os.environ.get("GITHUB_ACTIONS") == "true": chunk_size = len(master_set) # В Actions чекаем всё
    
    chunk_to_check = list(dict.fromkeys(new_configs + old_configs[:chunk_size]))
    print(f"\n⚡ ОЧЕРЕДЬ ПРОВЕРКИ (TCP PING):")
    print(f"   > Новых конфигов: {len(new_configs)}")
    print(f"   > Старых конфигов (плановый чек): {len(chunk_to_check) - len(new_configs)}")
    print(f"   > ВСЕГО В ОЧЕРЕДИ: {len(chunk_to_check)} (Потоков: {THREAD_COUNT})")

    # TCP Ping
    valid_in_chunk, failed_in_chunk, duplicate_in_chunk = [], [], 0
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
                    if status == "DUPLICATE":
                        duplicate_in_chunk += 1
                        continue
                        
                    if config not in state: state[config] = {}
                    state[config]['last_checked'] = now_ts
                    state[config]['status'] = status
                    
                    if status == "OK": valid_in_chunk.append(config)
                    elif status == "FAIL": failed_in_chunk.append(config)
            except: continue

    print(f"   > [OK] Прошли TCP проверку: {len(valid_in_chunk)}")
    print(f"   > [FAIL] Мертвые (отключены): {len(failed_in_chunk)}")
    if duplicate_in_chunk > 0: print(f"   > [ДУБЛИКАТЫ] Вырезано: {duplicate_in_chunk}")

    # GeoIP (Только для живых без локации)
    nodes_for_geoip = [cfg for cfg in valid_in_chunk if state[cfg].get('geoip', 'UN') == 'UN']
    if nodes_for_geoip:
        print(f"\n🌍 Запуск GeoIP для {len(nodes_for_geoip)} новых узлов ({GEOIP_PARALLEL_LEVEL} потоков)...", flush=True)
        with ThreadPoolExecutor(max_workers=GEOIP_PARALLEL_LEVEL) as geo_executor:
            geo_futures = [geo_executor.submit(lambda cfg: (cfg, check_ip_location_smart(get_server_info(cfg)[0])), cfg) for cfg in nodes_for_geoip]
            for f in as_completed(geo_futures):
                if SHOULD_EXIT: break
                try:
                    cfg, code = f.result()
                    state[cfg]['geoip'] = code
                except: continue

    save_state(state)

    # 5. Перезапись ALL_SOURCES.TXT
    # Оставляем локальные конфиги, которые:
    # 1. Либо прошли проверку (OK)
    # 2. Либо ЕЩЕ НЕ ПРОВЕРЯЛИСЬ (Pending) - чтобы не удалить их раньше времени
    alive_or_pending_local = []
    for cfg in local_raw_configs:
        st = state.get(cfg, {}).get('status')
        if st != 'FAIL': alive_or_pending_local.append(cfg)
        
    rewrite_all_sources(external_links, alive_or_pending_local)

    # 6. Сохраняем файлы стран и пушим
    save_and_organize(master_set, state)
    git_commit_push()
    
    gc.collect()
    print(f"\n🏁 ЦИКЛ ЗАВЕРШЕН ЗА {datetime.now() - start_time}.")

# ==============================================================================
# --- СТАРТ ---
# ==============================================================================

if __name__ == "__main__":
    try:
        socket.setdefaulttimeout(PORT_TIMEOUT)
        if os.environ.get("GITHUB_ACTIONS") == "true":
            print("\n[GITHUB ACTIONS] Запуск полного цикла...")
            run_update_cycle("Автоматический запуск GitHub Actions")
        else:
            run_update_cycle("Ручной запуск")
    except Exception as e:
        print(f"\n[FATAL ERROR]: {e}")
        sys.exit(1)

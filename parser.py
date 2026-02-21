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
# 🚀 VPN MONSTER ENGINE - ULTIMATE UNIVERSAL DAEMON
# ==============================================================================

# --- НАСТРОЙКИ ТАЙМЕРОВ И ЛИМИТОВ ---
UPDATE_INTERVAL_HOURS = 1       # Как часто запускать плановый цикл проверки (в часах)
HOURS_TO_COMPLETE_CYCLE = 12    # За сколько часов нужно прогнать ВСЮ базу (защита от бана GeoIP)
WATCHER_INTERVAL_SEC = 2.0      # Как часто проверять файл all_sources.txt на твои изменения
PORT_TIMEOUT = 3.5              # Таймаут TCP Ping (в секундах)
BLACKLIST_BAIL_DAYS = 3         # Сколько дней хранить мертвые узлы в блэклисте (чтобы не парсить их снова)

# --- НАСТРОЙКИ ПОТОКОВ ---
THREAD_COUNT = 150              # Жесткая многопоточность для быстрого TCP Ping
GEOIP_PARALLEL_LEVEL = 10       # Строго 10 потоков для GeoIP (чтобы не заблокировали API)

# --- ФАЙЛОВАЯ СИСТЕМА ---
LOCK_FILE = "monster_daemon.lock"
PERSISTENT_BLACKLIST = "persistent_blacklist.txt"
ALL_SOURCES_FILE = "all_sources.txt"
MONSTER_STATE_FILE = "monster_state.json"  # Умная память: кто когда проверялся и статус

# --- КОНФИГУРАЦИЯ СТРАН (PREMIUM MIRROR DESIGN) ---
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

# --- ГЛОБАЛЬНЫЕ БЛОКИРОВКИ ---
BLACKLIST_CACHE = set()
BLACKLIST_LOCK = threading.Lock()
SHOULD_EXIT = False

# ==============================================================================
# --- БАЗОВЫЕ ФУНКЦИИ ---
# ==============================================================================

def signal_handler(sig, frame):
    global SHOULD_EXIT
    print("\n[!] Получен сигнал остановки. Завершаем работу...", flush=True)
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
    """Безопасное сохранение файлов (защита от повреждения при сбое)."""
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

def get_server_info(config):
    """Достает IP и Порт не ломая конфиг."""
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
    """Добавляет флаги и сердечки."""
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
    """Случайный выбор API для обхода лимитов."""
    if SHOULD_EXIT: return "UN"
    time.sleep(random.uniform(0.1, 0.5))
    
    providers = [
        lambda h: requests.get(f"http://ip-api.com/json/{h}", timeout=4).json().get("countryCode"),
        lambda h: requests.get(f"https://ipwho.is/{h}", timeout=4).json().get("country_code"),
        lambda h: requests.get(f"https://freeipapi.com/api/json/{h}", timeout=4).json().get("countryCode"),
        lambda h: requests.get(f"https://ipapi.co/{h}/json/", timeout=4, headers={'User-Agent': get_random_ua()}).json().get("country_code"),
        lambda h: requests.get(f"https://ipapi.is/json/{h}", timeout=4).json().get("location", {}).get("country_code")
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
# --- ПАМЯТЬ И ГЛУБОКАЯ ЗАЧИСТКА (DEEP PURGE) ---
# ==============================================================================

def load_state():
    if os.path.exists(MONSTER_STATE_FILE):
        try:
            with open(MONSTER_STATE_FILE, 'r', encoding='utf-8') as f: return json.load(f)
        except: return {}
    return {}

def save_state(state):
    atomic_save(MONSTER_STATE_FILE, json.dumps(state, indent=2))

def load_persistent_blacklist():
    bl = set()
    if os.path.exists(PERSISTENT_BLACKLIST):
        try:
            with open(PERSISTENT_BLACKLIST, 'r') as f:
                for line in f:
                    parts = line.strip().split('|')
                    if len(parts) >= 2:
                        try:
                            if datetime.now() - datetime.fromisoformat(parts[1]) < timedelta(days=BLACKLIST_BAIL_DAYS):
                                bl.add(parts[0])
                        except: pass
        except: pass
    with BLACKLIST_LOCK:
        global BLACKLIST_CACHE
        BLACKLIST_CACHE = bl.copy()

def save_persistent_blacklist(new_dead_nodes):
    now_str = datetime.now().isoformat()
    with BLACKLIST_LOCK:
        for node in new_dead_nodes: BLACKLIST_CACHE.add(f"{node}|{now_str}")
    valid = [i if '|' in i else f"{i}|{now_str}" for i in BLACKLIST_CACHE]
    atomic_save(PERSISTENT_BLACKLIST, "\n".join(valid) + "\n")

def deep_purge_files(dead_configs):
    """Удаляет мертвецов ПРЯМО из исходного файла (включая Base64)"""
    if not dead_configs: return
    dead_set = set([c.strip() for c in dead_configs])
    purged_total = 0
    
    if os.path.exists(ALL_SOURCES_FILE):
        try:
            with open(ALL_SOURCES_FILE, 'r', encoding='utf-8') as f: lines = f.readlines()
            clean_lines, file_changed = [], False
            
            for line in lines:
                l_strip = line.strip()
                if not l_strip or l_strip.startswith('#') or l_strip.startswith('http'):
                    clean_lines.append(line)
                    continue
                
                # Если строка это Base64 подписка
                if not any(p in l_strip for p in ALLOWED_PROTOCOLS):
                    decoded = decode_base64(l_strip)
                    if decoded and any(p in decoded for p in ALLOWED_PROTOCOLS):
                        configs_in_b64 = decoded.splitlines()
                        clean_b64 = [cfg for cfg in configs_in_b64 if cfg.strip() not in dead_set]
                        if len(clean_b64) != len(configs_in_b64):
                            file_changed = True
                            purged_total += (len(configs_in_b64) - len(clean_b64))
                            if clean_b64: clean_lines.append(encode_base64("\n".join(clean_b64)) + "\n")
                        else: clean_lines.append(line)
                        continue

                # Если это обычная строка с конфигом
                if l_strip in dead_set:
                    file_changed = True
                    purged_total += 1
                else: clean_lines.append(line)
                    
            if file_changed: atomic_save(ALL_SOURCES_FILE, "".join(clean_lines))
        except Exception as e: print(f"[ERROR] Сбой зачистки: {e}")
            
    if purged_total > 0: print(f"🗑️ DEEP PURGE: Физически удалено {purged_total} мертвых конфигов из базы.")

# ==============================================================================
# --- ВОРКЕРЫ ДЛЯ ПРОВЕРКИ ---
# ==============================================================================

def check_worker(config, seen_lock, global_seen):
    h, p = get_server_info(config)
    if not h or not p: return None
    nid = f"{h}:{p}"
    
    with BLACKLIST_LOCK:
        if any(b.startswith(nid) for b in BLACKLIST_CACHE): return ("FAIL", nid, config)
            
    with seen_lock:
        if nid in global_seen: return None
        global_seen.add(nid)
        
    if is_node_alive(h, p): return ("OK", nid, config)
    else: return ("FAIL", nid, config)

# ==============================================================================
# --- ФИНАЛИЗАЦИЯ: GIT И СОХРАНЕНИЕ ---
# ==============================================================================

def save_and_organize(master_set, state):
    """Идеальное Зеркало: сохраняются ТОЛЬКО живые узлы из state."""
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

    # Принудительная перезапись файлов стран (если пустые - затираются)
    for country in COUNTRIES:
        valid = sorted(list(set(structured[country])))
        atomic_save(f"{country}.txt", "\n".join(valid) if valid else f"# No active nodes for {country}\n")

    valid_mix = sorted(list(set(final_mix)))
    atomic_save("mix.txt", "\n".join(valid_mix) if valid_mix else "# No active nodes found\n")
    atomic_save("sub_monster.txt", encode_base64("\n".join(valid_mix)) if valid_mix else "")
    
    valid_failed = sorted(list(set(failed_list)))
    atomic_save("failed_nodes.txt", "\n".join(valid_failed) if valid_failed else "# No failed nodes\n")

def git_commit_push():
    print("\n[Git Sync] Синхронизация файлов с облаком...", flush=True)
    try:
        subprocess.run(["git", "config", "--local", "user.name", "Monster-Ultra-Daemon"], check=True)
        subprocess.run(["git", "config", "--local", "user.email", "daemon@vpn-monster.com"], check=True)
        subprocess.run(["git", "add", "*.txt"], check=True)
        
        if not subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True).stdout.strip():
            print("[Git Sync] Изменений нет.")
            return

        ts = datetime.now().strftime('%d/%m/%Y %H:%M:%S')
        subprocess.run(["git", "commit", "-m", f"⚡ Auto-Sync Monster: {ts}"], check=True)
        
        if subprocess.run(["git", "push", "origin", "main"], capture_output=True).returncode != 0:
            print("[Git Sync] Конфликт. Принудительный пуш (--force)...")
            subprocess.run(["git", "push", "origin", "main", "--force"], check=True)
        print(f"[Git Sync] ✅ Подписки клиентов успешно обновлены!")
    except Exception as e: print(f"[Git Sync] ❌ Ошибка: {e}")

# ==============================================================================
# --- ГЛАВНЫЙ ПРОЦЕСС ОБНОВЛЕНИЯ ---
# ==============================================================================

def run_update_cycle(trigger_reason="Таймер"):
    start_time = datetime.now()
    now_ts = start_time.timestamp()
    
    print(f"\n{'='*70}")
    print(f"🔥 ЗАПУСК ЦИКЛА MONSTER ENGINE | Причина: {trigger_reason}")
    print(f"{'='*70}\n")
    
    load_persistent_blacklist()
    state = load_state()
    
    raw_configs = set()
    links_to_download = []
    
    # 1. Читаем all_sources.txt (Сырые данные + Внешние ссылки)
    if os.path.exists(ALL_SOURCES_FILE):
        with open(ALL_SOURCES_FILE, 'r', encoding='utf-8') as f:
            for line in f:
                l_strip = line.strip()
                if not l_strip or l_strip.startswith('#'): continue
                
                if l_strip.startswith('http'): links_to_download.append(l_strip)
                elif any(p in l_strip for p in ALLOWED_PROTOCOLS): raw_configs.add(l_strip)
                else:
                    decoded = decode_base64(l_strip)
                    if decoded and any(p in decoded for p in ALLOWED_PROTOCOLS):
                        for cfg in decoded.splitlines():
                            if any(p in cfg for p in ALLOWED_PROTOCOLS): raw_configs.add(cfg.strip())
                    
    # 2. Скачиваем ВСЕ ссылки КАЖДЫЙ РАЗ (Никакого кэширования ссылок!)
    if links_to_download:
        print(f"📡 Загрузка свежих данных из {len(links_to_download)} внешних источников...")
        for url in set(links_to_download):
            if SHOULD_EXIT: break
            try:
                r = requests.get(url, timeout=15, headers={'User-Agent': get_random_ua()})
                text = r.text
                if not any(p in text for p in ALLOWED_PROTOCOLS):
                    decoded = decode_base64(text)
                    if decoded: text = decoded
                pattern = r'(?:' + '|'.join(ALLOWED_PROTOCOLS).replace('://', '') + r')://[^\s#"\'<>,]+'
                for cfg in re.findall(pattern, text): raw_configs.add(cfg)
            except: continue

    master_set = list(raw_configs)
    print(f"🔍 Мастер-Лист: Найдено {len(master_set)} уникальных конфигураций.")

    # 3. Идеальное Зеркало: Удаляем из state призраков
    keys_to_delete = [cfg for cfg in state.keys() if cfg not in master_set]
    for k in keys_to_delete: del state[k]
    if keys_to_delete: print(f"🧹 Удалено {len(keys_to_delete)} несуществующих узлов (Зеркалирование).")

    if not master_set:
        save_state({})
        save_and_organize([], {})
        git_commit_push()
        return

    # 4. УМНЫЙ ЧАНКИНГ (Приоритет НОВЫМ конфигам, старые бьются на 12 частей)
    new_configs = [c for c in master_set if c not in state]
    old_configs = sorted([c for c in master_set if c in state], key=lambda c: state[c].get('last_checked', 0))
    
    # Берем ВСЕ новые + 1/12 старых (или больше, если это Action)
    chunk_size = max(500, len(master_set) // HOURS_TO_COMPLETE_CYCLE)
    if os.environ.get("GITHUB_ACTIONS") == "true": chunk_size = len(master_set) # В Actions чекаем всё
    
    chunk_to_check = new_configs + old_configs[:chunk_size]
    # Убираем дубликаты на всякий случай
    chunk_to_check = list(dict.fromkeys(chunk_to_check))
    
    print(f"⚖️ Очередь проверки: {len(new_configs)} НОВЫХ + {len(chunk_to_check) - len(new_configs)} старых (Итого: {len(chunk_to_check)})")

    # 5. TCP Ping
    dead_configs_for_purge, valid_in_chunk, new_dead_nodes, global_seen = [], [], set(), set()
    seen_lock = threading.Lock()
    
    print(f"⚡ TCP Ping ({THREAD_COUNT} потоков)...", flush=True)
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
                    elif status == "FAIL":
                        new_dead_nodes.add(nid)
                        dead_configs_for_purge.append(config)
            except: continue

    # 6. GeoIP (только для живых, у которых ЕЩЕ НЕТ GeoIP или прошел лимит времени)
    nodes_for_geoip = [cfg for cfg in valid_in_chunk if state[cfg].get('geoip', 'UN') == 'UN']
    
    if nodes_for_geoip:
        print(f"🌍 GeoIP для {len(nodes_for_geoip)} узлов ({GEOIP_PARALLEL_LEVEL} потоков)...", flush=True)
        with ThreadPoolExecutor(max_workers=GEOIP_PARALLEL_LEVEL) as geo_executor:
            geo_futures = [geo_executor.submit(lambda cfg: (cfg, check_ip_location_smart(get_server_info(cfg)[0])), cfg) for cfg in nodes_for_geoip]
            for f in as_completed(geo_futures):
                if SHOULD_EXIT: break
                try:
                    cfg, code = f.result()
                    state[cfg]['geoip'] = code
                except: continue

    save_state(state)

    # 7. Deep Purge (Вырезаем мусор навсегда)
    if dead_configs_for_purge:
        print("🧹 Запуск системы Глубокой Зачистки (Deep Purge)...")
        save_persistent_blacklist(new_dead_nodes)
        deep_purge_files(dead_configs_for_purge)
        # Удаляем из state сразу после зачистки
        for cfg in dead_configs_for_purge:
            if cfg in state: del state[cfg]
        save_state(state)

    # 8. Создание финальных файлов ТОЛЬКО на основе живых из ВСЕЙ базы
    save_and_organize(master_set, state)
    git_commit_push()
    
    gc.collect()
    print(f"\n🏁 ЦИКЛ ЗАВЕРШЕН ЗА {datetime.now() - start_time}.")

# ==============================================================================
# --- ДЕМОН-ПЕТЛЯ ---
# ==============================================================================

def start_daemon():
    if os.environ.get("GITHUB_ACTIONS") == "true":
        print("\n[GITHUB ACTIONS] Обнаружена среда CI/CD. Выполнение полного одиночного цикла...")
        run_update_cycle("Автоматический запуск GitHub Actions")
        return

    if os.path.exists(LOCK_FILE):
        print(f"[КРИТ] Обнаружен файл {LOCK_FILE}. Демон уже запущен.")
        return
        
    with open(LOCK_FILE, 'w') as f: f.write(str(os.getpid()))
    
    print(f"\n{'*'*70}")
    print(f"🛡️ VPN MONSTER DAEMON АКТИВЕН 🛡️")
    print(f"Интервал плановой проверки: каждые {UPDATE_INTERVAL_HOURS} час(ов).")
    print(f"Моментальное сканирование при редактировании {ALL_SOURCES_FILE} включено!")
    print(f"{'*'*70}\n")
    
    last_run_time = datetime.min
    last_sources_mod_time = get_file_mod_time(ALL_SOURCES_FILE)
    
    try:
        while not SHOULD_EXIT:
            now = datetime.now()
            trigger_reason = None
            
            if now - last_run_time >= timedelta(hours=UPDATE_INTERVAL_HOURS):
                trigger_reason = f"Плановое обновление (Таймер)"
            
            current_mod_time = get_file_mod_time(ALL_SOURCES_FILE)
            if current_mod_time > last_sources_mod_time:
                trigger_reason = f"Обнаружены НОВЫЕ ССЫЛКИ в {ALL_SOURCES_FILE}"
                last_sources_mod_time = current_mod_time
            
            if trigger_reason:
                run_update_cycle(trigger_reason)
                last_run_time = datetime.now()
                # Перечитываем mod_time, так как Deep Purge сам мог изменить файл
                last_sources_mod_time = get_file_mod_time(ALL_SOURCES_FILE)
                
                if SHOULD_EXIT: break
                print(f"\n💤 Ожидание... Следующий плановый запуск в {(last_run_time + timedelta(hours=UPDATE_INTERVAL_HOURS)).strftime('%H:%M:%S')}")
                print("👀 Готов мгновенно отреагировать на добавление новых ссылок в файл!\n")
            
            time.sleep(WATCHER_INTERVAL_SEC)
            
    finally:
        if os.path.exists(LOCK_FILE):
            try: os.remove(LOCK_FILE)
            except: pass
        print("🛑 Демон остановлен.")

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

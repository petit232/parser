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
MAX_BLACKLIST_SIZE = 2000   # Лимит записей в черном списке
GEOIP_LIMIT_PER_RUN = 350   # Лимит новых GeoIP проверок за запуск
THREAD_COUNT = 15           # Количество потоков для проверки портов

def signal_handler(sig, frame):
    """Корректное прерывание работы скрипта."""
    global SHOULD_EXIT
    print("\n[!] Сигнал остановки (SIGINT/SIGTERM). Завершаем работу...", flush=True)
    SHOULD_EXIT = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

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
    """Кодирование строки в Base64."""
    try:
        return base64.b64encode(data.encode('utf-8')).decode('utf-8')
    except Exception: return ""

def get_server_info(config):
    """Извлечение хоста и порта из конфига (поддержка VMess JSON)."""
    try:
        if config.startswith("vmess://"):
            decoded = decode_base64(config[8:])
            if decoded:
                v_data = json.loads(decoded)
                return str(v_data.get('add', '')).strip(), str(v_data.get('port', '')).strip()
        
        # Регулярка для vless, trojan, ss и др.
        match = re.search(r'://(?:[^@]+@)?([^:/#\?]+):(\d+)', config)
        if match:
            return match.group(1).strip(), match.group(2).strip()
    except Exception: pass
    return None, None

def is_node_alive(host, port, timeout=4):
    """Быстрая проверка доступности TCP порта."""
    if not host or not port: return False
    try:
        port_int = int(port)
        with socket.create_connection((host, port_int), timeout=timeout):
            return True
    except (socket.timeout, socket.error, ValueError):
        return False

def load_blacklist():
    """Загрузка черного списка с диска."""
    blacklist = {}
    if os.path.exists('blacklist.txt'):
        try:
            with open('blacklist.txt', 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if '|' in line:
                        node, timestamp = line.split('|')
                        blacklist[node] = datetime.fromisoformat(timestamp)
        except Exception as e:
            print(f"Ошибка загрузки blacklist: {e}", flush=True)
    return blacklist

def save_blacklist(blacklist):
    """Сохранение черного списка с ротацией и очисткой старых записей."""
    now = datetime.now()
    # Оставляем только те, что забанены недавно
    active = {n: ts for n, ts in blacklist.items() if now - ts < timedelta(hours=BLACKLIST_BAIL_HOURS)}
    # Сортируем по свежести и берем топ-N
    sorted_items = sorted(active.items(), key=lambda x: x[1], reverse=True)[:MAX_BLACKLIST_SIZE]
    try:
        with open('blacklist.txt', 'w', encoding='utf-8') as f:
            for node, ts in sorted_items:
                f.write(f"{node}|{ts.isoformat()}\n")
    except Exception as e:
        print(f"Ошибка сохранения blacklist: {e}", flush=True)

def check_ip_location(host):
    """Запрос страны через API с механизмом повторов и задержек."""
    global PROCESSED_COUNT
    if SHOULD_EXIT: return None

    with CACHE_LOCK:
        if host in IP_CACHE: return IP_CACHE[host]

    url = f"http://ip-api.com/json/{host}?fields=status,countryCode"
    
    # До 3-х попыток на случай сетевых сбоев
    for attempt in range(3):
        if SHOULD_EXIT: break
        try:
            # Рандомный Jitter для обхода анти-фрод систем
            time.sleep(random.uniform(2.5, 5.5))
            
            resp = requests.get(url, timeout=12)
            
            if resp.status_code == 429: # Rate limit
                print(f"  [!] Лимит API. Ожидание 120 сек...", flush=True)
                time.sleep(120)
                continue
                
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") == "success":
                    code = data.get("countryCode")
                    with CACHE_LOCK:
                        IP_CACHE[host] = code
                        PROCESSED_COUNT += 1
                        if PROCESSED_COUNT % 10 == 0:
                            print(f"  [GeoIP] Успешно проверено {PROCESSED_COUNT} новых узлов.", flush=True)
                    return code
                else:
                    break # Host invalid
        except Exception:
            time.sleep(attempt * 5 + 2)
            
    with CACHE_LOCK:
        IP_CACHE[host] = None
    return None

def load_current_database():
    """Загрузка всех текущих рабочих конфигов из файлов для дедупликации."""
    db = {} # config_string -> last_seen_datetime
    files = [f"{c}.txt" for c in COUNTRIES] + ["mix.txt"]
    now = datetime.now()
    
    for f_name in files:
        if os.path.exists(f_name):
            try:
                with open(f_name, 'r', encoding='utf-8') as f:
                    content = f.read()
                
                # Извлекаем дату последнего обновления файла
                date_match = re.search(r'# Updated: ([\d\-\s:]+)', content)
                file_dt = now
                if date_match:
                    try: file_dt = datetime.strptime(date_match.group(1), "%Y-%m-%d %H:%M:%S")
                    except: pass
                
                for line in content.splitlines():
                    cfg = line.strip()
                    if cfg and not cfg.startswith('#'):
                        # Храним самую позднюю дату для этого конфига
                        if cfg not in db or file_dt > db[cfg]:
                            db[cfg] = file_dt
            except Exception: pass
    return db

def save_and_cleanup(structured_data, db_knowledge):
    """Сохранение всех файлов, очистка старья и создание единой ссылки."""
    now = datetime.now()
    threshold = now - timedelta(hours=MAX_AGE_HOURS)
    all_valid_configs = []

    # 1. Обработка файлов по странам
    for country, info in COUNTRIES.items():
        file_name = f"{country}.txt"
        
        # Собираем то, что уже было (нужно отфильтровать по стране, если возможно)
        # Для простоты: берем новые + те, что уже лежали в ЭТОМ файле
        current_file_configs = {} 
        if os.path.exists(file_name):
            current_file_configs = load_existing_configs_internal(file_name)
        
        # Добавляем свеженайденные
        for nc in structured_data.get(country, []):
            current_file_configs[nc] = now
            
        # Оставляем только те, что не протухли
        valid_list = [c for c, ts in current_file_configs.items() if ts > threshold]
        all_valid_configs.extend(valid_list)
        
        try:
            # Маленькая пауза для стабильности ФС
            time.sleep(0.2)
            with open(file_name, 'w', encoding='utf-8') as f:
                if valid_list:
                    f.write("\n".join(sorted(list(set(valid_list)))))
                f.write(f"\n\n# Total: {len(valid_list)}\n# Updated: {now.strftime('%Y-%m-%d %H:%M:%S')}")
                f.write(f"\n# Auto-Clean: Nodes older than {MAX_AGE_HOURS}h removed.")
        except Exception as e:
            print(f"Ошибка записи {file_name}: {e}", flush=True)

    # 2. Обновление mix.txt (все живое)
    unique_mix = list(set(all_valid_configs))
    try:
        with open("mix.txt", 'w', encoding='utf-8') as f:
            if unique_mix:
                f.write("\n".join(sorted(unique_mix)))
            f.write(f"\n\n# Total: {len(unique_mix)}\n# Updated: {now.strftime('%Y-%m-%d %H:%M:%S')}")
    except Exception: pass

    # 3. ГЕНЕРАЦИЯ ЕДИНОЙ ССЫЛКИ ПОДПИСКИ (Base64)
    # Это файл sub_monster.txt — твоя вечная ссылка.
    sub_payload = "\n".join(unique_mix)
    encoded_payload = encode_base64(sub_payload)
    try:
        with open("sub_monster.txt", 'w', encoding='utf-8') as f:
            f.write(encoded_payload)
        print(f"[Subscription] sub_monster.txt обновлен. Всего узлов: {len(unique_mix)}", flush=True)
    except Exception as e:
        print(f"Ошибка создания подписки: {e}", flush=True)

def load_existing_configs_internal(file_name):
    """Внутренняя загрузка для синхронизации конкретного файла."""
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
    """Автоматический пуш в GitHub."""
    print("\n[Git] Начало синхронизации с репозиторием...", flush=True)
    try:
        subprocess.run(["git", "config", "--local", "user.name", "VPN-Monster-Bot"], check=True)
        subprocess.run(["git", "config", "--local", "user.email", "bot@vpn-monster.com"], check=True)
        subprocess.run(["git", "add", "*.txt"], check=True)
        
        # Проверяем наличие изменений
        status = subprocess.run(["git", "diff", "--cached", "--quiet"])
        if status.returncode == 0:
            print("[Git] Изменений в файлах не обнаружено.", flush=True)
            return

        msg = f"Update {datetime.now().strftime('%d.%m %H:%M')} | Auto-Cleaned"
        subprocess.run(["git", "commit", "-m", msg], check=True)
        subprocess.run(["git", "pull", "--rebase", "origin", "main"], check=False)
        subprocess.run(["git", "push", "origin", "main"], check=True)
        print("[Git] Успешно запушено.", flush=True)
    except Exception as e:
        print(f"[Git] Ошибка синхронизации: {e}", flush=True)

def thread_check_worker(config, blacklist, db_knowledge, seen_lock, global_seen):
    """Функция для потока: проверка порта и фильтрация."""
    host, port = get_server_info(config)
    if not host or not port: return None
    
    node_id = f"{host}:{port}"
    
    # ФИЛЬТРЫ:
    if config in db_knowledge: return None    # Уже есть в базе
    if node_id in blacklist: return None     # В черном списке
    
    with seen_lock:
        if node_id in global_seen: return None # Уже нашли в этом запуске
        global_seen.add(node_id)

    # Проверка порта
    if is_node_alive(host, port):
        return config
    else:
        return ("FAIL", node_id)

def process():
    start_run = datetime.now()
    print(f"--- ЗАПУСК ПАРСЕРА MONSTER VPN: {start_run.strftime('%H:%M:%S')} ---", flush=True)
    
    # 1. Загрузка ресурсов
    if not os.path.exists('all_sources.txt'):
        print("[!] Ошибка: Файл all_sources.txt не найден!", flush=True)
        return

    with open('all_sources.txt', 'r', encoding='utf-8') as f:
        sources = list(set([l.strip() for l in f if l.strip()]))

    blacklist = load_blacklist()
    db_knowledge = load_current_database()
    
    raw_configs = []
    print(f"Этап 1: Сбор сырых данных из {len(sources)} источников...", flush=True)
    
    for url in sources:
        if SHOULD_EXIT: break
        try:
            # Пропускаем свои же файлы, если они в списке
            if "sub_monster.txt" in url or "mix.txt" in url: continue
            
            headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
            r = requests.get(url, timeout=25, headers=headers)
            r.raise_for_status()
            text = r.text
            
            # Авто-декодирование Base64 подписок
            if not any(p in text for p in PROTOCOLS):
                decoded = decode_base64(text)
                if decoded: text = decoded
                
            # Поиск всех ссылок
            matches = re.findall(r'(?:vless|vmess|trojan|ss|hysteria2|tuic)://[^\s#"\'<>,]+', text)
            raw_configs.extend(matches)
            print(f"  + {url[:35]}... : Получено {len(matches)} ссылок", flush=True)
            gc.collect()
        except Exception as e:
            print(f"  [!] Ошибка в {url[:30]}: {e}", flush=True)

    # 2. Многопоточная проверка портов (Liveness Check)
    print(f"\nЭтап 2: Многопоточная проверка портов ({len(raw_configs)} узлов, {THREAD_COUNT} потоков)...", flush=True)
    valid_new_configs = []
    global_seen = set()
    seen_lock = threading.Lock()
    
    with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
        futures = [executor.submit(thread_check_worker, c, blacklist, db_knowledge, seen_lock, global_seen) for c in raw_configs]
        for future in as_completed(futures):
            if SHOULD_EXIT: break
            result = future.result()
            if result:
                if isinstance(result, tuple) and result[0] == "FAIL":
                    blacklist[result[1]] = datetime.now()
                else:
                    valid_new_configs.append(result)

    # 3. GeoIP Фильтрация (только для новых "живых" узлов)
    # Ограничиваем, чтобы не получить бан API
    random.shuffle(valid_new_configs)
    queue = valid_new_configs[:GEOIP_LIMIT_PER_RUN]
    
    structured = {c: [] for c in COUNTRIES}
    print(f"\nЭтап 3: Фильтрация GeoIP для {len(queue)} новых живых узлов...", flush=True)
    
    for cfg in queue:
        if SHOULD_EXIT: break
        host, _ = get_server_info(cfg)
        code = check_ip_location(host)
        
        if code:
            matched = False
            for c_name, c_info in COUNTRIES.items():
                if code == c_info["code"] or code == c_info.get("alt_code") or code == c_info.get("extra"):
                    structured[c_name].append(cfg)
                    matched = True
                    break
    
    # 4. Сохранение, очистка и пуш
    print("\nЭтап 4: Сохранение результатов и очистка базы...", flush=True)
    save_and_generate_sub(structured, db_knowledge)
    save_blacklist(blacklist)
    git_commit_and_push()
    
    print(f"--- ПАРСИНГ ЗАВЕРШЕН ЗА {datetime.now() - start_run} ---", flush=True)

if __name__ == "__main__":
    try:
        process()
    except Exception as e:
        print(f"КРИТИЧЕСКИЙ СБОЙ: {e}", flush=True)
        sys.exit(1)

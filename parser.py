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
from datetime import datetime, timedelta
from urllib.parse import quote, unquote
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- КОНФИГУРАЦИЯ СТРАН ---
# Полный список стран для зеркального отображения структуры и дизайна
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
BLACKLIST_BAIL_HOURS = 6    # Время бана за мертвый порт (в часах)
MAX_BLACKLIST_SIZE = 50000  # Максимальный размер черного списка узлов
GEOIP_LIMIT_PER_RUN = 3000  # Лимит проверок через API за один цикл
THREAD_COUNT = 60           # Количество потоков для сетевой проверки (TCP Ping)
GEOIP_PARALLEL_LEVEL = 10   # Уровень параллельности запросов к GeoIP API
PORT_TIMEOUT = 3.5          # Таймаут ожидания ответа (Reality/Hysteria требуют запаса)

def signal_handler(sig, frame):
    """Корректный выход из программы при получении системных сигналов."""
    global SHOULD_EXIT
    print("\n[!] Внимание: Получен сигнал остановки. Завершаем текущие потоки...", flush=True)
    SHOULD_EXIT = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def get_random_ua():
    """Генерация случайного User-Agent для обхода защиты Cloudflare на источниках."""
    uas = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36",
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0"
    ]
    return random.choice(uas)

def decode_base64(data):
    """Безопасное декодирование Base64 с автоматической коррекцией паддинга."""
    try:
        data = re.sub(r'[^a-zA-Z0-9+/=]', '', data)
        if not data: return ""
        missing_padding = len(data) % 4
        if missing_padding: data += '=' * (4 - missing_padding)
        return base64.b64decode(data).decode('utf-8', errors='ignore')
    except Exception: return ""

def encode_base64(data):
    """Кодирование строки в стандартный Base64 без переносов строк."""
    try:
        return base64.b64encode(data.encode('utf-8')).decode('utf-8')
    except Exception: return ""

def get_server_info(config):
    """
    Извлечение IP/Хоста и Порта из конфига. 
    Сохраняет все параметры после '#' и '?' для корректной работы Reality/TLS.
    """
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
    """TCP-проверка доступности порта узла."""
    if not host or not port: return False
    # Игнорируем локальные и технические адреса
    if host.startswith(('127.', '192.168.', '10.', '172.16.')) or host == 'localhost':
        return False
    try:
        port_int = int(port)
        with socket.create_connection((host, port_int), timeout=timeout):
            return True
    except: return False

# --- МОДУЛЬ ДИЗАЙНА (BEAUTIFIER) ---

def beautify_config(config, country_key=None, fallback_code="UN"):
    """
    Преобразует технические названия в премиальный формат: ❤️ 🏁 Страна | Код 🏁 ❤️
    Сохраняет все параметры шифрования и SNI.
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

# --- ИНТЕЛЛЕКТУАЛЬНЫЙ КЭШ IP ---

def pre_populate_ip_cache():
    """
    Анализ текущих файлов для предотвращения повторных запросов к API.
    В режиме Mirror Sync мы обновляем кэш при каждом запуске.
    """
    print("🧠 Загрузка базы знаний (IP Cache) из существующих подписок...", flush=True)
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
                                        # Извлекаем код страны из названия (label)
                                        match = re.search(r'\|\s*([A-Z]{2})\s*', unquote(cfg))
                                        if match:
                                            IP_CACHE[host] = match.group(1)
                                            loaded_count += 1
            except Exception: pass
            
    print(f"✅ В память загружено {loaded_count} известных IP. Они мгновенно пропустят проверку API.")

# --- ТУРБО-ДВИЖОК GEOIP (10 ЗЕРКАЛЬНЫХ ПРОВАЙДЕРОВ) ---

def api_01(h):
    try: return requests.get(f"http://ip-api.com/json/{h}?fields=status,countryCode", timeout=3).json().get("countryCode")
    except: return None
def api_02(h):
    try: return requests.get(f"https://ipwho.is/{h}", timeout=3).json().get("country_code")
    except: return None
def api_03(h):
    try: 
        r = requests.get(f"https://ip2c.org/{h}", timeout=3)
        return r.text.split(';')[1] if "1;" in r.text else None
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
    """Многопоточное определение страны. API вызывается только при отсутствии IP в кэше."""
    global PROCESSED_COUNT
    if SHOULD_EXIT: return None

    with CACHE_LOCK:
        if host in IP_CACHE: 
            return IP_CACHE[host]

    # Небольшая задержка для предотвращения Rate Limit
    time.sleep(random.uniform(0.1, 0.4))
    providers = [api_01, api_02, api_03, api_04, api_05, api_06, api_07, api_08, api_09, api_10]
    random.shuffle(providers)

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

# --- МОДУЛЬ ЧЕРНОГО СПИСКА ---

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

# --- СИСТЕМА СОХРАНЕНИЯ (FORCE MIRROR MODE) ---

def save_and_organize(structured, final_mix_list, failed_list):
    """
    Принудительная перезапись файлов. 
    В каждый файл добавляется уникальная метка времени и Sync ID, 
    чтобы гарантировать наличие изменений для Git.
    """
    now = datetime.now()
    now_str = now.strftime('%Y-%m-%d %H:%M:%S')
    ms_stamp = now.strftime('%f') # Микросекунды для гарантированного отличия
    sync_id = uuid.uuid4().hex[:8] 

    for country in COUNTRIES:
        file_name = f"{country}.txt"
        configs = structured.get(country, [])
        valid = sorted(list(set(configs)))
        
        try:
            with open(file_name, 'w', encoding='utf-8') as f:
                if valid:
                    f.write("\n".join(valid))
                    f.write(f"\n\n# --- MONSTER MIRROR SYNC INFO ---\n")
                    f.write(f"# Nodes: {len(valid)}\n# Time: {now_str}.{ms_stamp}\n# SyncID: {sync_id}\n")
                else:
                    f.write(f"# No active nodes for {country}\n# Time: {now_str}.{ms_stamp}\n# SyncID: {sync_id}\n")
        except Exception: pass

    valid_mix = sorted(list(set(final_mix_list)))
    try:
        with open("mix.txt", 'w', encoding='utf-8') as f:
            if valid_mix:
                f.write("\n".join(valid_mix))
                f.write(f"\n\n# --- MONSTER MIRROR SYNC INFO ---\n")
                f.write(f"# Total Mix: {len(valid_mix)}\n# Time: {now_str}.{ms_stamp}\n# SyncID: {sync_id}\n")
            else:
                f.write(f"# No active nodes found\n# Time: {now_str}.{ms_stamp}\n# SyncID: {sync_id}\n")
        
        with open("sub_monster.txt", 'w', encoding='utf-8') as f:
            f.write(encode_base64("\n".join(valid_mix)) if valid_mix else "")
            
        valid_failed = sorted(list(set(failed_list)))
        with open("failed_nodes.txt", 'w', encoding='utf-8') as f:
            if valid_failed:
                f.write("\n".join(valid_failed))
                f.write(f"\n\n# --- MONSTER MIRROR SYNC INFO ---\n")
                f.write(f"# Failed Count: {len(valid_failed)}\n# Time: {now_str}.{ms_stamp}\n# SyncID: {sync_id}\n")
            else:
                f.write(f"# No failed nodes detected\n# Time: {now_str}.{ms_stamp}\n# SyncID: {sync_id}\n")
                
        with open("sub_failed.txt", 'w', encoding='utf-8') as f:
            f.write(encode_base64("\n".join(valid_failed)) if valid_failed else "")
    except Exception: pass

def git_commit_push():
    """Силовое обновление репозитория с принудительным коммитом."""
    print("\n[Git] Синхронизация репозитория (Mirror Mode)...", flush=True)
    try:
        subprocess.run(["git", "config", "--local", "user.name", "VPN-Monster-Bot"], check=True)
        subprocess.run(["git", "config", "--local", "user.email", "bot@vpn-monster.com"], check=True)
        
        # Очищаем индекс и добавляем всё заново
        subprocess.run(["git", "add", "."], check=True)
        
        timestamp = datetime.now().strftime('%d/%m %H:%M:%S')
        msg = f"🚀 Monster Sync {timestamp} [Force Sync]"
        
        # Проверяем статус. Если изменений нет (хотя SyncID их гарантирует), делаем пустой коммит
        status = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True).stdout.strip()
        
        if not status:
            print("[Git] Контент идентичен. Выполняю принудительную ревизию...")
            subprocess.run(["git", "commit", "--allow-empty", "-m", msg], check=True)
        else:
            subprocess.run(["git", "commit", "-m", msg], check=True)
        
        # Силовой Push для перезаписи состояния
        subprocess.run(["git", "push", "origin", "main", "--force"], check=True)
        print("[Git] Зеркало успешно обновлено в GitHub!")
    except Exception as e:
        print(f"[Git] Критическая ошибка синхронизации: {e}")

# --- ФУНКЦИИ ВОРКЕРЫ (WORKERS) ---

def check_worker(config, blacklist, lock, seen):
    """Воркер для параллельной проверки портов."""
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
    """Воркер для получения местоположения IP."""
    host, _ = get_server_info(cfg)
    code = check_ip_location_smart(host)
    return (cfg, code)

# --- ГЛАВНЫЙ ДВИЖОК МОНСТРА ---

def process_monster_engine():
    start_time = datetime.now()
    print(f"\n{'='*50}\n🚀 MONSTER ENGINE SYNC СТАРТ: {start_time.strftime('%H:%M:%S')}\n{'='*50}", flush=True)
    
    # 1. Принудительный анализ текущего состояния
    pre_populate_ip_cache()
    
    # 2. Сбор источников из внешнего файла
    sources = []
    if os.path.exists('all_sources.txt'):
        with open('all_sources.txt', 'r', encoding='utf-8') as f:
            sources = list(set([l.strip() for l in f if l.strip() and l.startswith('http')]))
    
    if not sources:
        print("[!] ВНИМАНИЕ: Файл all_sources.txt пуст. Работа в режиме очистки.")

    blacklist = load_blacklist()
    raw_configs = []
    
    if sources:
        print(f"📡 Сбор данных из {len(sources)} источников...", flush=True)
        for url in sources:
            try:
                # Исключаем собственные файлы из обработки (защита от циклов)
                if any(x in url for x in ["sub_monster.txt", "mix.txt", "failed_nodes.txt", "sub_failed.txt"]):
                    continue
                
                r = requests.get(url, timeout=15, headers={'User-Agent': get_random_ua()})
                text = r.text
                
                # Попытка декодировать Base64 если это подписка
                if not any(p in text for p in ALLOWED_PROTOCOLS):
                    decoded = decode_base64(text)
                    if decoded: text = decoded
                
                # Извлечение всех поддерживаемых конфигов
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
    
    # 3. Фаза скоростной проверки портов
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
    
    # 4. Фаза определения Геолокации
    if valid_new:
        print(f"🌍 Турбо-GeoIP определение (Синхронный Mirror режим)...", flush=True)
        random.shuffle(valid_new)
        # Ограничиваем количество проверок API для стабильности
        queue = valid_new[:GEOIP_LIMIT_PER_RUN]
        
        with ThreadPoolExecutor(max_workers=GEOIP_PARALLEL_LEVEL) as geo_executor:
            geo_futures = [geo_executor.submit(geoip_parallel_worker, cfg) for cfg in queue]
            
            for i, future in enumerate(as_completed(geo_futures)):
                if SHOULD_EXIT: break
                try:
                    cfg, code = future.result()
                    matched = False
                    if code and code != "UN":
                        for c_name, c_info in COUNTRIES.items():
                            # Проверка совпадения по коду, альт-коду или экстра-коду (UK)
                            if code in [c_info["code"], c_info.get("alt_code"), c_info.get("extra")]:
                                beauty_cfg = beautify_config(cfg, c_name)
                                structured_data[c_name].append(beauty_cfg)
                                final_mix_list.append(beauty_cfg)
                                matched = True
                                break
                                
                    if not matched:
                        # Если страна не в списке COUNTRIES, отправляем в mix.txt с кодом из API
                        beauty_cfg = beautify_config(cfg, None, fallback_code=code)
                        final_mix_list.append(beauty_cfg)
                        
                    if i > 0 and i % 100 == 0:
                        print(f"   > Обработано {i}/{len(queue)}...", flush=True)
                except: continue
            
    # 5. Финализация и синхронизация
    print("💾 Прямая синхронизация файлов (Режим Зеркала)...", flush=True)
    save_and_organize(structured_data, final_mix_list, failed_new)
    save_blacklist(blacklist)
    
    # Принудительная очистка мусора перед Git
    global_seen.clear()
    gc.collect()
    
    # Отправка изменений
    git_commit_push()
    
    end_time = datetime.now()
    print(f"\n🏁 ЦИКЛ ЗАВЕРШЕН ЗА {end_time - start_time}.", flush=True)

if __name__ == "__main__":
    try:
        # Устанавливаем таймаут по умолчанию для всех сетевых операций
        socket.setdefaulttimeout(PORT_TIMEOUT)
        process_monster_engine()
    except Exception as fatal_error:
        print(f"\n[КРИТИЧЕСКАЯ ОШИБКА ДВИЖКА]: {fatal_error}")
        sys.exit(1)

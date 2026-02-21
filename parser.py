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
from datetime import datetime
from collections import defaultdict

# --- КОНФИГУРАЦИЯ СТРАН ---
# Словарь для сопоставления кодов стран с именами файлов и эмодзи флагами.
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
    "uk": {"flag": "🇬🇧", "code": "GB"},
    "hongkong": {"flag": "🇭🇰", "code": "HK"},
    "finland": {"flag": "🇫🇮", "code": "FI"},
    "france": {"flag": "🇫🇷", "code": "FR"}
}

# Поддерживаемые протоколы прокси
PROTOCOLS = ["vless://", "vmess://", "trojan://", "ss://", "hysteria2://", "tuic://"]

# Глобальные переменные для кеширования и управления процессом
IP_CACHE = {} 
CACHE_LOCK = threading.Lock()
UNRESOLVED_COUNT = 0 
PROCESSED_COUNT = 0
SHOULD_EXIT = False # Флаг для прерывания работы при таймауте GitHub Actions

def signal_handler(sig, frame):
    """Обработчик сигналов завершения системы (SIGINT, SIGTERM)."""
    global SHOULD_EXIT
    print("\n[!] Получен сигнал остановки системы. Пытаюсь экстренно сохранить прогресс...")
    SHOULD_EXIT = True

# Регистрация обработчиков сигналов
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def decode_base64(data):
    """
    Безопасное декодирование Base64.
    Очищает строку от не-ASCII символов и автоматически исправляет паддинг.
    """
    try:
        # Очистка: оставляем только валидные символы Base64
        data = re.sub(r'[^a-zA-Z0-9+/=]', '', data)
        if not data:
            return ""
        
        # Исправление отсутствующего паддинга (=)
        missing_padding = len(data) % 4
        if missing_padding:
            data += '=' * (4 - missing_padding)
            
        return base64.b64decode(data).decode('utf-8', errors='ignore')
    except Exception:
        return ""

def get_server_info(config):
    """Извлекает хост и порт из строки конфигурации прокси."""
    try:
        if config.startswith("vmess://"):
            decoded = decode_base64(config[8:])
            if decoded:
                v_data = json.loads(decoded)
                return v_data.get('add', '').strip(), str(v_data.get('port', '')).strip()
        
        # Универсальное регулярное выражение для vless, trojan, ss и др.
        match = re.search(r'://(?:[^@]+@)?([^:/#\?]+):(\d+)', config)
        if match:
            return match.group(1).strip(), match.group(2).strip()
    except Exception:
        pass
    return None, None

def check_ip_location(host):
    """
    Определяет страну IP-адреса через сервис ip-api.com.
    Включена безопасная задержка для предотвращения бана IP (лимит 45 зап/мин).
    """
    global UNRESOLVED_COUNT, PROCESSED_COUNT
    
    if SHOULD_EXIT: 
        return None

    with CACHE_LOCK:
        if host in IP_CACHE:
            return IP_CACHE[host]

    # Базовая валидация хоста
    if not host or len(host) < 3 or host.startswith("127.") or "localhost" in host:
        return None

    url = f"http://ip-api.com/json/{host}?fields=status,message,countryCode"
    
    for attempt in range(3): 
        if SHOULD_EXIT: break
        try:
            # БЕЗОПАСНАЯ ЗАДЕРЖКА: ~1.5 секунды между запросами для соблюдения лимита 45/мин.
            # Это защищает от автоматического бана на стороне сервера ip-api.
            time.sleep(random.uniform(1.45, 1.65))
            
            resp = requests.get(url, timeout=12)
            
            if resp.status_code == 429:
                # Если всё же поймали 429, ждем дольше
                print(f"(!) Превышен лимит (429). Ждем 60 секунд для сброса счетчика...")
                time.sleep(60)
                continue
                
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") == "success":
                    code = data.get("countryCode")
                    with CACHE_LOCK:
                        IP_CACHE[host] = code
                        PROCESSED_COUNT += 1
                        if PROCESSED_COUNT % 10 == 0:
                            print(f"[{datetime.now().strftime('%H:%M:%S')}] GeoIP Прогресс: {PROCESSED_COUNT} узлов проверено.")
                    return code
                else:
                    # IP не определен или зарезервирован
                    break
            else:
                time.sleep(2)
        except Exception:
            time.sleep(2)
            
    with CACHE_LOCK:
        UNRESOLVED_COUNT += 1
        IP_CACHE[host] = None
    return None

def validate_config(config):
    """Проверяет минимальную валидность строки прокси."""
    if not any(config.startswith(p) for p in PROTOCOLS): 
        return False
    if len(config) < 15: 
        return False
    host, port = get_server_info(config)
    return bool(host and port)

def sanitize_sources(file_path):
    """Очищает файл источников от дубликатов."""
    if not os.path.exists(file_path): 
        print(f"(!) Файл {file_path} не найден.")
        with open(file_path, 'w', encoding='utf-8') as f: pass
        return []
        
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.read().splitlines()
        
    clean, seen = [], set()
    for line in lines:
        s = line.strip().strip('",\'').strip()
        if s and s not in seen:
            clean.append(s)
            seen.add(s)
            
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write("\n".join(clean))
    print(f"Загружено уникальных источников: {len(clean)}")
    return clean

def save_results(structured_data, mix_data):
    """Сохраняет результаты по странам и общий микс."""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    for country, configs in structured_data.items():
        file_name = f"{country}.txt"
        with open(file_name, 'w', encoding='utf-8') as f:
            if configs:
                f.write("\n".join(sorted(list(set(configs)))))
            f.write(f"\n\n# Total: {len(configs)}\n# Updated: {now_str}")

    with open("mix.txt", 'w', encoding='utf-8') as f:
        if mix_data:
            f.write("\n".join(sorted(list(set(mix_data)))))
        f.write(f"\n\n# Total: {len(mix_data)}\n# Updated: {now_str}")
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Все файлы (.txt) успешно обновлены.")

def git_commit_and_push():
    """Синхронизирует изменения с GitHub."""
    try:
        subprocess.run(["git", "config", "--global", "user.name", "VPN-Monster-Bot"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "bot@vpn-monster.com"], check=True)
        subprocess.run(["git", "add", "*.txt"], check=True)
        
        status = subprocess.run(["git", "diff", "--cached", "--quiet"])
        if status.returncode == 0:
            print("Нет новых данных для коммита.")
            return

        commit_msg = f"Auto-Update: {datetime.now().strftime('%Y-%m-%d %H:%M')} | IP-Checks: {len(IP_CACHE)}"
        subprocess.run(["git", "commit", "-m", commit_msg], check=True)
        subprocess.run(["git", "pull", "--rebase"], check=True)
        
        res = subprocess.run(["git", "push"], capture_output=True, text=True)
        if res.returncode != 0:
            print(f"Ошибка Git Push: {res.stderr}")
        else:
            print(">>> Репозиторий GitHub обновлен.")
            
    except Exception as e:
        print(f"Ошибка Git: {e}")

def process():
    """Главный цикл работы парсера."""
    start_time = datetime.now()
    print(f"--- СТАРТ ПАРСЕРА: {start_time.strftime('%Y-%m-%d %H:%M:%S')} ---")
    
    source_file = 'all_sources.txt'
    sources = sanitize_sources(source_file)
    if not sources:
        return

    proto_groups = defaultdict(list)
    seen_nodes = set()
    
    print("Этап 1: Сбор и фильтрация конфигов...")
    for url in sources:
        if SHOULD_EXIT: break
        
        if url.startswith("http"):
            try:
                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
                with requests.get(url, timeout=30, headers=headers, stream=True) as r:
                    r.raise_for_status()
                    chunks = []
                    downloaded = 0
                    for chunk in r.iter_content(chunk_size=1024*1024, decode_unicode=True):
                        if chunk:
                            chunks.append(chunk)
                            downloaded += len(chunk)
                        if downloaded > 10 * 1024 * 1024: break
                    
                    content = "".join(chunks)
                    del chunks 
                    
                    if not any(p in content for p in PROTOCOLS):
                        decoded = decode_base64(content)
                        if decoded: content = decoded
                    
                    found_count = 0
                    for m in re.finditer(r'(?:vless|vmess|trojan|ss|hysteria2|tuic)://[^\s#"\'<>,]+', content):
                        cfg = m.group(0)
                        host, port = get_server_info(cfg)
                        if host and port:
                            node_id = f"{host}:{port}"
                            if node_id not in seen_nodes:
                                seen_nodes.add(node_id)
                                for p in PROTOCOLS:
                                    if cfg.startswith(p):
                                        proto_groups[p].append(cfg)
                                        found_count += 1
                                        break
                    
                    print(f"  + {url[:50]}... : +{found_count} узлов")
                    del content
                    gc.collect() 
            except Exception:
                continue
        else:
            host, port = get_server_info(url)
            if host and port:
                node_id = f"{host}:{port}"
                if node_id not in seen_nodes:
                    seen_nodes.add(node_id)
                    for p in PROTOCOLS:
                        if url.startswith(p):
                            proto_groups[p].append(url)
                            break

    total_unique = len(seen_nodes)
    if total_unique == 0:
        print("Ничего не найдено.")
        return

    print("\nРаспределение по протоколам:")
    for p, items in proto_groups.items():
        print(f"  - {p}: {len(items)}")

    # Формирование очереди (Round Robin)
    process_list = []
    limit = 350
    for p in proto_groups: random.shuffle(proto_groups[p])
    
    while len(process_list) < limit and any(proto_groups.values()):
        for p in list(proto_groups.keys()):
            if proto_groups[p]:
                process_list.append(proto_groups[p].pop(0))
            else:
                del proto_groups[p]
            if len(process_list) >= limit: break

    structured_data = {c: [] for c in COUNTRIES}
    mix_data = []
    
    print(f"\nЭтап 2: GeoIP проверка ({len(process_list)} узлов с защитой от бана)...")
    
    for cfg in process_list:
        if SHOULD_EXIT: break
        
        host, _ = get_server_info(cfg)
        country_code = check_ip_location(host)
        
        if country_code:
            matched = False
            for country_key, info in COUNTRIES.items():
                if country_code == info["code"] or country_code == info.get("alt_code"):
                    structured_data[country_key].append(cfg)
                    matched = True
                    break
        
        mix_data.append(cfg)

    # Сохранение
    save_results(structured_data, mix_data)
    git_commit_and_push()
    
    end_time = datetime.now()
    print(f"\n--- ГОТОВО: {end_time.strftime('%H:%M:%S')} (Заняло: {end_time - start_time}) ---")

if __name__ == "__main__":
    try:
        process()
    except Exception as e:
        print(f"\n[ОШИБКА]: {e}")
        git_commit_and_push()
        sys.exit(1)

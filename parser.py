import os
import re
import requests
import base64
import json
import threading
import time
import random
import subprocess
from datetime import datetime

# --- КОНФИГУРАЦИЯ СТРАН ---
# Используется для фильтрации и распределения конфигов по файлам
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

# Поддерживаемые протоколы
PROTOCOLS = ["vless://", "vmess://", "trojan://", "ss://", "hysteria2://", "tuic://"]

# Глобальные переменные для кеширования и статистики
IP_CACHE = {} 
CACHE_LOCK = threading.Lock()
UNRESOLVED_COUNT = 0 
PROCESSED_COUNT = 0

def decode_base64(data):
    """Безопасное декодирование Base64 данных."""
    try:
        data = data.strip()
        if not data:
            return ""
        # Исправление отсутствующего паддинга
        missing_padding = len(data) % 4
        if missing_padding:
            data += '=' * (4 - missing_padding)
        return base64.b64decode(data).decode('utf-8', errors='ignore')
    except Exception:
        return ""

def get_server_info(config):
    """Извлекает хост (IP/домен) и порт из прокси-конфига."""
    try:
        if config.startswith("vmess://"):
            v_data = json.loads(decode_base64(config[8:]))
            return v_data.get('add', ''), str(v_data.get('port', ''))
        
        # Регулярное выражение для извлечения адреса из vless, trojan, ss и т.д.
        match = re.search(r'://(?:[^@]+@)?([^:/]+):(\d+)', config)
        if match:
            return match.group(1), match.group(2)
    except Exception:
        pass
    return None, None

def check_ip_location(host):
    """
    Определяет код страны через GeoIP API (ip-api.com).
    Включает логику повторных попыток при 429 ошибке и рандомизированные задержки.
    """
    global UNRESOLVED_COUNT, PROCESSED_COUNT
    
    with CACHE_LOCK:
        if host in IP_CACHE:
            return IP_CACHE[host]

    url = f"http://ip-api.com/json/{host}?fields=status,message,countryCode"
    retries = 3
    backoff = 5
    
    for i in range(retries):
        try:
            # Анти-спам задержка: имитируем человеческую активность
            time.sleep(random.uniform(0.7, 1.3))
            
            resp = requests.get(url, timeout=15)
            
            if resp.status_code == 429:
                # Слишком много запросов — увеличиваем паузу
                print(f"(!) API лимит (429) для {host}, ожидание {backoff}с...")
                time.sleep(backoff)
                backoff *= 2
                continue
                
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") == "success":
                    code = data.get("countryCode")
                    with CACHE_LOCK:
                        IP_CACHE[host] = code
                        PROCESSED_COUNT += 1
                        if PROCESSED_COUNT % 10 == 0:
                            print(f"[{datetime.now().strftime('%H:%M:%S')}] Проверено хостов: {PROCESSED_COUNT}...")
                    return code
                elif data.get("message") == "reserved range":
                    break
                break
        except Exception as e:
            print(f"(!) Ошибка запроса к API для {host}: {e}")
            time.sleep(2)
            
    # Если не удалось определить, сохраняем None
    with CACHE_LOCK:
        UNRESOLVED_COUNT += 1
        IP_CACHE[host] = None
    return None

def validate_config(config):
    """Проверяет базовую валидность конфига перед обработкой."""
    if not any(config.startswith(p) for p in PROTOCOLS):
        return False
    if len(config) < 20:
        return False
    host, port = get_server_info(config)
    return bool(host and port)

def sanitize_sources(file_path):
    """Очищает список источников от мусора и дубликатов."""
    if not os.path.exists(file_path): 
        print(f"ВНИМАНИЕ: Файл {file_path} не найден. Создаю пустой.")
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
    print(f"Загружено источников: {len(clean)}")
    return clean

def git_commit_and_push():
    """Синхронизация обновленных файлов с репозиторием GitHub."""
    try:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Подготовка к Git Push...")
        
        # Настройка личности бота
        subprocess.run(["git", "config", "--global", "user.name", "Proxy-Parser-Bot"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "bot@proxy.local"], check=True)
        
        # Индексация всех текстовых файлов
        subprocess.run(["git", "add", "*.txt"], check=True)
        
        # Проверка наличия реальных изменений в контенте
        status_check = subprocess.run(["git", "diff", "--cached", "--quiet"])
        if status_check.returncode == 0:
            print(">>> Изменений в конфигах не найдено. Пропускаем Push.")
            return

        commit_msg = f"Auto-Update: Saved {len(IP_CACHE)} nodes | {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        subprocess.run(["git", "commit", "-m", commit_msg], check=True)
        
        # Пытаемся сделать Pull перед Push, чтобы избежать конфликтов
        subprocess.run(["git", "pull", "--rebase"], check=True)
        
        push_res = subprocess.run(["git", "push"], capture_output=True, text=True)
        if push_res.returncode != 0:
            print(f"(!) Ошибка Git Push: {push_res.stderr}")
        else:
            print(">>> Данные успешно отправлены в GitHub репозиторий.")
            
    except Exception as e:
        print(f"(!) Критическая ошибка Git: {e}")

def process():
    """Основной цикл работы парсера."""
    start_time = datetime.now()
    print(f"\n--- СТАРТ ПРОВЕРКИ: {start_time.strftime('%Y-%m-%d %H:%M:%S')} ---")
    
    # Предварительная проверка API
    try:
        test_api = requests.get("http://ip-api.com/json/8.8.8.8", timeout=10)
        print(f"Статус GeoIP API: {test_api.status_code}")
    except Exception as e:
        print(f"(!) Проблема с доступом к API: {e}")

    source_file = 'all_sources.txt'
    sources = sanitize_sources(source_file)
    if not sources:
        print("Список источников пуст.")
        return

    all_raw_links = []
    print("Извлечение ссылок из источников...")
    for url in sources:
        if url.startswith("http"):
            try:
                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
                resp = requests.get(url, timeout=30, headers=headers)
                if resp.status_code == 200:
                    content = resp.text
                    if not any(p in content for p in PROTOCOLS):
                        content = decode_base64(content)
                    
                    found = re.findall(r'(?:vless|vmess|trojan|ss|hysteria2|tuic)://[^\s#"\'<>,]+', content)
                    all_raw_links.extend(found)
                    print(f"  + {url}: {len(found)} прокси")
            except Exception as e:
                print(f"  - Ошибка {url}: {e}")
                continue
        else:
            all_raw_links.append(url)

    unique_configs = []
    seen_nodes = set()
    for cfg in all_raw_links:
        if not validate_config(cfg): continue
        host, port = get_server_info(cfg)
        node_id = f"{host}:{port}"
        if node_id not in seen_nodes:
            seen_nodes.add(node_id)
            unique_configs.append(cfg)

    total_found = len(unique_configs)
    print(f"Итого уникальных прокси для проверки: {total_found}")
    if total_found == 0:
        return

    random.shuffle(unique_configs)
    structured_data = {c: [] for c in COUNTRIES}
    mix_data = []
    
    # Ограничиваем нагрузку на API
    MAX_THREADS = 2 
    
    def thread_task(cfg):
        host, _ = get_server_info(cfg)
        country_code = check_ip_location(host)
        
        if country_code:
            for country_key, info in COUNTRIES.items():
                if country_code == info["code"] or country_code == info.get("alt_code"):
                    with CACHE_LOCK:
                        structured_data[country_key].append(cfg)
                    break
        with CACHE_LOCK:
            mix_data.append(cfg)

    print(f"Начинаю определение стран (потоков: {MAX_THREADS})...")
    active_threads = []
    for cfg in unique_configs:
        t = threading.Thread(target=thread_task, args=(cfg,))
        active_threads.append(t)
        t.start()
        
        if len(active_threads) >= MAX_THREADS:
            for t in active_threads: t.join()
            active_threads = []
            time.sleep(2) # Пауза для стабильности API
            
    for t in active_threads: t.join()

    # Сохранение результатов
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for country, configs in structured_data.items():
        with open(f"{country}.txt", 'w', encoding='utf-8') as f:
            if configs:
                f.write("\n".join(sorted(configs)))
            f.write(f"\n\n# Total: {len(configs)}\n# Updated: {now_str}")

    with open("mix.txt", 'w', encoding='utf-8') as f:
        f.write("\n".join(sorted(mix_data)))
        f.write(f"\n\n# Total: {len(mix_data)}\n# Updated: {now_str}")

    end_time = datetime.now()
    duration = end_time - start_time
    print("\n" + "="*50)
    print(f"ОТЧЕТ ЗАВЕРШЕН ({now_str})")
    print("-" * 50)
    for c, configs in structured_data.items():
        flag = COUNTRIES[c]['flag']
        print(f"{flag} {c.capitalize():<22}: {len(configs)} шт.")
    print("-" * 50)
    print(f"ВСЕГО УНИКАЛЬНЫХ              : {len(mix_data)}")
    print(f"НЕ ОТВЕТИЛИ (IP DOWN)         : {UNRESOLVED_COUNT}")
    print(f"ВРЕМЯ РАБОТЫ                  : {duration}")
    print("="*50 + "\n")

    git_commit_and_push()

if __name__ == "__main__":
    try:
        process()
    except Exception as e:
        print(f"\n[КРИТИЧЕСКАЯ ОШИБКА]: {e}")

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
    """Определяет страну IP-адреса через сервис ip-api.com."""
    global UNRESOLVED_COUNT, PROCESSED_COUNT
    
    if SHOULD_EXIT: 
        return None

    with CACHE_LOCK:
        if host in IP_CACHE:
            return IP_CACHE[host]

    # Базовая валидация хоста
    if not host or len(host) < 3 or host.startswith("127.") or "localhost" in host:
        return None

    url = f"http://ip-api.com/json/{host}?fields=status,countryCode"
    
    for attempt in range(2):
        if SHOULD_EXIT: break
        try:
            # Рандомизированная пауза ~1.4с для соблюдения лимитов (45 зап/мин)
            time.sleep(random.uniform(1.35, 1.65)) 
            resp = requests.get(url, timeout=10)
            
            if resp.status_code == 429:
                time.sleep(20)
                continue
                
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") == "success":
                    code = data.get("countryCode")
                    with CACHE_LOCK:
                        IP_CACHE[host] = code
                        PROCESSED_COUNT += 1
                        if PROCESSED_COUNT % 20 == 0:
                            print(f"[{datetime.now().strftime('%H:%M:%S')}] Прогресс GeoIP: {PROCESSED_COUNT} проверено.")
                    return code
                break
        except Exception:
            time.sleep(2)
            
    with CACHE_LOCK:
        UNRESOLVED_COUNT += 1
        IP_CACHE[host] = None
    return None

def validate_config(config):
    """Проверяет, является ли строка минимально валидным прокси-конфигом."""
    if not any(config.startswith(p) for p in PROTOCOLS): 
        return False
    if len(config) < 15: 
        return False
    host, port = get_server_info(config)
    return bool(host and port)

def sanitize_sources(file_path):
    """Очищает файл источников от дубликатов, пробелов и мусора."""
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
    print(f"Загружено источников: {len(clean)}")
    return clean

def save_results(structured_data, mix_data):
    """Записывает отфильтрованные данные в файлы и обновляет mix.txt."""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Сохранение по отдельным странам
    for country, configs in structured_data.items():
        file_name = f"{country}.txt"
        with open(file_name, 'w', encoding='utf-8') as f:
            if configs:
                f.write("\n".join(sorted(list(set(configs)))))
            f.write(f"\n\n# Total: {len(configs)}\n# Updated: {now_str}")

    # Сохранение общего микса
    with open("mix.txt", 'w', encoding='utf-8') as f:
        if mix_data:
            unique_mix = sorted(list(set(mix_data)))
            f.write("\n".join(unique_mix))
        f.write(f"\n\n# Total: {len(mix_data)}\n# Updated: {now_str}")
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Все файлы (.txt) обновлены.")

def git_commit_and_push():
    """Автоматизирует процесс отправки изменений в GitHub Actions."""
    try:
        # Настройка Git
        subprocess.run(["git", "config", "--global", "user.name", "Proxy-Parser-Bot"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "bot@proxy.local"], check=True)
        subprocess.run(["git", "add", "*.txt"], check=True)
        
        status = subprocess.run(["git", "diff", "--cached", "--quiet"])
        if status.returncode == 0:
            print("Нет изменений для отправки.")
            return

        commit_msg = f"Auto-Update: {datetime.now().strftime('%Y-%m-%d %H:%M')} | Nodes: {len(IP_CACHE)}"
        subprocess.run(["git", "commit", "-m", commit_msg], check=True)
        subprocess.run(["git", "pull", "--rebase"], check=True)
        
        res = subprocess.run(["git", "push"], capture_output=True, text=True)
        if res.returncode != 0:
            print(f"Ошибка Push: {res.stderr}")
        else:
            print(">>> Данные синхронизированы с GitHub.")
            
    except Exception as e:
        print(f"Ошибка Git: {e}")

def process():
    """Главная функция парсера."""
    start_time = datetime.now()
    print(f"--- ЗАПУСК ПАРСЕРА: {start_time.strftime('%Y-%m-%d %H:%M:%S')} ---")
    
    source_file = 'all_sources.txt'
    sources = sanitize_sources(source_file)
    if not sources:
        return

    # Используем множество для автоматической дедупликации на лету
    seen_nodes = set()
    unique_configs = []
    
    print("Сбор и фильтрация данных...")
    for url in sources:
        if SHOULD_EXIT: break
        
        if url.startswith("http"):
            try:
                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
                # Ограничиваем размер скачиваемого контента 15MB
                with requests.get(url, timeout=25, headers=headers, stream=True) as r:
                    r.raise_for_status()
                    chunks = []
                    downloaded = 0
                    for chunk in r.iter_content(chunk_size=1024*1024, decode_unicode=True):
                        if chunk:
                            chunks.append(chunk)
                            downloaded += len(chunk)
                        if downloaded > 15 * 1024 * 1024: 
                            break
                    
                    content = "".join(chunks)
                    del chunks # Освобождаем память
                    
                    if not any(p in content for p in PROTOCOLS):
                        decoded = decode_base64(content)
                        if decoded: content = decoded
                    
                    # Ищем совпадения и сразу фильтруем дубликаты
                    found_in_source = 0
                    for m in re.finditer(r'(?:vless|vmess|trojan|ss|hysteria2|tuic)://[^\s#"\'<>,]+', content):
                        cfg = m.group(0)
                        host, port = get_server_info(cfg)
                        if host and port:
                            node_id = f"{host}:{port}"
                            if node_id not in seen_nodes:
                                seen_nodes.add(node_id)
                                unique_configs.append(cfg)
                                found_in_source += 1
                    
                    print(f"  + {url[:50]}... : +{found_in_source} новых")
                    del content
                    gc.collect() # Очистка мусора после каждого источника
            except Exception:
                continue
        else:
            # Если в списке была прямая ссылка
            host, port = get_server_info(url)
            if host and port:
                node_id = f"{host}:{port}"
                if node_id not in seen_nodes:
                    seen_nodes.add(node_id)
                    unique_configs.append(url)

    total_unique = len(unique_configs)
    print(f"Всего уникальных узлов: {total_unique}")
    
    if total_unique == 0:
        return

    # Перемешиваем для разнообразия выборки
    random.shuffle(unique_configs)

    # Оптимальный лимит для работы в рамках 5-10 минут Actions
    limit = 350
    process_list = unique_configs[:limit]
    
    structured_data = {c: [] for c in COUNTRIES}
    mix_data = []
    
    print(f"Геолокация (Лимит сессии: {limit})...")
    
    for cfg in process_list:
        if SHOULD_EXIT: break
        
        host, _ = get_server_info(cfg)
        country_code = check_ip_location(host)
        
        if country_code:
            for country_key, info in COUNTRIES.items():
                if country_code == info["code"] or country_code == info.get("alt_code"):
                    structured_data[country_key].append(cfg)
                    break
        mix_data.append(cfg)

    # Сохранение и пуш
    save_results(structured_data, mix_data)
    git_commit_and_push()
    
    end_time = datetime.now()
    print(f"\n--- ГОТОВО: {end_time.strftime('%H:%M:%S')} (Затрачено: {end_time - start_time}) ---")

if __name__ == "__main__":
    try:
        process()
    except Exception as e:
        print(f"\n[КРИТИЧЕСКИЙ СБОЙ]: {e}")
        git_commit_and_push()
        sys.exit(1)

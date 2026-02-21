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
    print("\n[!] Получен сигнал остановки. Пытаюсь сохранить прогресс...")
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
    except:
        return ""

def get_server_info(config):
    """Извлекает хост и порт из строки конфигурации прокси."""
    try:
        if config.startswith("vmess://"):
            decoded = decode_base64(config[8:])
            if decoded:
                v_data = json.loads(decoded)
                return v_data.get('add', ''), str(v_data.get('port', ''))
        
        # Универсальное регулярное выражение для vless, trojan, ss и др.
        match = re.search(r'://(?:[^@]+@)?([^:/#\?]+):(\d+)', config)
        if match:
            return match.group(1), match.group(2)
    except:
        pass
    return None, None

def check_ip_location(host):
    """Определяет страну IP-адреса через сервис ip-api.com."""
    global UNRESOLVED_COUNT, PROCESSED_COUNT
    
    if SHOULD_EXIT: return None

    with CACHE_LOCK:
        if host in IP_CACHE:
            return IP_CACHE[host]

    # Валидация хоста
    if not host or len(host) < 3 or host.startswith("127."):
        return None

    url = f"http://ip-api.com/json/{host}?fields=status,countryCode"
    
    # Повторные попытки при ошибках сети или лимитах
    for _ in range(2):
        if SHOULD_EXIT: break
        try:
            # Лимит ip-api: 45 запросов в минуту. Пауза 1.4с позволяет идти стабильно.
            time.sleep(random.uniform(1.3, 1.6)) 
            resp = requests.get(url, timeout=8)
            
            if resp.status_code == 429:
                print(f"(!) Превышен лимит API. Ожидание...")
                time.sleep(10)
                continue
                
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") == "success":
                    code = data.get("countryCode")
                    with CACHE_LOCK:
                        IP_CACHE[host] = code
                        PROCESSED_COUNT += 1
                        if PROCESSED_COUNT % 10 == 0:
                            print(f"[{datetime.now().strftime('%H:%M:%S')}] Обработано IP: {PROCESSED_COUNT}")
                    return code
                break
        except:
            time.sleep(1)
            
    with CACHE_LOCK:
        UNRESOLVED_COUNT += 1
        IP_CACHE[host] = None
    return None

def validate_config(config):
    """Проверяет, является ли строка валидным прокси-конфигом."""
    if not any(config.startswith(p) for p in PROTOCOLS): return False
    if len(config) < 15: return False
    host, port = get_server_info(config)
    return bool(host and port)

def sanitize_sources(file_path):
    """Очищает файл источников от дубликатов и пустых строк."""
    if not os.path.exists(file_path): 
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
    return clean

def save_results(structured_data, mix_data):
    """Сохраняет результаты в файлы по странам и общий mix.txt."""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Сохранение по странам
    for country, configs in structured_data.items():
        with open(f"{country}.txt", 'w', encoding='utf-8') as f:
            if configs:
                f.write("\n".join(sorted(configs)))
            f.write(f"\n\n# Total: {len(configs)}\n# Updated: {now_str}")

    # Сохранение общего микса
    with open("mix.txt", 'w', encoding='utf-8') as f:
        if mix_data:
            f.write("\n".join(sorted(mix_data)))
        f.write(f"\n\n# Total: {len(mix_data)}\n# Updated: {now_str}")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Все файлы обновлены локально.")

def git_commit_and_push():
    """Синхронизирует обновленные файлы с репозиторием GitHub."""
    try:
        # Настройка параметров Git
        subprocess.run(["git", "config", "--global", "user.name", "Proxy-Parser-Bot"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "bot@proxy.local"], check=True)
        
        # Добавление всех .txt файлов в индекс
        subprocess.run(["git", "add", "*.txt"], check=True)
        
        # Проверка на наличие реальных изменений
        status_check = subprocess.run(["git", "diff", "--cached", "--quiet"])
        if status_check.returncode == 0:
            print("Изменений для фиксации нет.")
            return

        commit_msg = f"Auto-Update: {datetime.now().strftime('%Y-%m-%d %H:%M')}"
        subprocess.run(["git", "commit", "-m", commit_msg], check=True)
        
        # Rebase для предотвращения конфликтов в GitHub Actions
        subprocess.run(["git", "pull", "--rebase"], check=True)
        
        res = subprocess.run(["git", "push"], capture_output=True, text=True)
        if res.returncode != 0:
            print(f"Ошибка Git Push: {res.stderr}")
        else:
            print(">>> Репозиторий на GitHub успешно обновлен.")
    except Exception as e:
        print(f"Ошибка при работе с Git: {e}")

def process():
    """Основная управляющая функция парсера."""
    start_time = datetime.now()
    print(f"--- СТАРТ РАБОТЫ ПАРСЕРА: {start_time.strftime('%Y-%m-%d %H:%M:%S')} ---")
    
    source_file = 'all_sources.txt'
    sources = sanitize_sources(source_file)
    if not sources:
        print("Список источников пуст. Добавьте ссылки в all_sources.txt")
        return

    all_raw_links = []
    print("Загрузка и поиск ссылок в источниках...")
    for url in sources:
        if SHOULD_EXIT: break
        if url.startswith("http"):
            try:
                # Использование User-Agent для обхода базовых проверок на ботов
                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
                resp = requests.get(url, timeout=20, headers=headers)
                if resp.status_code == 200:
                    content = resp.text
                    
                    # Попытка декодировать контент, если ссылок в явном виде нет (base64 подписки)
                    if not any(p in content for p in PROTOCOLS):
                        decoded = decode_base64(content)
                        if decoded: content = decoded
                    
                    # Поиск всех прокси-ссылок по протоколам
                    found = re.findall(r'(?:vless|vmess|trojan|ss|hysteria2|tuic)://[^\s#"\'<>,]+', content)
                    all_raw_links.extend(found)
                    print(f"  + {url[:60]}... : Найдено {len(found)}")
            except:
                continue
        else:
            # Если в файле просто ссылка, а не URL источника
            all_raw_links.append(url)

    # Дедупликация на основе Host и Port
    unique_configs = []
    seen_nodes = set()
    for cfg in all_raw_links:
        if not validate_config(cfg): continue
        host, port = get_server_info(cfg)
        node_id = f"{host}:{port}"
        if node_id not in seen_nodes:
            seen_nodes.add(node_id)
            unique_configs.append(cfg)

    print(f"Итого уникальных прокси после очистки: {len(unique_configs)}")
    if not unique_configs: return

    # Перемешивание списка для равномерного распределения запросов по странам
    random.shuffle(unique_configs)

    # Лимит на количество проверок за один запуск GitHub Actions (для стабильности)
    limit = 250
    process_list = unique_configs[:limit]
    
    structured_data = {c: [] for c in COUNTRIES}
    mix_data = []
    
    print(f"Начинаю проверку геолокации (Лимит сессии: {limit})...")
    for cfg in process_list:
        if SHOULD_EXIT: break
        host, _ = get_server_info(cfg)
        code = check_ip_location(host)
        
        if code:
            # Поиск соответствия кода страны нашему списку
            for country_key, info in COUNTRIES.items():
                if code == info["code"] or code == info.get("alt_code"):
                    structured_data[country_key].append(cfg)
                    break
        
        # Добавляем в общий список
        mix_data.append(cfg)

    # Сохранение и пуш
    save_results(structured_data, mix_data)
    git_commit_and_push()
    
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"--- ГОТОВО: {end_time.strftime('%H:%M:%S')} ---")
    print(f"Длительность: {duration} | Ошибок GeoIP: {UNRESOLVED_COUNT}")

if __name__ == "__main__":
    try:
        process()
    except Exception as e:
        print(f"Критическая ошибка в основном цикле: {e}")
        # Попытка сохранить данные при фатальном сбое
        git_commit_and_push()

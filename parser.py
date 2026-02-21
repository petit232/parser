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
# Сопоставление кодов стран из API с именами файлов и флагами.
# Добавлены альтернативные коды для некоторых регионов.
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

# Список поддерживаемых протоколов для регулярного выражения
PROTOCOLS = ["vless://", "vmess://", "trojan://", "ss://", "hysteria2://", "tuic://"]

# Глобальные переменные для статистики и управления состоянием
IP_CACHE = {} 
CACHE_LOCK = threading.Lock()
UNRESOLVED_COUNT = 0 
PROCESSED_COUNT = 0
SHOULD_EXIT = False # Флаг для экстренного завершения (например, таймаут GitHub)

def signal_handler(sig, frame):
    """
    Обработчик сигналов завершения (SIGINT, SIGTERM).
    Позволяет скрипту корректно завершиться и сохранить данные при остановке GitHub Actions.
    """
    global SHOULD_EXIT
    print("\n[!] Получен сигнал завершения от системы. Пытаюсь сохранить текущий прогресс...")
    SHOULD_EXIT = True

# Регистрация обработчиков сигналов
signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def decode_base64(data):
    """
    Безопасное декодирование строк Base64 с автоматическим добавлением паддинга.
    Используется для обработки контента подписок.
    """
    try:
        data = data.strip()
        if not data:
            return ""
        missing_padding = len(data) % 4
        if missing_padding:
            data += '=' * (4 - missing_padding)
        return base64.b64decode(data).decode('utf-8', errors='ignore')
    except Exception as e:
        print(f"Ошибка декодирования Base64: {e}")
        return ""

def get_server_info(config):
    """
    Извлекает хост (IP/домен) и порт из различных форматов прокси-конфигов.
    """
    try:
        if config.startswith("vmess://"):
            v_data = json.loads(decode_base64(config[8:]))
            return v_data.get('add', ''), str(v_data.get('port', ''))
        
        # Универсальный поиск для vless, trojan, ss и т.д.
        match = re.search(r'://(?:[^@]+@)?([^:/]+):(\d+)', config)
        if match:
            return match.group(1), match.group(2)
    except Exception:
        pass
    return None, None

def check_ip_location(host):
    """
    Определяет код страны через ip-api.com.
    Реализован механизм повторных попыток при 429 (Rate Limit) и рандомизированные паузы.
    """
    global UNRESOLVED_COUNT, PROCESSED_COUNT
    
    if SHOULD_EXIT:
        return None

    with CACHE_LOCK:
        if host in IP_CACHE:
            return IP_CACHE[host]

    url = f"http://ip-api.com/json/{host}?fields=status,message,countryCode"
    retries = 2
    
    for i in range(retries):
        if SHOULD_EXIT:
            break
        try:
            # Лимит ip-api (бесплатно) — 45 запросов в минуту. 
            # Делаем паузу ~1.3 сек, чтобы гарантированно не попасть под бан.
            time.sleep(random.uniform(1.2, 1.6))
            
            resp = requests.get(url, timeout=10)
            
            if resp.status_code == 429:
                print(f"(!) Превышен лимит запросов API для {host}. Ожидание 15 секунд...")
                time.sleep(15)
                continue
                
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") == "success":
                    code = data.get("countryCode")
                    with CACHE_LOCK:
                        IP_CACHE[host] = code
                        PROCESSED_COUNT += 1
                        if PROCESSED_COUNT % 5 == 0:
                            print(f"[{datetime.now().strftime('%H:%M:%S')}] Обработано IP: {PROCESSED_COUNT}...")
                    return code
                elif data.get("message") == "reserved range":
                    break # Локальный IP, нет смысла проверять еще раз
                break
        except Exception as e:
            time.sleep(2)
            
    with CACHE_LOCK:
        UNRESOLVED_COUNT += 1
        IP_CACHE[host] = None
    return None

def validate_config(config):
    """
    Проверяет минимальную валидность конфига: протокол, длина, наличие хоста и порта.
    """
    if not any(config.startswith(p) for p in PROTOCOLS):
        return False
    if len(config) < 20:
        return False
    host, port = get_server_info(config)
    return bool(host and port)

def sanitize_sources(file_path):
    """
    Читает файл источников, удаляет дубликаты и лишние пробелы/кавычки.
    """
    if not os.path.exists(file_path): 
        print(f"Файл {file_path} не найден. Создаю новый.")
        with open(file_path, 'w', encoding='utf-8') as f:
            pass
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
    """
    Сохраняет распределенные по странам конфиги и общий список (mix.txt).
    """
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Сохранение по странам
    for country, configs in structured_data.items():
        file_name = f"{country}.txt"
        with open(file_name, 'w', encoding='utf-8') as f:
            if configs:
                f.write("\n".join(sorted(configs)))
            # Мета-информация в конце файла
            f.write(f"\n\n# Total: {len(configs)}\n# Updated: {now_str}")

    # Сохранение общего микса
    with open("mix.txt", 'w', encoding='utf-8') as f:
        if mix_data:
            f.write("\n".join(sorted(mix_data)))
        f.write(f"\n\n# Total: {len(mix_data)}\n# Updated: {now_str}")
        
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Все файлы (.txt) успешно обновлены.")

def git_commit_and_push():
    """
    Автоматический коммит и отправка изменений в репозиторий GitHub.
    """
    try:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Начало синхронизации с Git...")
        
        # Конфигурация пользователя для коммита
        subprocess.run(["git", "config", "--global", "user.name", "Proxy-Parser-Bot"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "bot@proxy.local"], check=True)
        
        # Добавление всех текстовых файлов
        subprocess.run(["git", "add", "*.txt"], check=True)
        
        # Проверка наличия изменений
        status_check = subprocess.run(["git", "diff", "--cached", "--quiet"])
        if status_check.returncode == 0:
            print("Изменений в файлах не обнаружено. Пуш отменен.")
            return

        commit_msg = f"Auto-Update: {datetime.now().strftime('%Y-%m-%d %H:%M')} (Verified: {len(IP_CACHE)})"
        subprocess.run(["git", "commit", "-m", commit_msg], check=True)
        
        # Подтягиваем изменения, если они были (rebase для чистоты истории)
        subprocess.run(["git", "pull", "--rebase"], check=True)
        
        # Финальный пуш
        push_res = subprocess.run(["git", "push"], capture_output=True, text=True)
        if push_res.returncode != 0:
            print(f"Ошибка Git Push:\n{push_res.stderr}")
        else:
            print(">>> Репозиторий успешно обновлен.")
            
    except Exception as e:
        print(f"Критическая ошибка Git: {e}")

def process():
    """
    Основная логика работы парсера.
    """
    start_time = datetime.now()
    print(f"--- ЗАПУСК ПАРСЕРА: {start_time.strftime('%Y-%m-%d %H:%M:%S')} ---")
    
    source_file = 'all_sources.txt'
    sources = sanitize_sources(source_file)
    if not sources:
        print("Работа завершена: источники не найдены.")
        return

    # Шаг 1: Сбор сырых данных из источников
    all_raw_links = []
    print("Сканирование источников...")
    for url in sources:
        if SHOULD_EXIT: break
        if url.startswith("http"):
            try:
                # Имитируем браузер для доступа к защищенным ресурсам
                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
                resp = requests.get(url, timeout=25, headers=headers)
                if resp.status_code == 200:
                    content = resp.text
                    # Если контент закодирован в base64 (часто в подписках)
                    if not any(p in content for p in PROTOCOLS):
                        content = decode_base64(content)
                    
                    found = re.findall(r'(?:vless|vmess|trojan|ss|hysteria2|tuic)://[^\s#"\'<>,]+', content)
                    all_raw_links.extend(found)
                    print(f"  + {url}: Найдено {len(found)}")
            except Exception as e:
                print(f"  - Ошибка при чтении {url}: {e}")
                continue
        else:
            # Прямая ссылка в файле
            all_raw_links.append(url)

    # Шаг 2: Удаление дубликатов по Host:Port
    unique_configs = []
    seen_nodes = set()
    for cfg in all_raw_links:
        if not validate_config(cfg): continue
        host, port = get_server_info(cfg)
        node_id = f"{host}:{port}"
        if node_id not in seen_nodes:
            seen_nodes.add(node_id)
            unique_configs.append(cfg)

    total_unique = len(unique_configs)
    print(f"Всего уникальных узлов для проверки: {total_unique}")
    if total_unique == 0:
        return

    # Перемешиваем очередь для равномерности запросов к разным странам
    random.shuffle(unique_configs)

    # Шаг 3: Ограничение выборки для GitHub Actions
    # Проверяем максимум 150 IP за раз, чтобы не вызвать таймаут и не забанить IP экшена
    limit = 150
    process_list = unique_configs[:limit]
    if total_unique > limit:
        print(f"ВНИМАНИЕ: Ограничение в {limit} проверок за сессию для стабильности.")

    structured_data = {c: [] for c in COUNTRIES}
    mix_data = []
    
    # Шаг 4: Идентификация стран (последовательно для минимизации банов API)
    print(f"Начинаю определение геолокации...")
    for cfg in process_list:
        if SHOULD_EXIT:
            break
            
        host, _ = get_server_info(cfg)
        country_code = check_ip_location(host)
        
        if country_code:
            # Ищем совпадение кода страны с нашими правилами
            matched = False
            for country_key, info in COUNTRIES.items():
                if country_code == info["code"] or country_code == info.get("alt_code"):
                    structured_data[country_key].append(cfg)
                    matched = True
                    break
        
        # Всегда добавляем в mix, если конфиг валиден
        mix_data.append(cfg)

    # Шаг 5: Финализация данных и сохранение
    save_results(structured_data, mix_data)
    
    # Шаг 6: Синхронизация с репозиторием
    git_commit_and_push()
    
    end_time = datetime.now()
    print(f"\n--- ОТЧЕТ ЗАВЕРШЕН ({end_time.strftime('%H:%M:%S')}) ---")
    print(f"Время работы: {end_time - start_time}")
    print(f"Обработано узлов: {len(process_list)}")
    print(f"Ошибок GeoIP: {UNRESOLVED_COUNT}")
    print("-" * 40)

if __name__ == "__main__":
    try:
        process()
    except Exception as e:
        print(f"\n[КРИТИЧЕСКИЙ СБОЙ]: {e}")
        # Пытаемся сохранить хоть что-то перед падением
        git_commit_and_push()
        sys.exit(1)

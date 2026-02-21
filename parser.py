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
        # Очистка: оставляем только валидные символы Base64 (A-Z, a-z, 0-9, +, /, =)
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
        # Игнорируем параметры после ? или #
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
    
    # Повторные попытки при ошибках сети или лимитах (45 запросов в минуту для ip-api)
    for attempt in range(2):
        if SHOULD_EXIT: break
        try:
            # Рандомизированная пауза ~1.4с для соблюдения лимитов
            time.sleep(random.uniform(1.35, 1.65)) 
            resp = requests.get(url, timeout=10)
            
            if resp.status_code == 429:
                # Если поймали Rate Limit, ждем дольше
                time.sleep(20)
                continue
                
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") == "success":
                    code = data.get("countryCode")
                    with CACHE_LOCK:
                        IP_CACHE[host] = code
                        PROCESSED_COUNT += 1
                        if PROCESSED_COUNT % 10 == 0:
                            print(f"[{datetime.now().strftime('%H:%M:%S')}] Статистика GeoIP: {PROCESSED_COUNT} проверено.")
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
        print(f"(!) Файл {file_path} не найден. Создаю пустой шаблон.")
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
    print(f"Загружено источников для обработки: {len(clean)}")
    return clean

def save_results(structured_data, mix_data):
    """Записывает отфильтрованные данные в файлы и обновляет mix.txt."""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Сохранение результатов в файлы...")
    
    # Сохранение по отдельным странам
    for country, configs in structured_data.items():
        file_name = f"{country}.txt"
        with open(file_name, 'w', encoding='utf-8') as f:
            if configs:
                # Сортируем для чистоты диффов в Git
                f.write("\n".join(sorted(list(set(configs)))))
            f.write(f"\n\n# Total: {len(configs)}\n# Updated: {now_str}")

    # Сохранение общего микса (ограничиваем до разумного количества для стабильности)
    with open("mix.txt", 'w', encoding='utf-8') as f:
        if mix_data:
            # Убираем дубликаты перед записью в микс
            unique_mix = sorted(list(set(mix_data)))
            f.write("\n".join(unique_mix))
        f.write(f"\n\n# Total: {len(mix_data)}\n# Updated: {now_str}")
    
    print(f"Успешно обновлено {len(structured_data) + 1} файлов.")

def git_commit_and_push():
    """Автоматизирует процесс отправки изменений в GitHub Actions."""
    try:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Синхронизация с GitHub...")
        
        # Конфигурация системного пользователя Git
        subprocess.run(["git", "config", "--global", "user.name", "Proxy-Parser-Bot"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "bot@proxy.local"], check=True)
        
        # Индексация всех текстовых файлов
        subprocess.run(["git", "add", "*.txt"], check=True)
        
        # Проверка: есть ли что коммитить
        status = subprocess.run(["git", "diff", "--cached", "--quiet"])
        if status.returncode == 0:
            print("Изменений для коммита не найдено.")
            return

        commit_msg = f"Auto-Update: {datetime.now().strftime('%Y-%m-%d %H:%M')} | Nodes: {len(IP_CACHE)}"
        subprocess.run(["git", "commit", "-m", commit_msg], check=True)
        
        # Pull с rebase для избежания конфликтов при параллельных запусках
        subprocess.run(["git", "pull", "--rebase"], check=True)
        
        # Push в текущую ветку
        res = subprocess.run(["git", "push"], capture_output=True, text=True)
        if res.returncode != 0:
            print(f"Ошибка при Push: {res.stderr}")
        else:
            print(">>> Данные успешно отправлены в репозиторий.")
            
    except Exception as e:
        print(f"Критическая ошибка Git: {e}")

def process():
    """Главная функция парсера."""
    start_time = datetime.now()
    print(f"--- ЗАПУСК ПАРСЕРА: {start_time.strftime('%Y-%m-%d %H:%M:%S')} ---")
    
    source_file = 'all_sources.txt'
    sources = sanitize_sources(source_file)
    if not sources:
        print("Остановка: файл all_sources.txt пуст.")
        return

    all_raw_links = []
    print("Начинаю сбор данных из источников...")
    
    for url in sources:
        if SHOULD_EXIT: break
        
        if url.startswith("http"):
            try:
                # Используем заголовки для предотвращения блокировок
                headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'}
                # Ограничиваем время ожидания и размер загрузки (20MB) для предотвращения OOM
                with requests.get(url, timeout=20, headers=headers, stream=True) as r:
                    r.raise_for_status()
                    content_parts = []
                    size_counter = 0
                    for chunk in r.iter_content(chunk_size=1024*1024, decode_unicode=True):
                        if chunk:
                            content_parts.append(chunk)
                            size_counter += len(chunk)
                        if size_counter > 20 * 1024 * 1024: # Лимит 20MB на файл
                            print(f"  ! {url[:40]}... слишком большой файл, обрезаю.")
                            break
                    
                    content = "".join(content_parts)
                    
                    # Пробуем декодировать Base64, если в тексте нет протоколов (формат подписок)
                    if not any(p in content for p in PROTOCOLS):
                        decoded = decode_base64(content)
                        if decoded: content = decoded
                    
                    # Поиск ссылок через Regex
                    # Используем finditer для экономии памяти при больших объемах
                    found_count = 0
                    for m in re.finditer(r'(?:vless|vmess|trojan|ss|hysteria2|tuic)://[^\s#"\'<>,]+', content):
                        all_raw_links.append(m.group(0))
                        found_count += 1
                    
                    print(f"  + {url[:50]}... : Найдено {found_count}")
                    
                    # Принудительная очистка памяти
                    del content
                    del content_parts
            except Exception as e:
                print(f"  - Ошибка в источнике {url[:40]}: {type(e).__name__}")
                continue
        else:
            # Если в файле просто ссылка на конфиг
            all_raw_links.append(url)

    # Дедупликация на основе уникальности Host и Port
    unique_configs = []
    seen_nodes = set()
    print("Фильтрация уникальных узлов...")
    for cfg in all_raw_links:
        if not validate_config(cfg): continue
        host, port = get_server_info(cfg)
        node_id = f"{host}:{port}"
        if node_id not in seen_nodes:
            seen_nodes.add(node_id)
            unique_configs.append(cfg)

    total_found = len(unique_configs)
    print(f"Всего уникальных узлов после очистки: {total_found}")
    
    if total_found == 0:
        print("Узлы не найдены. Выход.")
        return

    # Перемешиваем список, чтобы при каждом запуске проверялись разные IP
    random.shuffle(unique_configs)

    # Устанавливаем лимит проверок для одной сессии GitHub Actions
    # Оптимально 300, чтобы уложиться в лимиты API и время работы
    limit = 300
    process_list = unique_configs[:limit]
    
    structured_data = {c: [] for c in COUNTRIES}
    mix_data = []
    
    print(f"Начинаю проверку геолокации (Лимит сессии: {limit} из {total_found})...")
    
    for cfg in process_list:
        if SHOULD_EXIT: break
        
        host, _ = get_server_info(cfg)
        country_code = check_ip_location(host)
        
        if country_code:
            # Сопоставляем код страны с нашими категориями
            found_category = False
            for country_key, info in COUNTRIES.items():
                if country_code == info["code"] or country_code == info.get("alt_code"):
                    structured_data[country_key].append(cfg)
                    found_category = True
                    break
        
        # Всегда добавляем в общий микс (валидные узлы)
        mix_data.append(cfg)

    # Сохранение результатов в файлы
    save_results(structured_data, mix_data)
    
    # Пуш в репозиторий
    git_commit_and_push()
    
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"\n--- РАБОТА ЗАВЕРШЕНА: {end_time.strftime('%H:%M:%S')} ---")
    print(f"Длительность: {duration} | Проверено узлов: {len(process_list)} | Ошибки IP: {UNRESOLVED_COUNT}")

if __name__ == "__main__":
    try:
        process()
    except Exception as e:
        print(f"\n[КРИТИЧЕСКИЙ СБОЙ]: {e}")
        # Пытаемся спасти результаты даже при ошибке
        git_commit_and_push()
        sys.exit(1)

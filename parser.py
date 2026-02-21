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
    """Определяет страну IP-адреса через сервис ip-api.com с соблюдением лимитов."""
    global UNRESOLVED_COUNT, PROCESSED_COUNT
    
    if SHOULD_EXIT: 
        return None

    with CACHE_LOCK:
        if host in IP_CACHE:
            return IP_CACHE[host]

    # Базовая валидация хоста (не пустой, не локальный)
    if not host or len(host) < 3 or host.startswith("127.") or "localhost" in host:
        return None

    # API ip-api.com: 45 запросов в минуту для бесплатного плана
    url = f"http://ip-api.com/json/{host}?fields=status,message,countryCode"
    
    for attempt in range(3): # Увеличили количество попыток
        if SHOULD_EXIT: break
        try:
            # Рандомизированная пауза ~1.5с для строгого соблюдения лимитов
            time.sleep(random.uniform(1.4, 1.7)) 
            resp = requests.get(url, timeout=12)
            
            if resp.status_code == 429:
                # Если поймали Rate Limit, ждем значительно дольше
                print(f"(!) Rate Limit достигнут. Ожидание 30 секунд...")
                time.sleep(30)
                continue
                
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") == "success":
                    code = data.get("countryCode")
                    with CACHE_LOCK:
                        IP_CACHE[host] = code
                        PROCESSED_COUNT += 1
                        if PROCESSED_COUNT % 10 == 0:
                            print(f"[{datetime.now().strftime('%H:%M:%S')}] Статистика: {PROCESSED_COUNT} IP успешно проверено.")
                    return code
                else:
                    # Ошибка API (например, зарезервированный IP)
                    break
            else:
                time.sleep(2)
        except Exception as e:
            time.sleep(3)
            
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
    """Очищает файл источников от дубликатов и пустых строк."""
    if not os.path.exists(file_path): 
        print(f"(!) Источник {file_path} не найден. Создание пустого файла.")
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
    """Записывает результаты в файлы по странам и общий mix.txt."""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Сохранение по отдельным странам
    for country, configs in structured_data.items():
        file_name = f"{country}.txt"
        with open(file_name, 'w', encoding='utf-8') as f:
            if configs:
                # Финальная очистка дубликатов внутри списка страны
                unique_configs = sorted(list(set(configs)))
                f.write("\n".join(unique_configs))
            f.write(f"\n\n# Total: {len(configs)}\n# Updated: {now_str}")

    # Сохранение общего микса
    with open("mix.txt", 'w', encoding='utf-8') as f:
        if mix_data:
            unique_mix = sorted(list(set(mix_data)))
            f.write("\n".join(unique_mix))
        f.write(f"\n\n# Total: {len(mix_data)}\n# Updated: {now_str}")
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Обновление файлов завершено.")

def git_commit_and_push():
    """Синхронизация локальных изменений с GitHub репозиторием."""
    try:
        # Инициализация параметров Git
        subprocess.run(["git", "config", "--global", user.name, "VPN-Monster-Bot"], check=True)
        subprocess.run(["git", "config", "--global", user.email, "bot@vpn-monster.com"], check=True)
        
        # Добавляем все текстовые файлы
        subprocess.run(["git", "add", "*.txt"], check=True)
        
        # Проверяем наличие изменений (diff)
        status = subprocess.run(["git", "diff", "--cached", "--quiet"])
        if status.returncode == 0:
            print("Изменений для фиксации не обнаружено.")
            return

        commit_msg = f"Auto-Update: {datetime.now().strftime('%Y-%m-%d %H:%M')} | Verified: {len(IP_CACHE)}"
        subprocess.run(["git", "commit", "-m", commit_msg], check=True)
        
        # Pull для предотвращения конфликтов
        subprocess.run(["git", "pull", "--rebase"], check=True)
        
        # Push в репозиторий
        res = subprocess.run(["git", "push"], capture_output=True, text=True)
        if res.returncode != 0:
            print(f"Ошибка при выполнении Git Push: {res.stderr}")
        else:
            print(">>> Репозиторий GitHub успешно обновлен.")
            
    except Exception as e:
        print(f"Ошибка Git-интеграции: {e}")

def process():
    """Основная логика парсера и классификатора."""
    start_time = datetime.now()
    print(f"--- ЗАПУСК ПАРСЕРА: {start_time.strftime('%Y-%m-%d %H:%M:%S')} ---")
    
    source_file = 'all_sources.txt'
    sources = sanitize_sources(source_file)
    if not sources:
        return

    # Группировка по протоколам для обеспечения разнообразия выборки
    proto_groups = defaultdict(list)
    seen_nodes = set()
    
    print("Этап 1: Сбор сырых данных из источников...")
    for url in sources:
        if SHOULD_EXIT: break
        
        if url.startswith("http"):
            try:
                # Используем стандартные заголовки браузера
                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
                # Стримим контент, чтобы не перегружать память
                with requests.get(url, timeout=30, headers=headers, stream=True) as r:
                    r.raise_for_status()
                    chunks = []
                    downloaded_size = 0
                    for chunk in r.iter_content(chunk_size=1024*1024, decode_unicode=True):
                        if chunk:
                            chunks.append(chunk)
                            downloaded_size += len(chunk)
                        # Защита от слишком тяжелых файлов (лимит 10МБ)
                        if downloaded_size > 10 * 1024 * 1024:
                            print(f"  ! Источник {url[:40]}... слишком большой, обрезано.")
                            break
                    
                    content = "".join(chunks)
                    del chunks 
                    
                    # Если в тексте нет прямых ссылок, пробуем декодировать весь текст как Base64 (формат подписки)
                    if not any(p in content for p in PROTOCOLS):
                        decoded = decode_base64(content)
                        if decoded: content = decoded
                    
                    found_in_source = 0
                    # Регулярное выражение для поиска всех типов ссылок
                    for m in re.finditer(r'(?:vless|vmess|trojan|ss|hysteria2|tuic)://[^\s#"\'<>,]+', content):
                        cfg = m.group(0)
                        host, port = get_server_info(cfg)
                        if host and port:
                            node_id = f"{host}:{port}"
                            if node_id not in seen_nodes:
                                seen_nodes.add(node_id)
                                # Распределяем по группам протоколов
                                for p in PROTOCOLS:
                                    if cfg.startswith(p):
                                        proto_groups[p].append(cfg)
                                        found_in_source += 1
                                        break
                    
                    print(f"  + {url[:50]}... : Найдено {found_in_source} новых узлов.")
                    del content
                    gc.collect() # Принудительный сбор мусора для стабильности в GitHub Actions
            except Exception as e:
                print(f"  - Ошибка в источнике {url[:40]}: {type(e).__name__}")
                continue
        else:
            # Обработка прямых ссылок, если они есть в списке источников
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
    print(f"\nСтатистика найденных уникальных узлов:")
    for p, items in proto_groups.items():
        print(f"  - {p}: {len(items)}")
    
    if total_unique == 0:
        print("Валидные узлы не найдены.")
        return

    # Этап 2: Формирование очереди на проверку GeoIP
    # Используем алгоритм Round Robin, чтобы взять поровну из каждого типа протокола
    process_list = []
    limit_per_session = 350 # Безопасный лимит для ip-api.com
    
    # Перемешиваем узлы внутри каждой группы
    for p in proto_groups:
        random.shuffle(proto_groups[p])
    
    # Вытягиваем по одному из каждой группы, пока не достигнем лимита
    while len(process_list) < limit_per_session and any(proto_groups.values()):
        for p in list(proto_groups.keys()):
            if proto_groups[p]:
                process_list.append(proto_groups[p].pop(0))
            else:
                del proto_groups[p]
            if len(process_list) >= limit_per_session:
                break

    structured_data = {c: [] for c in COUNTRIES}
    mix_data = []
    
    print(f"\nЭтап 2: Проверка геолокации (Выбрано для проверки: {len(process_list)} из {total_unique})...")
    
    for cfg in process_list:
        if SHOULD_EXIT: break
        
        host, _ = get_server_info(cfg)
        country_code = check_ip_location(host)
        
        if country_code:
            # Сопоставляем код страны с нашими целевыми странами
            matched = False
            for country_key, info in COUNTRIES.items():
                if country_code == info["code"] or country_code == info.get("alt_code"):
                    structured_data[country_key].append(cfg)
                    matched = True
                    break
        
        # Все проверенные (или даже не определенные по стране) узлы идут в микс
        mix_data.append(cfg)

    # Этап 3: Сохранение и синхронизация
    save_results(structured_data, mix_data)
    git_commit_and_push()
    
    end_time = datetime.now()
    duration = end_time - start_time
    print(f"\n--- РАБОТА ЗАВЕРШЕНА: {end_time.strftime('%H:%M:%S')} ---")
    print(f"Общее время: {duration} | Ошибок GeoIP: {UNRESOLVED_COUNT}")

if __name__ == "__main__":
    try:
        process()
    except Exception as e:
        print(f"\n[КРИТИЧЕСКИЙ СБОЙ]: {e}")
        # Пытаемся сохранить хотя бы то, что успели обработать
        git_commit_and_push()
        sys.exit(1)

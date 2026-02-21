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
# Словарь содержит коды стран и соответствующие им названия файлов.
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

# Поддерживаемые протоколы прокси-серверов
PROTOCOLS = ["vless://", "vmess://", "trojan://", "ss://", "hysteria2://", "tuic://"]

# Глобальные переменные состояния
IP_CACHE = {} 
CACHE_LOCK = threading.Lock()
UNRESOLVED_COUNT = 0 
PROCESSED_COUNT = 0
SHOULD_EXIT = False 

def signal_handler(sig, frame):
    """Обработчик системных сигналов завершения."""
    global SHOULD_EXIT
    print("\n[!] Получен сигнал остановки (SIGINT/SIGTERM). Завершаю процессы...", flush=True)
    SHOULD_EXIT = True

signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

def decode_base64(data):
    """Безопасное декодирование Base64 с исправлением паддинга и очисткой мусора."""
    try:
        data = re.sub(r'[^a-zA-Z0-9+/=]', '', data)
        if not data:
            return ""
        missing_padding = len(data) % 4
        if missing_padding:
            data += '=' * (4 - missing_padding)
        return base64.b64decode(data).decode('utf-8', errors='ignore')
    except Exception:
        return ""

def get_server_info(config):
    """Извлечение IP/Хоста и порта из различных форматов конфигов."""
    try:
        if config.startswith("vmess://"):
            decoded = decode_base64(config[8:])
            if decoded:
                v_data = json.loads(decoded)
                return v_data.get('add', '').strip(), str(v_data.get('port', '')).strip()
        
        # Универсальный Regex для vless, trojan, ss и прочих
        match = re.search(r'://(?:[^@]+@)?([^:/#\?]+):(\d+)', config)
        if match:
            return match.group(1).strip(), match.group(2).strip()
    except Exception:
        pass
    return None, None

def check_ip_location(host):
    """Определение страны IP через ip-api.com с жестким соблюдением лимитов."""
    global UNRESOLVED_COUNT, PROCESSED_COUNT
    if SHOULD_EXIT: return None

    # Проверка кеша
    with CACHE_LOCK:
        if host in IP_CACHE:
            return IP_CACHE[host]

    # Валидация адреса
    if not host or len(host) < 3 or host.startswith("127.") or "localhost" in host:
        return None

    # Поля запроса: статус, сообщение и код страны
    url = f"http://ip-api.com/json/{host}?fields=status,message,countryCode"
    
    for attempt in range(3): 
        if SHOULD_EXIT: break
        try:
            # ЗАДЕРЖКА: 1.5 секунды между запросами для предотвращения бана IP GitHub-раннера.
            # Лимит ip-api: 45 запросов в минуту. Мы делаем максимум 40.
            time.sleep(1.5) 
            
            resp = requests.get(url, timeout=15)
            
            if resp.status_code == 429:
                print(f"(!) Rate Limit (429). Жду 75 секунд...", flush=True)
                time.sleep(75)
                continue
                
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") == "success":
                    code = data.get("countryCode")
                    with CACHE_LOCK:
                        IP_CACHE[host] = code
                        PROCESSED_COUNT += 1
                        if PROCESSED_COUNT % 10 == 0:
                            print(f"[{datetime.now().strftime('%H:%M:%S')}] GeoIP Прогресс: {PROCESSED_COUNT} проверено.", flush=True)
                    return code
                else:
                    # Ошибка в теле ответа (например, private IP)
                    break
            else:
                time.sleep(2)
        except Exception:
            time.sleep(2)
            
    with CACHE_LOCK:
        UNRESOLVED_COUNT += 1
        IP_CACHE[host] = None
    return None

def sanitize_sources(file_path):
    """Очистка списка источников от мусора и дубликатов."""
    if not os.path.exists(file_path): 
        print(f"(!) Источник {file_path} не найден. Создаю новый.", flush=True)
        with open(file_path, 'w', encoding='utf-8') as f: pass
        return []
        
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.read().splitlines()
    except Exception as e:
        print(f"Ошибка при чтении файла источников: {e}", flush=True)
        return []
        
    clean, seen = [], set()
    for line in lines:
        s = line.strip().strip('",\'').strip()
        if s and s not in seen:
            clean.append(s)
            seen.add(s)
            
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write("\n".join(clean))
    print(f"Загружено уникальных источников: {len(clean)}", flush=True)
    return clean

def save_results(structured_data, mix_data):
    """Сохранение отфильтрованных конфигов в текстовые файлы."""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Сохранение по странам
    for country, configs in structured_data.items():
        file_name = f"{country}.txt"
        try:
            with open(file_name, 'w', encoding='utf-8') as f:
                if configs:
                    # Удаляем дубликаты и сортируем
                    unique_list = sorted(list(set(configs)))
                    f.write("\n".join(unique_list))
                f.write(f"\n\n# Total: {len(configs)}\n# Updated: {now_str}")
        except Exception as e:
            print(f"Ошибка записи в {file_name}: {e}", flush=True)

    # Сохранение общего микса
    try:
        with open("mix.txt", 'w', encoding='utf-8') as f:
            if mix_data:
                unique_mix = sorted(list(set(mix_data)))
                f.write("\n".join(unique_mix))
            f.write(f"\n\n# Total: {len(mix_data)}\n# Updated: {now_str}")
    except Exception as e:
        print(f"Ошибка записи в mix.txt: {e}", flush=True)

def git_commit_and_push():
    """Синхронизация результатов с репозиторием GitHub."""
    print("Инициализация синхронизации с GitHub...", flush=True)
    try:
        subprocess.run(["git", "config", "--global", "user.name", "VPN-Monster-Bot"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "bot@vpn-monster.com"], check=True)
        
        subprocess.run(["git", "add", "*.txt"], check=True)
        
        # Проверка наличия изменений
        diff_check = subprocess.run(["git", "diff", "--cached", "--quiet"])
        if diff_check.returncode == 0:
            print("Изменений в файлах не обнаружено. Пропускаю коммит.", flush=True)
            return

        commit_msg = f"Auto-Update: {datetime.now().strftime('%Y-%m-%d %H:%M')} | Verified: {len(IP_CACHE)}"
        subprocess.run(["git", "commit", "-m", commit_msg], check=True)
        
        # Решение конфликтов через rebase
        subprocess.run(["git", "pull", "--rebase", "origin", "main"], check=False)
        
        # Финальный пуш
        res = subprocess.run(["git", "push", "origin", "main"], capture_output=True, text=True)
        
        if res.returncode != 0:
            print(f"Ошибка Git Push: {res.stderr}", flush=True)
        else:
            print(">>> Данные успешно запушены в репозиторий.", flush=True)
    except Exception as e:
        print(f"Критическая ошибка Git: {e}", flush=True)

def process():
    """Основной процесс парсинга и фильтрации."""
    start_time = datetime.now()
    print(f"--- ЗАПУСК ПАРСЕРА: {start_time.strftime('%Y-%m-%d %H:%M:%S')} ---", flush=True)
    
    sources = sanitize_sources('all_sources.txt')
    if not sources:
        print("Источники не найдены. Выход.", flush=True)
        return

    proto_groups = defaultdict(list)
    seen_nodes = set()
    
    print("Этап 1: Сбор данных из источников...", flush=True)
    for url in sources:
        if SHOULD_EXIT: break
        
        if url.startswith("http"):
            try:
                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
                with requests.get(url, timeout=30, headers=headers, stream=True) as r:
                    r.raise_for_status()
                    
                    # Читаем данные чанками для защиты от гигантских файлов
                    full_content = ""
                    for chunk in r.iter_content(chunk_size=1024*1024, decode_unicode=True):
                        if chunk: full_content += chunk
                        if len(full_content) > 12 * 1024 * 1024: # Лимит 12МБ
                            print(f"  ! Источник {url[:30]}... слишком большой, обрезаю.", flush=True)
                            break
                    
                    # Проверка на Base64 (подписки)
                    if not any(p in full_content for p in PROTOCOLS):
                        decoded = decode_base64(full_content)
                        if decoded: full_content = decoded
                    
                    found_count = 0
                    # Регулярное выражение для захвата всех типов прокси
                    for m in re.finditer(r'(?:vless|vmess|trojan|ss|hysteria2|tuic)://[^\s#"\'<>,]+', full_content):
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
                    
                    print(f"  + {url[:45]}... : Найдено {found_count}", flush=True)
                    del full_content
                    gc.collect() # Очистка памяти
            except Exception as e:
                print(f"  ! Ошибка в {url[:30]}: {e}", flush=True)
                continue
        else:
            # Обработка прямых строк конфигов в файле источников
            host, port = get_server_info(url)
            if host and port:
                nid = f"{host}:{port}"
                if nid not in seen_nodes:
                    seen_nodes.add(nid)
                    for p in PROTOCOLS:
                        if url.startswith(p):
                            proto_groups[p].append(url)
                            break

    if not seen_nodes:
        print("Валидных прокси не найдено.", flush=True)
        return

    # Ограничение очереди для предотвращения таймаутов GitHub Actions
    process_list = []
    limit_per_run = 380 
    
    # Перемешивание групп
    for p in proto_groups:
        random.shuffle(proto_groups[p])
    
    # Алгоритм Round Robin для разнообразия протоколов в результатах
    while len(process_list) < limit_per_run and any(proto_groups.values()):
        for p in list(proto_groups.keys()):
            if proto_groups[p]:
                process_list.append(proto_groups[p].pop(0))
            else:
                del proto_groups[p]
            if len(process_list) >= limit_per_run: break

    structured_data = {c: [] for c in COUNTRIES}
    mix_data = []
    
    print(f"\nЭтап 2: Проверка GeoIP ({len(process_list)} узлов)...", flush=True)
    for cfg in process_list:
        if SHOULD_EXIT: break
        
        host, _ = get_server_info(cfg)
        country_code = check_ip_location(host)
        
        if country_code:
            # Сверка кода страны с нашим списком
            for key, info in COUNTRIES.items():
                if country_code == info["code"] or country_code == info.get("alt_code"):
                    structured_data[key].append(cfg)
                    break
        
        mix_data.append(cfg)

    # Сохранение и фиксация изменений
    save_results(structured_data, mix_data)
    git_commit_and_push()
    
    end_time = datetime.now()
    print(f"\n--- РАБОТА ЗАВЕРШЕНА: {end_time.strftime('%H:%M:%S')} ---", flush=True)
    print(f"Общее время выполнения: {end_time - start_time}", flush=True)

if __name__ == "__main__":
    try:
        process()
    except Exception as e:
        print(f"\n[КРИТИЧЕСКИЙ СБОЙ ПРИЛОЖЕНИЯ]: {e}", flush=True)
        # Экстренная попытка сохранить данные
        try:
            git_commit_and_push()
        except:
            pass
        sys.exit(1)

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

# --- КОНФИГУРАЦИЯ ---
# Сопоставление кодов стран из API с именами файлов и флагами.
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

PROTOCOLS = ["vless://", "vmess://", "trojan://", "ss://", "hysteria2://", "tuic://"]
IP_CACHE = {} 
CACHE_LOCK = threading.Lock()
UNRESOLVED_COUNT = 0 # Счетчик IP, которые не ответили

def decode_base64(data):
    """Безопасное декодирование Base64."""
    try:
        data = data.strip()
        if not data: return ""
        missing_padding = len(data) % 4
        if missing_padding:
            data += '=' * (4 - missing_padding)
        return base64.b64decode(data).decode('utf-8', errors='ignore')
    except Exception as e:
        return ""

def get_server_info(config):
    """Извлекает хост (домен или IP) из конфига."""
    try:
        if config.startswith("vmess://"):
            v_data = json.loads(decode_base64(config[8:]))
            return v_data.get('add', ''), v_data.get('port', '')
        
        match = re.search(r'://(?:[^@]+@)?([^:/]+):(\d+)', config)
        if match:
            return match.group(1), match.group(2)
    except Exception:
        pass
    return None, None

def check_ip_location(host):
    """
    Определяет страну по IP или домену через GeoIP API.
    Внедрена логика повторных попыток и фильтр 'не отвечает'.
    """
    global UNRESOLVED_COUNT
    with CACHE_LOCK:
        if host in IP_CACHE:
            return IP_CACHE[host]

    url = f"http://ip-api.com/json/{host}?fields=status,message,countryCode"
    retries = 3
    backoff = 2

    for i in range(retries):
        try:
            # Случайная пауза для обхода анти-спам фильтров API
            time.sleep(random.uniform(0.5, 1.2))
            
            resp = requests.get(url, timeout=15)
            
            if resp.status_code == 429: # Лимит запросов
                time.sleep(backoff)
                backoff *= 2
                continue
                
            if resp.status_code == 200:
                data = resp.json()
                if data.get("status") == "success":
                    code = data.get("countryCode")
                    with CACHE_LOCK:
                        IP_CACHE[host] = code
                    return code
                elif data.get("message") == "reserved range":
                    break # Локальные/приватные IP
        except Exception:
            time.sleep(1)
            
    # Если мы здесь, значит IP не ответил или API выдал ошибку после ретраев
    with CACHE_LOCK:
        UNRESOLVED_COUNT += 1
        IP_CACHE[host] = None
    return None

def validate_config(config):
    """Проверяет минимальную техническую валидность конфига."""
    if len(config) < 15: return False
    host, port = get_server_info(config)
    if not host or not port: return False
    return True

def sanitize_sources(file_path):
    """Очистка списка источников (all_sources.txt)."""
    if not os.path.exists(file_path): 
        print(f"Ошибка: Файл {file_path} не найден.")
        return []
    with open(file_path, 'r', encoding='utf-8') as f:
        raw_lines = f.read().splitlines()
    clean = []
    seen = set()
    for line in raw_lines:
        s = line.strip().strip('",\'').strip()
        if s and s not in seen and (s.startswith("http") or any(p in s for p in PROTOCOLS)):
            clean.append(s)
            seen.add(s)
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write("\n".join(clean))
    print(f"Источники очищены. Уникальных ссылок: {len(clean)}")
    return clean

def git_commit_and_push():
    """
    Автоматический коммит и пуш изменений в репозиторий.
    Помогает, если GitHub Actions настроен на запуск скрипта.
    """
    try:
        print(f"[{datetime.now().strftime('%H:%M:%S')}] Проверка изменений в Git...")
        # Настройка пользователя (необходимо для работы в Actions)
        subprocess.run(["git", "config", "--global", "user.name", "GitHub Action Bot"], check=True)
        subprocess.run(["git", "config", "--global", "user.email", "actions@github.com"], check=True)
        
        # Добавляем все измененные .txt файлы
        subprocess.run(["git", "add", "*.txt"], check=True)
        
        # Проверяем, есть ли что коммитить
        status = subprocess.run(["git", "status", "--porcelain"], capture_output=True, text=True).stdout
        if not status:
            print("Изменений для коммита не обнаружено.")
            return

        commit_msg = f"Auto-update configs: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        subprocess.run(["git", "commit", "-m", commit_msg], check=True)
        subprocess.run(["git", "push"], check=True)
        print("Изменения успешно отправлены в репозиторий!")
    except Exception as e:
        print(f"Ошибка при выполнении Git Push: {e}")

def process():
    source_file = 'all_sources.txt'
    sources = sanitize_sources(source_file)
    if not sources: 
        print("Работа завершена: нет источников.")
        return

    all_raw_links = []
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Сбор данных из источников...")

    for url in sources:
        if url.startswith("http"):
            try:
                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
                resp = requests.get(url, headers=headers, timeout=25)
                if resp.status_code == 200:
                    text = resp.text
                    if not any(p in text for p in PROTOCOLS):
                        text = decode_base64(text)
                    found = re.findall(r'(?:vless|vmess|trojan|ss|hysteria2|tuic)://[^\s#"\'<>,]+', text)
                    all_raw_links.extend(found)
            except Exception as e: 
                print(f"Не удалось загрузить {url}: {e}")
                continue
        else:
            all_raw_links.append(url)

    # Дедупликация перед GeoIP
    unique_configs = []
    seen_uids = set()
    for cfg in all_raw_links:
        if not validate_config(cfg): continue
        host, port = get_server_info(cfg)
        uid = f"{host}:{port}"
        if uid not in seen_uids:
            seen_uids.add(uid)
            unique_configs.append(cfg)

    print(f"Найдено уникальных конфигов: {len(unique_configs)}")
    if not unique_configs:
        print("Уникальных конфигов не найдено. Выход.")
        return

    # Рандомизация для обхода защит API
    random.shuffle(unique_configs)

    structured_data = {c: [] for c in COUNTRIES}
    mix_data = []
    
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Идентификация стран через IP-API (многопоточность)...")
    
    def worker(cfg):
        host, _ = get_server_info(cfg)
        country_code = check_ip_location(host)
        
        if country_code:
            for c_name, info in COUNTRIES.items():
                if country_code == info["code"] or country_code == info.get("alt_code"):
                    with CACHE_LOCK:
                        structured_data[c_name].append(cfg)
                    break
        with CACHE_LOCK:
            mix_data.append(cfg)

    # Работа в несколько потоков
    max_workers = 5 
    threads = []
    
    for cfg in unique_configs:
        t = threading.Thread(target=worker, args=(cfg,))
        threads.append(t)
        t.start()
        
        if len(threads) >= max_workers:
            for t in threads: t.join()
            threads = []
            time.sleep(random.uniform(0.5, 1.5))
            
    for t in threads: t.join()

    # Сохранение файлов
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Сохранение результатов в файлы...")
    
    for country, configs in structured_data.items():
        with open(f"{country}.txt", 'w', encoding='utf-8') as f:
            if configs:
                f.write("\n".join(sorted(configs)))
            f.write(f"\n\n# Total: {len(configs)}\n# Updated: {now_str}")

    with open("mix.txt", 'w', encoding='utf-8') as f:
        f.write("\n".join(sorted(mix_data)))
        f.write(f"\n\n# Total: {len(mix_data)}\n# Updated: {now_str}")

    # Финальный отчет в консоль
    print("\n" + "="*45)
    print(f"ПРОФЕССИОНАЛЬНЫЙ ОТЧЕТ ({now_str})")
    print("="*45)
    for c, configs in structured_data.items():
        flag = COUNTRIES[c]['flag']
        print(f"{flag} {c.capitalize():<20}: {len(configs)} шт.")
    print("-" * 45)
    print(f"ВСЕГО В MIX (УНИКАЛЬНЫЕ)      : {len(mix_data)}")
    print(f"НЕ ОТВЕТИЛИ/НЕ ОПРЕДЕЛЕНЫ      : {UNRESOLVED_COUNT}")
    print(f"ВСЕГО ПРОВЕРЕНО ХОСТОВ         : {len(IP_CACHE)}")
    print("="*45)

    # Пытаемся запушить изменения (защита от того, что файлы не обновляются)
    git_commit_and_push()

if __name__ == "__main__":
    process()

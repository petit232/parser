import os
import re
import requests
import base64
from datetime import datetime

# --- МАКСИМАЛЬНЫЙ СПРАВОЧНИК СТРАН И МАРКЕРОВ ---
COUNTRIES = {
    "belarus": {"keys": ["🇧🇾", "by", "belarus", "беларусь", "минск", "minsk", "msq", "by.adr-cloud.ru", "by.cdn.titun.su"], "flag": "🇧🇾"},
    "kazakhstan": {"keys": ["🇰🇿", "kazakhstan", "казахстан", "алматы", "астана", "astana", "almaty", "ala", "tse", "kz.adrenaline-fast.ru", "kz1.sky-vault.top", "pavlodar"], "flag": "🇰🇿"},
    "germany": {"keys": ["🇩🇪", "germany", "германия", "frankfurt", "berlin", "fra", "falkenstein", "⚡️de", "germ.adrenaline-fast.ru", "de.cdn.stun.su", "de5.sky-vault.top", "freede.spectrum.vu", "dreieich", "hennigsdorf", "limburg", "nuremberg"], "flag": "🇩🇪"},
    "poland": {"keys": ["🇵🇱", "poland", "польша", "warsaw", "warszawa", "waw", "pl", "plr.strelkavpn.ru"], "flag": "🇵🇱"},
    "usa": {"keys": ["🇺🇸", "usa", "сша", "united states", "america", "jfk", "lax", "sjc", "microsoft", "volumedrive", "us"], "flag": "🇺🇸"},
    "sweden": {"keys": ["🇸🇪", "sweden", "швеция", "stockholm", "sto", "se", "sw.adr-cloud.ru", "game-sw.adrtun.ru", "secdn16.suio.me", "spånga", "östhammar"], "flag": "🇸🇪"},
    "netherlands": {"keys": ["🇳🇱", "netherlands", "нидерланды", "amsterdam", "ams", "nl", "download.lastilhame.monster"], "flag": "🇳🇱"},
    "latvia_lithuania": {"keys": ["🇱🇻", "🇱🇹", "latvia", "lithuania", "латвия", "литва", "riga", "vilnius", "rix", "vno", "lat.adrenaline-fast.ru"], "flag": "🇱🇻"},
    "russia": {"keys": ["🇷🇺", "russia", "россия", "moscow", "mow", "svo", "dme", "vko", "led", "saint-petersburg", "ru", "rus"], "flag": "🇷🇺"},
    "singapore": {"keys": ["🇸🇬", "singapore", "сингапур", "sin", "changi", "sg"], "flag": "🇸🇬"},
    "uk": {"keys": ["🇬🇧", "uk", "gb", "united kingdom", "london", "lon", "lhr"], "flag": "🇬🇧"},
    "hongkong": {"keys": ["🇭🇰", "hong kong", "гонконг", "hkg", "hk"], "flag": "🇭🇰"},
    "finland": {"keys": ["🇫🇮", "finland", "финляндия", "helsinki", "hel", "fi"], "flag": "🇫🇮"},
    "france": {"keys": ["🇫🇷", "france", "франция", "paris", "cdg", "ovh", "fr"], "flag": "🇫🇷"}
}

PROTOCOLS = ["vless://", "vmess://", "trojan://", "ss://", "hysteria2://", "tuic://"]

def decode_base64(data):
    """Декодирует содержимое, если оно представлено в формате Base64."""
    try:
        data = data.strip()
        if not data: return ""
        missing_padding = len(data) % 4
        if missing_padding:
            data += '=' * (4 - missing_padding)
        return base64.b64decode(data).decode('utf-8', errors='ignore')
    except Exception:
        return data

def get_unique_id(config):
    """Извлекает уникальный адрес и порт сервера для удаления дубликатов."""
    match = re.search(r'://([^/?#@]+@)?([^/?#:]+:[0-9]+|[^/?#:]+)', config)
    return match.group(2) if match else config

def sanitize_sources(file_path):
    """
    Защита от затупка: чистит all_sources.txt от дубликатов ссылок, 
    мусорных символов и пустых строк. Обновляет файл.
    """
    if not os.path.exists(file_path):
        return []
    
    print(f"Очистка и проверка {file_path}...")
    with open(file_path, 'r', encoding='utf-8') as f:
        raw_lines = f.read().splitlines()

    clean_sources = []
    seen = set()

    for line in raw_lines:
        # Удаляем кавычки, запятые, лишние пробелы по краям
        s = line.strip().strip('",\'').strip()
        
        # Пропускаем пустые строки и дубликаты
        if not s or s in seen:
            continue
        
        # Проверка: является ли это валидной ссылкой или прокси-конфигом
        if s.startswith("http") or any(proto in s for proto in PROTOCOLS):
            clean_sources.append(s)
            seen.add(s)

    # Перезаписываем файл-источник чистыми данными
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write("\n".join(clean_sources))
    
    print(f"Очистка завершена. Было: {len(raw_lines)}, стало: {len(clean_sources)}")
    return clean_sources

def process():
    source_file = 'all_sources.txt'
    
    # Сначала чистим источники (Защита от затупка)
    sources = sanitize_sources(source_file)
    
    if not sources:
        print("Список источников пуст после очистки.")
        return

    all_raw_links = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    timestamp_mark = f"\n\n# Last Update: {now}"

    print(f"Начинаю сбор данных из {len(sources)} проверенных источников...")

    for url in sources:
        if url.startswith("http"):
            try:
                print(f"Загрузка: {url}")
                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
                resp = requests.get(url, headers=headers, timeout=30)
                if resp.status_code == 200:
                    text = resp.text
                    if not any(p in text for p in PROTOCOLS):
                        text = decode_base64(text)
                    
                    found = re.findall(r'(?:vless|vmess|trojan|ss|hysteria2|tuic)://[^\s#"\'<>,]+', text)
                    all_raw_links.extend(found)
                    print(f"--- Найдено: {len(found)} шт.")
            except Exception as e:
                print(f"--- Ошибка загрузки {url}: {e}")
        elif any(proto in url for proto in PROTOCOLS):
            found = re.findall(r'(?:vless|vmess|trojan|ss|hysteria2|tuic)://[^\s#"\'<>,]+', url)
            all_raw_links.extend(found if found else [url])

    structured_data = {country: set() for country in COUNTRIES}
    mix_data = set()
    unique_check = set()

    print("Фильтрация дубликатов конфигов и распределение по странам...")

    for config in all_raw_links:
        config = config.strip()
        uid = get_unique_id(config)
        
        if uid in unique_check:
            continue
        unique_check.add(uid)

        config_lower = config.lower()
        assigned = False
        
        # 1. Поиск по флагам
        for country, info in COUNTRIES.items():
            if info["flag"] in config:
                structured_data[country].add(config)
                assigned = True
                break
        
        # 2. Поиск по ключевым словам
        if not assigned:
            for country, info in COUNTRIES.items():
                for key in info["keys"]:
                    key_low = key.lower()
                    if len(key_low) <= 3:
                        if re.search(r'[^a-z0-9]' + re.escape(key_low) + r'[^a-z0-9]', f" {config_lower} "):
                            structured_data[country].add(config)
                            assigned = True
                            break
                    elif key_low in config_lower:
                        structured_data[country].add(config)
                        assigned = True
                        break
                if assigned:
                    break

        mix_data.add(config)

    # СОХРАНЕНИЕ
    print("Сохранение результатов...")
    for country in COUNTRIES:
        filename = f"{country}.txt"
        configs = sorted(list(structured_data[country]))
        with open(filename, 'w', encoding='utf-8') as f:
            if configs:
                f.write("\n".join(configs))
            f.write(timestamp_mark)

    with open("mix.txt", 'w', encoding='utf-8') as f:
        if mix_data:
            f.write("\n".join(sorted(list(mix_data))))
        f.write(timestamp_mark)

    print(f"Готово! Всего уникальных серверов сохранено: {len(mix_data)}")

if __name__ == "__main__":
    process()

import os
import re
import requests
import base64
import json
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
    """Очистка all_sources.txt от дублей и мусора."""
    if not os.path.exists(file_path):
        return []
    
    print(f"Очистка и проверка {file_path}...")
    with open(file_path, 'r', encoding='utf-8') as f:
        raw_lines = f.read().splitlines()

    clean_sources = []
    seen = set()

    for line in raw_lines:
        s = line.strip().strip('",\'').strip()
        if not s or s in seen:
            continue
        if s.startswith("http") or any(proto in s for proto in PROTOCOLS):
            clean_sources.append(s)
            seen.add(s)

    with open(file_path, 'w', encoding='utf-8') as f:
        f.write("\n".join(clean_sources))
    
    print(f"Очистка завершена. Источников: {len(clean_sources)}")
    return clean_sources

def identify_country(config):
    """
    Улучшенная логика идентификации страны.
    Проверяет флаги, домены и ключевые слова с использованием регулярных выражений.
    """
    config_lower = config.lower()
    
    # Специальная обработка для VMess (декодируем JSON, чтобы заглянуть внутрь)
    if config_lower.startswith("vmess://"):
        try:
            v_data = json.loads(decode_base64(config[8:]))
            search_text = (v_data.get('ps', '') + " " + v_data.get('add', '') + " " + v_data.get('sni', '')).lower()
        except:
            search_text = config_lower
    else:
        search_text = config_lower

    # 1. Сначала ищем флаги (самый надежный маркер)
    for country, info in COUNTRIES.items():
        if info["flag"] in config:
            return country

    # 2. Поиск по ключам с защитой от частичных совпадений
    for country, info in COUNTRIES.items():
        for key in info["keys"]:
            k_low = key.lower()
            
            # Если ключ — эмодзи или спецсимвол, ищем просто вхождением
            if any(ord(char) > 127 for char in k_low):
                if k_low in search_text:
                    return country
                continue

            # Регулярка: ищем ключ так, чтобы он не был частью другого слова
            # Границы: начало строки, конец строки, знаки пунктуации, точки, тире
            pattern = r'(?i)(?:\.|\-|_|/|@|\s|^)' + re.escape(k_low) + r'(?:\.|\-|_|/|@|\s|:|\?|#|$)'
            if re.search(pattern, search_text):
                return country
            
            # Дополнительная проверка: если ключ является частью домена (например, .by)
            if f".{k_low}." in search_text or search_text.endswith(f".{k_low}"):
                return country

    return None

def process():
    source_file = 'all_sources.txt'
    sources = sanitize_sources(source_file)
    
    if not sources:
        print("Список источников пуст.")
        return

    all_raw_links = []
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    timestamp_mark = f"\n\n# Last Update: {now}"

    print(f"Сбор данных из {len(sources)} источников...")

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
            except Exception as e:
                print(f"Ошибка загрузки {url}: {e}")
        elif any(proto in url for proto in PROTOCOLS):
            found = re.findall(r'(?:vless|vmess|trojan|ss|hysteria2|tuic)://[^\s#"\'<>,]+', url)
            all_raw_links.extend(found if found else [url])

    structured_data = {country: set() for country in COUNTRIES}
    mix_data = set()
    unique_check = set()

    print("Анализ локаций и удаление дубликатов...")

    for config in all_raw_links:
        config = config.strip()
        uid = get_unique_id(config)
        
        if uid in unique_check:
            continue
        unique_check.add(uid)

        country = identify_country(config)
        if country:
            structured_data[country].add(config)
        
        mix_data.add(config)

    # СОХРАНЕНИЕ
    print("Запись файлов...")
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

    print(f"Успех! Уникальных конфигов: {len(mix_data)}")

if __name__ == "__main__":
    process()

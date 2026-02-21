import os
import re
import requests
import base64
from datetime import datetime

# --- МАКСИМАЛЬНЫЙ СПРАВОЧНИК МАРКЕРОВ ---
# Объединяет флаги, города, аэропорты и технические домены
COUNTRIES = {
    "belarus": {"keys": ["🇧🇾", "by", "belarus", "беларусь", "минск", "minsk", "msq", "by.adr-cloud.ru"], "flag": "🇧🇾"},
    "kazakhstan": {"keys": ["🇰🇿", "kazakhstan", "казахстан", "astana", "almaty", "ala", "tse", "kz"], "flag": "🇰🇿"},
    "germany": {"keys": ["🇩🇪", "germany", "германия", "frankfurt", "berlin", "fra", "falkenstein", "de", "ger"], "flag": "🇩🇪"},
    "poland": {"keys": ["🇵🇱", "poland", "польша", "warsaw", "warszawa", "waw", "pl"], "flag": "🇵🇱"},
    "usa": {"keys": ["🇺🇸", "usa", "сша", "united states", "america", "jfk", "lax", "sjc", "us"], "flag": "🇺🇸"},
    "sweden": {"keys": ["🇸🇪", "sweden", "швеция", "stockholm", "sto", "se", "sw.adr-cloud.ru"], "flag": "🇸🇪"},
    "netherlands": {"keys": ["🇳🇱", "netherlands", "нидерланды", "amsterdam", "ams", "nl"], "flag": "🇳🇱"},
    "latvia_lithuania": {"keys": ["🇱🇻", "🇱🇹", "latvia", "lithuania", "латвия", "литва", "riga", "vilnius", "rix", "vno"], "flag": "🇱🇻"},
    "russia": {"keys": ["🇷🇺", "russia", "россия", "moscow", "mow", "svo", "dme", "vko", "led", "ru"], "flag": "🇷🇺"},
    "singapore": {"keys": ["🇸🇬", "singapore", "сингапур", "sin", "sg"], "flag": "🇸🇬"},
    "uk": {"keys": ["🇬🇧", "uk", "gb", "united kingdom", "london", "lon", "lhr"], "flag": "🇬🇧"},
    "hongkong": {"keys": ["🇭🇰", "hong kong", "гонконг", "hkg", "hk"], "flag": "🇭🇰"},
    "finland": {"keys": ["🇫🇮", "finland", "финляндия", "helsinki", "hel", "fi"], "flag": "🇫🇮"},
    "france": {"keys": ["🇫🇷", "france", "франция", "paris", "cdg", "fr"], "flag": "🇫🇷"}
}

PROTOCOLS = ["vless://", "vmess://", "trojan://", "ss://", "hysteria2://", "tuic://"]

def decode_base64(data):
    try:
        data = data.strip()
        missing_padding = len(data) % 4
        if missing_padding: data += '=' * (4 - missing_padding)
        return base64.b64decode(data).decode('utf-8')
    except: return data

def get_unique_id(config):
    # Извлечение IP и PORT для удаления дубликатов (чтобы не было одинаковых серверов)
    match = re.search(r'://([^/?#@]+@)?([^/?#:]+:[0-9]+|[^/?#:]+)', config)
    return match.group(2) if match else config

def process():
    all_raw_links = []
    source_file = 'all_sources.txt'
    
    if not os.path.exists(source_file):
        print("Файл all_sources.txt не найден!")
        return

    with open(source_file, 'r', encoding='utf-8') as f:
        lines = f.read().splitlines()

    for line in lines:
        line = line.strip()
        if not line: continue
        
        # Если ссылка на подписку
        if line.startswith("http") and not any(p in line for p in PROTOCOLS):
            try:
                resp = requests.get(line, timeout=20)
                content = decode_base64(resp.text)
                for sub_line in content.splitlines():
                    if any(proto in sub_line for proto in PROTOCOLS):
                        all_raw_links.append(sub_line.strip())
            except: pass
        # Если это прямая ссылка или текст с кодами
        elif any(proto in line for proto in PROTOCOLS):
            found = re.findall(r'(?:vless|vmess|trojan|ss|hysteria2|tuic)://[^\s,]+', line)
            all_raw_links.extend(found if found else [line])

    structured_data = {country: set() for country in COUNTRIES}
    mix_data = set()
    unique_check = set()

    for config in all_raw_links:
        uid = get_unique_id(config)
        if uid in unique_check: continue
        unique_check.add(uid)

        config_lower = config.lower()
        assigned = False
        
        # 1. Приоритет флагам
        for country, info in COUNTRIES.items():
            if info["flag"] in config:
                structured_data[country].add(config)
                assigned = True
                break
        
        # 2. Поиск по ключам
        if not assigned:
            for country, info in COUNTRIES.items():
                for key in info["keys"]:
                    if len(key) <= 3:
                        if re.search(r'[^a-z0-9]' + re.escape(key.lower()) + r'[^a-z0-9]', f" {config_lower} "):
                            structured_data[country].add(config)
                            assigned = True
                            break
                    elif key.lower() in config_lower:
                        structured_data[country].add(config)
                        assigned = True
                        break
                if assigned: break

        mix_data.add(config)

    # Запись файлов (перезаписываем старые файлы по тем же ссылкам)
    for country, configs in structured_data.items():
        with open(f"{country}.txt", 'w', encoding='utf-8') as f:
            f.write("\n".join(sorted(list(configs))))

    with open("mix.txt", 'w', encoding='utf-8') as f:
        f.write("\n".join(sorted(list(mix_data))))

    print(f"Обновлено в {datetime.now()}. Всего уникальных: {len(mix_data)}")

if __name__ == "__main__":
    process()

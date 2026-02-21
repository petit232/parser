import os
import re
import requests
import base64

# --- НАСТРОЙКИ ГЕОГРАФИИ ---
COUNTRIES = {
    "belarus": {"keys": ["by", "bel", "minsk", "бел", "минск"], "flag": "🇧🇾"},
    "kazakhstan": {"keys": ["kz", "kaz", "almaty", "astana", "алм", "аст", "каз"], "flag": "🇰🇿"},
    "germany": {"keys": ["de", "ger", "frankfurt", "berlin", "герм", "франк", "берл"], "flag": "🇩🇪"},
    "poland": {"keys": ["pl", "pol", "warsaw", "warsz", "gdansk", "польш", "варш", "гдан"], "flag": "🇵🇱"},
    "usa": {"keys": ["us", "usa", "america", "united", "states", "ny", "york", "сша", "амер"], "flag": "🇺🇸"},
    "sweden": {"keys": ["se", "swe", "stockholm", "швец", "сток"], "flag": "🇸🇪"},
    "netherlands": {"keys": ["nl", "neth", "amsterdam", "нидер", "амстер"], "flag": "🇳🇱"},
    "latvia_lithuania": {"keys": ["lv", "lt", "latv", "lith", "riga", "vilnius", "латв", "литв"], "flag": "🇱🇻"}
}

PROTOCOLS = ["vless://", "vmess://", "trojan://", "ss://", "hysteria2://", "tuic://"]

def decode_base64(data):
    try:
        data = data.strip()
        missing_padding = len(data) % 4
        if missing_padding:
            data += '=' * (4 - missing_padding)
        return base64.b64decode(data).decode('utf-8')
    except:
        return data

def process():
    all_raw_data = []
    source_file = 'all_sources.txt'

    if not os.path.exists(source_file):
        return

    with open(source_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    for line in lines:
        line = line.strip()
        if not line: continue
        if any(proto in line.lower() for proto in PROTOCOLS):
            all_raw_data.append(line)
        elif line.startswith("http"):
            try:
                resp = requests.get(line, timeout=15)
                content = decode_base64(resp.text)
                all_raw_data.extend(content.splitlines())
            except:
                pass

    structured_data = {country: set() for country in COUNTRIES}
    mix_data = set()
    unique_check = set()

    for config in all_raw_data:
        config = config.strip()
        if not any(proto in config.lower() for proto in PROTOCOLS):
            continue

        # Убираем дубли по адресу сервера
        server_match = re.search(r'://([^/?#@]+@)?([^/?#:]+:[0-9]+|[^/?#:]+)', config)
        if server_match:
            server_address = server_match.group(2)
            if server_address in unique_check:
                continue
            unique_check.add(server_address)

        # Сортировка БЕЗ ПЕРЕИМЕНОВАНИЯ
        found_country = False
        for country, info in COUNTRIES.items():
            for key in info["keys"]:
                if key.lower() in config.lower(): # Ищем ключ во всей строке
                    structured_data[country].add(config)
                    found_country = True
                    break
            if found_country: break
        
        mix_data.add(config)

    # Чистим старые файлы
    for f in os.listdir('.'):
        if f.endswith('.txt') and f not in ['all_sources.txt', 'requirements.txt']:
            os.remove(f)

    # Сохраняем как есть
    for country, configs in structured_data.items():
        if configs:
            with open(f"{country}.txt", 'w', encoding='utf-8') as f:
                f.write("\n".join(configs))

    if mix_data:
        with open("mix.txt", 'w', encoding='utf-8') as f:
            f.write("\n".join(mix_data))

if __name__ == "__main__":
    process()

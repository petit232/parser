import os
import re
import requests
import base64

# --- ТОЧНЫЕ НАСТРОЙКИ ГЕОГРАФИИ ---
# Используем регулярные выражения \b для точного поиска слов
COUNTRIES = {
    "belarus": {"keys": [r"by", r"bel", r"minsk", r"беларусь", r"минск"], "flag": "🇧🇾"},
    "kazakhstan": {"keys": [r"kz", r"kaz", r"almaty", r"astana", r"казахстан", r"алматы"], "flag": "🇰🇿"},
    "germany": {"keys": [r"de", r"ger", r"frankfurt", r"germany", r"германия"], "flag": "🇩🇪"},
    "poland": {"keys": [r"pl", r"pol", r"warsaw", r"poland", r"польша"], "flag": "🇵🇱"},
    "usa": {"keys": [r"us", r"usa", r"america", r"united states", r"сша"], "flag": "🇺🇸"},
    "sweden": {"keys": [r"se", r"swe", r"stockholm", r"швеция"], "flag": "🇸🇪"},
    "netherlands": {"keys": [r"nl", r"neth", r"amsterdam", "нидерланды"], "flag": "🇳🇱"},
    "russia": {"keys": [r"ru", r"rus", r"russia", r"россия", r"moscow"], "flag": "🇷🇺"}
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

        # 1. Уникальность по адресу (чтобы не дублировать Anycast)
        server_match = re.search(r'://([^/?#@]+@)?([^/?#:]+:[0-9]+|[^/?#:]+)', config)
        if server_match:
            server_address = server_match.group(2)
            if server_address in unique_check:
                continue
            unique_check.add(server_address)

        # 2. Ищем страну ТОЛЬКО в названии (после #)
        name_part = ""
        if '#' in config:
            name_part = config.split('#')[-1].lower()
        
        found_country = False
        if name_part:
            for country, info in COUNTRIES.items():
                for key in info["keys"]:
                    # Поиск целого слова, чтобы 'us' не находилось в 'anycast'
                    if re.search(r'\b' + key + r'\b', name_part):
                        structured_data[country].add(config)
                        found_country = True
                        break
                if found_country: break
        
        mix_data.add(config)

    # Чистим старые файлы
    for f in os.listdir('.'):
        if f.endswith('.txt') and f not in ['all_sources.txt', 'requirements.txt']:
            os.remove(f)

    # Сохраняем оригиналы
    for country, configs in structured_data.items():
        if configs:
            with open(f"{country}.txt", 'w', encoding='utf-8') as f:
                f.write("\n".join(configs))

    if mix_data:
        with open("mix.txt", 'w', encoding='utf-8') as f:
            f.write("\n".join(mix_data))

if __name__ == "__main__":
    process()

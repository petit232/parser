import os
import re
import requests
import base64

# --- НАСТРОЙКИ ГЕОГРАФИИ И СМАЙЛИКОВ ---
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
        # Пробуем декодировать, если это base64 подписка
        missing_padding = len(data) % 4
        if missing_padding:
            data += '=' * (4 - missing_padding)
        return base64.b64decode(data).decode('utf-8')
    except:
        return data

def clean_name(config_line):
    if '#' in config_line:
        base_link, name = config_line.split('#', 1)
        # Убираем мусор, рекламу и лишние символы
        name = re.sub(r'(@[\w\d_]+|http\S+|www\S+|\.com|\.net|\.org|:[0-9]+)', '', name)
        name = name.replace('_', ' ').replace('-', ' ').strip()
        return base_link, name
    return config_line, ""

def get_protocol(link):
    for p in PROTOCOLS:
        if link.lower().startswith(p):
            return p.replace('://', '').upper()
    return "VPN"

def process():
    all_raw_data = []

    # Читаем из локального файла
    if os.path.exists('sources/my_links.txt'):
        with open('sources/my_links.txt', 'r', encoding='utf-8') as f:
            all_raw_data.extend(f.readlines())

    # Читаем из внешних ссылок
    if os.path.exists('sources/external_urls.txt'):
        with open('sources/external_urls.txt', 'r', encoding='utf-8') as f:
            for url in f:
                url = url.strip()
                if url:
                    try:
                        resp = requests.get(url, timeout=15)
                        content = decode_base64(resp.text)
                        all_raw_data.extend(content.splitlines())
                    except:
                        print(f"Не удалось загрузить: {url}")

    structured_data = {country: set() for country in COUNTRIES}
    mix_data = set()
    unique_check = set()

    for line in all_raw_data:
        line = line.strip()
        if not any(proto in line.lower() for proto in PROTOCOLS):
            continue

        base_link, name = clean_name(line)
        
        # Убираем дубли по адресу сервера
        if base_link in unique_check:
            continue
        unique_check.add(base_link)

        # Агрессивный поиск
        for country, info in COUNTRIES.items():
            match = False
            search_area = (name + base_link).lower()
            for key in info["keys"]:
                if key.lower() in search_area:
                    match = True
                    break
            
            if match:
                proto_name = get_protocol(base_link)
                # Дизайн: ❤️ [Эмодзи] Страна | Протокол | № ❤️
                counter = len(structured_data[country]) + 1
                beauty_name = f"❤️ {info['flag']} {country.capitalize()} | {proto_name} | {counter} ❤️"
                final_link = f"{base_link}#{beauty_name}"
                
                structured_data[country].add(final_link)
                mix_data.add(final_link)
                break

    # Сохраняем результат
    for country, configs in structured_data.items():
        if configs:
            with open(f"{country}.txt", 'w', encoding='utf-8') as f:
                f.write("\n".join(configs))

    if mix_data:
        with open("mix.txt", 'w', encoding='utf-8') as f:
            f.write("\n".join(mix_data))

if __name__ == "__main__":
    process()

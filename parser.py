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
        data = data.strip()
        missing_padding = len(data) % 4
        if missing_padding:
            data += '=' * (4 - missing_padding)
        return base64.b64decode(data).decode('utf-8')
    except:
        return data

def clean_name(config_line):
    if '#' in config_line:
        base_link, name = config_line.split('#', 1)
        # Убираем мусор, рекламу и лишние символы из названия
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
    source_file = 'all_sources.txt'

    if not os.path.exists(source_file):
        print(f"Файл {source_file} не найден! Создай его в корне.")
        return

    # Читаем единый файл источников
    with open(source_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    for line in lines:
        line = line.strip()
        if not line: continue

        # Если это прямая ссылка на VPN (vless:// и т.д.)
        if any(proto in line.lower() for proto in PROTOCOLS):
            all_raw_data.append(line)
        
        # Если это HTTP ссылка на внешнюю подписку
        elif line.startswith("http"):
            try:
                resp = requests.get(line, timeout=15)
                content = decode_base64(resp.text)
                all_raw_data.extend(content.splitlines())
            except:
                print(f"Не удалось загрузить подписку: {line}")

    structured_data = {country: set() for country in COUNTRIES}
    mix_data = set()
    unique_check = set() # Сюда пишем адреса серверов для удаления дублей

    for config in all_raw_data:
        config = config.strip()
        if not any(proto in config.lower() for proto in PROTOCOLS):
            continue

        base_link, name = clean_name(config)
        
        # Жесткая проверка на дубликаты (по адресу сервера)
        # Отрезаем всё после протокола до знака # или ? чтобы получить уникальный хост
        server_match = re.search(r'://([^/?#@]+@)?([^/?#:]+:[0-9]+|[^/?#:]+)', base_link)
        if server_match:
            server_address = server_match.group(2)
            if server_address in unique_check:
                continue
            unique_check.add(server_address)

        # Сортировка по странам
        for country, info in COUNTRIES.items():
            match = False
            search_area = (name + base_link).lower()
            for key in info["keys"]:
                if key.lower() in search_area:
                    match = True
                    break
            
            if match:
                proto_name = get_protocol(base_link)
                counter = len(structured_data[country]) + 1
                # Дизайн: ❤️ [Эмодзи] Страна | Протокол | № ❤️
                beauty_name = f"❤️ {info['flag']} {country.capitalize()} | {proto_name} | {counter} ❤️"
                final_link = f"{base_link}#{beauty_name}"
                
                structured_data[country].add(final_link)
                mix_data.add(final_link)
                break

    # Удаляем старые файлы перед записью
    for f in os.listdir('.'):
        if f.endswith('.txt') and f not in ['all_sources.txt', 'requirements.txt']:
            os.remove(f)

    # Сохраняем новые результаты
    for country, configs in structured_data.items():
        if configs:
            with open(f"{country}.txt", 'w', encoding='utf-8') as f:
                f.write("\n".join(configs))

    if mix_data:
        with open("mix.txt", 'w', encoding='utf-8') as f:
            f.write("\n".join(mix_data))
        print(f"Парсинг окончен. Уникальных серверов: {len(mix_data)}")

if __name__ == "__main__":
    process()

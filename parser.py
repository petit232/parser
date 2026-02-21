import os
import re
import requests
import base64

# --- ПОЛНЫЙ СПРАВОЧНИК МАРКЕРОВ (Флаги, Города, Аэропорты, Домены) ---
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
    try:
        data = data.strip()
        missing_padding = len(data) % 4
        if missing_padding: data += '=' * (4 - missing_padding)
        return base64.b64decode(data).decode('utf-8')
    except: return data

def get_unique_id(config):
    # Извлечение IP и PORT для удаления дубликатов
    match = re.search(r'://([^/?#@]+@)?([^/?#:]+:[0-9]+|[^/?#:]+)', config)
    return match.group(2) if match else config

def process():
    all_raw_links = []
    source_file = 'all_sources.txt'
    
    if not os.path.exists(source_file):
        print("ОШИБКА: Файл all_sources.txt не найден!")
        return

    with open(source_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()

    # Сбор и парсинг всех данных
    for line in lines:
        line = line.strip()
        if not line: continue
        
        # Если это HTTP ссылка на подписку
        if line.startswith("http") and not any(p in line for p in PROTOCOLS):
            try:
                resp = requests.get(line, timeout=20)
                content = decode_base64(resp.text)
                for sub_line in content.splitlines():
                    if any(proto in sub_line for proto in PROTOCOLS):
                        all_raw_links.append(sub_line.strip())
            except: print(f"Пропуск URL (ошибка): {line}")
        
        # Если это прямая ссылка или скопированный текст с кодом
        elif any(proto in line for proto in PROTOCOLS):
            # Извлекаем все вхождения протоколов из строки (на случай если в строке несколько кодов)
            found = re.findall(r'(?:vless|vmess|trojan|ss|hysteria2|tuic)://[^\s,]+', line)
            if found:
                all_raw_links.extend(found)
            else:
                all_raw_links.append(line)

    structured_data = {country: set() for country in COUNTRIES}
    mix_data = set()
    unique_check = set()

    for config in all_raw_links:
        config = config.strip()
        uid = get_unique_id(config)
        
        # Глобальная очистка повторов
        if uid in unique_check: continue
        unique_check.add(uid)

        config_lower = config.lower()
        assigned = False
        
        # 1. Сначала ищем по флагам (самый точный метод)
        for country, info in COUNTRIES.items():
            if info["flag"] in config:
                structured_data[country].add(config)
                assigned = True
                break
        
        # 2. Поиск по ключам (города, аэропорты, домены)
        if not assigned:
            for country, info in COUNTRIES.items():
                for key in info["keys"]:
                    if len(key) <= 3: # Короткие ключи (us, de, by) ищем как отдельные слова
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

    # ОБНОВЛЕНИЕ ФАЙЛОВ (Перезапись внутри существующих имен)
    for country, configs in structured_data.items():
        filename = f"{country}.txt"
        with open(filename, 'w', encoding='utf-8') as f:
            if configs:
                f.write("\n".join(sorted(configs)))
            else:
                f.write("") # Файл остается пустым, ссылка в Nekobox не ломается

    # Общий MIX файл
    with open("mix.txt", 'w', encoding='utf-8') as f:
        if mix_data:
            f.write("\n".join(sorted(mix_data)))
        else:
            f.write("")
    
    print(f"Парсинг завершен! Уникальных серверов: {len(mix_data)}")

if __name__ == "__main__":
    process()

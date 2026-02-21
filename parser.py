import os
import re
import requests
import base64

# --- ОБЪЕДИНЕННЫЙ СЛОВАРЬ МАРКЕРОВ (Флаги, Города, Аэропорты, Домены) ---
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

# Ключи для удаления мусора (чтобы не путать с другими странами)
BLACKLIST = ["anycast", "relay", "multi", "wangcai2", "bl"]

PROTOCOLS = ["vless://", "vmess://", "trojan://", "ss://", "hysteria2://", "tuic://"]

def decode_base64(data):
    try:
        data = data.strip()
        missing_padding = len(data) % 4
        if missing_padding: data += '=' * (4 - missing_padding)
        return base64.b64decode(data).decode('utf-8')
    except: return data

def process():
    all_raw_data = []
    source_file = 'all_sources.txt'
    if not os.path.exists(source_file): return

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
            except: pass

    structured_data = {country: set() for country in COUNTRIES}
    mix_data = set()
    unique_check = set()

    for config in all_raw_data:
        config = config.strip()
        if not any(proto in config.lower() for proto in PROTOCOLS): continue

        # Дедупликация (по хосту и порту)
        server_match = re.search(r'://([^/?#@]+@)?([^/?#:]+:[0-9]+|[^/?#:]+)', config)
        if server_match:
            addr = server_match.group(2)
            if addr in unique_check: continue
            unique_check.add(addr)

        # Анализируем всю строку (и название, и сам адрес сервера)
        config_lower = config.lower()
        assigned = False
        
        # Сначала ищем по флагам (самый высокий приоритет)
        for country, info in COUNTRIES.items():
            if info["flag"] in config:
                structured_data[country].add(config)
                assigned = True
                break
        
        # Если флаг не найден, ищем по ключам
        if not assigned:
            for country, info in COUNTRIES.items():
                for key in info["keys"]:
                    # Используем поиск с границами для коротких ключей (типа 'us', 'de')
                    if len(key) <= 3:
                        if re.search(r'[^a-z0-9]' + re.escape(key) + r'[^a-z0-9]', f" {config_lower} "):
                            structured_data[country].add(config)
                            assigned = True
                            break
                    elif key in config_lower:
                        structured_data[country].add(config)
                        assigned = True
                        break
                if assigned: break

        mix_data.add(config)

    # Чистим старое и пишем новое
    for f in os.listdir('.'):
        if f.endswith('.txt') and f not in ['all_sources.txt', 'requirements.txt']:
            os.remove(f)

    for country, configs in structured_data.items():
        if configs:
            with open(f"{country}.txt", 'w', encoding='utf-8') as f:
                f.write("\n".join(configs))

    if mix_data:
        with open("mix.txt", 'w', encoding='utf-8') as f:
            f.write("\n".join(mix_data))

if __name__ == "__main__":
    process()

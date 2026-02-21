import os
import re
import requests
import base64
from datetime import datetime

# --- ПОЛНЫЙ И ПОДРОБНЫЙ СПРАВОЧНИК СТРАН ---
# Здесь собраны все маркеры: Флаги, Города, Аэропорты и технические домены
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
    """Декодирует данные подписки, если они в формате base64."""
    try:
        data = data.strip()
        if not data: return ""
        missing_padding = len(data) % 4
        if missing_padding:
            data += '=' * (4 - missing_padding)
        return base64.b64decode(data).decode('utf-8', errors='ignore')
    except:
        return data

def get_unique_id(config):
    """Извлекает IP:PORT для жесткого удаления дубликатов."""
    match = re.search(r'://([^/?#@]+@)?([^/?#:]+:[0-9]+|[^/?#:]+)', config)
    return match.group(2) if match else config

def process():
    all_raw_links = []
    source_file = 'all_sources.txt'
    
    if not os.path.exists(source_file):
        print("Критическая ошибка: all_sources.txt не найден!")
        return

    with open(source_file, 'r', encoding='utf-8') as f:
        # Читаем ссылки, очищая от кавычек, пробелов и запятых
        sources = [l.strip().strip('",') for l in f.read().splitlines() if l.strip()]

    print(f"Запуск процесса. Найдено источников: {len(sources)}")

    for url in sources:
        # Если это HTTP ссылка на подписку или RAW файл
        if url.startswith("http") and not any(p in url for p in PROTOCOLS):
            try:
                print(f"Скачиваю: {url}")
                resp = requests.get(url, timeout=30)
                if resp.status_code == 200:
                    text = resp.text
                    # Если внутри нет протоколов, пробуем декодировать Base64
                    if not any(p in text for p in PROTOCOLS):
                        text = decode_base64(text)
                    
                    # Регулярка для извлечения всех ссылок из текста
                    found = re.findall(r'(?:vless|vmess|trojan|ss|hysteria2|tuic)://[^\s#"\'<>,]+', text)
                    all_raw_links.extend(found)
                    print(f"--- Найдено конфигов: {len(found)}")
            except Exception as e:
                print(f"--- Ошибка при загрузке {url}: {e}")
        
        # Если в файл закинули саму ссылку vless://...
        elif any(proto in url for proto in PROTOCOLS):
            found = re.findall(r'(?:vless|vmess|trojan|ss|hysteria2|tuic)://[^\s#"\'<>,]+', url)
            all_raw_links.extend(found)

    structured_data = {country: set() for country in COUNTRIES}
    mix_data = set()
    unique_check = set()

    # Сортировка по странам и удаление дублей
    print("Начинаю фильтрацию и сортировку...")
    for config in all_raw_links:
        config = config.strip()
        uid = get_unique_id(config)
        
        # Если сервер с таким IP:PORT уже есть — в корзину его
        if uid in unique_check:
            continue
        unique_check.add(uid)

        config_lower = config.lower()
        assigned = False
        
        # 1. Приоритет флагам
        for country, info in COUNTRIES.items():
            if info["flag"] in config:
                structured_data[country].add(config)
                assigned = True
                break
        
        # 2. Поиск по ключам (города, сокращения)
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
                if assigned: break

        mix_data.add(config)

    # Запись результатов в файлы
    print("Записываю файлы...")
    for country in COUNTRIES:
        filename = f"{country}.txt"
        with open(filename, 'w', encoding='utf-8') as f:
            if structured_data[country]:
                # Сортируем для красоты и стабильности
                f.write("\n".join(sorted(list(structured_data[country]))))
            else:
                f.write("") # Пустой файл, чтобы ссылка не билась

    with open("mix.txt", 'w', encoding='utf-8') as f:
        if mix_data:
            f.write("\n".join(sorted(list(mix_data))))

    print(f"Завершено успешно! Всего уникальных конфигов: {len(mix_data)}")

if __name__ == "__main__":
    process()

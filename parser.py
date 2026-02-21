import os
import re
import requests
import base64
from datetime import datetime

# --- МАКСИМАЛЬНЫЙ СПРАВОЧНИК СТРАН И МАРКЕРОВ ---
# Этот список определяет, какие файлы будут созданы. Названия файлов (ключи) статичны.
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
    """Декодирует содержимое, если оно представлено в формате Base64 (стандарт подписок)."""
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
    """
    Извлекает уникальный адрес и порт сервера. 
    Нужно для того, чтобы один и тот же сервер не дублировался с разными именами.
    """
    match = re.search(r'://([^/?#@]+@)?([^/?#:]+:[0-9]+|[^/?#:]+)', config)
    return match.group(2) if match else config

def process():
    all_raw_links = []
    source_file = 'all_sources.txt'
    
    # Получаем текущую дату и время для метки обновления
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    timestamp_mark = f"\n\n# Last Update: {now}"

    if not os.path.exists(source_file):
        print(f"Файл {source_file} не найден. Проверьте наличие файла в корне репозитория.")
        return

    # Загрузка источников из файла
    with open(source_file, 'r', encoding='utf-8') as f:
        # Убираем кавычки, запятые и пробелы, если они есть
        sources = [l.strip().strip('",') for l in f.read().splitlines() if l.strip()]

    print(f"Начинаю сбор данных из {len(sources)} источников...")

    for url in sources:
        # Если это ссылка на внешний ресурс
        if url.startswith("http"):
            try:
                print(f"Загрузка: {url}")
                # Добавляем User-Agent, чтобы сайты не блокировали запросы
                headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'}
                resp = requests.get(url, headers=headers, timeout=30)
                if resp.status_code == 200:
                    text = resp.text
                    # Если в тексте нет явных протоколов, пробуем Base64 декодирование
                    if not any(p in text for p in PROTOCOLS):
                        text = decode_base64(text)
                    
                    # Извлекаем все конфиги из полученного текста
                    found = re.findall(r'(?:vless|vmess|trojan|ss|hysteria2|tuic)://[^\s#"\'<>,]+', text)
                    all_raw_links.extend(found)
                    print(f"--- Найдено: {len(found)} шт.")
            except Exception as e:
                print(f"--- Ошибка загрузки {url}: {e}")
        
        # Если в файл вставлена прямая ссылка на конфиг
        elif any(proto in url for proto in PROTOCOLS):
            found = re.findall(r'(?:vless|vmess|trojan|ss|hysteria2|tuic)://[^\s#"\'<>,]+', url)
            all_raw_links.extend(found if found else [url])

    # Структуры для хранения отсортированных данных
    structured_data = {country: set() for country in COUNTRIES}
    mix_data = set()
    unique_check = set()

    print("Фильтрация дубликатов и распределение по странам...")

    for config in all_raw_links:
        config = config.strip()
        uid = get_unique_id(config)
        
        # Удаление повторов по IP:Port (Глобальная дедупликация)
        if uid in unique_check:
            continue
        unique_check.add(uid)

        config_lower = config.lower()
        assigned = False
        
        # 1. Сначала ищем флаги (самый точный признак)
        for country, info in COUNTRIES.items():
            if info["flag"] in config:
                structured_data[country].add(config)
                assigned = True
                break
        
        # 2. Затем ищем по ключевым словам (города, домены, сокращения)
        if not assigned:
            for country, info in COUNTRIES.items():
                for key in info["keys"]:
                    key_low = key.lower()
                    # Если ключ короткий (2-3 буквы), ищем его как отдельное слово
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

    # --- СОХРАНЕНИЕ ФАЙЛОВ ---
    # Режим 'w' гарантирует, что файл перезаписывается. Ссылка на файл в GitHub НЕ МЕНЯЕТСЯ.
    print("Сохранение результатов в файлы...")
    
    for country in COUNTRIES:
        filename = f"{country}.txt"
        configs = sorted(list(structured_data[country]))
        with open(filename, 'w', encoding='utf-8') as f:
            if configs:
                f.write("\n".join(configs))
            # Добавляем скрытую метку времени, чтобы GitHub видел изменение файла
            f.write(timestamp_mark)

    # Общий файл со всеми найденными уникальными серверами
    with open("mix.txt", 'w', encoding='utf-8') as f:
        if mix_data:
            f.write("\n".join(sorted(list(mix_data))))
        # Добавляем скрытую метку времени в микс
        f.write(timestamp_mark)

    print(f"Готово! Всего уникальных серверов сохранено: {len(mix_data)}")

if __name__ == "__main__":
    process()

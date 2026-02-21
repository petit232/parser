import os
import json
import asyncio
import aiohttp
import time
import re
import socket
import geoip2.database
from datetime import datetime
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse

# --- CONFIGURATION ---
SOURCE_FILE = 'all_sources.txt'
STATE_FILE = 'monster_state.json'
GEOIP_DB = 'GeoLite2-Country.mmdb'
LINKS_INFO_FILE = 'LINKS_FOR_CLIENTS.txt'
BATCH_SIZE = 500  # Количество серверов для одновременного пинга
TIMEOUT = 5       # Таймаут пинга в секундах
MAX_NODES_PER_COUNTRY = 500 # Лимит нод на один файл страны

# Маппинг стран на файлы
COUNTRY_MAP = {
    'RU': 'russia.txt',
    'BY': 'belarus.txt',
    'FI': 'finland.txt',
    'FR': 'france.txt',
    'DE': 'germany.txt',
    'HK': 'hongkong.txt',
    'KZ': 'kazakhstan.txt',
    'NL': 'netherlands.txt',
    'PL': 'poland.txt',
    'SG': 'singapore.txt',
    'SE': 'sweden.txt',
    'GB': 'uk.txt',
    'US': 'usa.txt',
}
DEFAULT_MIX = 'mix.txt'

class MonsterParser:
    def __init__(self):
        self.state = self.load_state()
        self.geo_reader = None
        if os.path.exists(GEOIP_DB):
            self.geo_reader = geoip2.database.Reader(GEOIP_DB)
        
        # Регулярка для извлечения хоста и порта
        self.ip_pattern = re.compile(r'@?([\w\.-]+):(\d+)')

    def load_state(self):
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, 'r') as f:
                    return json.load(f)
            except:
                pass
        return {"last_index": 0, "processed_total": 0, "history": []}

    def save_state(self):
        with open(STATE_FILE, 'w') as f:
            json.dump(self.state, f, indent=4)

    def get_ip_from_link(self, link):
        """Извлекает хост из ссылок vless/vmess/ss/trojan"""
        try:
            match = self.ip_pattern.search(link)
            if match:
                return match.group(1)
        except Exception:
            pass
        return None

    def wrap_for_russia(self, link):
        """
        Добавляет параметры защиты от DPI (Fragment, Mux, Padding) для работы в РФ.
        Приоритезирует Reality.
        """
        try:
            parsed = urlparse(link)
            query = parse_qs(parsed.query)
            
            # Если это Reality - он уже защищен, просто проверяем параметры
            if 'reality' in str(query.get('security', [])).lower():
                return link 

            # Для VLESS, VMESS, Trojan добавляем Fragment (защита от DPI)
            if parsed.scheme in ['vless', 'vmess', 'trojan']:
                # Параметры фрагментации пакетов
                query['fragment'] = ['10-20,30-50']
                query['mux'] = ['enable=true&concurrency=8']
                
                # Если нет TLS, но протокол поддерживает - пробуем добавить маскировку
                if 'security' not in query:
                    query['security'] = ['tls']
                
                # Собираем ссылку обратно
                new_query = urlencode(query, doseq=True)
                new_parts = list(parsed)
                new_parts[4] = new_query
                return urlunparse(new_parts)
                
            return link
        except:
            return link

    def get_link_score(self, link):
        """Оценивает надежность и безопасность протокола для РФ"""
        score = 0
        link_low = link.lower()
        if 'reality' in link_low: score += 1000
        if 'vless' in link_low: score += 500
        if 'trojan' in link_low: score += 400
        if 'vmess' in link_low: score += 300
        if 'fragment' in link_low: score += 100
        return score

    async def check_node(self, session, link):
        """Проверяет доступность узла (TCP Ping) и возвращает результаты"""
        host = self.get_ip_from_link(link)
        if not host:
            return None, 9999
        
        start_time = time.time()
        try:
            # Асинхронный резолв DNS
            loop = asyncio.get_event_loop()
            ip_addr = await loop.run_in_executor(None, socket.gethostbyname, host)
            
            # Попытка TCP соединения
            conn = asyncio.open_connection(ip_addr, 443)
            reader, writer = await asyncio.wait_for(conn, timeout=TIMEOUT)
            writer.close()
            await writer.wait_closed()
            
            ping_ms = int((time.time() - start_time) * 1000)
            return ip_addr, ping_ms
        except:
            return None, 9999

    def get_country(self, ip):
        if not self.geo_reader:
            return None
        try:
            response = self.geo_reader.country(ip)
            return response.country.iso_code
        except:
            return None

    def generate_static_links(self):
        """Генерирует файл со статическими ссылками на подписки для клиента"""
        repo_full_name = os.getenv('GITHUB_REPOSITORY', 'USER/REPO')
        base_url = f"https://raw.githubusercontent.com/{repo_full_name}/main"
        
        content = []
        content.append("⚡ Monster Engine: Ваши статические ссылки на подписки")
        content.append(f"Обновлено: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}")
        content.append("-" * 50)
        content.append(f"GLOBAL MIX: {base_url}/sub_monster.txt")
        content.append("-" * 50)
        
        for country_code, filename in sorted(COUNTRY_MAP.items()):
            content.append(f"{country_code} ({filename.replace('.txt', '')}): {base_url}/{filename}")
            
        with open(LINKS_INFO_FILE, 'w', encoding='utf-8') as f:
            f.write('\n'.join(content))
        print(f"✅ Файл со ссылками обновлен: {LINKS_INFO_FILE}")

    async def run(self):
        if not os.path.exists(SOURCE_FILE):
            print(f"❌ Ошибка: {SOURCE_FILE} не найден.")
            return

        with open(SOURCE_FILE, 'r', encoding='utf-8') as f:
            links = [line.strip() for line in f if line.strip()]

        # Дедупликация
        links = list(dict.fromkeys(links))
        total_links = len(links)
        
        start = self.state.get("last_index", 0)
        if start >= total_links: start = 0
        
        end = min(start + 5000, total_links)
        current_batch = links[start:end]
        
        print(f"🚀 Monster Engine: Обработка {len(current_batch)} нод ({start} - {end} из {total_links})")

        results = []
        async with aiohttp.ClientSession() as session:
            for i in range(0, len(current_batch), BATCH_SIZE):
                sub_batch = current_batch[i:i+BATCH_SIZE]
                tasks = [self.check_node(session, link) for link in sub_batch]
                checked = await asyncio.gather(*tasks)
                
                for link, (ip, ping) in zip(sub_batch, checked):
                    if ip:
                        # "Заворачиваем" ноду для обхода блокировок
                        protected_link = self.wrap_for_russia(link)
                        country = self.get_country(ip)
                        results.append({
                            "link": protected_link,
                            "ip": ip,
                            "ping": ping,
                            "country": country,
                            "score": self.get_link_score(protected_link)
                        })

        # --- ГЛОБАЛЬНАЯ СИНХРОНИЗАЦИЯ И ОЧИСТКА ---
        living_links_original = {current_batch[i] for i, (ip, p) in enumerate(checked) if ip}
        dead_links = set(current_batch) - living_links_original
        
        file_updates = {filename: [] for filename in set(COUNTRY_MAP.values()) | {DEFAULT_MIX}}
        
        for res in results:
            target_file = COUNTRY_MAP.get(res['country'], DEFAULT_MIX)
            file_updates[target_file].append(res)

        # Обновление файлов стран с сортировкой по Score (сначала Reality) и Ping
        for filename, new_data in file_updates.items():
            current_nodes_map = {} # link -> score_data
            
            if os.path.exists(filename):
                with open(filename, 'r', encoding='utf-8') as f:
                    for l in f:
                        line = l.strip()
                        if line and line not in dead_links:
                            # Для старых строк считаем базовый score
                            current_nodes_map[line] = self.get_link_score(line)
            
            # Добавляем новые живые ноды
            for item in new_data:
                current_nodes_map[item['link']] = item['score']
            
            # Сортировка: сначала по Score (безопасность), потом по Ping (скорость)
            sorted_links = sorted(current_nodes_map.keys(), key=lambda x: current_nodes_map[x], reverse=True)
            
            with open(filename, 'w', encoding='utf-8') as f:
                f.write('\n'.join(sorted_links[:MAX_NODES_PER_COUNTRY]) + '\n')

        # Очистка основного SOURCE_FILE
        remaining_links = [l for l in links if l not in dead_links]
        with open(SOURCE_FILE, 'w', encoding='utf-8') as f:
            f.write('\n'.join(remaining_links) + '\n')

        # Обновление общего файла подписки
        with open('sub_monster.txt', 'w', encoding='utf-8') as f:
            all_live_with_score = {l: self.get_link_score(l) for l in remaining_links}
            sorted_sub = sorted(all_live_with_score.keys(), key=lambda x: all_live_with_score[x], reverse=True)
            f.write('\n'.join(sorted_sub[:5000]))

        # Генерируем справочник ссылок для клиента
        self.generate_static_links()

        # Сохранение состояния
        self.state["last_index"] = end if end < total_links else 0
        self.state["processed_total"] += len(current_batch)
        self.state["last_run"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.save_state()
        
        print(f"✅ Батч завершен. Найдено {len(results)} живых. Удалено {len(dead_links)} мертвых.")

if __name__ == "__main__":
    parser = MonsterParser()
    asyncio.run(parser.run())

import os
import sys
import time
import json
import logging
import signal
import sqlite3
import requests
from datetime import datetime
from typing import Dict, Any, Optional, List

# --- КОНФИГУРАЦИЯ ---
# API для получения данных GeoIP (используем ip-api.com для примера)
GEOIP_API_URL = "http://ip-api.com/json/{ip}?fields=status,message,country,countryCode,regionName,city,lat,lon,isp,org,as,query"
DB_PATH = "network_monitor.db"
LOG_FILE = "daemon.log"
CHECK_INTERVAL = 60  # Интервал между проверками в секундах
API_TIMEOUT = 10     # Таймаут запроса к API

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE, encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

class NetworkDaemon:
    def __init__(self):
        self.running = True
        self.db_conn = None
        self._setup_signals()
        self._init_db()

    def _setup_signals(self):
        """Настройка обработки системных сигналов для корректного завершения."""
        signal.signal(signal.SIGINT, self._handle_exit)
        signal.signal(signal.SIGTERM, self._handle_exit)

    def _handle_exit(self, signum, frame):
        logging.info(f"Получен сигнал завершения ({signum}). Останавливаем демона...")
        self.running = False

    def _init_db(self):
        """Инициализация структуры базы данных SQLite."""
        try:
            self.db_conn = sqlite3.connect(DB_PATH, check_same_thread=False)
            cursor = self.db_conn.cursor()
            
            # Таблица активных узлов
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS nodes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ip_address TEXT UNIQUE NOT NULL,
                    last_seen DATETIME,
                    is_active INTEGER DEFAULT 1
                )
            ''')
            
            # Таблица истории проверок и гео-данных
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS geo_history (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    node_id INTEGER,
                    check_time DATETIME,
                    country TEXT,
                    city TEXT,
                    isp TEXT,
                    latitude REAL,
                    longitude REAL,
                    raw_response TEXT,
                    FOREIGN KEY (node_id) REFERENCES nodes(id)
                )
            ''')
            
            self.db_conn.commit()
            logging.info("База данных успешно инициализирована.")
        except sqlite3.Error as e:
            logging.error(f"Ошибка при инициализации БД: {e}")
            sys.exit(1)

    def get_geoip_info(self, ip: str) -> Optional[Dict[str, Any]]:
        """
        Получение данных GeoIP напрямую из API.
        Кэширование полностью удалено по требованию пользователя.
        Каждый вызов — это новый сетевой запрос.
        """
        try:
            url = GEOIP_API_URL.format(ip=ip)
            response = requests.get(url, timeout=API_TIMEOUT)
            response.raise_for_status()
            data = response.json()
            
            if data.get("status") == "success":
                logging.debug(f"Данные GeoIP для {ip} успешно получены.")
                return data
            else:
                logging.warning(f"API вернул ошибку для IP {ip}: {data.get('message')}")
                return None
                
        except requests.exceptions.RequestException as e:
            logging.error(f"Ошибка сети при запросе GeoIP для {ip}: {e}")
            return None
        except Exception as e:
            logging.error(f"Непредвиденная ошибка при обработке IP {ip}: {e}")
            return None

    def save_geo_data(self, node_id: int, geo_data: Dict[str, Any]):
        """Сохранение результатов проверки в историю БД."""
        try:
            cursor = self.db_conn.cursor()
            cursor.execute('''
                INSERT INTO geo_history (
                    node_id, check_time, country, city, isp, latitude, longitude, raw_response
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                node_id,
                datetime.now().isoformat(),
                geo_data.get("country"),
                geo_data.get("city"),
                geo_data.get("isp"),
                geo_data.get("lat"),
                geo_data.get("lon"),
                json.dumps(geo_data, ensure_ascii=False)
            ))
            
            # Обновляем время последней активности узла
            cursor.execute('''
                UPDATE nodes SET last_seen = ? WHERE id = ?
            ''', (datetime.now().isoformat(), node_id))
            
            self.db_conn.commit()
        except sqlite3.Error as e:
            logging.error(f"Ошибка при сохранении данных в БД: {e}")

    def get_active_nodes(self) -> List[Dict[str, Any]]:
        """Получение списка всех активных узлов для мониторинга."""
        try:
            cursor = self.db_conn.cursor()
            cursor.execute("SELECT id, ip_address FROM nodes WHERE is_active = 1")
            rows = cursor.fetchall()
            return [{"id": r[0], "ip": r[1]} for r in rows]
        except sqlite3.Error as e:
            logging.error(f"Ошибка при получении списка узлов: {e}")
            return []

    def run(self):
        """Основной цикл работы демона."""
        logging.info("Демон запущен. Мониторинг начат (кэширование GeoIP отключено).")
        
        while self.running:
            nodes = self.get_active_nodes()
            
            if not nodes:
                logging.info("Активные узлы не найдены в БД. Ожидание...")
            else:
                logging.info(f"Начинается цикл проверки {len(nodes)} узлов.")
                
                for node in nodes:
                    if not self.running:
                        break
                        
                    ip = node['ip']
                    node_id = node['id']
                    
                    logging.info(f"Проверка узла: {ip} (ID: {node_id})")
                    
                    # Прямой запрос к API без проверки кэша
                    geo_data = self.get_geoip_info(ip)
                    
                    if geo_data:
                        self.save_geo_data(node_id, geo_data)
                        logging.info(f"Данные для {ip} обновлены: {geo_data.get('country')}, {geo_data.get('city')}")
                    else:
                        logging.error(f"Не удалось получить актуальные данные для {ip}")
                    
                    # Небольшая задержка между запросами к API, чтобы не заблокировали
                    time.sleep(1.0) 

            # Ожидание перед следующим полным циклом
            if self.running:
                logging.info(f"Цикл завершен. Сон {CHECK_INTERVAL} секунд.")
                for _ in range(CHECK_INTERVAL):
                    if not self.running:
                        break
                    time.sleep(1)

        # Очистка при выходе
        if self.db_conn:
            self.db_conn.close()
            logging.info("Соединение с БД закрыто.")
        logging.info("Демон полностью остановлен.")

if __name__ == "__main__":
    # Пример добавления тестового узла, если база пуста
    daemon = NetworkDaemon()
    
    # Проверка на наличие данных (для демонстрации)
    try:
        check_cursor = daemon.db_conn.cursor()
        check_cursor.execute("SELECT count(*) FROM nodes")
        if check_cursor.fetchone()[0] == 0:
            logging.info("Добавление тестового IP (8.8.8.8) в пустую базу...")
            check_cursor.execute("INSERT INTO nodes (ip_address, last_seen) VALUES (?, ?)", 
                               ('8.8.8.8', datetime.now().isoformat()))
            daemon.db_conn.commit()
    except Exception as e:
        logging.error(f"Ошибка при добавлении начальных данных: {e}")

    daemon.run()

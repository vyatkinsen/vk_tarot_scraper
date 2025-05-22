# VK Tarot Scraper

Проект автоматизирует сбор данных из групп ВКонтакте, передачу через Kafka, сохранение в MongoDB и индексирование в Elasticsearch (ELK).

## Структура проекта

```
vk_tarot_scraper/
├── docker-compose.yml      # Поднимает все сервисы (Kafka, Zookeeper, MongoDB, ELK, сервисы приложения)
├── logstash.conf           # Конфигурация Logstash для чтения из MongoDB и записи в Elasticsearch
├── requirements.txt        
├── vk_scraper/             # Сервис парсинга VK → Kafka
│   ├── Dockerfile
│   ├── vk_requirements.txt
│   ├── vk_scraper.py
│   └── .env.example        # пример переменных окружения
├── db_service/             # Kafka → MongoDB консьюмер
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── KafkaToMongo.py
│   ├── DelDubl.py          # утилиты для работы с дубликатами
│   └── DeleteDublicates.py
└── logstash-mongodb/       # Образ Logstash с плагином MongoDB
    ├── Dockerfile
    └── logstash_sqlite.db  # placeholder‑файл для MongoDB input плагина
```

## Предварительные требования

* Docker Engine
* Docker Compose v2+

## Настройка

### 1. VK Scraper

1. Перейдите в папку `vk_scraper/`:

   ```bash
   cd vk_scraper
   ```
2. Скопируйте `.env.example` в `.env` и заполните:

   ```dotenv
   VK_TOKEN="YOUR_TOKEN"        
   KAFKA_BOOTSTRAP_SERVERS=kafka:9092
   ```

### 2. DB Service

По умолчанию никаких дополнительных настроек не требуется — все переменные заданы в `docker-compose.yml`.

### 3. Logstash

Конфигурация в файле `logstash.conf` (корень проекта). Плагин MongoDB уже установлен в образе `logstash-mongodb/Dockerfile`.

## Запуск всех сервисов

В корне проекта выполните:

```bash
docker-compose up --build -d
```

Docker Compose создаст и запустит:

* **Zookeeper + Kafka + Kafka UI** (порт 9000)
* **MongoDB + Mongo Express** (порт 8081)
* **vk\_scraper** (парсер → Kafka topic `vk_users`)
* **db\_service** (Kafka → MongoDB collection `vk_users`)
* **Elasticsearch** (порт 9200)
* **Logstash** (читает из MongoDB и пишет в ES индекс `vk_users_data`)
* **Kibana** (порт 5601)

## Доступ к интерфейсам

* Kafka UI: [http://localhost:9000](http://localhost:9000)
* Mongo Express: [http://localhost:8081](http://localhost:8081)
* Kibana: [http://localhost:5601](http://localhost:5601)

## Остановка и удаление контейнеров

```bash
docker-compose down
```

## Полезные команды

* Просмотр логов одного сервиса:

  ```bash
  docker-compose logs -f logstash
  ```
* Просмотр состояния контейнеров:

  ```bash
  docker-compose ps
  ```


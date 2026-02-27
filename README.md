# Kafka KRaft Cluster Demo

Стенд с кластером Kafka (3 ноды) на базе KRaft, UI-панелью и примером продюсера.

## Запуск инфраструктуры

```bash
docker-compose up -d
```

остановка с полной очисткой
docker compose down -v

После запуска доступен UI: http://localhost:8080

Запуск продюсера

pip install confluent-kafka
python producer.py

svmaksimov

import json
import time
from confluent_kafka import Producer

# Конфигурация для быстрой заливки
conf = {
    'bootstrap.servers': 'localhost:9091,localhost:9092,localhost:9093',
    'client.id': 'bulk-seeder',
    'linger.ms': 100,           # Копим сообщения перед отправкой пачкой
    'batch.size': 1000000,      # Увеличиваем размер батча (1МБ)
    'compression.type': 'lz4',  # Сжимаем данные для скорости
    'acks': 1                   # Подтверждение только от лидера (быстрее, чем all)
}

producer = Producer(conf)
TOPIC = 'user-catalog'
TOTAL_USERS = 300000

print(f"🚀 Начинаем заливку {TOTAL_USERS} пользователей в '{TOPIC}'...")
start_time = time.time()

try:
    for i in range(1, TOTAL_USERS + 1):
        user_id = f"user_{i}"
        user_data = {
            "id": user_id,
            "name": f"Name_{i}",
            "email": f"user_{i}@example.com",
            "department": random.choice(["IT", "HR", "Sales", "Legal", "Support"]) if 'random' in globals() else "IT"
        }
        
        # Ключ (id) критически важен для компактного топика!
        producer.produce(
            TOPIC, 
            key=user_id, 
            value=json.dumps(user_data)
        )

        # Каждые 10000 записей сбрасываем буфер, чтобы не переполнить память
        if i % 10000 == 0:
            producer.poll(0)
            print(f"✅ Отправлено {i}...")

    # Ждем завершения всех отправок
    print("⏳ Ожидание подтверждения от брокеров...")
    producer.flush()

    end_time = time.time()
    print(f"🏁 Готово! {TOTAL_USERS} записей загружено за {round(end_time - start_time, 2)} сек.")

except Exception as e:
    print(f"❌ Ошибка: {e}")

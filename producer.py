import json
import time
import random
from confluent_kafka import Producer

# Настройки подключения к нашему кластеру
conf = {
    'bootstrap.servers': 'localhost:9091,localhost:9092,localhost:9093',
    'client.id': 'python-producer'
}

producer = Producer(conf)

def delivery_report(err, msg):
    if err is not None:
        print(f"Ошибка доставки: {err}")
    else:
        print(f"Заказ отправлен в топик {msg.topic()} [партиция {msg.partition()}]")

print("🚀 Продюсер запущен. Отправляем заказы в Kafka...")

try:
    order_id = 1
    while True:
        # Генерируем фейковый заказ
        data = {
            "order_id": order_id,
            "user_id": random.randint(100, 999),
            "item": random.choice(["Laptop", "Mouse", "Keyboard", "Monitor"]),
            "price": round(random.uniform(10.0, 1000.0), 2),
            "timestamp": time.time()
        }
        
        # Отправляем в топик 'orders'
        producer.produce(
            'orders', 
            key=str(data["order_id"]), 
            value=json.dumps(data), 
            callback=delivery_report
        )
        
        # Сбрасываем буфер, чтобы данные отправились немедленно
        producer.poll(0)
        
        order_id += 1
        time.sleep(2)  # Пауза 2 секунды между заказами

except KeyboardInterrupt:
    print("\n🛑 Остановка продюсера...")
finally:
    producer.flush() # Ждем завершения всех отправок перед выходом

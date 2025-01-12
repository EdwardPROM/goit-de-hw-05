import time
import random
from kafka import KafkaProducer
import json
from configs import kafka_config

# Налаштування Kafka
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Ідентифікатор топіка
topic_name = "edwardprom_building_sensors"

# Генерація ID датчика (однаковий для одного запуску скрипта)
sensor_id = random.randint(1000, 9999)

print(f"Sensor Producer started with ID: {sensor_id}")

try:
    while True:
        # Генерація випадкових даних
        temperature = random.uniform(25, 45)  # Температура: 25-45
        humidity = random.uniform(15, 85)    # Вологість: 15-85
        timestamp = time.time()              # Час отримання даних

        # Структура даних
        data = {
            "sensor_id": sensor_id,
            "temperature": round(temperature, 2),
            "humidity": round(humidity, 2),
            "timestamp": timestamp
        }

        # Відправка даних у Kafka
        producer.send(topic_name, value=data)
        print(f"Sent data: {data}")

        # Затримка між повідомленнями (наприклад, 5 секунд)
        time.sleep(5)

except KeyboardInterrupt:
    print("Sensor Producer stopped manually.")
finally:
    producer.close()

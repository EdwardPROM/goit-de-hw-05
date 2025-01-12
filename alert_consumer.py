from kafka import KafkaConsumer
import json
from configs import kafka_config

# Налаштування Kafka Consumer
consumer = KafkaConsumer(
    "edwardprom_temperature_alerts",
    "edwardprom_humidity_alerts",
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',  # Почати читати з початку топіка, якщо немає збережених оффсетів
    enable_auto_commit=True
)

print("Alert Consumer started and listening to 'edwardprom_temperature_alerts' and 'edwardprom_humidity_alerts'...")

try:
    for message in consumer:
        # Отримані дані
        alert = message.value

        # Виведення сповіщення на екран
        print(f"Alert received: {alert}")

except KeyboardInterrupt:
    print("Alert Consumer stopped manually.")
finally:
    consumer.close()

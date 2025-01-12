from kafka import KafkaConsumer, KafkaProducer
import json
from configs import kafka_config

# Налаштування Kafka Consumer
consumer = KafkaConsumer(
    "edwardprom_building_sensors",
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    auto_offset_reset='earliest',  # Почати читати з початку топіка, якщо немає збережених оффсетів
    enable_auto_commit=True
)

# Налаштування Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=kafka_config['bootstrap_servers'],
    security_protocol=kafka_config['security_protocol'],
    sasl_mechanism=kafka_config['sasl_mechanism'],
    sasl_plain_username=kafka_config['username'],
    sasl_plain_password=kafka_config['password'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Топіки для сповіщень
temperature_alerts_topic = "edwardprom_temperature_alerts"
humidity_alerts_topic = "edwardprom_humidity_alerts"

print("Sensor Consumer started and listening to 'edwardprom_building_sensors'...")

try:
    for message in consumer:
        # Отримані дані
        data = message.value
        sensor_id = data['sensor_id']
        temperature = data['temperature']
        humidity = data['humidity']
        timestamp = data['timestamp']

        # Виведення всіх даних сенсорів у термінал
        print(f"Sensor Data: ID={sensor_id}, Temperature={temperature}°C, Humidity={humidity}%, Timestamp={timestamp}")

        # Перевірка температури
        if temperature > 40:
            alert = {
                "sensor_id": sensor_id,
                "temperature": temperature,
                "timestamp": timestamp,
                "alert": "High temperature detected!"
            }
            producer.send(temperature_alerts_topic, value=alert)
            print(f"Temperature Alert: {alert}")

        # Перевірка вологості
        if humidity > 80 or humidity < 20:
            alert = {
                "sensor_id": sensor_id,
                "humidity": humidity,
                "timestamp": timestamp,
                "alert": "Humidity out of range detected!"
            }
            producer.send(humidity_alerts_topic, value=alert)
            print(f"Humidity Alert: {alert}")

except KeyboardInterrupt:
    print("Sensor Consumer stopped manually.")
finally:
    consumer.close()
    producer.close()

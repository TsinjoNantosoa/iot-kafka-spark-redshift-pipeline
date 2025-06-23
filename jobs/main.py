import os
import random
import uuid
from datetime import datetime, timedelta

from confluent_kafka import SerializingProducer
import simplejson as json

LONDON_COORDINATES = {"latitude": 51.4074, "longitude": -0.1278}
BIRMINGHAM_COORDINATES = {"latitude": 52.4862, "longitude": -1.8904}

LATITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['latitude'] - LONDON_COORDINATES['latitude']) / 100
LONGITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['longitude'] - LONDON_COORDINATES['longitude']) / 100

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_topic')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_topic')

start_time = datetime.now()
start_location = LONDON_COORDINATES.copy()


def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30, 60))
    return start_time


def generate_gps_data(device_id, timestamp, vehicle_type='private'):
    return {
        'id': str(uuid.uuid4()),
        'deviceId': device_id,
        'timestamp': timestamp,
        'speed': random.uniform(0, 40),  # vitesse in km/h
        'direction': 'North-East',
        'vehicle': vehicle_type
    }


def generate_traffic_camera_data(device_id, timestamp, camera_id):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'cameraId': camera_id,
        'timestamp': timestamp,
        'snapshot': 'Based64EncodeString'

    }


def simulate_vehicle_movement():
    global start_location

    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT

    start_location['latitude'] += random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += random.uniform(-0.0005, 0.0005)
    return start_location


def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()
    return {
        'id': str(uuid.uuid4()),
        'deviceId': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'speed': random.uniform(10, 40),
        'direction': 'North-East',
        'make': 'BMW',
        'model': 'CS500',
        'year': '2025',
        'fuelType': 'Hybrid'
    }


def simulate_journey(producer, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data['timestamp'])
        traffic_data_camera = generate_traffic_camera_data(device_id,vehicle_data['timestamp'], 'Tsinjo-Cam123')
        print(vehicle_data)
        # Pour l’envoyer à Kafka, tu peux ajouter ceci si nécessaire :
        # producer.produce(topic=VEHICLE_TOPIC, value=json.dumps(vehicle_data))
        break


if __name__ == "__main__":
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Kafka error: {err}')
    }
    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer, 'Vehicle_tsinjo-123')
    except KeyboardInterrupt:
        print('The simulation was ended by the user.')
    except Exception as e:
        print(f'Unexpected error occurred: {e}')

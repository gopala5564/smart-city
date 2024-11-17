import os
from confluent_kafka import SerializingProducer
import simplejson as json
from datetime import datetime, timedelta
import random
import uuid
import time

global BIRMINGHAM_COORDINATES
global LONDON_COORDINATES



#environment variables for configuration
KAFKA_BOOTSTRAP_SERVERS = os.environ.get('KAFKA_BOOTSTRAP_SERVER', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_data')



def get_next_time():
    global start_time
    start_time+=timedelta(seconds=random.randint(30,60))
    return start_time

def generate_gps_data(device_id, timestamp, vehicle_type='private'):
    return {
        'id':uuid.uuid4(),
        'device_id': device_id,
        'timestamp': timestamp,
        'speed': random.uniform(0,40), #km/h
        'direction': 'North-East',
        'vehicleType':vehicle_type   }

def generate_traffic_camera_data(device_id, timestamp, location, camera_id):
    return {
        'id':uuid.uuid4(),
        'deviceId': device_id,
        'cameraId': camera_id,
        'location': location,
        'timestamp': timestamp,
        'snapshot':   'Base64EncodedString' }

def generate_weather_data(device_id, timestamp, location):
    return {
        'id':uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': timestamp,
        'location': location,
        'temperature': random.uniform(-5,26), #Celsius
        'weatherCondition': random.choice(['sunny', 'cloudy', 'Rainy', 'Snow']),
        'precipitation': random.uniform(0,25),
        'windSpeed': random.uniform(0, 100),
        'humidity': random.randint(0,100),
        'airQualityIndex': random.uniform(0,500)
    }

def  generate_emergency_data(device_id, timestamp, location):
    return {
        'id':uuid.uuid4(),
        'device_id': device_id,
        'incidentId': uuid.uuid4(),
        'timestamp': timestamp,
        'location': location,
        'type': random.choice(['Accident', 'Fire', 'Medical', 'Police', 'None']),
        'status': random.choice(['Active', 'Resolved']),
        'Description': 'Descrption of the incident'
    }


def simuate_vehicle_movement():
    global start_location

    start_location['latitude']+=LATITIDUE_INCREMENT
    start_location['longitude']+=LONGITUDE_INCREMENT

    #add randomness to simulate actual road travel
    start_location['latitude']+=random.uniform(-0.0005, 0.0005)
    start_location['longitude']+=random.uniform(-0.0005, 0.0005)

    return start_location

def generate_vehicle_data(device_id):
    location = simuate_vehicle_movement()
    return {
        'id': uuid.uuid4(),
        'device_id': device_id,
        'timestamp': get_next_time().isoformat(),
        'location':(location['latitude'], location['longitude']),
        'speed': random.uniform(10, 40),
        'direction': 'North-East',
        'make': 'BMW',
        'model': 'C500',
        'year': 2024,
        'fueltype': 'Hybrid'
    }

def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f"Object of type {obj.__class__.__name__} is not serializable")

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed:{err}")
    else:
        print(f"Message delivered to {msg.topic()} [msg.partition()]")

def produce_data_to_kafka(producer, TOPIC, data):
    producer.produce(
        TOPIC,
        key=str(data['id']),
        value=json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery=delivery_report

    )
    producer.flush()

def simulate_journey(producer, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data['timestamp'])
        traffic_camera_data = generate_traffic_camera_data(device_id, vehicle_data['timestamp'], vehicle_data['location'],camera_id='Nikon_!23')
        weather_data = generate_weather_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])
        emergency_incident_data = generate_emergency_data(vehicle_data, vehicle_data['timestamp'], vehicle_data['location'])
        if (vehicle_data['location'][0]>= BIRMINGHAM_COORDINATES['latitude'] and vehicle_data['location'<=BIRMINGHAM_COORDINATES['longitude']]):
            print('vehicle has reached brimingham. Simulation ending..')
            break
        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_incident_data)
        time.sleep(2)

if __name__=="__main__":
    print(f"inside the main functions")
    BIRMINGHAM_COORDINATES={"latitude":52.489471, "longitude": -1.898575}
    LONDON_COORDINATES={"latitude": 51.5074, "longitude": -0.1278} # type: ignore


# Calculate the movement increments
    LATITIDUE_INCREMENT = (BIRMINGHAM_COORDINATES['latitude']-LONDON_COORDINATES['latitude'])/100
    print(LATITIDUE_INCREMENT)
    LONGITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['longitude']-LONDON_COORDINATES['longitude'])/100
    print(LONGITUDE_INCREMENT)
    start_time = datetime.now()
    start_location = LONDON_COORDINATES.copy()
    print(f"the birmingham coordinates are {BIRMINGHAM_COORDINATES}")
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'kafka error: {err}')
    }
    producer = SerializingProducer(producer_config)
    try:
        simulate_journey(producer, 'Vehicle-CodewithYu-123')
    except KeyboardInterrupt:
        print(f'Simulation ended by the user')
    except Exception as e:
        print(f'An unexpected error occurred: {e}')
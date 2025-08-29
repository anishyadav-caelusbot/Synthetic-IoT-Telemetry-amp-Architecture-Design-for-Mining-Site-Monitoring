# Import required libraries for random data generation, JSON handling, threading, MQTT communication, and datetime
import random
import json
import time
import threading
import argparse
import paho.mqtt.client as mqtt
from datetime import datetime

# Set up command-line argument parser to accept config file path
parser = argparse.ArgumentParser(description="Synthetic IoT Telemetry Generator for Mining Site")
parser.add_argument('--config', type=str, default='config.json', help='Path to config JSON file')
args = parser.parse_args()

# Function to load configuration from JSON file with fallback to defaults
def load_config(config_file):
    # Default configuration values if JSON file is missing or incomplete
    default_config = {
        "num_vehicles": 5,
        "num_fixed_assets": 10,
        "num_env_sensors": 20,
        "num_personnel": 15,
        "frequency": 10,
        "variance": 0.1,
        "seed": 42,
        "mode": "normal",
        "broker": "localhost",
        "port": 1883,
        "aws_endpoint": None,
        "cert": None,
        "key": None,
        "ca": None
    }
    try:
        # Attempt to read and parse the config JSON file
        with open(config_file, 'r') as f:
            config = json.load(f)
        # Update defaults with values from JSON file
        default_config.update(config)
    except FileNotFoundError:
        # Log warning if config file is not found, use defaults
        print(f"Config file {config_file} not found, using defaults")
    return default_config

# Load configuration from file or defaults
config = load_config(args.config)

# Validate the operation mode
if config['mode'] not in ['normal', 'fault', 'intermittent']:
    raise ValueError("Mode must be 'normal', 'fault', or 'intermittent'")

# Set random seed for reproducible data generation
random.seed(config['seed'])

# Initialize MQTT client for publishing telemetry data
client = mqtt.Client()
# Configure TLS for AWS IoT Core if specified
if config['broker'] == 'aws' and config['aws_endpoint']:
    # Ensure all certificate paths are provided for AWS
    if not all([config['cert'], config['key'], config['ca']]):
        raise ValueError("AWS IoT requires cert, key, and CA paths")
    # Set up TLS with certificates for secure connection
    client.tls_set(ca_certs=config['ca'], certfile=config['cert'], keyfile=config['key'])
    endpoint = config['aws_endpoint']
    port = 8883  # Default AWS IoT Core MQTT port
else:
    # Use local broker settings
    endpoint = config['broker']
    port = config['port']
# Connect to the MQTT broker (local or AWS)
client.connect(endpoint, port, 60)
# Start the MQTT client loop to handle network events
client.loop_start()

# Generate device IDs for each asset type
vehicles = [f"{typ}_{i:03d}" for typ in ['haul_truck', 'loader', 'excavator'] for i in range(config['num_vehicles'] // 3 + 1)][:config['num_vehicles']]
fixed_assets = [f"{typ}_{i:03d}" for typ in ['fan', 'vent_system', 'conveyor'] for i in range(config['num_fixed_assets'] // 3 + 1)][:config['num_fixed_assets']]
env_sensors = [f"env_{typ}_{i:03d}" for typ in ['co_co2', 'methane', 'dust', 'temp_hum', 'ground_stab'] for i in range(config['num_env_sensors'] // 5 + 1)][:config['num_env_sensors']]
personnel = [f"personnel_{i:03d}" for i in range(config['num_personnel'])]

# Combine all device IDs into a single list
all_devices = vehicles + fixed_assets + env_sensors + personnel

# Define metric ranges for each device type (5 critical metrics per type)
metric_ranges = {
    'vehicle': {
        'gps': {'lat': (37.7, 37.8), 'lon': (-122.5, -122.4)},  # GPS coordinates
        'speed': (0, 50),  # Speed in km/h
        'engine_temp': (60, 90),  # Engine temperature in °C
        'vibration': (0.5, 2.0),  # Vibration in g
        'fuel_level': (20, 100)  # Fuel/battery level in %
    },
    'fixed': {
        'vibration': (0.1, 1.0),  # Vibration in mm/s
        'motor_temp': (40, 70),  # Motor temperature in °C
        'operational_status': ['running', 'idle', 'stopped'],  # Operational status
        'airflow_rate': (100, 500),  # Airflow in m³/s
        'power_consumption': (500, 2000)  # Power in W
    },
    'env': {
        'co_co2': (0, 50),  # CO/CO₂ levels in ppm
        'methane': (0, 5),  # Methane levels in ppm
        'dust': (0, 100),  # Dust concentration in µg/m³
        'temp_hum': {'temp': (10, 30), 'hum': (40, 80)},  # Temperature (°C) and humidity (%)
        'ground_vib': (0, 1)  # Ground vibration in mm/s
    },
    'personnel': {
        'co_level': (0, 50),  # CO exposure in ppm
        'proximity_alert': [False, True],  # Proximity alert status
        'heart_rate': (60, 100),  # Heart rate in bpm
        'location': {'x': (0, 1000), 'y': (0, 1000)},  # Location in meters
        'fall_detection': [False, True]  # Fall detection status
    }
}

# Function to determine device type based on ID
def get_device_type(device_id):
    if 'truck' in device_id or 'loader' in device_id or 'excavator' in device_id:
        return 'vehicle'
    elif 'fan' in device_id or 'vent' in device_id or 'conveyor' in device_id:
        return 'fixed'
    elif 'env' in device_id:
        return 'env'
    elif 'personnel' in device_id:
        return 'personnel'

# Function to simulate telemetry data for a device
def simulate_data(device_id, mode):
    # Get device type for metric selection
    typ = get_device_type(device_id)
    metrics = {}
    status = 'normal'
    
    # Generate metrics based on defined ranges with variance
    ranges = metric_ranges[typ]
    for key, val in ranges.items():
        if isinstance(val, dict):  # Handle nested metrics like gps or temp_hum
            metrics[key] = {k: random.uniform(v[0], v[1]) * (1 + random.uniform(-config['variance'], config['variance'])) for k, v in val.items()}
        elif isinstance(val, list) and isinstance(val[0], str):  # Handle enum status values
            metrics[key] = random.choice(val)
        elif isinstance(val, list) and isinstance(val[0], bool):  # Handle boolean values
            metrics[key] = random.choice(val)
        else:  # Handle numeric ranges
            metrics[key] = random.uniform(val[0], val[1]) * (1 + random.uniform(-config['variance'], config['variance']))
    
    # Inject faults in fault mode with 20% probability per metric
    if mode == 'fault' and random.random() < 0.2:
        if typ == 'vehicle':
            metrics['engine_temp'] += 50  # Simulate overheating
            status = 'fault_overheat'
        elif typ == 'fixed':
            metrics['operational_status'] = 'jammed'  # Simulate equipment jam
            status = 'fault_jam'
        elif typ == 'env':
            metrics['methane'] += 10  # Simulate gas spike
            status = 'fault_gas'
        elif typ == 'personnel':
            metrics['fall_detection'] = True  # Simulate fall detection
            status = 'fault_fall'
    
    # Construct JSON payload with device details and metrics
    payload = {
        'device_id': device_id,
        'type': typ,
        'timestamp': datetime.utcnow().isoformat() + 'Z',
        'metrics': metrics,
        'status': status
    }
    return json.dumps(payload)

# Function to publish data for a device in a loop
def publish_loop(device_id, mode):
    buffer = []  # Buffer for intermittent mode
    while True:
        # Generate data for the device
        data = simulate_data(device_id, mode)
        if mode == 'intermittent' and random.random() < 0.3:  # 30% chance of outage
            buffer.append(data)  # Buffer data during outage
            time.sleep(random.randint(10, 30))  # Simulate outage duration
            # Publish buffered data upon reconnection
            for buffered_data in buffer:
                client.publish(f"mining/{device_id}", buffered_data, qos=1)
            buffer = []  # Clear buffer after sending
        else:
            # Publish data immediately in normal or fault mode
            client.publish(f"mining/{device_id}", data, qos=1)
        # Wait for the configured frequency before next publish
        time.sleep(config['frequency'])

# Start a thread for each device to publish data concurrently
threads = []
for device in all_devices:
    t = threading.Thread(target=publish_loop, args=(device, config['mode']))
    t.daemon = True  # Ensure threads terminate when main process exits
    t.start()
    threads.append(t)

# Keep the main thread running to maintain daemon threads
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    # Gracefully stop MQTT client on interrupt
    client.loop_stop()
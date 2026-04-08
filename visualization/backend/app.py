import json
import threading
import logging
from flask import Flask, send_from_directory
from flask_socketio import SocketIO
from kafka import KafkaConsumer
import pymysql
from collections import deque

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

app = Flask(__name__, static_folder='../frontend')
socketio = SocketIO(app, cors_allowed_origins="*")

MAX_CACHE_SIZE = 20
recent_warnings = deque(maxlen=MAX_CACHE_SIZE)

DB_CONFIG = {
    'host': 'mysql',
    'user': 'root',
    'password': 'root123',
    'database': 'traffic_warning',
    'charset': 'utf8mb4'
}

def save_to_mysql(data):
    try:
        conn = pymysql.connect(**DB_CONFIG)
        cursor = conn.cursor()
        sql = """
            INSERT INTO warnings 
            (segment_id, grade, avg_speed, total_vehicles, window_start, window_end, level)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(sql, (
            data['segmentId'], data['grade'], data['avgSpeed'],
            data['totalVehicles'], data['windowStart'], data['windowEnd'], data['level']
        ))
        conn.commit()
        cursor.close()
        conn.close()
        logger.info(f"Saved to MySQL: {data['segmentId']}")
    except Exception as e:
        logger.error(f"MySQL error: {e}")

def kafka_consumer_thread():
    logger.info("Starting Kafka consumer thread...")
    try:
        consumer = KafkaConsumer(
            'traffic-warning',
            bootstrap_servers='kafka:9092',
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='visualization-group-v3',
            value_deserializer=lambda m: None if not m or not m.strip() else json.loads(m.decode('utf-8').strip())
        )
        logger.info("Kafka consumer connected, listening...")
        for msg in consumer:
            try:
                data = msg.value
                if data is None:
                    logger.warning("Received empty message, skipping")
                    continue
                required_fields = ['segmentId', 'grade', 'avgSpeed', 'totalVehicles', 'windowStart', 'windowEnd', 'level']
                if not all(field in data for field in required_fields):
                    logger.warning(f"Message missing fields: {data}")
                    continue
                logger.info(f"Received message: {data}")
                save_to_mysql(data)
                recent_warnings.append(data)
                logger.info(f"Cached message, cache size now: {len(recent_warnings)}")
                socketio.emit('new_warning', data)
                logger.info("Emitted via WebSocket")
            except Exception as e:
                logger.error(f"Error processing message: {e}", exc_info=True)
                continue
    except Exception as e:
        logger.error(f"Kafka consumer error: {e}", exc_info=True)

@app.route('/')
def index():
    return send_from_directory('/app/frontend', 'index.html')

@socketio.on('connect')
def handle_connect(auth):   # 修复：添加参数
    logger.info('Client connected via WebSocket')
    for warning in recent_warnings:
        socketio.emit('new_warning', warning)
        logger.info(f"Sent cached warning: {warning['segmentId']}")
    
    test_msg = {
        "segmentId": 0,
        "grade": "test",
        "avgSpeed": 0,
        "totalVehicles": 0,
        "windowStart": 0,
        "windowEnd": 0,
        "level": "risky",
        "timestamp": "test"
    }
    socketio.emit('new_warning', test_msg)
    logger.info("Sent test message to newly connected client")

if __name__ == '__main__':
    logger.info("Starting Flask app...")
    thread = threading.Thread(target=kafka_consumer_thread, daemon=True)
    thread.start()
    socketio.run(app, host='0.0.0.0', port=5000, debug=False)
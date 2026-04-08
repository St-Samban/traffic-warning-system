import json
import time
import random
from kafka import KafkaProducer

# Kafka 连接配置（外部端口 29092）
producer = KafkaProducer(
    bootstrap_servers='localhost:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 路段配置：30个路段，按ID范围分配等级
SEGMENT_CONFIG = []
# ID 1-5 快速路
for sid in range(1, 6):
    SEGMENT_CONFIG.append({
        'segmentId': sid,
        'grade': 'gradeOne',      # 快速路
        'vFree': 80,
        'kJam': 150
    })
# ID 6-23 主干道
for sid in range(6, 24):
    SEGMENT_CONFIG.append({
        'segmentId': sid,
        'grade': 'gradeTwo',      # 主干道
        'vFree': 60,
        'kJam': 150
    })
# ID 24-30 次干道
for sid in range(24, 31):
    SEGMENT_CONFIG.append({
        'segmentId': sid,
        'grade': 'gradeThree',    # 次干道
        'vFree': 40,
        'kJam': 150
    })

print(f"开始生成数据，共 {len(SEGMENT_CONFIG)} 个路段，每秒为每个路段生成一条记录。")

while True:
    timestamp_ms = int(time.time() * 1000)  # 毫秒时间戳
    for seg in SEGMENT_CONFIG:
        segmentId = seg['segmentId']
        grade = seg['grade']
        vFree = seg['vFree']
        kJam = seg['kJam']

        # 随机生成密度 k (0, kJam)
        k = random.uniform(0.1, kJam - 0.1)  # 避免边界值
        # Greenshields 模型
        v = vFree * (1 - k / kJam)
        q = v * k   # 辆/小时/车道

        record = {
            'segmentId': segmentId,
            'grade': grade,
            'vFree': vFree,
            'q': round(q, 2),            # 交通量（辆/小时）
            'v': round(v, 2),            # 速度（km/h）
            'k': round(k, 2),            # 密度（辆/km/车道）
            'timestamp': timestamp_ms,
            'kJam': kJam
        }
        producer.send('traffic-raw', value=record)
        # 可选：打印，但生产环境可注释
        # print(f"Sent: {record}")
    # 每秒生成一批
    time.sleep(1)
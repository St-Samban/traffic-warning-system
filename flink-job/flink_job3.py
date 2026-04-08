from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import WatermarkStrategy, TimestampAssigner
from pyflink.common.time import Duration, Time
from pyflink.datastream.window import TumblingEventTimeWindows
from pyflink.datastream.functions import ProcessWindowFunction
from pyflink.common.typeinfo import Types
from pyflink.datastream.state import StateTtlConfig, ValueStateDescriptor
import json
from datetime import datetime
import os

os.environ['PYFLINK_PYTHON'] = '/usr/bin/python3'

# 初始化执行环境
env = StreamExecutionEnvironment.get_execution_environment()
env.set_parallelism(1)
env.set_python_executable("/usr/bin/python3")

# 添加 Kafka 连接器 JAR（路径请根据实际情况调整）
current_dir = os.path.dirname(os.path.abspath(__file__))
jar_path = f"file://{current_dir}/flink-sql-connector-kafka-1.17.2.jar"
env.add_jars(jar_path)


# ==================== 自定义时间戳提取器 ====================
class MyTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp):
        # value 是元组 (segmentId, grade, q, v, k, timestamp)
        return value[5]   # timestamp 在第6个字段


# ==================== 窗口处理函数（含状态管理） ====================
class TrafficWarningWindowFunction(ProcessWindowFunction):
    def open(self, runtime_context):
        # 配置状态 TTL 30 秒
        ttl_config = StateTtlConfig.new_builder(Time.seconds(30)) \
            .set_update_type(StateTtlConfig.UpdateType.OnCreateAndWrite) \
            .set_state_visibility(StateTtlConfig.StateVisibility.NeverReturnExpired) \
            .build()
        # 状态存储 (prevSpeed, prevWindowEnd)
        descriptor = ValueStateDescriptor("prev_state", Types.TUPLE([Types.DOUBLE(), Types.LONG()]))
        descriptor.enable_time_to_live(ttl_config)
        self.prev_state = runtime_context.get_state(descriptor)

    def process(self, key, context, elements):
        # 1. 聚合当前窗口指标
        elements_list = list(elements)
        speeds = [e[3] for e in elements_list]      # v 在元组第4个字段（索引3）
        # 注意：q 用于计算车辆数，但我们需要窗口内的总车辆数 = 累加 vehiclesThisSec
        # 这里我们已经在解析阶段计算了 vehiclesThisSec 并存入元组？下面解析会说明。
        # 为了简化，我们在解析阶段就计算出每秒的车辆数，并将元组定义为 (segmentId, grade, q, v, k, timestamp, vehiclesThisSec)
        # 这样在窗口聚合时直接累加 vehiclesThisSec 即可。
        # 因为 elements 来自解析后的流，我们将在解析阶段增加 vehiclesThisSec 字段。
        # 但为了与现有结构一致，我们需要调整解析后的元组结构。
        # 以下假设 elements 的每个元素是 (segmentId, grade, q, v, k, timestamp, vehiclesThisSec)
        # 那么：
        vehicles_per_sec = [e[6] for e in elements_list]
        avg_speed = sum(speeds) / len(speeds) if speeds else 0.0
        total_vehicles = sum(vehicles_per_sec)

        window_start = context.window().start
        window_end = context.window().end

        # 2. 获取前一个窗口的状态
        prev_state = self.prev_state.value()
        prev_speed = None
        if prev_state is not None:
            prev_speed, prev_end = prev_state
            # 如果窗口间隔超过10秒，清空状态（前一个窗口信息失效）
            if window_start - prev_end > 10000:  # 10秒 = 10000毫秒
                prev_speed = None
                self.prev_state.clear()

        # 3. 根据规则判断等级
        level = "normal"
        if avg_speed < 20 and total_vehicles > 5:
            level = "severe"
        elif avg_speed < 30 and total_vehicles > 10:
            level = "severe"
        elif avg_speed < 30 and total_vehicles > 5:
            level = "risky"
        elif prev_speed is not None and prev_speed > 30 and avg_speed < 0.7 * prev_speed and avg_speed > 20:
            # 速度骤降且流量条件? 规则里没有流量条件，但可以增加 total_vehicles > 5 避免空载误报
            # 根据用户要求，增加流量条件
            if total_vehicles > 5:
                level = "risky"
        elif avg_speed > 45:
            level = "normal"
        else:
            level = "normal"

        # 4. 更新状态（存储当前窗口的速度和结束时间）
        self.prev_state.update((avg_speed, window_end))

        # 5. 仅当等级非 normal 时输出预警
        if level != "normal":
            # 获取 grade（从任意一个元素中取，因为同一路段grade相同）
            grade = elements_list[0][1] if elements_list else "unknown"
            warning_msg = {
                'segmentId': key,
                'grade': grade,
                'avgSpeed': round(avg_speed, 2),
                'totalVehicles': round(total_vehicles, 2),
                'windowStart': window_start,
                'windowEnd': window_end,
                'level': level,
                'timestamp': datetime.now().isoformat()
            }
            yield json.dumps(warning_msg)


def main():
    # ========== Kafka 消费者配置 ==========
    kafka_consumer = FlinkKafkaConsumer(
        topics='traffic-raw',
        deserialization_schema=SimpleStringSchema(),
        properties={
            'bootstrap.servers': 'localhost:29092',     # 容器内部使用服务名，在集群应使用kafka:9092，在本地测试应使用localhost:29092
            'group.id': 'flink-group',
            'auto.offset.reset': 'latest'
        }
    )

    # 数据源
    data_stream = env.add_source(kafka_consumer)

    # 解析 JSON 并转换为元组，同时计算每秒车辆数
    def parse_and_calc(row):
        try:
            data = json.loads(row)
            # 字段严格对应：segmentId, grade, q, v, k, timestamp
            seg_id = data['segmentId']
            grade = data['grade']
            q = data['q']                # 辆/小时
            v = data['v']                # km/h
            k = data['k']                # 辆/km/车道
            ts = data['timestamp']
            # 计算每秒车辆数
            vehicles_this_sec = q / 3600.0
            # 返回元组
            return (seg_id, grade, q, v, k, ts, vehicles_this_sec)
        except Exception as e:
            return None

    parsed_stream = data_stream.map(parse_and_calc).filter(lambda x: x is not None)

    # 显式指定元组类型
    parsed_stream = parsed_stream.map(
        lambda x: x,
        output_type=Types.TUPLE([
            Types.INT(),           # segmentId
            Types.STRING(),        # grade
            Types.DOUBLE(),        # q
            Types.DOUBLE(),        # v
            Types.DOUBLE(),        # k
            Types.LONG(),          # timestamp
            Types.DOUBLE()         # vehiclesThisSec
        ])
    )

    # 水印策略（事件时间，允许5秒乱序）
    watermark_strategy = WatermarkStrategy.for_bounded_out_of_orderness(Duration.of_seconds(5)) \
        .with_timestamp_assigner(MyTimestampAssigner())

    with_timestamps = parsed_stream.assign_timestamps_and_watermarks(watermark_strategy)

    # 按路段分组
    keyed_stream = with_timestamps.key_by(lambda x: x[0])

    # 10秒滚动事件时间窗口
    windowed_stream = keyed_stream.window(TumblingEventTimeWindows.of(Time.seconds(10))) \
        .process(TrafficWarningWindowFunction(),
                 output_type=Types.STRING())

    # 将预警结果输出到 Kafka 和 print
    kafka_producer = FlinkKafkaProducer(
        topic='traffic-warning',
        serialization_schema=SimpleStringSchema(),
        producer_config={'bootstrap.servers': 'localhost:29092'}# 容器内部使用服务名，在集群应使用kafka:9092，在本地测试应使用localhost:29092

    )
    windowed_stream.add_sink(kafka_producer)
    windowed_stream.print()

    env.execute("Traffic Warning Job")


if __name__ == "__main__":
    main()
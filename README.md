基于 Kafka + Flink 的智慧城市交通拥堵实时预警系统
项目背景
城市交通拥堵是影响城市运行效率与居民生活质量的核心问题。传统的拥堵监测基于固定检测器与离线统计，存在数据更新慢、预警滞后等缺点。本项目采用流处理技术栈（Apache Kafka + Apache Flink），设计并实现了一个端到端的实时交通拥堵预警系统，能够从模拟交通流数据中实时识别拥堵状态并输出预警，同时提供可视化大屏展示与数据持久化能力。

系统架构
https://docs/architecture.png

（注：实际图片请放置在 docs/architecture.png，以下是架构描述）

系统分为四大模块：

数据模拟层：Python 数据生成器，基于 Greenshields 交通流模型，每秒为 30 个不同等级的路段生成包含密度、速度、交通量的 JSON 记录，发送至 Kafka。

消息接入层：Apache Kafka 作为统一消息总线，traffic-raw 主题接收原始数据，traffic-warning 主题输出预警结果。

实时计算层：Apache Flink 作业消费 traffic-raw，进行 JSON 解析、事件时间与水印分配、10 秒滚动窗口聚合、状态管理（ValueState + TTL），并根据多级预警规则输出 severe/risky 等级预警到 traffic-warning。

可视化与持久化层：

Flask + SocketIO 后端消费 traffic-warning，将预警存入 MySQL，并通过 WebSocket 推送到前端。

前端基于 ECharts 展示实时预警趋势与最新预警列表。

所有服务通过 Docker Compose 编排，与原有 Kafka/Flink 集群共享同一 Docker 网络。

技术选型说明
组件	技术	版本	说明
流处理引擎	Apache Flink	1.18.1 (镜像) / PyFlink 1.17.2	使用 PyFlink DataStream API，事件时间处理
消息队列	Apache Kafka	7.4.0 (Confluent 镜像)	内外网分离监听 (9092/29092)
协调服务	ZooKeeper	7.4.0 镜像	Kafka 依赖
数据生成器	Python + kafka-python	Python 3.10.12, kafka-python 2.0.2	每秒为 30 个路段生成记录
后端服务	Flask + Flask-SocketIO	Flask 2.2.5, Flask-SocketIO 5.3.6	WebSocket 实时推送，REST API 提供静态页面
数据库	MySQL	8.0	存储预警历史
前端	HTML + ECharts + Socket.IO	ECharts 5.4.3, Socket.IO 4.5.4	实时图表与预警列表
容器化	Docker + Docker Compose	Docker 29.0.1, Compose 2.40.3	服务编排，统一网络 traffic-net
关键依赖版本（细粒度）
PyFlink: 1.17.2 (安装命令 pip3 install apache-flink==1.17.2)

Kafka 连接器 JAR: flink-sql-connector-kafka-1.17.2.jar (下载自 Maven 中央仓库，大小 5.31 MB)

Python: 3.10.12 (WSL Ubuntu 22.04) / 容器内使用 python:3.10-slim 基础镜像

符号链接: 在 WSL 中执行 sudo ln -s /usr/bin/python3 /usr/local/bin/python 使 python 指向 python3

Docker 镜像版本:

confluentinc/cp-zookeeper:7.4.0

confluentinc/cp-kafka:7.4.0

flink:1.18.1-scala_2.12

mysql:8.0

python:3.10-slim

部署步骤
前置条件
Windows 10/11 + WSL2 (Ubuntu 22.04) 或原生 Linux

Docker Desktop (启用 WSL2 集成) 及 Docker Compose

Git (用于克隆仓库)

至少 8GB 内存，20GB 可用磁盘空间

第一步：克隆项目并准备环境
bash
git clone <your-repo-url> traffic-warning-system
cd traffic-warning-system
第二步：启动 Kafka + Flink 基础集群
项目根目录下已提供 docker-compose.yml（内外网分离配置）。执行：

bash
docker-compose up -d
验证服务状态：

bash
docker-compose ps
# 应看到 zookeeper, kafka, jobmanager, taskmanager 均为 Up 状态
创建 Kafka 主题：

bash
docker exec -it kafka kafka-topics --bootstrap-server localhost:29092 --create --topic traffic-raw --partitions 1 --replication-factor 1
docker exec -it kafka kafka-topics --bootstrap-server localhost:29092 --create --topic traffic-warning --partitions 1 --replication-factor 1
第三步：在 Flink 容器中安装 Python 环境
Flink 官方镜像未预装 Python，需要手动在 JobManager 和 TaskManager 中安装。

进入 JobManager 容器：

bash
docker exec -it jobmanager bash
执行以下安装命令：

bash
apt update
apt install -y python3 python3-pip
pip3 install apache-flink==1.17.2
# 设置软链接（可选）
ln -s /usr/bin/python3 /usr/bin/python
exit
同样操作 TaskManager（因为任务执行需要 Python 环境）：

bash
docker exec -it taskmanager bash
apt update
apt install -y python3 python3-pip
pip3 install apache-flink==1.17.2
exit
复制 Flink 作业脚本和 Kafka 连接器 JAR：

bash
# 复制脚本到 JobManager
docker cp flink-job/flink_job.py jobmanager:/opt/flink/
# 复制 Kafka 连接器 JAR 到 lib 目录（自动加载）
docker cp flink-job/flink-sql-connector-kafka-1.17.2.jar jobmanager:/opt/flink/lib/
修改脚本中的 Kafka 地址（容器内使用 kafka:9092）：

bash
docker exec -it jobmanager sed -i 's/localhost:29092/kafka:9092/g' /opt/flink/flink_job.py
第四步：提交 Flink 作业到集群
bash
docker exec -it jobmanager flink run -py /opt/flink/flink_job.py
访问 Flink Web UI：http://localhost:8081，确认作业状态为 RUNNING。

第五步：启动模拟数据生成器
在 WSL 终端中（确保已安装 kafka-python）：

bash
pip3 install kafka-python==2.0.2
cd ~/traffic-warning-system/data-generator
python3 generator.py &
数据生成器将每秒为 30 个路段发送一条记录到 traffic-raw 主题。

第六步：启动可视化模块（MySQL + Flask + 前端）
进入可视化目录并启动服务：

bash
cd ~/traffic-warning-system/visualization
docker-compose -f docker-compose.vis.yml up -d --build
验证容器状态：

bash
docker-compose -f docker-compose.vis.yml ps
# 应看到 mysql-vis 和 flask-backend 均为 Up 状态
第七步：访问可视化大屏
打开浏览器访问 http://localhost:5000，应看到实时预警趋势图表和预警列表。初始时可能无数据，待 Flink 作业产生预警后会自动显示。

第八步：手动测试预警消息（可选）
通过 Kafka 生产者发送一条测试预警：

bash
docker exec -it kafka kafka-console-producer --bootstrap-server localhost:29092 --topic traffic-warning
> {"segmentId": 99, "grade": "gradeOne", "avgSpeed": 10, "totalVehicles": 20, "windowStart": 1234567890000, "windowEnd": 1234567900000, "level": "severe", "timestamp": "2026-04-07T10:00:00"}
前端大屏应立即显示该条预警。

项目文件结构
text
traffic-warning-system/
├── docker-compose.yml                  # Kafka + Flink 集群编排
├── data-generator/
│   └── generator.py                    # Greenshields 模型数据生成器
├── flink-job/
│   ├── flink_job.py                    # PyFlink 实时计算作业
│   └── flink-sql-connector-kafka-1.17.2.jar
├── visualization/
│   ├── docker-compose.vis.yml          # 可视化服务编排
│   ├── backend/
│   │   ├── Dockerfile
│   │   ├── app.py                      # Flask + SocketIO 后端
│   │   └── requirements.txt
│   ├── frontend/
│   │   └── index.html                  # ECharts 大屏页面
│   └── mysql/
│       └── init.sql                    # 数据库初始化脚本
└── README.md
效果截图
Flink Web UI 作业监控
https://docs/flink-ui.png

实时预警大屏
https://docs/dashboard.png

（注：实际截图请放置在 docs/ 目录下，此处为占位描述）

常见问题
Flink 作业提交后无预警输出

检查数据生成器是否运行 (ps aux | grep generator)

检查 Kafka traffic-raw 主题是否有数据 (docker exec -it kafka kafka-console-consumer ...)

查看 Flink TaskManager 日志 (docker logs taskmanager)

可视化大屏无法连接 WebSocket

确认 flask-backend 容器日志无报错 (docker logs flask-backend)

检查浏览器控制台是否有连接错误

MySQL 连接失败

确保 mysql-vis 容器已完全启动（可查看日志）

检查 app.py 中 DB_CONFIG 的 host 是否为 mysql

容器内 Python 环境丢失

若重启 Docker 容器，之前安装的 Python 会消失。建议构建自定义 Flink 镜像（参考 Dockerfile.flink）或每次重启后重新执行安装命令。

贡献与许可
本项目为毕业设计作品，遵循 MIT 许可。欢迎参考与改进。

作者：李寒潇
指导老师：赵波
日期：2026年4月


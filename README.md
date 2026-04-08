1：项目环境通过docker进行配置；通过docker-compose.yml pull oficial images
1.1：由于需要在jobmanager，taskmanager安装python3，python3-pip，apache-flink；设置python->python3软链接；复制flink\_job脚本，JAR包；修改flink\_job.py脚本中的kafka地址。因此我们需要新增Dockerfile.flink，并修改compose.yml文件



2：本项目采用"apache-flink==1.17.2"，对应版本的JAR"flink-sql-connector-kafka-1.17.2"



3：操作流程：
提交数据流①模拟数据②真实数据
4：编译器环境：在WSL下下载了python3，并通过软链接将python命令链接至python3

bash

python verision

python3.10.12

4：三种方式：
①本地嵌入Flink集群：直接在WSL中，python3 data\_generating.py，再在另一个WSL中python3 flink\_job3.py即可

②docker网路集群：为了在Web上检查flink作业，我们需要将作业提交至docker的flink集群：
1，由于官方的flink镜像没有预装python，所以我们需要"apt update, apt install python3 python3-pip, pip3 install apache-flink==1.17.2"

2，将flink\_job.py，flink-sql-connector-kafka-1.17.2.jar，通过cp命令复制到容器内部路径"opt/flink/lib"（事实上，该项目的路径应为opt/flink）

3，（关键！）

当前脚本中 Kafka 的 bootstrap.servers 配置为 localhost:29092，这是在宿主机外部访问时使用的地址。在容器内部，应该使用 Kafka 的服务名 kafka:9092（内部通信端口）。我们需要修改脚本。

方法一：直接在容器内修改脚本，在容器内的 bash 中执行：

bash

sed -i 's/localhost:29092/kafka:9092/g' /opt/flink/flink\_job3.py

4，经过实践，事实上我们不仅需要在jobmanager，同样需要在taskmanager内install python3，python3-pip

5，执行命令：
flink run -py /opt/flink/flink\_job3.py

得到输出：
Job has been submitted with JobID <job\_id>
③一个错误：在jobmanager容器内，我运行了命令python3 run opt/flink/flink\_job3.py，我预期其能在flink集群上运行并将作业提交至Web，但失败了。原因是，这相当于一个套娃式操作。必须使用flink run -py命令


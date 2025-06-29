## Spark Streaming with Kafka

---

### 1.Kafka
- Kafka는 대용량 실시간 데이터 스트리밍 플랫폼으로, 메시지 브로커 역할을 수행 
- 주로 데이터 수집, 전달, 처리 시스템 간 비동기 통신에 사용됩니다.

| 용어               | 설명                                                                 |
|--------------------|----------------------------------------------------------------------|
| **Producer**        | Kafka로 데이터를 보내는 주체. 예: 로그 수집기, IoT 디바이스 등         |
| **Consumer**        | Kafka에서 데이터를 읽는 주체. 예: Spark, Flink, 데이터 분석 시스템 등  |
| **Topic**           | 메시지를 구분하는 논리적 채널. Producer는 Topic에 데이터를 보냄         |
| **Partition**       | Topic을 나누는 물리적 단위. 병렬성과 확장성을 위한 구조               |
| **Offset**          | Partition 내 각 메시지의 고유 위치. Consumer는 Offset을 기준으로 읽음  |
| **Broker**          | Kafka 서버 인스턴스. 여러 Broker가 모여 클러스터를 구성                 |


### 2. Spark Streaming with Kafka example
- Kafka 토픽으로부터 실시간 데이터를 수신하여 pyspark 스트리밍 처리 후 콘솔에 출력하는 간단한 실습 진행
- docker-compose.yml 파일 사용하여 Spark/Kafka 인스턴스 생성
- Spark master 인스턴스 접속 > 아래 명령어로 python file Spark에 제출

```shell
# Start history server after ssh into the spark master container
./sbin/start-history-server.sh

# How to use spark-submit (work 폴더로 이동)
spark-submit --master spark://spark:7077 word.py
```

- Kafka topic 생성 및 producer 
```shell
# Write Kafka event
cd /opt/bitnami/kafka/bin

# create a topic
./kafka-topics.sh --create --topic <topic> --bootstrap-server localhost:9092

# start console producer
./kafka-console-producer.sh --bootstrap-server 127.0.0.1:9092 --topic <topic> --producer.config /opt/bitnami/kafka/config/producer.properties
```

- 위에서 생성한 producer client에서 단어 입력 시 spark streaming 처리 후 단어 개수 확인
```shell
# Start Spark with Kafka
spark-submit --master spark://spark:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 spark_kafka.py

```

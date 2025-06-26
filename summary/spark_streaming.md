## Spark Streaming

---

### 1. Spark Streaming이란?
- Core Spark의 확장 프로그램으로  **분산 스트림 처리 프로세싱**을 지원한다
- 크게 2가지 방식의 스트리밍 처리 API 존재 : Dstream / Structured Streaming
- Kafka, S3, HDFS 등의 실시간 데이터 소스에서 데이터를 받아 처리하고, 그 결과를 Database, HDFS에 적재함
![streaming-arch.png](..%2Fimage%2Fstreaming-arch.png)

#### 1) Dstream API (RDD 방식)
- 실시간으로 들어오는 데이터 스트림을 마이크로 배치로 RDD 처리
  - Dstream(연속적인 RDD 시퀀스)로 구성하여, spark 분산 처리 엔진 사용해 병렬 처리됨
- 자바나 파이썬의 객체와 함수에 매우 의존적 → 스트림 처리 엔진이 제공하는 최적화 기법을 적용하지 못함
- 기본적으로 처리 시간을 기준으로 동작함 → 이벤트 시간 기준으로 처리하고 싶은 경우에는 자체적으로 구현해야 함
  - 처리 시간 기준 처리 : 스트리밍 application에 레코드가 도착한 시간을 기반으로 처리하는 방식
  - 이벤트 시간 기준 처리 : 원천 시스템에서 각 레코드에 기록한 타임스탬프를 기반으로 데이터를 처리하는 방식
- 마이크로 배치 형태로만 동작 → 다른 처리 모드를 지원 X


#### 2) Structured Streaming API (DataFrmae/Dataset 방식)
- DataFrame이나 Dataset에(구조적 데이터 모델) 직접 통합할 수 있는 구조적 스트리밍 API
- 배치 애플리케이션 뿐만 아니라 나머지 컴포넌트와 쉽게 연동 가능
- Scala, Java, Python, R 그리고 SQL을 사용해 구조적 처리를 할 수 있는 모든 환경에서 사용 가능
- 이벤트 시간 데이터 처리를 지원함 (모든 윈도우 연산에서 자동으로 이벤트 시간 처리를 지원)
- 데이터가 도착할 때마다 자동으로 증분 형태의 연산 결과를 만들어 냄 → 통합형 application을 작성할 때 큰 도움
  - 서로 다른 처리 시스템에 대한 배치와 스트리밍용 코드를 별도로 관리 필요 X
  - 동기화하지 않을 때 발생할 수 있는 위험성 제거 가능
![structured-streaming-example-model.png](..%2Fimage%2Fstructured-streaming-example-model.png)


### 2. Structured Streaming 기초
- Spark SQL 기반 스트림 처리 프레임워크(Dataframe, Dataset, SQL 사용)
- 데이터 스트림을 '데이터가 연속적으로 추가되는 테이블' 처럼 다룸
  - 새로운 데이터는 row 단위로 쌓으며, 행 추가 시 테이블 업데이트하고 결과 행을 외부 저장장치에 기록
![datastream.png](..%2Fimage%2Fdatastream.png)


- 이벤트 시간 데이터 처리 (데이터 생성 시간 기준)를 지원하여 데이터가 늦게 업로드되거나, 순서가 뒤섞인 채로 시스템에 들어와도 처리 가능
- 엔진이 자동으로 뎅터의 현재 이벤트 시간을 추척하고, 오래된 상태를 정리하는 watermarking 도입하여 데이터 보관 주기 제한을 둠 
  - 이벤트 발생 시각을 기준으로 watermark 설정 → 발생 시각과 도착 시간이 차이나는, 데이터 지연 임계값을 지정하여 임계값 이상의 데이터는 삭제

![structured-streaming-window.png](..%2Fimage%2Fstructured-streaming-window.png)
![structured-streaming-late-data.png](..%2Fimage%2Fstructured-streaming-late-data.png)

### 3. Structured Streaming deep-dive

#### 1) Input / Sink Sources
- Input Sources
  - Apache Kafka 0.10 이상 버전
  - HDFS, S3 등 분산 파일 시스템
  - 테스트용 socket

- Sink : 스트림의 결과를 저장할 목적지
  - Apache Kafka 0.10 이상 버전
  - 거의 모든 파일 포맷
  - 출력 레코드의 임의 연산을 실행하는 foreach 싱크
  - 테스트용 콘솔 싱크
  - 디버깅용 메모리 싱크

#### 2) Output Mode

- Append
  - 새로운 로우가 결과 테이블에 추가되면, 추가된 데이터만 출력하는 모드
  - Aggregation 로직과 같은 상태 유지 연산이 불가함


- Update
  - 이전 출력 결과에서 변경된 로우만 출력
  - 이 모드를 지원하는 싱크는 반드시 저수준 업데이트를 지원해야 하며, 쿼리에서 집계연산이 없다면 append와 동일


- Complete
  - 결과 테이블의 전체 상태를 싱크로 출력
  - 모두 데이터가 계속 변경될 수 있는 일부 상태 기반 데이터 다룰 때 유용
  - 저수준 업데이트 지원하지 않을 때도 유용


#### 3) Processing Model
Spark Streaming의 경우 read > transform > sink 로 구성된 micro-batch 형태이며, 이러한 micro-batch가 주기적으로 반복 실행되는 컨셉

![processing_model_spark_streaming.png](..%2Fimage%2Fprocessing_model_spark_streaming.png)

1. Spark Driver가 micro-batch(job으로 볼 수 있음) 코드를 Spark SQL 엔진에 제출
2. Spark SQL 엔진은 해당 코드를 compile하여 Execution plan을 세움
3. Backgroung Streaming Thread에서 micro-batch(job)을 트리거함
4. 백그라운드 프로세스에서 micro-batch(job) 로직이 실행됨. 하나의 micro-batch 프로세스 수행 후 백그라운드는 죽지 않고 그 다음 input 들어올 때까지 대기 후 작업 반복 수행


#### 4) Trigger Setting

- **Unspecified** : default로 적용된 기준으로, 현재 배치가 끝나면 다음 배치를 수행. 이때 다음 input 데이터가 들어오지 않았으면 들어올 때까지 대기
- **Time Interval** : 예를 들어 5분 주기의 배치. 이전 배치주기가 interval보다 길면 해당 배치가 끝나자마자 다음 배치가 시작. 데이터가 input 되지 않으면 배치는 시작 X
- **one-time (deprecated)** : 실행 시점에 읽을 수 있는 데이터가 있는지 확인하고, 있다면 단 한 번 마이크로배치를 실행
- **Available-now** : 트리거 실행 시점에 읽을 수 있는 모든 데이터를 여러 마이크로배치로 나눠서 끝까지 처리한 후 종료 (maxFilesPerTrigger for file sources)
- **Continuous** : 새롭게 적용된 방식으로, milli second 단위의 실시간 스트리밍을 지원. 이 방식은 주기가 매우 짧기 때문에 micro-batch 방식은 적절하지 않아, continuous trigger 방식을 채택하는 아직 실험적인 부분으로 명시됨.


```python
spark = SparkSession \
    .builder \
    .appName("StructuredStreamingSum") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .config("spark.sql.streaming.schemaInference", "true") \
    .config("maxFilesPerTrigger", 1) \
    .getOrCreate()
```
- `config("spark.streaming.stopGracefullyOnShutdown", "true")`
  - 스트리밍 작업을 graceful하게 종료하기 위해 설정하는 값
  - 스트리밍 작업은 계속해서 데이터 처리하고 있음에 따라, 미설정 시 처리 중인 작업 완료하지 않고 종료될 수 있고 데이터 손실 발생 가능함
- `config("maxFilesPerTrigger", 1)`
  - 각 트리거링 당 읽을 파일의 최대 수를 설정
  - 제한을 두어 디스크 I/O가 크게 오르거나, 해당 작업을 위한 리소스 사용량 등 작업 처리에 장애 발생하지 않도록
- `option("checkpointLocation", "checkpoint")`
  - 스트리밍 처리의 상태를 유지하기 위한 체크포인트 위치를 설정
- `trigger(processingTime='5 seconds')`
  - 처리 간격


#### 5) Fault Tolerance

- Spark Goal : **end-to-end exactly-once semantics**
  - 어떠한 레코드도 놓치지 않고, 중복 없이 데이터를 생성

- How?
  - checkpoint & WAL (write-ahead-log)을 기반으로 어디까지 처리했고, 어떤 상태였는지를 기록
  - Read Position(Offsets), State information(group by와 같은 연산 시 이전 배치에서 sum 해놓은 정보를 저장해 놓음으로써 재연산하는 낭비를 줄일 수 있게 함)을 기록함으로써 달성
  - Replayable한, 재현성이 있는 kafka와 같은 소스 사용 필요
  - 같은 input에는 동일한 output을 내는 일관된 로직 필요
  - 재실행이나 중복이 발생해도 시스템의 상태나 결과에 영향이 없어야 함


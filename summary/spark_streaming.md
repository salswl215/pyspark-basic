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


### 2. Structured Streaming
- Spark SQL 기반 스트림 처리 프레임워크(Dataframe, Dataset, SQL 사용)
- 데이터 스트림을 '데이터가 연속적으로 추가되는 테이블' 처럼 다룸
  - 새로운 데이터는 row 단위로 쌓으며, 행 추가 시 테이블 업데이트하고 결과 행을 외부 저장장치에 기록
![datastream.png](..%2Fimage%2Fdatastream.png)


- 이벤트 시간 데이터 처리 (데이터 생성 시간 기준)를 지원하여 데이터가 늦게 업로드되거나, 순서가 뒤섞인 채로 시스템에 들어와도 처리 가능
- 엔진이 자동으로 뎅터의 현재 이벤트 시간을 추척하고, 오래된 상태를 정리하는 watermarking 도입하여 데이터 보관 주기 제한을 둠 
  - 이벤트 발생 시각을 기준으로 watermark 설정 → 발생 시각과 도착 시간이 차이나는, 데이터 지연 임계값을 지정하여 임계값 이상의 데이터는 삭제

![structured-streaming-window.png](..%2Fimage%2Fstructured-streaming-window.png)
![structured-streaming-late-data.png](..%2Fimage%2Fstructured-streaming-late-data.png)
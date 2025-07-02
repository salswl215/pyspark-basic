## Spark streaming - Stateful transformation & Window operation

---

### 1.Stateful Transformation
Spark에서 데이터 연산 시 filter, count 등의 연산자를 사용하는데 크게 2 가지 유형으로 구분 가능

- Stateless Transformation
  - 과거의 데이터와 상관 없이 현재의 데이터만 사용하는 연산자 : `map()`, `filter()`, `flatMap()`, `distinct()`
  - 변환이 한 개의 RDD 또는 DataFrame의 각 요소에 독립적으로 적용
  - 상태를 유지하지 않기 때문에 병렬 처리 및 분산 실행이 효율적
  - `OutputMode('complete')` 가능, 로그 분석, 필터링과 같은 데이터 변환 작업 시 사용

- Stateful Transformation
  - 이전 입력 데이터의 상태를 유지하면서 새로운 데이터를 처리하는 연산자 : `reduceByKey()`, `updateStateByKey()`, `window()`
  - RDD 또는 DataFrame을 그룹화 하거나 윈도우 연산(Window Operations)을 수행 시 >> 스트리밍 데이터 처리 시 많이 활용됨
  - `OutputMode('complete')` 불가
  - 실시간 사용자 수, 누적 방문 수 , 세션 유지나 상태기반 처리, 실시간 트래픽 모니터링와 같은 케이스


### 2. Window operation
spark streaming에서는 데이터를 윈도우 단위로 sliding 해가며 transfomation할 수 있는 windowed computations를 제공
- Window operation 종류 : tumbling window, sliding window, session window

![streaming_window.png](..%2Fimage%2Fstreaming_window.png)


#### 1) Tumbling Window
- 고정된 크기의 시간 간격으로 데이터를 나눠 처리
- 각 윈도우는 서로 독립적이며 중복 없이 순차적으로 배치됨
- create time과 processing time이 다른 데이터가 순서도 뒤죽박죽으로 Kafka에 입력되어도, event time 기반으로 결과 출력됨
- 배치 단위로 데이터를 그룹화 할 때 사용

```python
window_duration = "5 minutes"
window_agg_df = tf_df \
                .groupBy(window(col("create_date"), window_duration)) \
                .sum("amount").withColumnRenamed("sum(amount)", "total_amount")

# Start running the query that prints the running counts to the console
query = window_agg_df \
        .writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="5 seconds") \
        .start()
```


#### 2) Sliding Window
- tumbling window와 마찬가지로 고정된 크기의 window를 가지지만 slide의 지속 시간이 window 지속 시간보다 작으면 window가 겹칠 수 있음
  - 동일한 데이터가 여러 window에 포함될 수 있지만, spark는 중복 처리가 가능하며 변화하는 데이터에 강점이 있음
- Window Size와 Slide Interval은 별도 지정 가능
- 이전 데이터를 일부 유지하면서 새로운 데이터를 포함하는 구조이기 때문에, 가장 최근의 변화를 반영하는 데 적합

```python
window_duration = "10 minutes"
sliding_duration = "5 minutes"
window_agg_df = tf_df \
                .groupBy(window(col("create_date"), window_duration, sliding_duration)) \
                .sum("amount").withColumnRenamed("sum(amount)", "total_amount")
```

#### 3) Session Window
- Window size가 고정된 시간 간격이 아닌, 사용자 행동(activity)의 "빈도"와 "간격"에 따라 동적으로 크기가 변함
  - 어떤 입력이 들어오면 시작되며, 그 이후 지정된 간격(gap duration) 내에 다음 입력이 들어오면 윈도우가 확장
  - 가장 마지막 입력 이후, gap duration 내에 새로운 입력이 없으면 session window는 닫힘
- 웹 세션처럼 연속적인 이벤트 사이에 일정 시간이 지나면 새 세션으로 간주해야 하는 시나리오에서 유용
- Spark 3.2 이상에서 지원
- `session_window(timeColumn, gapDuration)`
  - `timeColumn` : window를 계산할 기준 열 이름 또는 열
  - `gapDuration` : 세션 시간 초과를 지정하는 python 문자열/열. 정적 값이거나 동적으로 지정하는 표현식

```python
# session window
windowedCountsDF = \
  eventsDF \
    .withWatermark("eventTime", "10 minutes") \
    .groupBy("deviceId", session_window("eventTime", "5 minutes")) \
    .count()
```

### 3. Watermark

#### 1) Event Time & Processing Time
- Event Time : 데이터 내에 존재하는 데이터 발생 시간 (주로 이벤트가 발생한 시간)
- Processing Time : 스트림 처리 엔진에서 데이터를 처리하는 시간 (해당 데이터를 받아 처리한 서버 시간)

#### 2) 데이터 지연
- 실시간 스트리밍 데이터 처리 시 event time과 processing time 사이에 지연 발생
- 데이터 처리 엔진은 언제 윈도우 집계를 종료하고 결과 출력할지 결정하는 매커니즘 필요 : **Watermark**
  - 12:04에 생성된 이벤트가 12:11에 스파크 스트리밍에 수신된 경우 [12:00, 12:10] 윈도우에 업데이트 필요 -> But! 지연 이벤트를 무기한 기다릴 수는 없기에 임계값을 설정하고 임계값 넘는 이벤트는 버리는 방식으로 상태를 제어하는 매커니즘 필요 

#### 3) Watermark
- 지연 이벤트를 수용하기 위해 window의 state를 관리하는 기법 (state가 무한정 커지지 못하도록 제어)
- Spark는 최대 이벤트 시간이 지정된 워터마크보다 큰 경우에만 윈도우 집계를 종료하고 출력
  - `(max event time seen by the engine - late threshold > T)` 인 T 시간에 해당하는 지연 이벤트만 처리하도록 허용
- Outputmode에 따른 Watermark
  - `Complete` : 전체를 출력함에 따라 watermark는 무의미함
  - `Update` : `10:50 → 11:00 AM` 윈도우는 이벤트가 계속 들어오면 그때마다 이 윈도우에 대해 결과가 갱신되어 출력. 워터마크보다 오래된 윈도우는 state에서 제거되고, 더 이상 업데이트 안되며 이전 결과도 덮어쓰기가 가능해야 함에 따라 일부 sink에서는 사용 불가함
  - `Append`  : `10:50 AM → 11:00`AM 윈도우의 결과를 출력하려면, **지금까지 본 가장 늦은 이벤트 시간 - 10분**이 11:00 AM보다 커야 함. 출력 시점 전까지는 내부 state만 유지하고, 모든 집계가 완전히 끝난 윈도우만 출력

![streaming_watermakr_image.png](..%2Fimage%2Fstreaming_watermakr_image.png)


### 4. Join Operations
- Structured Streaming에서는 스트리밍 Dataframe과 정적(Static) Dataframe / 다른 스트리밍 Dataframe 사이의 조인을 지원
- Static 데이터 : 주로 참조 데이터

#### 1) Stream-Static Join
- Inner join 및 일부 outer join 지원 (left join)
```python
staticDf = spark.read. ...              # 정적 DataFrame
streamingDf = spark.readStream. ...     # 스트리밍 DataFrame

# inner join
streamingDf.join(staticDf, "type")
# left outer join
streamingDf.join(staticDf, "type", "left_outer")
```

#### 2) Stream-Stream Join
- 두 입력 스트림 모두 미완전한 데이터 상태이기 때문에 미래의 데이터와 현재의 데이터가 매칭될 가능성이 있어 조인이 복잡
  - 한 쪽 스트림에서 들어온 행이, 다른 쪽 스트림에서 미래에 들어올 데이터와 조인될 수 있음에 따라 과거 데이터를 state에 두고 이후 들어오는 데이터와 매칭 여부를 확인해서 결과를 생성
  - 워터마크(watermark)를 이용해 지연 데이터 처리 및 상태 크기를 제한 가능

- **Inner Join**
  - 어떤 컬럼, 어떤 조건으로도 inner join 가능함
  - 단, 아무 조건 없이는 모든 과거 상태 저장해야 함에 따라 State 무한히 커질 수 있음
  - 상태 제어 조건
    - 워터마크 설정
      - 각 스트림에 지연 허용 시간을 설정
    - 시간 범위 조인 조건 : 시간 관계를 명시
      - 시간 범위 조건 : `...JOIN ON leftTime BETWEEN rightTime AND rightTime + INTERVAL 1 HOUR`
      - 윈도우 조건 : `...JOIN ON leftWindow = rightWindow`

```python
from pyspark.sql.functions import expr

impressions = spark.readStream. ...
clicks = spark.readStream. ...

# Apply watermarks on event-time columns
impressionsWithWatermark = impressions.withWatermark("impressionTime", "2 hours")
clicksWithWatermark = clicks.withWatermark("clickTime", "3 hours")

# Join with event-time constraints
impressionsWithWatermark.join(
  clicksWithWatermark,
  expr("""
    clickAdId = impressionAdId AND
    clickTime >= impressionTime AND
    clickTime <= impressionTime + interval 1 hour
    """)
)
```

- **Outer Join** 
  - Watermark & Event time 조건 설정을 통한 상태 제어 필수
    - 매칭되지 않은 행에 대해 NULL 값을 반환해야 하는데, 어느 시점에 ‘이 행은 절대 매칭되지 않음’을 판단하여 NULL을 반환할지 결정 필요

```python
impressionsWithWatermark.join(
  clicksWithWatermark,
  expr("""
    clickAdId = impressionAdId AND
    clickTime >= impressionTime AND
    clickTime <= impressionTime + interval 1 hour
  """),
  "leftOuter"  # 또는 "rightOuter", "fullOuter", "leftSemi" 등
)
```

reference : 
- https://www.databricks.com/blog/feature-deep-dive-watermarking-apache-spark-structured-streaming


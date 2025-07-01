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


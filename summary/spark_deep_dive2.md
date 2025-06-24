## Spark Deep Dive (2)

---

### 1. Spark Cache
- Spark 연산 시 동일한 데이터에 대해 재연산을 수행하지 않도록 Executor 메모리 또는 디스크에 DataFrame(/RDD/Dataset)을 저장하는 메커니즘
- 캐시 사용 시, 연산 시 데이터셋이 메모리 내에 존재함이 보장됨에 따라 성능 향상 및 실패 시 비용을 줄일 수 있음
- cache 사용에는 `cache()` / `persist()` 가 있음

#### 1.1 cache() vs. persist()
- Spark에서 RDD, DataFrame, 또는 Dataset을 메모리나 디스크에 저장하여 재사용 가능하도록 만드는 방법
- 두 방식 모두 lazy evaluation 임에 따라, transformation 연산(`.filter()`, `.select()`, `.withColumn()`을 해도 즉시 실행되지 않고, action 연산(`.count()`, `.show()`, `.collect()`)이 호출될 떄 캐싱이 일어남.
- 차이
  - `cache()` : 단순히 memeroy에만 결과를 저장
  - `persist()` : 다양한 storage level에 결과를 저장 가능

```python
from pyspark import StorageLevel

df.cache()                            # 단순 캐싱

df.persist(StorageLevel.MEMORY_ONLY)  # 메모리에만 저장
df.persist(StorageLevel.MEMORY_AND_DISK)  # 메모리 부족하면 디스크 사용 (cache()와 동일)
df.persist(StorageLevel.DISK_ONLY)  # 디스크에만 저장
df.persist(StorageLevel.MEMORY_AND_DISK_SER)  # 직렬화해서 저장 (메모리 절약)
df.persist(StorageLevel.OFF_HEAP)  # off-heap 메모리에 저장 (rare)

df.unpersist()                        # 메모리/디스크에서 제거
```

#### 1.2 무조건 cache?!
- 큰 규모의 데이터 셋에서는 캐시 비용이 높아지므로 오히려 재연산을 하는 것이 나을 때 있음
- 추가로, 더 이상 쓰이지 않는 RDD라고 해서 자동으로 캐시가 해제되지 않음에 따라, unpersist 함수를 호출하거나 메모리 공간의 부족으로 축출(evict)되기 전까지는 메모리에 남아있게 되니 주의!
- spark는 executor가 메모리 부족을 겪을 때 뺄 파티션을 결정하기 위해 LRU(Least Recently Used) caching 방식을 사용하여 쓰인지 가장 오래된 데이터를 제거함.

### 2. Repartition & Coalesce
Spark 연산 과정에서 파티션이 과다하게 많거나 너무 적은 상황이 있을 수 있는데, 이때 repartition & coalesce 를 통해 파티션 개수를 조정할 수 있음.
파티션은 Spark의 분산 처리 단위로, 적절한 파티션 수를 유지하면 클러스터 자원을 효율적으로 사용할 수 있음.

#### 1) `repartition()`

- 데이터셋을 지정한 n개의 파티션으로 분산 가능 (파티션 수를 늘리거나 줄일 수 있음)
- shuffle이 발생함에 따라 비용이 높은 연산에 속함
  - 각 워커노드에 분산되어 존재하는 모든 파티션에 대해 셔플링이 일어나는, 전역적인 재배치 작업으로 비용이 많이 듬
  - hash 기반 파티셔닝으로 전 노드에 균일하게 배치되지 않을 수 있음 (비교적 균등분산)
- 메모리 이슈가 있거나, skewed 상황 시 reparition 작업 수행하면 개선될 수 있음!
- RDD, Dataframe 사용 가능

#### 2) `repartiotionByRange()`
- 해시값이 아닌 정렬된 범위 기반의 데이터 값(timestamp, numerical ranges)을 기반으로 파티셔닝
- shuffle 발생하며, dataframe에서만 사용 가능

#### 3) `coalesce()` 

- 파티션 수를 줄이기 위해 사용
- 가능한 셔플을 피하고, 데이터가 인접한 노드/파티션에 남아있도록 하여 상대적으로 연산 비용이 낮음.
- RDD, Dataframe 사용 가능

```python
#1-1.파티션 수를 2개가 되도록 리파티셔닝 작업 수행
df_repartitioned = df.repartition(2)

#1-2.파티션 수가 2개가 되도록 + age 컬럼값을 기준으로 파티셔닝을 진행한다.
df_repartitioned = df.repartitionByRange(2, "Age")

#2. 기존의 파티션 수가 2개 이상일 때, 2개로 줄인다.
df_coalesced = df.coalesce(2)
```  

### 3. SQL Hint
hint는 쿼리 상에서 조인이나 파티셔닝 등에 대해 사용자가 원하는 방식대로 쿼리를 수행하도록 지정해주는 문법

```python
#1. 작은 테이블 table2를 브로드캐스트하여 조인하도록 sql hint 구성한 코드 (broadcast)
df1.createOrReplaceTempView("table1")
df2.createOrReplaceTempView("table2")
spark.sql("SELECT /*+ BROADCAST(table2) */ * FROM table1 JOIN table2 ON table1.Name = table2.Name")

#2. repartition/coalesce 으로 파티션닝하도록 hint 구성한 코드
df.createOrReplaceTempView("table")
spark.sql("SELECT /*+ REPARTITION(3, c) */ * FROM table")

#3. soft merge join hint
df1.createOrReplaceTempView("table1")
df2.createOrReplaceTempView("table2")
spark.sql("SELECT /*+ MERGE(table1, tabl2) */ * FROM table1 JOIN table2 ON table1.Name = table2.Name")
```

### 4. 자료 카운팅하는 Accumulator
ETL 처리 과정에서, 누적 처리 데이터 양을 집계한다고 하자. Spark는 분산 데이터 처리함에 따라 각 executor의 처리된 결과를 조합한 공유변수가 필요하다!

**Accumulator**는 Spark의 공유 변수로 각 task에서 해당 공유변수를 접근할 수 있도록 드라이버 프로세스에서 각 워커노드로 accumulator를 전파한다.
각 task에서는 변수에 값을 더하거나 빼는 것이 가능하며(write-only, cannot read!), 이러한 변화가 드라이버 프로세스에 역전파된다. (드라이버만 해당 변수 read 가능)

- accumulator는 결합 및 가환 연산만을 지원 -> 즉 연산 순서를 바꾸어도 결과가 같음! (분산 컴퓨팅에서는 데이터의 작업 순서가 보장되지 않기 때문)
- action연산을 수행할 때에만 accumulator 값이 갱신된다.

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext

# accumulator 선언
even_count = sc.accumulator(0)

# RDD 예제 데이터
nums = sc.parallelize([1, 2, 3, 4, 5, 6])

# accumulator 사용
def count_even(x):
    if x % 2 == 0:
        even_count.add(1)
    return x

# map 연산에서 accumulator 사용 >> map()만 호출 시 누적X / collect()까지 해야함!
nums.map(count_even).collect()

# 결과 확인 (드라이버에서만 읽을 수 있음)
print("짝수 개수:", even_count.value)  # 출력: 짝수 개수: 3
```

### 5. Speculative Execution

- Spark가 분산 처리 시 특정 노드에 배치된 task가 오래 걸려 전체 작업이 지연될 수 있는데, 이 때 전체 작업 속도 향상을 위해 해당 task를 복제하여 다른 노드에 두어 병렬 수행 시키는 것
- 다른 노드의 복제 task와 원 task 중 먼저 완료되는 task를 성공처리하고 다른 시도는 종료됨
- 클러스터 내 노드 간 성능 차이가 있는 경우 speculative execution 기능을 활성화하는 것을 추천

```python
spark.speculation true                  # (default = false)
spark.speculation.interval 500ms        # Spark가 speculate 할 작업 확인하는 빈도
spark.speculation.multiplier 1.5        # task 실행시간의 중앙값보다 얼마나 느릴 때 speculative execution을 고려해야 하는지
spark.speculation.quantile 0.9          # speculation이 활성화 되기 전 완료되어야 하는 task 비율
```

- 장점
  - 일부 작업으로 인해 전체 task execution이 길어지는 경우 해당 기능 활성화 시 실행시간 줄일 수 있음
  - 특정 vm이 비정상적이거나 task 실행하기에 비적합한 노드가 클러스터에 존재하는 경우 완화 가능

- 단점
  - Prod 작업에서 장기간 사용 시 task 실패 확률 높아질 수 있음
  - skewed data로 인해 오래 걸리는 task의 경우 speculative exectuion이 원래 작업보다 먼저 완료된다는 보장 X

### 6. Job Scheduling
application이 제출되었을 때 내부 job들이 어떤 기준으로 수행순서가 정해지는지, application 자체가 여러 개가 수행될 때는 이들 간의 수행 순서가 어떻게 되는지 파악

#### 1) 애플리케이션 간 스케줄링 : Dynamic Resource Allocation
- Spark application 다수 존재 시 어떻게 리소스를 관리할 것인지에 대한 스케줄링 (리소스 공유 단위 : Executor)
- 기본 방식은 정적 자원 할당으로, 먼저 들어온 App1 작업이 끝날때까지 App2에 리소스를 할당하지 않음
- 동적 자원 할당 방식의 경우, App1의 작업 중 하나의 stage가 끝났을 때 릴리즈 된 리소스가 있다면 App2에서 사용 가능하며 추후 리로스 더 필요할 경우 cluster에 추가 요청 (사용량에 따라 Executor 수 조정)
  - 활성화 조건 : `spark.dynamicAllocation.enabled = true`
    - `spark.shuffle.service.enabled` = true (외부 shuffle 서비스)
    - `spark.dynamicAllocation.shuffleTracking.enabled` = true
    - `spark.decommission.enabled` = true 및 관련 설정

#### 2) 애플리케이션 내 스케줄링 : Spark Job Scheduler
- application 하나에 대해 내부 job들에 대한 스케줄링 (리소스 공유 단위: CPU cores)
- 기본적으로는 하나의 application 내 각 job들이 수행될 때 순서대로 수행되는 FIFO 방식을 사용한다. 
- 하지만 각 job들이 순서대로 수행되는 것이 아니라, 아래 설정을 통해 쓰레드들을 통해 병렬적으로 수행될 수 있도록 설정 가능 : **Fair Scheduler Pools**
  - Fair Scheduler Pools : spark job은 스케줄링 pool 안에서 동작하는데 이 pool의 job 실행방식을 "fair"로 설정해서 해당 pool을 스케줄러로 사용할 때 여러 job들이 병렬적으로 돌아가게 하는 것
    - 여러 Job들을 pool이라는 단위로 그룹화하여, 각 그룹마다 자원 분배 전략을 설정할 수 있음
      - `schedulingMode`: `FIFO` 또는 `FAIR` 
      - `weight`: 자원 우선 순위 
      - `minShare`: 최소한 확보해야 하는 코어 수

```shell
spark-submit \
  --master yarn \
  --deploy-mode cluster \
  --conf spark.scheduler.mode=FAIR \
  --conf spark.scheduler.allocation.file=/etc/spark/conf/fairscheduler.xml \
  --conf spark.executor.instances=5 \
  --conf spark.executor.memory=4g \
  --conf spark.executor.cores=2 \
  --class com.example.MyApp \
  my-app.jar
```

- --conf spark.scheduler.mode=FAIR: Spark의 스케줄러 모드를 지정하는 옵션으로 이 경우에는 FAIR 스케줄링 모드를 사용함.
- --conf spark.scheduler.allocation.file=/path/to/scheduling-pool.xml: 스케줄러 할당 파일의 경로를 지정하는 옵션으로 이 파일에는 스케줄링 pool의 정의가 포함되어 있음.


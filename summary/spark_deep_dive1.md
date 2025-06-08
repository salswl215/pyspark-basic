## Spark Deep Dive (1)

---

### 1. Spark Memory
- executor에서 사용하는 메모리는 실제 spark 작업이 실행되는 jvm에 관련된 메모리와 그렇지 않은 메모리로 구분된다. 참고로 spark-submit 시 설정하는 --executor-memory은 아래 on-heap memory를 의미한다.


- **jvm 내부 메모리(on-heap memory)**: 각 메모리는 설정값을 통해 메모리 비중을 설정할 수 있다.
  - reserved memory: spark에 예약된 메모리로, 300MB 정도 여유분을 가진다.
  - execution memory(operations): task 실행에 필요한 데이터가 저장되는 메모리
  - storage memory(caching): 데이터 캐시, 브로드캐스트에 사용되는 메모리
  - user memory: udf, spark 메타데이터, RDD Lineage 등이 저장되는 메모리

- **jvm 외부 메모리(off-heap memory)**
  - offheap memory: 네트워크 버퍼(셔플링, 원격 저장소의 파티션 읽기), 프로젝트 텅스텐, spark memory가 부족할 때 빌려 사용하기도 함.
  - external process memory: sparkR, pythonR 등에 사용되는데 거의 알 필요 없음.


- **통합 메모리 관리**
  - Spark 1.6부터는 통합 메모리 관리자가 도입되어 Dynamic 메모리를 할당
  - Storage와 Execution이 공유하는 통합 메모리 컨테이너에서 메모리가 할당되며, 사용되지 않는 Execution 메모리는 Storage 메모리로 사용될 수 있다.
  - 즉, 메모리 공간을 유연하게 사용할 수 있어 이전의 문제점(잦은 disk spill과 이로 인한 재연산)이 개선

### 2. Spark Join Strategy

#### 1) Broadcast Hash Join

- 작은 테이블을 모든 워커 노드에 브로드캐스트(복사)하여 큰 테이블과 해시 기반 조인을 수행.
- 어떤 shuffle도 발생하지 않아 Spark가 제공하는 가장 쉽고 빠른 join
- 조건: 작은 테이블이 spark.sql.autoBroadcastJoinThreshold보다 작을 경우 (기본값: 10MB)

#### 2) Shuffle Hash Join

- 두 테이블을 조인 키 기준으로 셔플(shuffle)하여, 해시 테이블을 생성하고 조인을 수행
- shuffle : 데이터를 파티션 간에 재분배 하는 과정 >> 파티션 재배치 + 네트워크 전송
- 네트워크 셔플과 디스크 I/O 발생
  - 데이터가 여러 노드에 분산되어 있으므로, 조인 전에 데이터를 모아야 하는데, 네트워크를 통해 다른 노드로 데이터를 전송하게 되다 보니 네트워크 I/O 발생하게 됨

#### 3) Sort-Merge Join

- 양쪽 테이블을 조인 키 기준으로 정렬한 후 병합(Merge)하여 조인을 수행
  - shuffle 작업 후 동일한 key를 가진 record가 동일한 partition에 위치
  - sort : 각 데이터를 조인 키에 따라 정렬 후 각 dataset에서 key 순서대로 데이터를 순회하며 key 일치하는 row끼리 조인
- 대용량 데이터 조인 시 가장 많이 사용되는 기본 조인 전략

#### 4) Shuffled Hash Join

- AQE(Adaptive Query Execution)가 활성화된 경우, 실행 중 셔플 후 작은 쪽을 자동으로 해시 테이블로 선택하여 효율적으로 조인을 수행
- 실행 중 통계 수집에 따라 판단하며, shuffle은 하되 해시 빌드를 최적으로 구성한다. AQE가 활성화된 Spark 3.0 이상에서만 사용 가능

#### 5) SCartesian Product Join

- 조인 조건이 없는 경우 모든 레코드를 곱집합 형태로 조인합니다. 매우 비효율적이며, 조인 조건 누락 시 의도치 않게 발생 가능
- 매우 느림


### 3. Dynamic Partition Pruning (DPP)

#### 1) 배경

- 일반적인 데이터 시스템에서의 pruning이란 얻고자 하는 데이터를 가지고 있지 않은 파일들을 읽지 않고 스킵하는 형태의 최적화를 말한다. spark 외에도 mysql 등 여러 쿼리 엔진들은 내부적으로 *predicate pushdown을 사용해 효율적으로 데이터를 로드한다.
  - *predicate pushdown : 쿼리에 필터 조건이 있을 때 데이터 소스(disk)에서부터 애초에 필터 조건에 해당하는 데이터만 읽어서 I/O cost를 줄이는 방식
- 일단 쿼리의 경우 Static Partiton Pruning(SPP)로도 충분하다. 다만. 아래의 쿼리처럼 join 추가된 경우 다르다.
  - where date 테이블에 대해서는 진행된 SPP가 join으로 이어지는 sales에는 적용되지 못한다.
  - Fact-Dimension 테이블 간의 조인의 경우 Fact 테이블의 크기가 보통 매우 크기 때문에 fact 테이블의 SPP 부재는 많은 양의 데이터 스캔을 발생시킨다.
  - fact 테이블에도 predicate pushdown이 적용될 수 있도록 spark에 지원하는 기술이 DPP이다.

```sql
select * from sales
join date on sales.day = date.day
where date.day_of_week='Mon'
```

![spark_execution.png](..%2Fimage%2Fdpp_exp.png)

#### 2) DPP 원리
- Dimension에 적용된 필터 조건을 fact table scan 전에 동일하게 적용하여 predicate pushdown을 구현 (dimension 테이블의 필터를 fact table 쿼리의 서브쿼리로 inject)
- 하지만 이 방식의 문제점은, 빨간색 점선 박스 내 로직(scan 후 filtering)이 그림처럼 2번 중복 계산되어야 한다는 점이다. 
- 이 때, 브로드캐스트 해시조인 방식을 채택하여 filter scan된 dimension 테이블은 해시 테이블로 생성되어 브로드캐스트 되고, 이를 fact table scan 전 필터로 적용하여 중복 쿼리 문제를 해결 가능하다.

![spark_execution.png](..%2Fimage%2Fdpp_exp3.png)



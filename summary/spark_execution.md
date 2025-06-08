## Spark 동작

![spark_execution.png](..%2Fimage%2Fspark_execution.png)

1. DataFrame이나 SQL Query 등의 코드를 제출
2. 코드가 유효할 경우, 코드를 Logical Plan으로 변환
3. Catalyst Optimizer를 통해서 최적화과정을 진행
4. 최적화된 Logical Plan이 어떻게 클러스터에서 실행될지 정의하는 Physical Plan이 생성
5. 위의 단계가 완료되면 Spark는 Physical Plan을 실행/처리하고, 모든 계산을 수행하여 출력

### 1. Logical Plan
- Logical Plan은 수행하는 모든 연산 단계에 대한 추상화이며, 클러스터의 Driver(마스터 노드) 또는 Executor(작업자 노드)에 대해 아무것도 참조하지 않는다.
- Spark Context는 Logical Plan을 생성/저장하고, 이는 사용자의 표현을 최적화된 버전으로 변환한다.

#### 1) unresolved logical plan(검증 전 논리적 실행 계획)
- 코드의 유효성과 테이블이나 컬럼의 존재 여부만을 판단하는 과정, 실행 계획은 검증되지 않은 상태
- spark analyzer는 컬럼과 테이블을 검증하기 위해 Catalog, 모든 테이블의 저장소 그리고 Dataframe 정보를 활용
- 필요한 테이블이나 컬럼이 Catalog에 없다면 검증 전 논리적 실행 계획이 만들어지지 않음

#### 2) logical plan(검증된 논리적 실행 계획)
- 코드의 유효성과 구문상의 검증이 완료되어 논리적 실행 계획 생성
- 테이블과 컬럼에 대한 결과는 Catalyst Optimizer로 전달

#### 3) optimized logical plan(최적화된 논리적 실행 계획)
- 검증된 논리적 실행계획을 Catalyst Optimizer로 전달하여 조건절 푸쉬 다운이나 선택절 구문을 이용해 논리적 실행 계획을 최적화함

### 2. Physical Plan

#### 1) physical plans(물리적 실행계획들)
- 최적화된 논리적 실행 계획을 가지고 하나 이상의 물리적 실행 계획을 생성
- 다양한 물리적 실행 전략을 생성(각 operator별 어떤 알고리즘을 적용할 지)하고 비용 모델을 이용해서 비교한 후 최적의 전략을 선택

#### 2) Cost Model(비용 모델)
- 가장 좋은 plan은 비용기반 모델(모델의 비용을 비교)
- 테이블의 크기나 파티션 수 등 물리적 속성을 고려해 지정된 조인 연산 수행에 필요한 비용을 계산하고 비교함

#### 3) Selected Physical Plan(최적의 물리적 실행 계획)
- 위의 비용 모델에 따라 최적의 물리적 실행 계획을 도출

#### 4) 물리적 실행 계획은 일련의 RDD와 transformation으로 변환(컴파일)
- 각 머신에서 실행 될 Java Bytecode를 생성함(compile)

### 3. Example : Logical vs. Physical Plan
```sql
select * from employees where age > 30
```
#### Logical Plan (Analyzed and Optimized)
- Read Employees table
- Filter rows where age > 30
- Project the name column

#### Physical Plan
- Use a Parquet scan to read the employees table
- Apply predicate pushdown to filter age > 30
- Prune columns to read only the name column
- Use executor tasks to process partitions in parallel

### 4. Adaptive Query Execution (Adaptive Plan)

#### 1) 배경
Spark 3.0 버전에서 동적으로 최적화해주는 프레임워크인 Adaptive Query Execution(AQE)가 처음 등장
코스트 기반 최적화(Cost-Based Optimization, CBO)의 경우, 올바른 Join 타입을 선택하거나 다자간 조인에서 Join의 순서를 조정하는 방식으로 최적화를 실행할 수 있습니다. 
하지만, CBO에는 통계치 수집에 많은 비용이 들어가고, 통계치가 오래된 경우에는 예측이 부정확해져 최적화가 되지 않는 등의 한계가 있었습니다. 
이러한 한계를 극복하고자 등장한 것이 AQE(Adaptive Query Execution)로, AQE는 런타임 시 발생하는 다양한 통계치를 수집해 성능 개선을 가능하게 합니다.

(출처: https://databricks.com/blog/2020/05/29/adaptive-query-execution-speeding-up-spark-sql-at-runtime.html)

#### 2) AQE?
- 데이터의 row 수, distinct value의 수 와 같은 통계치를 가지고 데이터 작업 중(런타임)에 최적화하는 기능
- 크게 2가지의 최적화 기법이 존재하며, partition의 수를 줄여주는 기능과 최적화된 Join을 적용하는 기능
- 너무 많은 partition은 많은 task가 필요하거나 I/O를 많이 유발할 수 있기 때문에 적절한 수가 필요한데, AQE 기능이 적절한 partition의 수를 지정해줌

```sql
select * from large_table join small_table on id
```
**Without AQE** : shuffle join 이용

**With AQE** : small_table 사이즈 확인 후 broadcast join 진행

#### 3) AQE 기능
- **Dynamically coalescing shuffle partitions**

  - 작은 크기의 파티션이 있다면 그것들을 하나의 파티션으로 합쳐서 결과적으로 각 파티션들의 크기를 비슷하게 만들어, 무분별하게 파티션 개수가 많아지는 것을 방지하는 기능.
  - spark의 분산 작업 특성 상, 셔플링은 처리속도가 매우 느린 연산이기 때문에 최적화에 있어서 중요한 대상
    - 만약 셔플 후 파티션 개수가 적다면 파티션 당 데이터 크기가 커져서, executor의 메모리 크기를 벗어나 OOM(Out Of Memory)를 발생시키거나 디스크를 이용한 연산이 수행되고 또한 분산 처리를 그만큼 활용하지 못해 성능이 떨어진다. 
    - 반대로 셔플 후 파티션 개수가 많다면 많은 수의 파티션에 대한 메타데이터를 보관하는 스파크 드라이버에 부하가 늘어나고 파티션 스케줄리링에 대한 비용이 커진다. 무엇보다 작은 크기의 파티션들을 셔플링 시 네트워크 I/O 비용이 커져(원인: 네트워크 패킷 헤더 overhead 등) 성능이 떨어진다. 
    - 따라서, 적절한 파티션 개수를 설정하여 셔플링하는 것이 중요


- **Dynamically coalescing shuffle partitions**

  - AQE가 셔플링을 통해 알게 된 조인 테이블의 데이터 사이즈가 spark.sql.autoBroadcastJoinThreshold(default 10MB)에 설정된 값보다 작다면, 이를 브로드캐스트 해시 조인 전략으로 변경한다. 
  - Broadcast Join은 작은 테이블만 브로드캐스트하므로 shuffle이 없어져 성능 개선 & 디스크 I/O, CPU 자원 절약
  - **브로드캐스트 해시 조인** : 한쪽 테이블이 충분히 작은 경우, 해당 테이블을 모든 워커 노드에 브로드캐스트(전파)하여, 큰 테이블과 해시조인을 수행.


- **Dynamically optimizing skew joins**

  - AQE는 큰 사이즈의 파티션을 자동으로 서브 파티셔닝 해주어 조인 처리 속도를 높이고 executor의 메모리 낭비를 줄인다.
  - join 시 파티션 하나에 데이터가 많이 몰려있는 상황을 skew라 하는데, 데이터 사이즈가 커 조인 시 오래 걸리거나 데이터 처리 과정에서 disk spill/OOM 발생이 가능하다. 

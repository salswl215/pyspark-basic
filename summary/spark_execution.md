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
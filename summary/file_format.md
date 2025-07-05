## File Formats & Apache Iceberg

### 1. File Format

#### 1) Json
- 데이터 교환을 위해 사람이 읽기 편한 텍스트 기반 형식
- 장점
  - 가독성이 좋음
  - 대부분의 프로그래밍 언어 ㅁ치 데이터 처리에서 사용 가능
  - 객체, 배열, 문자열, 숫자, 불리언, null 등 다양한 데이터 타입 표현 가능한 점에서 구조적으로 유연함
- 단점
  - 빅데이터를 처리에 부적합 (텍스트 기반 및 부족한 compression -> 큰 storage 필요)
  - binary 포맷에 비해 느린 파싱 속도
  - built-in 스키마가 부재함에 따라 잘못된 데이터 입력이 가능하며, 검증도 어려움


#### 2) Apache Parquet
- 컬럼 기반의 저장 포맷으로 대용량 데이터 처리 시 적합
- OLAP (분석 위주 쿼리)에 적합


- 장점
  - 컬럼 단위의 데이터 압축 및 저장으로 대용량 파일 Scan에 효율적
  - 다양한 압축 기법과 인코딩 형태를 지원 (Column 단위로 데이터가 균일하여 높은 압축률)
  - 쿼리 시 필요한 컬럼만 읽기 때문에 효율적인 쿼리 수행 및 Disk I/O 가 적음
  - Schema가 포함되어 있어, 메타데이터 관리가 편하며 데이터 무결성 보존 가능 (스키마 변경 지원)
- 단점
  - Parquet 파일 쓰기 작업 시 컴퓨팅 리소스 더 필요
  - 대용량 데이터를 한꺼번에 처리할 때 최적화되어 있어, 작은 파일 여러 개일 경우 성능 하락 가능
  - 컬럼 단위 접근이 어렵고, 분석용 쿼리에는 부적합 (binary 형태)


- 파일 구조
  - Header - 하나 이상의 Blocks - Footer
  - Header : parquet 포맷의 파일임을 알려주는 매직 숫자 포함
  - Footer : 포맷 버전, 스키마, 추가 key-value 쌍, 모든 블록에 대한 메타 데이터 저장
  - Blocks : 행 그룹을 저장하며, 행 그룹은 행에 대한 컬럼 데이터를 포함한 청크데이터로 구성됨. 각 컬럼 청크들은 페이지에 기록되는데, 페이지에는 동일한 컬럼값만 포함하고 있어 압축할 때 유리함
![parquet.png](..%2Fimage%2Fparquet.png)

#### 3) Apache Avro
- Row 기반 스토리지 형식으로 json 데이터로 스키마를 지정하고 이 스키마 파일을 통해 serialize/deserialize 를 진행
  - serialize: 메모리에 있는 객체나 데이터를 바이트 형태로 바꿔서 저장/전송 가능하게 만드는 과정 
  - deserialize: 직렬화된 데이터를 다시 원래 구조로 복원하는 과정
- OLTP (쓰기/읽기 빈번한 트랜잭션 처리)에 적합


- 장점
  - 스키마 정보를 바탕으로 구조를 이해하므로, 불필요한 데이터 탐색 없이 빠르게 읽고 씀 >> 실시간 시스템에서 주로 쓰임
  - 스키마 정보를 함께 가지고 있어서, 어떤 구조인지 몰라도 데이터 복원이 가능함
  - 스키마 기반 이진 포맷이라, JSON처럼 텍스트 처리 없이 바로 바이너리 형태로 변환 가능
  - Schema evolution을 지원 (스키마 변경에도 기존 데이터와의 호환성 유지)
- 단점
  - binary 형태임에 따라 사람이 읽기 어려움 (디버깅 불편)
  - 컬럼 기반 분석에 부적합
  - 스키마 정의 필요하며 관리 필요


- 파일 구조
  - Header - 하나 이상의 Data Blocksㅁㅍ
  - Header : Avro 포맷임을 알려주는 매직 바이트 & 스키마 정보(json 형식), 싱크 마커, codec(압축) 등 메타정보 포함
  - Data Blocks : 실제 로우 데이터가 직렬화된 형태로 저장
![avro.png](..%2Fimage%2Favro.png)


#### 4) ORC
- 컬럼 기반의 파일 포맷으로 Hive 빅데이터 처리 프레임워크에 최적화되어 있음
- 장점
  - Parquet 보다 나은 압축률 제공
  - 컬럼 기반 파일 포맷임에 따라 효율적인 쿼리 수행
  - Scheam Evolution 지원
- 단점
  - ORC 파일 쓰기시 높은 컴퓨팅 자원 필요
  - binary 포맷임에 따라 읽을 수 없음



### 2. File Compression 
- File Compression : 데이터의 크기를 줄여 저장, 전송, 데이터 처리 시 이점을 줌

#### 1) Gzip
- 일반적으로 많이 쓰이는 compression algorithm으로 csv, json과 같은 텍스트 기반 포맷에 쓰임
- 장점 : 높은 압축률, 범용성
- 단점 : 느림, Not splittable

#### 2) Snappy
- Google에서 개발된 compression algorithm으로, 대용량 데이터 처리 시 속도 및 효율성 측면에서 널리 사용됨
- 장점 : 압축/압축해제 속도가 빠름 (압축률보다는 속도에 집중), Parquet/Avro와 같이 쓰이는 경우 많음
- 단점 : Gzip에 비해 낮은 압축률

#### 3) Splittability?
- 분산 시스템에서 대용량 파일을 여러 노드에서 나눠 처리하려면 파일의 중간에서 읽을 수 있어야 함
- Gzip처럼 전체가 하나의 압축 스트림이면, 처리하려면 파일 전체를 한 노드가 읽어야 하므로 병목 발생
- 반면 Snappy + Parquet/Avro처럼 block 구조를 쓰면, 각 블록을 다른 워커가 병렬 처리 가능


### 3. Apache Iceberg
- 데이터 레이크에 저장된 대규모 분석 데이터 세트를 위해 설계된 오픈 소스 테이블 형식


#### 1) Apache Iceberg 특징
- Schema Evolution
  - 데이터 구조 변경 시 레코드 단위로 수정/삭제 가능 >> 대량의 데이터 rewirte 불필요
- Partitioning & Data Layout
  - hideen partition : Iceberg가 자동으로 최적화된 파티션 처리를 수행. 스키마와 분리된 파티션 메타데이터 관리
  - parition layout : 다양한 변환 함수 기반의 파티션 전략을 지원하여 스키마 변경과 무관하게 파티션 관리 가능 (ex. 시간 컬럼에 대해 다양한 변환(day, month, hour) 적용 가능)
- Snapshot Isolation
  - 데이터를 읽는 작업은 특정 시점의 스냅샷 기준으로만 처리함에 따라 ACID를 보장
  - 모든 변경이 immutable metadata snapshot으로 저장됨에 따라 읽는 중간에 쓰기가 일어나도 영향 X
- Incremental & Time-Travel 쿼리
  - Time Travel 기능을 지원하여, 데이터 시간에 따른 변경 내역 추적 가능하여 이전 버전의 데이터 쿼리/롤백 가능
  - 실시간 데이터 변경 처리하여 스트리밍 데이어 수집/분석 작업에 적합
- Multi-Engine Support
  - Apache Spark, Trino, Flink, Hive, Presto 등 다양한 엔진/프레임워크에서 사용 가능

#### 2) Apache Iceberg 구성 요소
![iceberg.png](..%2Fimage%2Ficeberg.png)

- **Catalog Layer**
  - Metadata pointer로서 가장 최신 시점의 테이블 상태, 즉 최신 metadata file의 위치 정보 지님

- **Metadata Layer**
  - 테이블 변경이 일어나는 시점마다 metadata file 및 snapshot을 만듬
  - metadata file
    - 테이블 형상은 metadata file로 관리되며, 테이블 변경이 일어나면 새로운 metadata file이 생기고 기존의 것을 대체하게 됨
    - 테이블 스키마, 파티셔닝, 스냅샷 세부정보 등이 포함
      - 스냅샷(snapshot) 기능을 통해 특정 시점의 테이블 형상을 파일로 기록함 (time-travel 가능하게함!)
  - manifest list
    - 스냅샷(=특정 시점의 테이블 형상)은 하나의 manifest list를 참조하며, manifest list는 하나 이상의 manifest file에 대한 메타정보 저장
    - manifest list를 통해 스냅샷과 연관된 manifest file 위치 찾음 (manifest file 관리하는 것이 list)
  - manifest file
    - 스냅샷에 포함된 data file에 대한 모든 정보(data file 위치, 파티션 정보)와 통계 정보(null/nan 개수 등) 기록됨


- **Data Layer**
  - 실제 데이터를 저장하는 곳으로, 테이블에 정의된 파일 포맷(ex. parquet, orc...) 형식으로 데이터 파일을 저장
  - manitest file의 메타 정보를 이용하여 필요 데이터 파일에 접근 가능
  - V2 부터는 변경사항이 있을 때 기존의 data file에 변경사항을 반영 후 새로 write하는 (Copy-On-Write) 방식과 데이터 변경 분만 새로 write하여 기존 data file과 병합하여 읽는(Merged-On-Read) 방식 모두 지원


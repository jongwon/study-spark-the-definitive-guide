# 5. Structured API 기본 연산

Structured API 는 DataFrame을 기준으로 설명한다. (Dataset은 Java/scala 에서 클래스를 정의해서 읽어들인 경우에만 작용되며 데이터 타입이 있는 StructField 의 StructType 이라고 봐야 한다.)

## 5.1 스키마

DataFrame 의 칼럼명과 데이터 타입을 정의한다.

스키마는 어떻게 정의되는가?

- 소스 데이터를 읽어서 추측해 낸다.
- 직접 데이터 타입(StructType) 을 정의한 후 해당 타입으로 데이터를 읽어낸다.

```scala
spark.read.format("json").load("/data/flight-data/json/2015-summary.json").schema

>>> output
org.apache.spark.sql.types.StructType = StructType(
    StructField(DEST_COUNTRY_NAME,StringType,true),
    StructField(ORIGIN_COUNTRY_NAME,StringType,true),
    StructField(count,LongType,true)
)
```

- 스키마는 StructField 를 리스트로 갖는 StructType 이다.
- DataFrame 에서 읽어들일 데이터 타입을 미리 정의해서 읽어낼려면 StructType 을 정의하고 해당 스키마를 지정한 다음 데이터를 읽으면 된다.

  ```scala
  import org.apache.spark.sql.types.{StructField, StructType, StringType, IntegerType}
  import org.apache.spark.sql.types.Metadata

  val mySchema = StructType(Array(
        StructField("DEST_COUNTRY_NAME", StringType, true),
        StructField("ORIGIN_COUNTRY_NAME", StringType, true),
        StructField("count", IntegerType, true)
    ))

  val df = spark.read.format("json").schema(mySchema).load("/data/flight-data/json/2015-summary.json")

  >>> output
  mySchema: org.apache.spark.sql.types.StructType = StructType(
    StructField(DEST_COUNTRY_NAME,StringType,true),
    StructField(ORIGIN_COUNTRY_NAME,StringType,true),
    StructField(count,IntegerType,true)
  )

  ```

- 위와 같이 count의 데이터 타입을 LongType이 아닌 IntegerType 으로 정의하면 데이터를 IntType 으로 받아낸다.

## 5.2 칼럼과 표현식

- 칼럼

  - DataFrame은 여러 칼럼이 Row 형식으로 있다.
  - 직접 생성하려면 Seq 함수를 사용한다.
  - 생성: col 이나 column 함수, \$ 혹은 틱 마크(')를 사용한다.
  - 표현식을 이용해 칼럼을 선택, 조작, 제거하는 등의 작업을 할 수 있다.
  - 칼럼은 DataFrame 에서 특정 칼럼을 참조하거나 조인을 하기위해 필요하다.

    ```scala
    import org.apache.spark.sql.functions.{col, column}

    col("이름")
    column("전화번호")
    $생년월일
    '단위
    ```

- 표현식
  - DataFrame 레코드의 여러 값에 대한 트렌스포메이션 대상이 되는 집합
  - expr 함수를 사용
    ```
    expr("이름") == col("이름")
    ```
  - 칼럼도 표현식의 일종이며
  - 칼럼의 트렌스포메이션은 파싱된 표현식과 동일한 논리적 실행계획으로 컴파일 된다.
  - SQL 구문과 스칼라 코드 조각이 동일하게 동작하는 이유는 동일한 논리적 실행계획으로 컴파일 되기 때문이다.
  -

## 5.3 Record와 Row

- DataFrame의 한 줄을 레코드라고 한다.
- DataFrame의 레코드의 데이터 타입은 Row 이다.
- Dataset의 레코드(Java, scala만 지원)는 case class 이거나 JavaBean 이다.
- ```
  df.first() 는 첫번째 Row 객체를 반환한다. / 하나의 레코드를 반환한다.
  ```
- Row 객체는 스카마 정보가 없다. Row를 소유하는 DataFrame이 스카마 정보를 갖는다.

## 5.4 DataFrame

#### 생성

- DataFrame 을 코드로 직접 생성하려면 spark 세션에서 createDataFrame을 활용할 수 있는데, Rdd 데이터와 StructType 객체를 넘겨야 한다.
  ```
  val df = spark.createDataFrame(rdd, StructType(Array(
    StructField("name1", StringType, true),
    ...
  )))
  ```
-

#### select / selectExpr

- DataFrame 에서 일부 컬럼만 떼어내어 새로운 DataFrame 을 만들어 내기 위해 select 함수를 이용한다.
- select 안에 expr 함수를 쓸 수 있고 이를 간단하게 selectExpr 메쏘드를 제공한다.
- as와 alias 구문을 사용할 수 있다.
- avg, count 함수를 쓸 수 있다.

#### 스파크 데이터 타입으로 변환

- lit 함수를 사용해 상수항 칼럼을 추가할 수 있다.
  ```
    df.select(expr("*"), lit(1).as("One"))
  ```

#### 칼럼 추가

- DataFrame 에 withColumn 메쏘드를 사용해 컬럼을 추가한다.
  ```
  df.withColumn("One", lit(1))
  df.withColumn("SameName", expr("Name == Word"))
  ```

#### 칼럼명 바꾸기

- withColumnRenamed 메쏘드로 컬럼 이름을 바꿀 수 있다.

#### 예약어

- 공백이나 - 등의 문자는 컬럼명에 사용할 수 없지만, 써야 할 경우 ` (백틱?)을 써주면 된다.

#### 대소문자 구분

- 스파크는 대소문자를 구분하지 않는다.
- 만약 구분하게 만들고 싶다면 설정해주어야 한다.
  ```
  set spark.sql.caseSensitive true
  ```

#### 칼럼 제거

- 칼럼을 삭제하는건 DataFrame 의 drop 메소드를 사용한다.

#### 타입 변경하기

- 데이터 타입 변경은 cast 메쏘드를 쓴다.

#### Row 필터링

- where 나 filter 메쏘드를 사용한다.

#### 고유한 Row 얻기

- distinct 함수를 사용하면 중복값을 걸러준다.

#### 무작위 샘플 만들기

- sample 메쏘드를 이용한다.
- 복원 추출과 비복원 추출을 설정할 수 있고 , 표본 데이터 추출 비율을 조절할 수 있다.(?)

#### 임의 분할

- randomSplit 메쏘드를 쓰면 임의 분할이 가능하다.

#### Row 합치기와 추가하기

- union 메쏘드를 쓴다.

#### Row 정렬하기

- orderBy 메쏘드를 쓴다. asc, desc 함수를 써서 정렬의 방향을 설정할 수 있다.
- 정렬의 방향으로 쓸 수 있는 함수
  - asc
  - desc
  - asc_nulls_first
  - asc_nulls_last
  - desc_nulls_first
  - desc_nulls_last

#### Row 수 제한

- limit 메쏘드를 쓴다.

#### repartition과 coalesce

- 파티션을 늘리거나 다시 만들 때 repartition 을 쓴다. 셔플이 발생한다.
- 파티션 수를 줄일 때는 coalesce 를 쓴다. 셔플 없이 진행된다.

#### 드라이버로 Row 데이터 수집하기

- 드라이버 프로그램은 스파크 애플리케이션의 상태 정보를 주로 저장하지만 데이터를 일부 확인하려면 드라이버로 데이터를 샘플링하거나 수집해야 한다.

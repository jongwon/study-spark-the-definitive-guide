# 9. 데이터 소스

- 이 장의 목표는 스파크의 핵심 데이터소스를 이용해 데이터를 읽고 쓰는 방법을 터득하는 것이다.

- 스파크에서 지원하는 핵심 데이터 소스
    - CSV
    - JSON
    - 파케이 (Parquet)
    - ORC
    - JDBC / ODBC 연결
    - 일반 텍스트 파일

- 커뮤니티에서 만든 중요 데이터 소스
    - 카산드라
    - HBase
    - 몽고디비
    - AWS Redshift
    - XML
    - 기타 수많은 데이터소스

## 9.1 데이터소스 API의 구조
- 9.1.x에서는 데이터소스 API의 전체적을 구조를 설명한다.

### 9.1.1 읽기 API 구조
- 데이터 읽기의 핵심 구조
    ```scala
    DataFrameReader.format(...).option("key", "value").schema(...).load()
    ```
- format() : 기본값 파케이("parquet")
- option() : 데이터를 읽는 방법에대한 옵션을 key/value로 설정
- schema() : 데이터를 읽을때 스키마를 지정할수도 있고 지정하지 않으면 자동추론 한다.
- 이외의 데이터 포맷별 추가 옵션이 존재한다.

### 9.1.2 데이터 읽기의 기초
- 스파크에서 데이터를 읽을 때는 기본적으로 DataFrameReader를 사용한다.

    ```scala
    spark.read
    ```

- DataFrameReader를 얻은 다음에 다음과 같은 값을 지정해야 한다.
    - 포맷
    - 스키마
    - 읽기 모드
    - 옵션

- 포맷, 스키마, 옵션은 트랜스포메이션을 추가로 정의할 수 있는 DataFrameReader를 반환한다.  
그리고 데이터를 읽을 경로를 지정해야 한다.
 
    ```scala
    spark.read.format("csv")
        .option("header", "true")
        .option("mode", "FAILFAST")
        .option("inferSchema", "true")
        .load("some/path/to/file.csv")
    ```

- 읽기 모드

    - 형식에 맞지않거나, 반정형 데이터소스를 다룰때 주로 사용한다.

    - 읽기 모드는 스파크가 형식에 맞지 않는 데이터를 만났을 때의 동작 방식을 지정하는 옵션이다.

    |  <center>읽기 모드</center>  | <center>설명</center>
    | :---------: | :------------------------------|
    |    permissive (기본값)    |  오류 레코드의 모든 필드를 null로 설정하고 모든 오류 레코드를 _corrupt_record라는 문자열 컬럼에 기록                    |
    |    dropMalformed    |  형식에 맞지 않는 레코드가 포함된 로우를 제거           |
    |    failFast    |  형식에 맞지 않는 레코드를 만나면 즉시 종료           |

### 9.1.3 쓰기 API 구조
- 데이터 쓰기의 핵심 구조
    ```scala
    DataFrameWriter.format(...).option(...).partitionBy(...).bucketBy(...).sortBy(...).save()
    ```

### 9.1.4 데이터 쓰기의 기초
- 데이터 쓰기는 데이터 읽기와 매우유사하며 DataFrameReader대신 DataFrameWriter를 사용한다.

- Dataset(DataFrame)의 write속성을 이용해 DataFrameWriter에 접근한다.
    ```scala
    dataFrame.write
    ```
- DataFrameWriter를 얻은 다음에는 포맷(format), 옵션(option) 그리고 저장(save) 모드를 지정해야 하며, 데이터가 저장될 경로를 반드시 입력해야 한다.
    ```scala
    dataframe.write.format("csv")
        .option("mode", "OVERWRITE")
        .option("dateFormat", "yyyy-MM-dd")
        .option("path", "path/to/file(s)")
        .save()
    ```
    |  <center>저장 모드</center>  | <center>설명</center>
    | :---------: | :------------------------------|
    |    append    |  해당 경로에 이미 존재하는 파일 목록에 결과 파일을 추가|
    |    overwrite    |  이미 존재하는 모든 데이터를 완전히 덮어쓴다    |
    |    errorIfExists  (기본값)   |  해당 경로에 데이터나 파일이 존재하는 경우 오류를 발생시키면서 쓰기 작업이 실패한다     |
    |    ignore   |  해당 경로에 데이터나 파일이 존재하는 경우 아무런 처리도 하지 않는다.  |

## 9.2 CSV 파일
- CSV는 콤마(,)로 구분된 값을 의미한다.
- CSV는 각 줄이 레코드이며 레코드의 각 필드를 콤마로 구분하는 일반적인 텍스트 파일 포맷이다.
- 읽으려는 CSV파일이 비표준적인 방식으로 기록되어있을 가능성때문에 CSV리더는 많은 옵션을 제공한다.

### 9.2.1 CSV 옵션
<!--CSV 데이터소스 옵션-->
|  <div style="width:70px;"><center>읽기/쓰기</center></div>  | <center>키</center>  | <center>사용 가능한 값</center>| <center>기본값</center>  | <center>설명</center>
|:---:|:---|:---|:---|:---|
| 모두  | sep                      | 단일문자       | ,     | 각 필드와 값을 구분하는데 사용되는 단일 문자|
| 모두  | header                   | true, false   | false&nbsp;&nbsp;&nbsp;&nbsp;  | 첫 번째 줄이 컬럼명인지 나타내는 불리언값|
| 읽기  | escape                   | 모든 문자열    | \      | 스파크가 파일에서 이스케이프 처리할 문자|
| 읽기  | inferSchema              | true, false   | false  | 스파크가 파일을 읽을때 컬럼의 데이터 타입을 추론할지 정의|
| 읽기  | ignoreLeadingWhiteSpace  | true, false   | false  | 값을 읽을 때 값의 선행 공백을 무시할지 정의|
| 읽기  | ignoreTrailingWhiteSpace | true, false   | false  | 값을 읽을 때 값의 후행 공백을 무시할지 정의|
| 모두  | nullValue                | 모든 문자열    | ""     | 파일에서 null 값을 나타내는 문자|
| 모두  | nanValue                 | 모든 문자열    | NaN    | CSV 파일에서 NaN이나 값없음을 나타내는 문자를 선언|
| 모두  | positiveInf              | 모든 문자열 또는 문자 | Inf | 양의 무한 값을 나타내는 문자(열)를 선언 |
| 모두  | negativeInf              | 모든 문자열 또는 문자 | -Inf | 음의 무한 값을 나타내는 문자(열)를 선언 |              
| 모두  | compression 또는 codec   | none, <br/>uncompressed, <br/>bzip2,<br/>deflate,<br/>gzip,<br/>lz4,<br/>snappy | none | 스파크가 파일을 읽고 쓸 때 사용하는 압축 코덱을 정의 |
| 모두  | dateFormat | 자바의 SimpleDateFormat형식을 따르는 문자, 문자열 | yyyy-MM-dd| 날짜 데이터 타입인 모든 필드에서 사용할 날짜 형식|
| 모두  | timestampFormat| 자바의 SimpleDateFormat형식을 따르는 문자, 정수 | yyyy-MM-dd'T'HH:mm:ss.SSSZZ | 타임스탬프 데이터 타입인 모든 필드에서 사용할 날짜 형식 |
| 읽기  | maxColumns| 모든 정수 | 20480 | 파일을 구성하는 최대 컬럼 수를 선언 |
| 읽기  | maxCharsPerColumn | 모든 정수 | 1000000 | 컬럼의 문자 최대 길이를 선언|
| 읽기  | escapeQuotes | true, false | true | 스파크가 파일의 라인에 포함된 인용부호를 이스케이프할지 선언 |
| 읽기  | maxMalformedLogPerPartition | 모든 정수 | 10 | 스파크가 각 파티션별로 비정상적인 레코드를 발견했을 때 기록할 최대수. 이 숫자를 초과하는 비정상적인 레코드는 무시됨|
| 쓰기  | quoteAll | true, false| false | 인용부호 문자가 있는값을 이스케이프 처리하지 않고, 전체 값을 인용부호로 묶을지 여부 |
| 읽기  | multiLine | true, false| false| 하나의 논리적 레코드가 여러 줄로 이루어진 CSV 파일 읽기를 허용할지 여부 |

### 9.2.2 CSV 파일 읽기
- CSV용 DataFrameReader 생성
    ```scala
    spark.read.format("csv")
    ```
- 스키마와 읽기 모드 옵션 지정 (inferSchema를 true로 하면 스키마를 추론한다.)
    ```scala
    spark.read.format("csv")
        .option("header", "true")
        .option("mode", "FAILFAST")
        .option("inferSchema", "true")
        .load("/data/data/flight-data/csv/2010-summary.csv").show(5)

    +-----------------+-------------------+-----+
    |DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
    +-----------------+-------------------+-----+
    |    United States|            Romania|    1|
    |    United States|            Ireland|  264|
    |    United States|              India|   69|
    |            Egypt|      United States|   24|
    |Equatorial Guinea|      United States|    1|
    +-----------------+-------------------+-----+
    ```
- 비정상적인 데이터를 얼마나 수용할 수 있을지 읽기 모드로 지정
    ```scala
    // 읽기 성공 (스키마와 데이터 모두 문자형)
    import org.apache.spark.sql.types.{StructField, StructType, StringType, LongType}
    
    val myManualSchema = new StructType(Array(
        new StructField("DEST_COUNTRY_NAME", StringType, true),
        new StructField("ORIGIN_COUNTRY_NAME", StringType, true),
        new StructField("count", LongType, false)
    ))
    
    spark.read.format("csv")
        .option("header", "true")
        .option("mode", "FAILFAST")
        .schema(myManualSchema)
        .load("/data/flight-data/csv/2010-summary.csv")
        .show(5)

    +-----------------+-------------------+-----+
    |DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
    +-----------------+-------------------+-----+
    |    United States|            Romania|    1|
    |    United States|            Ireland|  264|
    |    United States|              India|   69|
    |            Egypt|      United States|   24|
    |Equatorial Guinea|      United States|    1|
    +-----------------+-------------------+-----+
    ```

    ```scala
    // 읽기 오류 (스키마는 숫자형이고, 데이터는 문자형)
    val myManualSchema = new StructType(Array(
        new StructField("DEST_COUNTRY_NAME", LongType, true),
        new StructField("ORIGIN_COUNTRY_NAME", LongType, true),
        new StructField("count", LongType, false) 
    ))

    spark.read.format("csv")
    .option("header", "true")
    .option("mode", "FAILFAST")
    .schema(myManualSchema)
    .load("/data/flight-data/csv/2010-summary.csv")
    .take(5)

    // 에러
    org.apache.spark.SparkException: Job aborted due to stage failure: Task 0 in stage 24.0 failed 1 times, most recent failure: Lost task 0.0 in stage 24.0 (TID 35, localhost, executor driver): org.apache.spark.SparkException: Malformed records are detected in record parsing. Parse Mode: FAILFAST.
    ```

### 9.2.3 CSV 파일 쓰기
- CSV 파일을 읽어 들여 TSV 파일로 내보내기
    - maxColumns, inferSchema옵션을 제외하면 데이터 읽기때와 동일한 옵션을 적용할 수 있다.
        ```scala
        val csvFile = spark.read.format("csv")
        .option("header", "true")
        .option("mode", "FAILFAST")
        .schema(myManualSchema)
        .load("/data/flight-data/csv/2010-summary.csv")
        
        csvFile.write.format("csv").mode("overwrite").option("sep", "\t")
        .save("/tmp/my-tsv-file.tsv")
        ```

## 9.3 JSON 파일
- multiLine옵션을 사용하면 여러줄에 걸쳐서 작성된 JSON파일(한라인에 한 row형태가 아닌)을 제대로 읽을 수 있다.
https://sparkbyexamples.com/spark/read-json-multiple-lines-in-spark/

### 9.3.1 JSON 옵션
|  <div style="width:70px;"><center>읽기/쓰기</center></div>  | <center>키</center>  | <center>사용 가능한 값</center>| <center>기본값</center>  | <center>설명</center>
|:---:|:---|:---|:---|:---|
| 모두 | compression 또는<br/>codec | none, uncompressed, bzip2, deflate, gzip, lz4, snappy | none | 스파크가 파일을 읽고 쓸 때 사용하는 압축 코덱을 정의합니다. |
| 모두  | dateFormat | 자바의 SimpleDateFormat형식을 따르는 문자, 문자열   | yyyy-MM-dd  | 날짜 데이터 타입인 모든 필드에서 사용할 날짜 형식을 정의 합니다. |
| 모두  | timestampFormat| 자바의 SimpleDateFormat형식을 따르는 문자, 문자열 | yyyy-MM-dd'T'HH:mm:ss.SSSZZ | 타임스탬프 데이터 타입인 모든 필드에서 사용할 날짜 형식 |
| 읽기  | primitiveAsString | true, false | false | 모든 프리미티브 값을 문자열로 추정할지 정의 |
| 읽기  | allowComments | true, false | false | JSON 레코드에서 자바나 C++ 스타일로 된 코멘트를 무시할지 정의 |
| 읽기  | allowUnquotedFieldNames | true, false | false | 인용부호로 감싸여 있지 않은 JSON 필드명을 허용할지 정의 |
| 읽기  | allowSingleQuotes | true, false | true | 인용부호로 큰 따옴표(") 대신 작은따옴표(')를 허용할지 정의 |
| 읽기  | allowNumericLeadingZeros | true, false | false | 숫자 앞에 0을 허용할지 정의 (예:00012) |
| 읽기  | allowBackslashEscapingAnyCharacter | true, false | false | 백슬래시 인용부호 메커니즘을 사용한<br/>인용부호를 허용할지 정의 |
| 읽기  | columnNameOfCorruptRecord | 모든 문자열 | spark.sql.columnNameOfCorruptRecord속성의 설정값 | permissive 모드에서 생성된 비정상 문자열을 가진 새로운 필드명을 변경할 수 있다.<br/>이 값을 설정하면 spark.sql.columnNameOfCorruptRecord 설정값 대신 적용된다. |
| 읽기  | multiLine | true, false | false | 줄로 구분되지 않은 JSON 파일의 읽기를 허용할지 정의 |

### 9.3.2 JSON 파일 읽기
- JSON 읽기
    ```scala
    spark.read.format("json").option("mode", "FAILFAST").schema(myManualSchema)
    .load("/data/flight-data/json/2010-summary.json").show(5)

    +-----------------+-------------------+-----+
    |DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
    +-----------------+-------------------+-----+
    |    United States|            Romania|    1|
    |    United States|            Ireland|  264|
    |    United States|              India|   69|
    |            Egypt|      United States|   24|
    |Equatorial Guinea|      United States|    1|
    +-----------------+-------------------+-----+
    ```

### 9.3.3 JSON 파일 쓰기
- 파티션당 하나의 파일을 만들며 DataFrame을 단일 폴더에 저장.
- JSON객체는 한줄에 하나씩 기록.
- spark는 데이터 소스에 관계없이 원하는 형식으로 저장 가능 (csv -> json)
    ```scala
    csvFile.write.format("json").mode("overwrite").save("/tmp/my-json-file.json")
    ```

## 9.4 파케이 파일
- 아파치 스파크와 잘 호환되며 스파크의 기본 파일 포맷이다.
- 오픈소스 컬럼 기반의 데이터 저장 방식
- csv, json보다 적은용량 (snappy압축방식을 사용하면 30%이하 수준)
- 전체 파일을 읽는 대신 개별 컬럼을 읽음  
csv, json보다 훨씬 효율적으로 동작하므로 큰 용량의 데이터는 파케이 포맷으로 저장하는것이 좋다.
- 복합 데이터 타입을 지원한다. (배열, 맵, 구조체)

### 9.4.1 파케이 파일 읽기
- 파케이는 옵션이 거의 없다.  
데이터 저장시 자체 스키마를 사용해 데이터를 저장하기 때문이다.
- 파케이 파일은 스키마가 파일 자체에 내장되어 있으므로 스키마를 추정할 필요가 없다.
- 스키마를 설정할 수도 있지만 이런작업은 거의 필요가 없다.

    ```scala
    spark.read.format("parquet")
        .load("/data/flight-data/parquet/2010-summary.parquet").show(5)

    +-----------------+-------------------+-----+
    |DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
    +-----------------+-------------------+-----+
    |    United States|            Romania|    1|
    |    United States|            Ireland|  264|
    |    United States|              India|   69|
    |            Egypt|      United States|   24|
    |Equatorial Guinea|      United States|    1|
    +-----------------+-------------------+-----+
    ```
- 파케이 옵션

|  <div style="width:70px;"><center>읽기/쓰기</center></div>  | <center>키</center>  | <center>사용 가능한 값</center>| <center>기본값</center>  | <center>설명</center>
|:---:|:---|:---|:---|:---|
| 모두 | compression 또는<br/>codec | none, uncompressed, bzip2, deflate, gzip, lz4, snappy | none | 스파크가 파일을 읽고 쓸 때 사용하는 압축 코덱을 정의. |
| 읽기 | mergeSchema | true, false | spark.sql.parquet.mergeSchema 속성의 설정값 | 동일한 테이블이나 폴더에 신규 추가된 파케이 파일에 컬럼을 점진적으로 추가할 수 있다. 이러한 기능을 활성/비활성화 하기위해 이 옵션을 사용 |

### 9.4.2 파케이 파일 쓰기 
- 파티션당 하나의 파일을 만들며 DataFrame을 단일 폴더에 저장.
- 파일 경로만 명시하면 된다. (csv, json에 비해 추가옵션이 거의 없음)
    ```scala
    csvFile.write.format("parquet").mode("overwrite")
    .save("/tmp/my-parquet-file.parquet")
    ```

## 9.5 ORC 파일
- 컬럼 기반의 파일 포맷
- ORC(Optimized Row Columnar)는 Hadoop에서 데이터처리 최적화를 위해 개발되었다. https://box0830.tistory.com/207
- 대규모 스트리밍 읽기에 최적화되었고 로우를 빠르게 찾아낼 수 있다.
- 경우에 따라서는 파케이보다 파일 용량이 더 작다.
- ORC와 파케이의 차이점 (두 포맷은 매우 유사하나 근본적인 차이점이 있다.)
    - ORC : 하이브에 최적화
    - 파케이 : 스파크에 최적화

### 9.5.1 ORC 파일 읽기
```scala
spark.read.format("orc").load("/data/flight-data/orc/2010-summary.orc").show(5)

+-----------------+-------------------+-----+
|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
+-----------------+-------------------+-----+
|    United States|            Romania|    1|
|    United States|            Ireland|  264|
|    United States|              India|   69|
|            Egypt|      United States|   24|
|Equatorial Guinea|      United States|    1|
+-----------------+-------------------+-----+
```

### 9.5.2 ORC 파일 쓰기 
```scala
csvFile.write.format("orc").mode("overwrite").save("/tmp/my-json-file.orc")
```

## 9.6 SQL 데이터베이스 
- SQLite, MySQL, PostgreSQL, Oracle같은 데이터베이스에 JDBC로 연결할 수 있다.

- 데이터베이스는 원시 파일 형태(로그, CSV와같은)가 아니므로 고려해야할 것들이 더 많다.  
예) 데이터베이스 인증/접속 정보, 네트워크 접속 상태
- 예제는 쉬운 실행을 위해 SQLite로 작성되었다.  
운영에서 사용시에는 Mysql, Oracle등의 전통적인 RDB를 사용해야 한다.

- 데이터를 읽고 쓰기 위해서는 스파크 클래스패스에 JDBC드라이버를 추가해야 한다.
    ```properties
    # spark-shell실행시 드라이버 추가
    ./bin/spark-shell --driver-class-path postgresql-9.4.1207.jar --jars postgresql-9.4.1207.jar
    ```
- JDBC 데이터소스 옵션
    |  <div style="width:70px;"><center>속성명</center></div>  | <center>의미</center>  
    |:---|:---|
    | url   | 접속을 위한 JDBC URL. 소스 시스템에 특화된 설정은 URL에 있다.  <br/>(예: jdbc:postgresql://localhost/test?user=fred&password=secret)   |
    | dbtable    | 읽을 JDBC 테이블을 설정한다. SQL쿼리의 FROM절에 유효한 모든 것을 사용할 수 있다. <br/>예를들어 ***테이블 대신 괄호 안에 서브쿼리***를 사용할 수도 있다.    |
    | driver    | 지정한 URL에 접속할 때 사용할 JDBC 드라이버 클래스명.   |
    | partitionColumn, lowerBound, upperBound     | 세 가지 옵션은 항상 같이 지정해야 하며 추가로 numPartitions도 반드시 지정해야 한다. 다수의 워커에서 병렬로 테이블을 나눠 읽는 방법을 정의 한다. partitionColumn을 기준으로 데이터를 병렬로 읽게 된다. 반드시 해당 테이블의 수치형 컬럼이어야 한다. lowerBound, upperBound는 가져올 데이터의 최소~최대 범위를 정의한다. <br/>만약 partitionColumn이 seq(게시판 글목록 번호)이고 lowerBound가 1 upperBound가 100 numPartitions가 10 이라면 10개의 워커에서 동시접속하여 각각 분할조회를 하게된다. 이때 각 워커에서 자동생성되는 where절 조건은 다음과 같다. seq between 1 and 10, seq between 11 and 20 ... seq between 91 and 100 |
    | numPartitions    | 테이블의 데이터를 병렬로 읽거나 쓰기 작업에 사용할 수 있는 최대 파티션 수를 결정한다. 이 속성은 최대 동시 **JDBC 연결** 수를 결정한다. 쓰기에 사용되는 파티션 수가 이 값을 초과하는 경우 쓰기 연산 전에 coalesce(numPartitions)를 실행해 파티션 수를 이 값에 맞게 줄이게 된다.  |
    | fetchsize    | 한 번에 얼마나 많은 로우를 가져올지 결정하는 JDBC 패치 크기를 설정한다. 이 옵션은 기본적으로 패치 크기가 작게 설정된 JDBC 드라이버의 성능을 올리는데 도움이 된다. (예:오라클의 경우 패치 크기가 10). 이 옵션은 읽기에만 적용된다.   |
    | batchsize    | 한 번에 얼마나 많은 로우를 저장할지 결정하는 JDBC의 배치 크기를 설정한다. 이 옵션은 JDBC 드라이버의 성능을 향상시킬 수 있다. 이 옵션은 쓰기에만 적용되며 기본값은 1000   |
    | isolationLevel    | 현재 연결에 적용되는 트랜잭션 격리 수준을 정의. 이 값은 JDBC Connection 객체에서 정의하는 표준 트랜잭션 격리 수준에 해당하는 NONE, READ_COMMITTED, READ_UNCOMMITTED, REPEATABLE_READ, SERIALIZABLE중 하나가 될 수 있다. 기본값은 READ_UNCOMMITTED. 이 옵션은 쓰기에만 적용된다. [(java.sql.Connection API 참고)](https://docs.oracle.com/javase/8/docs/api/java/sql/Connection.html) |
    | truncate    | JDBC writer 관련 옵션. SaveMode.Overwrite가 활성화되면 스파크는 기존 테이블을 삭제하거나 재생성하는 대신 데이터베이스의 truncate 명령을 실행한다. 이 옵션의 기본값은 false. 쓰기에만 적용된다.   |
    | createTableOptions    | JDBC writer관련 옵션이다. 이 옵션을 지정하면 테이블 생성시 특정 테이블의 데이터베이스와 파티션 옵션을 설정할 수 있다. (예: ENGINE=InnoDB DEFAULT CHARSET=utf8 COMMENT='Spark SQL test table')   |
    | createTableColumnTypes    | 테이블을 생성할 때 기본값 대신 사용할 데이터베이스 컬럼 데이터 타입을 정의 한다. 데이터 타입 정보는 반드시 CREATE TABLE에서 사용하는 컬럼 정의구문과 동일한 형식으로 지정해야 한다. (예 name CHAR(64), comments VARCHAR(1024)). 지정된 데이터 타입은 유효한 스파크 SQL 데이터 타입이어야 한다. 이 옵션은 쓰기에만 적용된다.   |

### 9.6.1 SQL 데이터베이스 읽기
- SQLite에서 데이터 읽기. (파일 읽기 및 다른 데이터소스 읽기와 비슷하다.)
    ```scala
    // SQLite 접속설정 변수
    val driver =  "org.sqlite.JDBC"
    val path = "file:///data//data/flight-data/jdbc/my-sqlite.db"
    val url = s"jdbc:sqlite:/${path}"
    val tablename = "flight_info"

    // SQLite JDBC 접속 테스트
    import java.sql.DriverManager
    val connection = DriverManager.getConnection(url)
    connection.isClosed()
    connection.close()

    // SQLite 테이블을 읽어서 DataFrame 생성 (사용자 인증 정보가 필요 없어서 간단하다.)
    val dbDataFrame = spark.read
        .format("jdbc")
        .option("url", url)
        .option("dbtable", tablename)
        .option("driver",  driver).load()
    ```

- PostgreSQL과같은 RDB는 설정정보가 더 많다.
    ```scala
    // PostgreSQL과같은 일반적인 RDB는 더 많은 설정이 필요. (바로 위 예제와 동일한 작업 수행)
    val pgDF = spark.read
        .format("jdbc")
        .option("driver", "org.postgresql.Driver")
        .option("url", "jdbc:postgresql://database_server")
        .option("dbtable", "schema.tablename")
        .option("user", "username")
        .option("password","my-secret-password").load()
    ```

- SQLite로부터 생성된 DataFrame은 기존 예제에서의 DataFrame과 같다.   
DataFrame의 스키마는 DB의 테이블 정보를 읽어서 만들어낸다.
    ```scala
    // 중복 데이터가 제거된 국가 목록을 조회.
    dbDataFrame.select("DEST_COUNTRY_NAME").distinct().show(5)

    +-----------------+
    |DEST_COUNTRY_NAME|
    +-----------------+
    |         Anguilla|
    |           Russia|
    |         Paraguay|
    |          Senegal|
    |           Sweden|
    +-----------------+
    ```

### 9.6.2 쿼리 푸시다운 
- DataFrame을 만들기 전에 데이터를 필터링할 수 있다. (원하는 컬럼만 조회)  
실행계획의 PushedFilters부분에서 확인

    ```scala
    dbDataFrame.select("DEST_COUNTRY_NAME").distinct().explain

    == Physical Plan ==
    *(2) HashAggregate(keys=[DEST_COUNTRY_NAME#45], functions=[])
    +- Exchange hashpartitioning(DEST_COUNTRY_NAME#45, 200)
    +- *(1) HashAggregate(keys=[DEST_COUNTRY_NAME#45], functions=[])
        +- *(1) Scan JDBCRelation(flight_info) [numPartitions=1] [DEST_COUNTRY_NAME#45] PushedFilters: [], ReadSchema: struct<DEST_COUNTRY_NAME:string>
    ```
- filter(where)처리는 RDB로 위임(push down)한다. filter처리가 spark가 아닌 RDB에서 처리된다는 의미.  
실행계획의 PushedFilters부분에서 확인
    ```scala
    dbDataFrame.filter("DEST_COUNTRY_NAME in ('Anguilla', 'Sweden')").explain

    == Physical Plan ==
    *(1) Scan JDBCRelation((SELECT DISTINCT(DEST_COUNTRY_NAME) FROM flight_info)
        AS flight_info) [numPartitions=1] [DEST_COUNTRY_NAME#110] PushedFilters: [*In(DEST_COUNTRY_NAME, [Anguilla,Sweden])], ReadSchema: struct<DEST_COUNTRY_NAME:string>
    ```

- 모든 스파크 함수를 SQL에 맞게 변환하지 못하므로 전체 쿼리를 DB에 전달해  
DB에서 처리후 결과만 DataFrame으로 받을 수 있다.

- option에 테이블명 대신 SQL쿼리를 서브쿼리로 명시하면 된다.  

    ```scala
    // // in Scala
    val pushdownQuery = """(SELECT DISTINCT(DEST_COUNTRY_NAME) FROM flight_info)
    AS flight_info"""
    
    val dbDataFrame = spark.read.format("jdbc")
    .option("url", url).option("dbtable", pushdownQuery).option("driver",  driver)
    .load()
    
    dbDataFrame.explain()

    == Physical Plan ==
    *(1) Scan JDBCRelation((SELECT DISTINCT(DEST_COUNTRY_NAME) FROM flight_info)
        AS flight_info) [numPartitions=1] [DEST_COUNTRY_NAME#117] PushedFilters: [], ReadSchema: struct<DEST_COUNTRY_NAME:string>
        pushdownQuery: String =
        (SELECT DISTINCT(DEST_COUNTRY_NAME) FROM flight_info)
        AS flight_info
        dbDataFrame: org.apache.spark.sql.DataFrame = [DEST_COUNTRY_NAME: string]
    ```

#### 데이터베이스 병렬로 읽기
- SQL 데이터베이스를 병렬로 읽으려면 몇 가지 수동 설정이 필요하다.
- 옵션 목록 중 numPartition 옵션을 사용해 읽기/쓰기 동시 작업수를 제한할 수 있다.

    ```scala
    val dbDataFrame = spark.read.format("jdbc")
        .option("url", url)
        .option("dbtable", tablename)
        .option("driver", driver)
        .option("numPartitions", 10) // 동시 접속수
        .load()

    dbDataFrame.select("DEST_COUNTRY_NAME").distinct().show()

    +--------------------+
    |   DEST_COUNTRY_NAME|
    +--------------------+
    |            Anguilla|
    |              Russia|
    |            Paraguay|
    |             Senegal|
    |              Sweden|
    |            Kiribati|
    |              Guyana|
    |         Philippines|
    |            Malaysia|
    |           Singapore|
    |                Fiji|
    |              Turkey|
    |             Germany|
    |         Afghanistan|
    |              Jordan|
    |               Palau|
    |Turks and Caicos ...|
    |              France|
    |              Greece|
    |              Taiwan|
    +--------------------+
    ```

- predicates 옵션으로 조건절을 줄 수 있고 predicates옵션의 배열수만큼 파티션이 생성된다.  
특정 rdd파티션에 특정 조건에 해당하는 데이터만 위치시킬 수 있다.
    ```scala
    val props = new java.util.Properties
    props.setProperty("driver", "org.sqlite.JDBC")

    val predicates = Array(
        "DEST_COUNTRY_NAME = 'Sweden' OR ORIGIN_COUNTRY_NAME = 'Sweden'",
        "DEST_COUNTRY_NAME = 'Anguilla' OR ORIGIN_COUNTRY_NAME = 'Anguilla'")
    spark.read.jdbc(url, tablename, predicates, props).show()
    spark.read.jdbc(url, tablename, predicates, props).rdd.getNumPartitions // 2

    +-----------------+-------------------+-----+
    |DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
    +-----------------+-------------------+-----+
    |           Sweden|      United States|   65|
    |    United States|             Sweden|   73|
    |         Anguilla|      United States|   21|
    |    United States|           Anguilla|   20|
    +-----------------+-------------------+-----+
    ```

- predicates 옵션 사용시 조건절을 잘못 정의하면 중복 로우가 많이 발생할 수 있다.
    ```scala
    val props = new java.util.Properties
    props.setProperty("driver", "org.sqlite.JDBC")

    val predicates = Array(
        "DEST_COUNTRY_NAME != 'Sweden' OR ORIGIN_COUNTRY_NAME != 'Sweden'",
        "DEST_COUNTRY_NAME != 'Anguilla' OR ORIGIN_COUNTRY_NAME != 'Anguilla'")
    spark.read.jdbc(url, tablename, predicates, props).count() // 510
    ```

#### 슬라이딩 윈도우 기반의 파티셔닝
- 수치형 count 컬럼을 기준으로 분할 한다.  
처음과 마지막 파티션 사이의 최솟값, 최댓값을 사용한다. (lowerBound, upperBound)

- numPartitions에 설정된 값만큼 병렬로 쿼리를 요청하며 numPartitions만큼 파티션수가 생성된다.
    ```scala
    val colName = "count"
    val lowerBound = 0L
    val upperBound = 348113L // this is the max count in our database
    val numPartitions = 10

    spark.read.jdbc(url,tablename,colName,lowerBound,upperBound,numPartitions,props)
    .count() // 255
    ```

### 9.6.3 SQL 데이터베이스 쓰기
- 설정된 JDBC URI, 쓰기모드에 따라 데이터를 쓰게 된다.
    ```scala
    val csvFile = spark.read.format("csv")
    .option("header", "true").option("mode", "FAILFAST").schema(myManualSchema)
    .load("/data/flight-data/csv/2010-summary.csv")

    // 덮어쓰기
    val newPath = "jdbc:sqlite://tmp/my-sqlite.db"
    csvFile.write.mode("overwrite").jdbc(newPath, tablename, props)
    spark.read.jdbc(newPath, tablename, props).count() // 255

    // 파일에 row추가
    csvFile.write.mode("append").jdbc(newPath, tablename, props)
    spark.read.jdbc(newPath, tablename, props).count() // 765
    ```

## 9.7 텍스트 파일
- 파일의 각 줄은 DataFrame의 레코드가 된다.
- 로그 파일을 구조화된 포맷으로 파싱할 수도 있다.

### 9.7.1 텍스트 파일 읽기
- textFile메서드에 text파일을 지정하면 된다.
    ```scala
    // csv파일을 ,로 분할해서 파싱
    spark.read.textFile("/data/flight-data/csv/2010-summary.csv")
        .selectExpr("split(value, ',') as rows").show()
    ```

### 9.7.2 텍스트 파일 쓰기
- 텍스트 파일을 쓸 때는 문자열 컬럼이 하나만 존재해야 한다.
그렇지 않으면 작업이 실패한다.
    ```scala
    csvFile.select("DEST_COUNTRY_NAME").write.text("/tmp/simple-text-file.txt")

    // 컬럼을 기준으로 디렉토리를 생성해서 분할 저장 (partytionBy("count"))
    csvFile.limit(10).select("DEST_COUNTRY_NAME", "count")
    .write.partitionBy("count").text("/tmp/five-csv-files2.csv")
    ```

## 9.8 고급 I/O 개념
- 쓰기 작업 전에 파티션 수를 조절함으로써 병렬 쓰기할 수를 제어할 수 있다. (속도 향상)
- 버켓팅, 파티셔닝을 조절해서 데이터의 저장 구조를 제어할 수 있다. (분할 저장)

### 9.8.1 분할 가능한 파일 타입과 압축 방식
- 특정 파일 포맷은 분할을지원 한다.
- 전체 파일이 아닌 일부의 파일만 읽을 수 있으므로 성능 향상에 도움이 된다.  
HDFS같은 시스템을 사용한다면 분할된 파일을 여러 블록으로 나누어 분산 저장을 하기 때문에 훨씬 더 최적화가 가능.
- 모든 파일이 압축을 지원하지는 않는다.
- 파케이 파일 포맷을 사용하면 데이터의 크기가 기본적으로 작고 압축이 지원되서 추천한다.

### 9.8.2 병렬로 데이터 읽기
- 여러 익스큐터가 여러 파일을 동시에 읽을 수 있다.  
각 파일은 DataFrame의 파티션이 된다.

### 9.8.3 병렬로 데이터 쓰기
- 파일이나 데이터 수는 데이터를 쓰는 시점에 DataFrame이 가진 파티션 수에 따라 달라진다.
- 기본적으로 데이터 파티션당 하나의 파일이 작성된다.
- 옵션에 지정된 파일명은 실제로는다수의 파일을 가진 디렉터리이다.

    ```scala
    // 폴더 안에 5개의 파일을 생성
    csvFile.repartition(5).write.mode("overwrite").format("csv").save("/tmp/multiple.csv")
    ```
- 파티셔닝
    - 파티셔닝은 데이터를 디렉토리로 나누어 저장하는 방식.

    - 파니셔닝은 필터링을 자주 사용하는 테이블을 가진 경우에 사용할 수 있는 가장 손쉬운 최적화 방식이다.  
    예) 날짜를 기준으로 파티션을 만든다.
        ```scala
        // DEST_COUNTRY_NAME컬럼을 기준으로 디렉터리를 분할해서 저장
        csvFile.limit(10).write.mode("overwrite")
            .partitionBy("DEST_COUNTRY_NAME").save("/tmp/partitioned-files.parquet")
        ```

- 버켓팅
    - 파티션은 데이터를 디렉토리로 나누어 저장하는 방식이고, 버켓팅은 데이터를 파일별로 나누어 저장한다.

    - 동일한 버킷 ID를 가진 데이터가 하나의 물리적 파티션에 모두 모여 있기 때문에 데이터를 읽을 때 셔플을 피할 수 있다.

    - 데이터를 읽을때를 고려해서 파티셔닝 되므로 조인이나 집계시 발생하는 고비용의 셔플을 피할 수 있다.

    - 버켓팅은 saveAsTable메서드를 사용해서만 저장할 수 있다.
        ```scala
        // 버켓단위로 데이터를 모아서 일정 수의 파일로 저장
        val numberBuckets = 10
        val columnToBucketBy = "count"

        csvFile.write.format("csv").mode("overwrite")
            .option("path", "/tmp/bucket_csv")
            .bucketBy(numberBuckets, columnToBucketBy).saveAsTable("bucketedFiles")
        ```

### 9.8.4 복합 데이터 유형 쓰기
- 스파크에는 다양한 자체 데이터 타입을 제공한다. (불리언, 숫자, 문자열, 복합 데이터 타입(구조체, 배열, 맵) )
- 스파크에서는 잘 작동하지만 모든 데이터 파일 포맷에 적합한것은 아니다.  
CSV는 복합 데이터 타입을 지원하지 않지만 파케이나 ORC는 지원한다.

### 9.8.5 파일 크기 관리
- 파일크기는 저장시보다 읽을때 중요한 요소이다.
- 결과 파일 수는 파일을 쓰는 시점의 Dataset내의 RDD파티션 수와 같다.
- 작은 파일을 많이 생성하면 메타데이터에 엄청난 관리 부하가 발생한다.  
HDFS같은 많은 파일 시스템은 작은크기의 파일을 잘 다루지 못한다.
- 큰 파일의경우 몇 개의 로우가 필요하더라도 전체 데이터 블록을 읽어야하기 때문에 너무 큰 파일도 좋지 않다.  
스파크 2.2 버전부터 파일 크기를 제어하는 기능이 추가 되었다.  
maxRecordsPerFile옵션에 파일당 레코드 수를 지정하면 각 파일에 기록될 레코드 수를 조절할 수 있다.

    http://www.gatorsmile.io/anticipated-feature-in-spark-2-2-max-records-written-per-file/

    ```scala
    // 파일당 최대 5,000개의 로우를 포함하도록 한다.
    df.write.option("maxRecordsPerFile", 5000)
    ```

## 9.9 정리
- 이 장에서는 스파크에서 데이터를 읽고 쓸 때 사용할 수 있는 다양한 옵션을 알아보았다.
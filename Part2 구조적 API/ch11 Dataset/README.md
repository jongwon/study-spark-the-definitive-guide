# 11. Dataset
- DataFrame은 Row 타입의 Dataset이다. (Dataset[Row] = DataFrame)

- Spark 1.6에서 처음 소개되었고 Spark 2.0에서는 DataFrame 과 Dataset 이 통합되었다.

- Dataset은 JVM을 사용하는 언어인 스칼라, 자바에서만 사용가능.

- 스칼라에서는 케이스 클래스(Case Class), 자바에서는 자바 빈(Java Bean) 객체를 사용해 Dataset의 Row를 정의

- Dataset은 DataFrame보다 느리다. 
  Row포맷을 사용자 정의 데이터 타입으로 변환하는 과정이 추가되기 때문이다.

## 11.1 Dataset을 사용할 시기
- Dataset을 사용해야하는 이유
  - 타입 안정성(type-safe)  
  정확도와 방어적 코드를 가장 중요시 한다면 성능이 조금 희생되더라도  
  DataFrame보다 Dataset을 사용한다.  
  - 문자열끼리 뺄셈을 하는것처럼 데이터 타입이 유효하지 않은 작업은   
      Dataset을 사용할 경우 컴파일  타임에 오류를 잡아낼 수 있다.

## 11.2 Dataset 생성
- Dataset을 생성하려면 스키마를 미리 알고 있어야 한다.
- 자바 Encoders, 스칼라 case class로 스키마를 정의할 수 있다.

### 11.2.1 자바:Encoders
- 데이터 타입 클래스를 정의한 다음 DataFrame(Dataset[Row])에 지정해 인코딩 할 수 있다.
    ```java
    import org.apache.spark.sql.Encoders;

    public class Flight implements Serializable {
        
        String DEST_COUNTRY_NAME;
        String ORIGIN_COUNTRY_NAME;
        Long DEST_COUNTRY_NAME;
    }

    Dataset<Flight> flights = spark.read.parquet("/data/flight-data/parquet/2010-summary.parquet/").as(Encoders.bean(Flight.class));
    ```

### 12.2.2 스칼라:케이스 클래스
- 스칼라에서 Dataset을 생성하려면 스칼라 case class구문을 사용해 데이터 타입을 정의해야 한다.  
  https://docs.scala-lang.org/tour/case-classes.html

- case class의 특징
    - case class 정의 예
        ```scala
        case class Flight(DEST_COUNTRY_NAME: String,
                        ORIGIN_COUNTRY_NAME: String, count: BigInt)

        val flightsDF = spark.read.parquet("/data/flight-data/parquet/2010-summary.parquet/")
        val flights = flightsDF.as[Flight]
        ```

    - 불변성
        - case class의 내부 변수들은 val키워드로 선언되므로 최초 생성시를 제외하고 이후에는 변경 불가.
    - 패턴 매칭으로 분해 가능
        - 패턴매칭은 로직 분기를 단순화해 버그를 줄이고 가독성을 좋게 만든다.
        ```scala
        abstract class Notification

        case class Email(sender: String, title: String, body: String) extends Notification
        case class SMS(caller: String, message: String) extends Notification
        case class VoiceRecording(contactName: String, link: String) extends Notification

        def showNotification(notification: Notification): String = {
            // 스칼라 패턴매칭중 case 클래스를 사용한 매칭
            notification match {
                case Email(sender, title, _) =>
                    s"You got an email from $sender with title: $title"
                case SMS(number, "GG") =>
                    "메시지가 GG일경우."
                case SMS(number, message) =>
                    s"You got an SMS from $number! Message: $message"
                case VoiceRecording(name, link) =>
                    s"You received a Voice Recording from $name! Click the link to hear it: $link"
            }
        }

        val someSms = SMS("12345", "Are you there?")
        val someVoiceRecording = VoiceRecording("Tom", "voicerecording.org/id/123")

        println(showNotification(someSms))  // prints You got an SMS from 12345! Message: Are you there?
        println(showNotification(someVoiceRecording))  // you received a Voice Recording from Tom! Click the link to hear it: voicerecording.org/id/123
        ```
    - 참조값 대신 클래스 구조를 기반으로 비교.  
    인스턴스를 비교하면 같은 레퍼런스를 보는지를 비교하는것이 아니라 값이 같은지를 비교한다.
        ```scala
        val someSms = SMS("12345", "Are you there?")
        val someSms2 = SMS("12345", "Are you there?")

        println(s"someSms와 someSms2의 비교결과 => ${someSms == someSms2}") // someSms와 someSms2의 비교결과 => true
        ```

    - 컴파일러가 case class생성시 자동생성하는 것들
        - 컴파일러는  클래스 이름과 같은 이름의 팩토리 메소드를 추가한다.  
        객체 생성시 new키워드 없이 생성
            ```scala
            // new SMS("12345", "Are you there?") // 일반 클래스
            SMS("12345", "Are you there?") // 케이스 클래스
            ```

        - 케이스 클래스의 파라미터의 목록을 val 접두사를 붙인다. (불변)
            ```scala
            val someSms2 = SMS("12345", "Are you there?")
            println(someSms2.caller) // "12345"
            someSms2.caller = "123" // error: reassignment to val someSms2.caller = "123"
            ```
            
        - 컴파일러는 케이스 클래스에서 일부를 변경한 복사본을 생성하는 copy 메소드를 추가한다.
            ```scala
            val someSms = SMS("12345", "Are you there?")
            val someSms2 = someSms.copy() // someSms를 그대로 복사
            val someSms3 = someSms.copy(caller="55") // caller변수를 55로 변경하고 복사
            ```

        - 그외에 toString, hashCode, equals 메소드들을 자동으로 추가한다.

## 11.3 액션
- Dataset에 collect, take, count같은 액션을 적용할 수 있고   
case class에 접근할 때에는 row객체와는 다르게 .변수명으로 곧바로 접근 가능하다.   
(row객체는 getBoolean, getAs와같은 메서드를 호출해서 타입을 신경써야한다.)

    ```scala
    flights.show(2)
    flights.first.DEST_COUNTRY_NAME // United States

    +-----------------+-------------------+-----+
    |DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|count|
    +-----------------+-------------------+-----+
    |    United States|            Romania|    1|
    |    United States|            Ireland|  264|
    +-----------------+-------------------+-----+
    ```

## 11.4 트랜스포메이션
- Dataset의 트랜스포메이션은 DataFrame과 동일하다.

### 11.4.1 필터링
- Flight클래스를 파라미터로 사용해 불리언값을 반환하는 함수를 만들어서 테스트해본다.

    ```scala
    // 함수 정의
    def originIsDestination(flight_row: Flight): Boolean = {
    return flight_row.ORIGIN_COUNTRY_NAME == flight_row.DEST_COUNTRY_NAME
    }

    // filter적용
    flights.filter(flight_row => originIsDestination(flight_row)).first()

    // 드라이버에 모든 데이터를 모은다음 배열의 필터수행
    flights.collect().filter(flight_row => originIsDestination(flight_row))
    ```

### 11.4.2 매핑
- 특정값을 다른 값으로 매핑할때 사용한다.

    ```scala
    // flight객체를 flight.DEST_COUNTRY_NAME값으로 매핑
    val destinations = flights.map(f => f.DEST_COUNTRY_NAME)

    // 드라이버로 5개의 row를 가져온다.
    val localDestinations = destinations.take(5)
    ```

## 11.5 조인
- Dataset의 조인은 DataFrame과 동일하다.
- Dataset은 joinWith라는 데이터타입을 잃지않는 메서드를 사용할수 있다.  
joinWith로 join을 하면 Dataset 안쪽에 다른 두 개의 중첩된 Dataset으로 구성된다.

    ```scala
    val flightsMeta = spark.range(500).map(x => (x, scala.util.Random.nextLong))
        .withColumnRenamed("_1", "count").withColumnRenamed("_2", "randomData")
        .as[FlightMetadata]

    // Dataset[(Flight, FlightMetadata)] 형태
    val flights2 = flights
        .joinWith(flightsMeta, flights.col("count") === flightsMeta.col("count"))

    flights2.take(2)

    // 결과
    /*
    Array((Flight(United States,Uganda,1),FlightMetadata(1,-2504779212731526667)), (Flight(United States,French Guiana,1),FlightMetadata(1,-2504779212731526667)))
    */

    flights2.show(2)

    // 결과
    +--------------------+--------------------+
    |                  _1|                  _2|
    +--------------------+--------------------+
    |[United States, U...|[1, 6531429796494...|
    |[United States, F...|[1, 6531429796494...|
    +--------------------+--------------------+
    ```
- Dataset은 일반 join역시 잘 동작한다.  
그러나 DataFrame을 반환하므로 데이터 타입을 잃는다.
    ```scala
    // DataFrame[Row] 형태
    val flights2 = flights.join(flightsMeta, Seq("count")) 
    flights2.take(2) 

    // 결과
    /*
    Array[org.apache.spark.sql.Row] = Array([1,United States,Uganda,-2145071802571395558], [1,United States,French Guiana,-2145071802571395558])
    */

    flights2.show(2)
    +-----+-----------------+-------------------+-------------------+
    |count|DEST_COUNTRY_NAME|ORIGIN_COUNTRY_NAME|         randomData|
    +-----+-----------------+-------------------+-------------------+
    |    1|    United States|             Uganda|2868495997080901059|
    |    1|    United States|      French Guiana|2868495997080901059|
    +-----+-----------------+-------------------+-------------------+
    ```

## 11.6 그룹화와 집계
- Dataset의 그룹화와 집계는 DataFrame과같이 groupBy, rollup, cube메서드를 사용할 수 있다.  

- groupBy의경우 Dataset대신 DataFrame을 반환하기 때문에 데이터 타입을 잃게된다.
    ```scala
    flights.groupBy("DEST_COUNTRY_NAME").count() // Dataset[Row]
    ```

- 데이터타입을 잃지 않기위한 방법  
groupByKey메서드는 Dataset의 특정 키를 기준으로 그룹화하고 형식화된 Dataset을 반환한다.
    ```scala
    flights.groupByKey(x => x.DEST_COUNTRY_NAME).count() // Dataset[scala.Tuple2]
    flights.groupByKey(x => x.DEST_COUNTRY_NAME).count().first._1 // Anguilla
    flights.groupByKey(x => x.DEST_COUNTRY_NAME).count().first._1.getClass // java.lang.String
    ```

- 실행계획을 실행해서 성능 차이를 비교 (groupByKey를 사용하면 성능은 떨어진다.)
    ```scala
    // groupBy 실행계획
    flights.groupBy("DEST_COUNTRY_NAME").count().explain
    // 결과 =>
    == Physical Plan ==
    *(2) HashAggregate(keys=[DEST_COUNTRY_NAME#0], functions=[count(1)])
    +- Exchange hashpartitioning(DEST_COUNTRY_NAME#0, 200)
        +- *(1) HashAggregate(keys=[DEST_COUNTRY_NAME#0], functions=[partial_count(1)])
            +- *(1) FileScan parquet [DEST_COUNTRY_NAME#0] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/data/data/flight-data/parquet/2010-summary.parquet], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<DEST_COUNTRY_NAME:string>

    // groupByKey 실행계획
    flights.groupByKey(x => x.DEST_COUNTRY_NAME).count().explain
    // 결과 =>
    == Physical Plan ==
    *(3) HashAggregate(keys=[value#42], functions=[count(1)])
    +- Exchange hashpartitioning(value#42, 200)
        +- *(2) HashAggregate(keys=[value#42], functions=[partial_count(1)])
            +- *(2) Project [value#42]
                +- AppendColumns <function1>, newInstance(class $line23894649824.$read$$iw$$iw$Flight), [staticinvoke(class org.apache.spark.unsafe.types.UTF8String, StringType, fromString, input[0, java.lang.String, true], true, false) AS value#42]
                    +- *(1) FileScan parquet [DEST_COUNTRY_NAME#0,ORIGIN_COUNTRY_NAME#1,count#2L] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/data/data/flight-data/parquet/2010-summary.parquet], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<DEST_COUNTRY_NAME:string,ORIGIN_COUNTRY_NAME:string,count:bigint>
    ```

- Dataset의 groupByKey호출 이후에 flatMapGroups를 사용해서 그룹화된 데이터를 다룰 수 있다.
    ```scala
    def grpSum(countryName:String, values: Iterator[Flight]) = {
        // dropWhile은 scala iterator의 메서드이다
        // dropWhile안의 boolean값이 true일경우 삭제한다.
        values.dropWhile(_.DEST_COUNTRY_NAME == "Russia").map(flight => (countryName, flight))
    }
    flights.groupByKey(flight => flight.DEST_COUNTRY_NAME).flatMapGroups(grpSum).show(5)

    +--------+--------------------+
    |      _1|                  _2|
    +--------+--------------------+
    |Anguilla|[Anguilla, United...|
    |Paraguay|[Paraguay, United...|
    | Senegal|[Senegal, United ...|
    |  Sweden|[Sweden, United S...|
    |Kiribati|[Kiribati, United...|
    +--------+--------------------+
    ```

- 그룹을 축소
    ```scala
    def sum2(left:Flight, right:Flight) = {
        Flight(left.DEST_COUNTRY_NAME, null, left.count + right.count)
    }
    flights.groupByKey(x => x.DEST_COUNTRY_NAME).reduceGroups((left, right) => sum2(left, right))
    .take(5)

    flights.groupBy("DEST_COUNTRY_NAME").count().explain
    ```

## 11.7 정리
- Dataset의 기초와 Dataset 사용이 적합한 경우를 예제와 함께 알아 보았다.
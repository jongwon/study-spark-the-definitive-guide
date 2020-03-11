# 6. 다양한 데이터 타입 다루기

## 6.1 API는 어디서 찾을까

API는 계속 변하지만 핵심적인 데이터 변환용 함수는 
[Dataset](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Dataset), 
[Column](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Column)
API 이다.

## 6.2 스파크 데이터 타입으로 변환하기

### lit 함수

- 다른 언어(java, python, R...)의 데이터 타입을 스파크 데이터 타입으로 변환한다.
- Row 의 칼럼에 데이터를 넣을 때 사용한다.

## 6.3 불리언 데이터 타입 다루기

### Boolean

- 모든 필터링 작업에서 사용되는 기본 데이터 타입
- 값 : true, false
- 연산자 : and, or
- 주로 df.where나 filter 같은 구문에서 사용할 수 있다.
- 연속된 조건들은 기본적으로 and 구문으로 추가된다. or 구문을 사용하고 싶다면 filter 객체에 or 메서드를 사용한다.

  ```scala
  val priceFilter = col("UnitPrice") > 6000
  val descriptFilter = col("Description").contains("POSTAGE")
  df.where(priceFilter.or(descriptFilter)).show()
  ```

* 컬럼명만 사용해서 필터링 할 수도 있다. (BooleanType 인 칼럼인 경우)

  ```scala
  val DOTCodeFilter = col("StockCode") === "DOT"
  val priceFilter = col("UnitPrice") > 600
  val descripFilter = col("Description").contains("POSTAGE")

  println(descripFilter.getClass)
  df.withColumn("isExpensive", DOTCodeFilter.and(priceFilter.or(descripFilter)))
    .where("isExpensive")
    .select("unitPrice", "isExpensive").show(5)
  ```

## 6.4 수치형 데이터 타입 다루기

- 유용한 수식 표현
  - pow : 표시된 지수만큼 제곱
    
    ```scala
    val equation = pow(col("Quantity") * col("UnitPrice"), 2) + 5
    df.select(expr("CustomerId"), equation.alias("realQuantity")).show(2)
    +----------+------------------+
    |CustomerId|      realQuantity|
    +----------+------------------+
    |   17850.0|239.08999999999997|
    |   17850.0|          418.7156|
    +----------+------------------+
    ```
  - round : 반올림

    ```scala
    import org.apache.spark.sql.functions.{round, bround}

    df.select(round(col("UnitPrice"), 1).alias("rounded"), col("UnitPrice")).show(5)
    +-------+---------+
    |rounded|UnitPrice|
    +-------+---------+
    |    2.6|     2.55|
    |    3.4|     3.39|
    |    2.8|     2.75|
    |    3.4|     3.39|
    |    3.4|     3.39|
    +-------+---------+
    ```
  - bound : 내림
    ```scala
    df.select(round(lit("2.5")), bround(lit("2.5"))).show(2)
    +-------------+--------------+
    |round(2.5, 0)|bround(2.5, 0)|
    +-------------+--------------+
    |          3.0|           2.0|
    |          3.0|           2.0|
    +-------------+--------------+
    ```
  - corr : 피어슨 상관계수 구하기
    ```scala
    import org.apache.spark.sql.functions.{corr}

    df.stat.corr("Quantity", "UnitPrice")
    df.select(corr("Quantity", "UnitPrice")).show()

    +-------------------------+
    |corr(Quantity, UnitPrice)|
    +-------------------------+
    |     -0.04112314436835551|
    +-------------------------+
    ```

  - describe : 요약 통계 (집계(count), 평균(mean), 표준편차(stddev), 최솟값(min), 최댓값(max))
    ```scala
    df.describe().show()

    +-------+-----------------+------------------+--------------------+------------------+------------------+------------------+--------------+
    |summary|        InvoiceNo|         StockCode|         Description|          Quantity|         UnitPrice|        CustomerID|       Country|
    +-------+-----------------+------------------+--------------------+------------------+------------------+------------------+--------------+
    |  count|             3108|              3108|                3098|              3108|              3108|              1968|          3108|
    |   mean| 536516.684944841|27834.304044117645|                null| 8.627413127413128| 4.151946589446603|15661.388719512195|          null|
    | stddev|72.89447869788873|17407.897548583845|                null|26.371821677029203|15.638659854603892|1854.4496996893627|          null|
    |    min|           536365|             10002| 4 PURPLE FLOCK D...|               -24|               0.0|           12431.0|     Australia|
    |    max|          C536548|              POST|ZINC WILLIE WINKI...|               600|            607.49|           18229.0|United Kingdom|
    +-------+-----------------+------------------+--------------------+------------------+------------------+------------------+--------------+
    ```

  - describe를 사용하지않고 각 함수를 개별사용 : 
    count 집계, min 최솟값, max 최댓값, stddev 표준편차, mean 평균

    ```scala
    df.select(count('InvoiceNo), min('InvoiceNo), max('InvoiceNo), mean('InvoiceNo), stddev('InvoiceNo)).show

    +----------------+--------------+--------------+----------------+----------------------+
    |count(InvoiceNo)|min(InvoiceNo)|max(InvoiceNo)|  avg(InvoiceNo)|stddev_samp(InvoiceNo)|
    +----------------+--------------+--------------+----------------+----------------------+
    |            3108|        536365|       C536548|536516.684944841|     72.89447869788873|
    +----------------+--------------+--------------+----------------+----------------------+
    ```
- StatFunctions패키지의 다양한 통계 함수 사용
  - 데이터의 백분위수를 계산
    ```scala
    val colName = "UnitPrice"
    val quantileProbs = Array(0.5)
    val relError = 0.05
    var value = df.stat.approxQuantile("UnitPrice", quantileProbs, relError)
    println(value(0)) //2.51
    ```

  - 교차표
    ```scala
    df.stat.crosstab("StockCode", "Quantity").limit(3).show()

    +------------------+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
    |StockCode_Quantity| -1|-10|-12| -2|-24| -3| -4| -5| -6| -7|  1| 10|100| 11| 12|120|128| 13| 14|144| 15| 16| 17| 18| 19|192|  2| 20|200| 21|216| 22| 23| 24| 25|252| 27| 28|288|  3| 30| 32| 33| 34| 36|384|  4| 40|432| 47| 48|480|  5| 50| 56|  6| 60|600| 64|  7| 70| 72|  8| 80|  9| 96|
    +------------------+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
    |             22578|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  1|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|
    |             21327|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  2|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|
    |             22064|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  1|  0|  0|  0|  1|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  1|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|  0|
    +------------------+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+---+
    ```
  - 자주 사용하는 항목 쌍 확인
    ```scala
    df.stat.freqItems(Seq("StockCode", "Quantity")).show()

    +--------------------+--------------------+
    | StockCode_freqItems|  Quantity_freqItems|
    +--------------------+--------------------+
    |[90214E, 20728, 2...|[200, 128, 23, 32...|
    +--------------------+--------------------+
    ```
  - 모든 로우에 고유 ID 값을 추가 (0부터 시작)
    ```scala
    import org.apache.spark.sql.functions.monotonically_increasing_id

    df.select(monotonically_increasing_id()).show(2) // 0부터 시작
    // df.select(monotonically_increasing_id() + 1 ).show(2) // 1부터 시작

    +-----------------------------+
    |monotonically_increasing_id()|
    +-----------------------------+
    |                            0|
    |                            1|
    +-----------------------------+
    ```

## 6.5 문자열 데이터 타입 다루기
- 정규 표현식을 사용해서 데이터 추출, 치환, 문자열 존재 여부, 대/소문자 변환 처리 가능
- initcap : 문자열을 공백으로 나눠 모든 단어의 첫 글자를 대문자로 변경
  ```scala
  import org.apache.spark.sql.functions.{initcap}

  df.select(initcap(col("Description"))).show(2, false)
  ```
- lower : 문자열 전체를 소문자로 변경  
upper : 문자열 전체를 대문자로 변경
  ```scala
  import org.apache.spark.sql.functions.{lower, upper}

  df.select(col("Description"),
  lower(col("Description")),
  upper(lower(col("Description")))).show(2)
  ```
- ltrim : 좌측 공백제거  
  rtrim : 우측 공백제거  
  trim : 공백제거  
  lpad : 좌측에 값 추가  
  rpad : 우측에 값 추가
  ```scala
  import org.apache.spark.sql.functions.{lit, ltrim, rtrim, rpad, lpad, trim}

  df.select(
      ltrim(lit("    HELLO    ")).as("ltrim"),
      rtrim(lit("    HELLO    ")).as("rtrim"),
      trim(lit("    HELLO    ")).as("trim"),
      lpad(lit("HELLO"), 3, " ").as("lp"),
      rpad(lit("HELLO"), 10, " ").as("rp")).show(2)
  ```

## 6.5.1 정규 표현식
- 자바의 정규 표현식을 활용

- regexp_replace : 값 치환
  ```scala
  import org.apache.spark.sql.functions.regexp_replace

  val simpleColors = Seq("black", "white", "red", "green", "blue")
  val regexString = simpleColors.map(_.toUpperCase).mkString("|")
  // the | signifies `OR` in regular expression syntax
  df.select(
    regexp_replace(col("Description"), regexString, "COLOR").alias("color_clean"),
    col("Description")).show(2)
  ```
- regexp_extract : 값 추출 
  ```scala
  import org.apache.spark.sql.functions.regexp_extract

  val regexString = simpleColors.map(_.toUpperCase).mkString("(", "|", ")")
  // the | signifies OR in regular expression syntax
  df.select(
      regexp_extract(col("Description"), regexString, 1).alias("color_clean"),
      col("Description")).show(2)
  ```
- translate : 별도의 정규식을 설정안해도 문자를 교체해준다. (문자 한글자 단위로)
  ```scala
  import org.apache.spark.sql.functions.translate

  df.select(translate(col("Description"), "LEET", "1337"), col("Description"))
    .show(2)
  ```
- contains : 값 추출없이 단순히 값의 존재 여부를 확인
  ```scala
  // hasSimpleColor라는 boolean값으로 filter한다.
  val containsBlack = col("Description").contains("BLACK")
  val containsWhite = col("DESCRIPTION").contains("WHITE")
  df.withColumn("hasSimpleColor", containsBlack.or(containsWhite))
    .where("hasSimpleColor")
    .select("Description").show(3, false)
  
  +----------------------------------+
  |Description                       |
  +----------------------------------+
  |WHITE HANGING HEART T-LIGHT HOLDER|
  |WHITE METAL LANTERN               |
  |RED WOOLLY HOTTIE WHITE HEART.    |
  +----------------------------------+

  // 동적으로 인수가 변하는 상황에서의 filter
  val simpleColors = Seq("black", "white", "red", "green", "blue")
  val selectedColumns = simpleColors.map(color => {
    col("Description").contains(color.toUpperCase).alias(s"is_$color")
  }):+expr("*") // could also append this value

  df.select(selectedColumns:_*).where(col("is_white").or(col("is_red")))
    .select("Description").show(3, false)

  +----------------------------------+
  |Description                       |
  +----------------------------------+
  |WHITE HANGING HEART T-LIGHT HOLDER|
  |WHITE METAL LANTERN               |
  |RED WOOLLY HOTTIE WHITE HEART.    |
  +----------------------------------+
  ```

## 6.6 날짜와 타임스탬프 데이터 타입 다루기
- 스파크는 두가지 종류의 시간 관련 정보만 집중 관리한다.  
  날짜(date), 날짜+시간정보(timestamp)
- inferSchema옵션이 활성화된 경우 컬럼의 데이터 타입을 최대한 정확하게 식별하려 시도한다.
- 스파크는 날짜 포맷을 명시하지않아도 데이터 타입을 자동으로 찾아준다.
- 스파크는 자바의 날짜와 타임스탬프를 사용한다.

- current_date()  : 현재 날짜  
  current_timestamp() : 현재 날짜+시간
  ```scala
  // 현재 날짜, 날짜+시간 조회
  val dateDF = spark.range(10)
    .withColumn("today", current_date())
    .withColumn("now",   current_timestamp())
  dateDF.createOrReplaceTempView("dateTable")
  dateDF.show(3)

  +---+----------+--------------------+
  | id|     today|                 now|
  +---+----------+--------------------+
  |  0|2020-03-11|2020-03-11 05:27:...|
  |  1|2020-03-11|2020-03-11 05:27:...|
  |  2|2020-03-11|2020-03-11 05:27:...|
  +---+----------+--------------------+
  ```
- date_sub() : 오늘 날짜에서 빼기  
  date_add() : 오늘 날짜에서 더하기
  ```scala
  import org.apache.spark.sql.functions.{date_add, date_sub}

  dateDF.select(date_sub(col("today"), 5), date_add(col("today"), 5)).show(3)
  ```

- datediff() : 두날짜 사이의 일수 반환  
  months_between() : 두날짜 사이의 개월수 반환
  ```scala
  import org.apache.spark.sql.functions.{datediff, months_between, to_date}

  dateDF.withColumn("week_ago", date_sub(col("today"), 7))
    .select(datediff(col("week_ago"), col("today"))).show(1)
    
  dateDF.select(
      to_date(lit("2016-01-01")).alias("start"),
      to_date(lit("2017-05-22")).alias("end"))
    .select(months_between(col("start"), col("end"))).show(1)
  ```

- to_date() : 문자열을 날짜로 변환  
  ```scala
  import org.apache.spark.sql.functions.{to_date, lit}
  
  spark.range(5).withColumn("date", lit("2017-01-01"))
    .select(to_date(col("date"))).show(1)
  +---------------+
  |to_date(`date`)|
  +---------------+
  |     2017-01-01|
  +---------------+

  // 날짜를 파싱할수없는 잘못된 값이 들어올경우 에러 대신 null 반환
  dateDF.select(to_date(lit("2016-20-12")),to_date(lit("2017-12-11"))).show(1)
  
  +---------------------+---------------------+
  |to_date('2016-20-12')|to_date('2017-12-11')|
  +---------------------+---------------------+
  |                 null|           2017-12-11|
  +---------------------+---------------------+

  // to_date사용시 format지정 (dateFormat은 경우에따라 없어도 된다)
  import org.apache.spark.sql.functions.to_date

  val dateFormat = "yyyy-dd-MM"
  val cleanDateDF = spark.range(1).select(
      to_date(lit("2017-12-11"), dateFormat).alias("date"),  // format이 없어도 된다.
      to_date(lit("2017-20-12"), dateFormat).alias("date2")) // format이 있어야 한다
  cleanDateDF.createOrReplaceTempView("dateTable2")
  cleanDateDF.show

  +----------+----------+
  |      date|     date2|
  +----------+----------+
  |2017-11-12|2017-12-20|
  +----------+----------+
  ```

- to_timestamp() : 문자열을 날짜+시간으로 변환
  ```scala
  import org.apache.spark.sql.functions.to_timestamp
  
  cleanDateDF.select(to_timestamp(col("date"), dateFormat)).show()

  +----------------------------------+
  |to_timestamp(`date`, 'yyyy-dd-MM')|
  +----------------------------------+
  |               2017-12-11 00:00:00|
  +----------------------------------+
  ```

- 날짜비교
  ```scala
  cleanDateDF.filter(col("date2") > lit("2017-12-12")).show() // lit함수 사용
  cleanDateDF.filter(col("date2") > "'2017-12-12'").show() // 문자열 리터럴

  +----------+----------+
  |      date|     date2|
  +----------+----------+
  |2017-11-12|2017-12-20|
  +----------+----------+
  ```

## 6.7 null 값 다루기
- 6.7.1 ~ 6.7.5에서 설명
- 스파크에서는 빈 문자열이나 대체 값 대신 null 값을 사용해야 최적화를 수행할 수 있다.
- 스파크에는 명시적으로 null값을 갖고있는 row를 제거하거나, null값을 특정값으로 채워넣는 기능이 있다.

## 6.7.1 coalesce
- 인수로 지정한 여러 컬럼중 null이 아닌 첫번째 값을 반환
- 모든 컬럼이 null이 아닌 값을 가지는 경우 첫번째 컬럼의 값을 반환
  ```scala
  import org.apache.spark.sql.functions.coalesce

  df.select(coalesce(col("Description"), col("CustomerId"))).show()
  ```

## 6.7.2 ifnull, nullif, nvl, nvl2
- ifnull : 첫번째 값이 null이면 두번째 값을 반환, 첫번째 값이 null이 아니면 첫번째 값을 반환 
- nullif : 두값이 같으면 null을 반환, 두값이 다르면 첫번째 값을 반환
- nvl : 첫번째 값이 null이면 두번째 값을 반환, 첫번째 값이 null이 아니면 첫번째 값을 반환
- nvl2 : 첫번째 값이 null이 아니면 두번째 값을 반환, 첫번째 값이 null이면 세번째 인수로 지정된 값을 반환

  ```scala
  // spark sql
  SELECT 
    ifnull(null, 'return_value'),
    nullif('value', 'value'),
    nvl(null, 'return_value'),
    nvl2('not_null','return_value','else_value')
    FROM dfTable LIMIT 1
  ```

## 6.7.3 drop
- null값을 가진 로우를 제거한다.
- drop메서드의 인수로 any를 지정한 경우 컬럼값중 하나라도 null을 가지면 해당 로우를 제거한다. 
- all을 지정한 경우 모든 컬럼의 값이 null이거나 NaN인 경우에만 해당 로우를 제거한다.

  ```scala
  df.na.drop()
  df.na.drop("any") 
  // => SQL로 변환하면 SELECT * FROM 테이블 WHERE Description IS NOT NULL

  df.na.drop("all")
  df.na.drop("all", Seq("StockCode", "InvoiceNo"))
  ```

## 6.7.4 fill
- 하나 이상의 컬럼을 특정 값으로 채울 수 있다.

  ```scala
  // String데이터 타입의 null값을 다른 값으로 채움
  df.na.fill("All Null values become this string")

  // Integer데이터 타입의 null값을 다른 값으로 채움
  df.na.fill(5, Seq("StockCode", "InvoiceNo"))

  // Map타입을 사용해 다수의 컬럼에 fill메서드 적용
  val fillColValues = Map("StockCode" -> 5, "Description" -> "No Value")
  df.na.fill(fillColValues)
  ```

## 6.7.5 replace
- drop, fill 메서드 외에도 null값을 유연하게 대처하기위한 방법이며
조건에 따라 다른값으로 대체(replace)한다.

  ```scala
  // Description컬럼 값이 ""인경우 UNKNOWN으로 변경
  df.na.replace("Description", Map("" -> "UNKNOWN"))
  ```

## 6.8 정렬하기
- asc_nulls_first, desc_nulls_first, asc_nulls_last, desc_nulls_last 함수로
DataFrame을 정렬할때 null값이 표시되는 기준을 지정할 수 있다.
- [Column API 링크](https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.Column)

## 6.9 복합 데이터 타입 다루기
- 6.9.1 ~ 6.9.3에서 설명
- 복합 데이터 타입에는 구조체(struct), 배열(array), 맵(map)이 있다.

## 6.9.1 구조체
- DataFrame내부의 DataFrame으로 생각할 수 있다.
- 쿼리문에서 다수의 컬럼을 괄호로 묶어 구조체를 만들 수 있다.

  ```scala
  import org.apache.spark.sql.functions.struct

  val complexDF = df.select(struct("Description", "InvoiceNo").alias("complex"))
  complexDF.createOrReplaceTempView("complexDF")

  complexDF.select("complex.Description")
  complexDF.select(col("complex").getField("Description"))

  complexDF.select("complex.*")
  ```

## 6.9.2 배열
- split : 문자열을 배열로 변환

  ```scala
  import org.apache.spark.sql.functions.split

  df.select(split(col("Description"), " ")).show(2)
  ```

- size :배열의 크기를 조회
  ```scala
  import org.apache.spark.sql.functions.size

  df.select(size(split(col("Description"), " "))).show(2) // shows 5 and 3
  df.createOrReplaceTempView("test")
  ```

- array_contains : 배열에 특정 값이 존재하는지 확인

  ```scala
  import org.apache.spark.sql.functions.array_contains

  df.select(array_contains(split(col("Description"), " "), "WHITE")).show(2)
  ```

- explode : 배열을 받아서 로우로 변환
  ```scala
  import org.apache.spark.sql.functions.{split, explode}

  val testDF = df.withColumn("splitted", split(col("Description"), " "))
        .withColumn("exploded", explode(col("splitted")))
        .select("Description", "InvoiceNo", "exploded", "splitted")
  testDF.show(5)

  +--------------------+---------+--------+--------------------+
  |         Description|InvoiceNo|exploded|            splitted|
  +--------------------+---------+--------+--------------------+
  |WHITE HANGING HEA...|   536365|   WHITE|[WHITE, HANGING, ...|
  |WHITE HANGING HEA...|   536365| HANGING|[WHITE, HANGING, ...|
  |WHITE HANGING HEA...|   536365|   HEART|[WHITE, HANGING, ...|
  |WHITE HANGING HEA...|   536365| T-LIGHT|[WHITE, HANGING, ...|
  |WHITE HANGING HEA...|   536365|  HOLDER|[WHITE, HANGING, ...|
  +--------------------+---------+--------+--------------------+
  ```

## 6.9.3 맵
- map함수로 컬럼의 키-값 쌍을 이용해 생성.

  ```scala
  import org.apache.spark.sql.functions.map

  // map함수로 map생성
  df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map")).show(2)

  // map에 key를 전달하면 value가 반환
  df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))
    .selectExpr("complex_map['WHITE METAL LANTERN']").show(2)
  +--------------------------------+
  |complex_map[WHITE METAL LANTERN]|
  +--------------------------------+
  |                            null|
  |                          536365|
  +--------------------------------+

  // explode함수로 map을 row로 반환
  df.select(map(col("Description"), col("InvoiceNo")).alias("complex_map"))
    .selectExpr("explode(complex_map)").show(2)
  +--------------------+------+
  |                 key| value|
  +--------------------+------+
  |WHITE HANGING HEA...|536365|
  | WHITE METAL LANTERN|536365|
  +--------------------+------+
  ```

## 6.10 JSON 다루기
- 스파크에서는 문자열 형태의 JSON을 직접 조작할 수 있다.
- JSON문자열을 DataFrame으로 변환 가능.
  ```scala
  val jsonDF = spark.range(1).selectExpr("""
    '{"myJSONKey" : {"myJSONValue" : [1, 2, 3]}}' as jsonString""")
    jsonDF.show

  +--------------------+
  |         complex_map|
  +--------------------+
  |[WHITE HANGING HE...|
  |[WHITE METAL LANT...|
  +--------------------+
  ```

- JSON객체를 인라인 쿼리로 조회. 중첩이 없는 단일 JSON객체라면 json_tuple을 사용.
  ```scala
  import org.apache.spark.sql.functions.{get_json_object, json_tuple}

  jsonDF.select(
      get_json_object(col("jsonString"), "$.myJSONKey.myJSONValue[1]") as "column",
      json_tuple(col("jsonString"), "myJSONKey")).show(2)
  
  +------+--------------------+
  |column|                  c0|
  +------+--------------------+
  |     2|{"myJSONValue":[1...|
  +------+--------------------+
  ```

- to_json => 문자열을 객체로 변환
  ```scala
  import org.apache.spark.sql.functions.to_json

  df.selectExpr("(InvoiceNo, Description) as myStruct")
    .select(to_json(col("myStruct"))).show(2)

  +-----------------------+
  |structstojson(myStruct)|
  +-----------------------+
  |   {"InvoiceNo":"536...|
  |   {"InvoiceNo":"536...|
  +-----------------------+
  ```

- from_json => 객체를 문자열로 변환
  ```scala
  import org.apache.spark.sql.functions.from_json
  import org.apache.spark.sql.types._

  // from_json시에 스키마 지정 필수
  val parseSchema = new StructType(Array(
    new StructField("InvoiceNo",StringType,true),
    new StructField("Description",StringType,true)))
    
  // InvoiceNo, Description컬럼을 Map으로 변환후
  // 문자열을 객체로 변환, 객체를 문자열로
  df.selectExpr("(InvoiceNo, Description) as myStruct")
    .select(to_json(col("myStruct")).alias("newJSON"))
    .select(from_json(col("newJSON"), parseSchema), col("newJSON")).show(2)

  +---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
  |InvoiceNo|StockCode|         Description|Quantity|        InvoiceDate|UnitPrice|CustomerID|       Country|
  +---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
  |   536365|   85123A|WHITE HANGING HEA...|       6|2010-12-01 08:26:00|     2.55|   17850.0|United Kingdom|
  |   536365|    71053| WHITE METAL LANTERN|       6|2010-12-01 08:26:00|     3.39|   17850.0|United Kingdom|
  +---------+---------+--------------------+--------+-------------------+---------+----------+--------------+
  ```

## 6.11 사용자 정의 함수
- 스파크는 사용자 정의 함수(User Defined Function)를 사용할 수 있다.
- UDF는 하나 이상의 컬럼을 입력으로 받고, 데이터를 반환할 수 있다. (RDB의 function과 같다.)
- 스칼라, 파이썬, 자바로 UDF를 개발할 수 있지만 언어별로 동작하는 방식이 다르므로 성능에 영향을 미칠 수 있다.
  ```scala
  import org.apache.spark.sql.functions.udf

  // UDF정의
  val power3udf = udf(power3(_:Double):Double)
  // Spark SQL및 문자열 표현식에서 사용하기위한 등록
  spark.udf.register("power3", power3(_:Double):Double)
  // 실행
  udfExampleDF.selectExpr("power3(num)").show(2)
  ```
- (일반코드) 스칼라,자바,파이썬 + (UDF) 스칼라, 자바 => 권장 (데이터는 JVM에서만 처리)
- (일반코드) 스칼라,자바,파이썬 + (UDF) 파이썬 => 워커노드의 익스큐터(JVM)와 워커노드의 파이썬 프로세서간의 데이터 직렬화에 큰 부하가 발생하기 때문에 권장하지않는다. (데이터가 JVM과 파이썬 프로세서간에 직렬화로 오고 간다.)
- [UDF가 언어별 동작하는 방식](https://dzone.com/articles/pyspark-java-udf-integration-1)



## 6.12 Hive UDF
- 하이브 문법을 이용해서도 UDF를 사용할 수 있다.
- SparkSession생성시 SparkSession.builder().enableHiveSupport() 를 명시 해야한다.

## 6.13 정리
- 스파크 SQL을 사용하면 SQL을 알고있는 일반 개발자들이 쉽게 스파크를 사용할 수 있다.
- 이 장에서는 Dataset, Column의 필수 API를 설명했다.

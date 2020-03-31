# 12. RDD

## 12.1 저수준 API란?
- 두 종류의 저수준 API가 있다.
  - 분산 데이터 처리 => RDD
  - 분산형 공유 변수 => 브로드캐스트 변수와 어큐뮬레이터

### 12.1.1 저수준 API는 언제 사용할까

- 다음과 같은 상황에서 저수준 API를 사용한다.
  - 고수준 API로 할 수 없는 세밀한 작업을 해야 할 경우.
    - 클러스터의 물리적 데이터 배치를 해야 하는 경우
    - 데이터의 세밀한 재처리가 필요한 경우
  - 기존 코드가 RDD로 되어 있는 경우.
  - 사용자가 정의한 공유 변수를 다루어야 하는 경우.

- 스파크의 모든 **워크로드**는 저수준 형태로 컴파일 된다.

- 스파크를 잘 알고 있는 개발자라 하더라도 구조적 API (Dataset, DataFrame, SQL) 위주로 사용하는 것이 좋다. (최적화를 자동으로 해주기 때문)

- DataFrame의 트랜스포메이션을 호출하면 실제로 다수의 RDD 트랜스포메이션으로 변환된다.

### 12.1.2 저수준 API는 어떻게 사용할까

- 저수준 API에 접근하려면 sparkContext 를 통해서 해야 한다.

  ```
    spark.sparkContext
  ```

## 12.2 RDD 개요
- 불변성이고 병렬처리할 수 있는 파티셔닝된 레코드의 모음.

- RDD는 스파크 1.x버전의 핵심 API이다.

- 사용자가 실행한 모든 DataFrame, Dataset 코드는 RDD로 컴파일된다.

- RDD단위로 Job이 수행된다. (스파크 UI에서 확인가능.)

- DataFrame과 RDD의 차이
  - DataFrame의 레코드 : 스키마를 알고 있는 필드로 구성된 구조화된 로우
  - RDD의 레코드 : 자바, 스칼라, 파이썬의 객체.

- 스파크2.x 버전에서도 사용할 수 있지만 잘 사용하지 않는다.
  - 최적화에 수작업이 필요하다.
    - 구조적 API는 자동으로 데이터를 최적화하고 압축된 바이너리 포맷으로 저장하지만   
    저수준 API에서 동일한 공간 효율성과 성능을 얻으려면 이런 포맷 타입을 직접 구현해  
    모든 저수준 연산 과정에서 사용해야 한다.
    - 스파크SQL에서 자동으로 수행되는 필터 재정렬과 집계 같은 최적화 기법을 직접 구현해야한다.

- RDD와 Dataset 사이의 전환은 매우 쉬우므로 경우에 따라서 두 API를 모두 사용해 장점을 활용할 수 있다.

### 12.2.1 RDD 유형
- 스파크 API에는 수많은 RDD 하위 클래스가 존재한다.

- 사용자는 두 가지 타입의 RDD를 만들 수 있다.
  - 제네릭 RDD
  - 키-값 RDD

- 각 RDD는 RDD abstract class를 구현한 구현체이다.  
scala RDD api => https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.rdd.RDD  
scala RDD source => https://github.com/apache/spark/blob/v2.4.5/core/src/main/scala/org/apache/spark/rdd/RDD.scala

- 각 RDD는 다음 다섯가지 주요 메서드(괄호 안의 메서드)를 구현한다.
  - 파티션의 목록 (getPartitions)
  - 각 조각을 연산하는 함수 (compute)
  - 다른 RDD와의 의존성 목록 (getDependencies)
  - 부가적으로(Optionally) RDD가 어떻게 파티션될지 지정  (partitioner)  
  예) HashPartition, RangePartition
  - 부가적으로(Optionally) 각 조각을 연산하기 위한 기본 위치 목록 (getPreferredLocations)  
  예) HDFS 파일의 블록 위치

- RDD에는 '로우'라는 개념이 없다. (String, Integer같은 객체일뿐)

- 파이썬에서는 자바, 스칼라에서 보다 성능저하가 크기 때문에 구조적 API사용이 권장된다.

### 12.2.2 RDD는 언제 사용할까
- 정말 필요한 경우가 아니라면 수동으로 RDD를 생성하지 않는다.

- 데이터의 세부적인 제어가 필요할때 사용.

### 12.2.3 Dataset과 RDD의 케이스 클래스
- Dataset과 case class를 사용해서 만들어진 RDD의 차이점
  - 케이스 클래스를 사용해서 만들어진 RDD는 **RDD**``[클래스타입``]일 뿐이다.  
  Dataset은 구조적 API가제공하는 풍부한 기능과 최적화기법을 사용할 수 있고 RDD는 아니다.
   
## 12.3 생성
- 지금까지는 RDD의 주요 속성을 알아보았고 이제 RDD를 사용하는 방법을 알아보겠다.

### 12.3.1 DataFrame, Dataset으로 RDD 생성하기
- RDD를 얻을 수 있는 가장 쉬운 방법은 DataFrame, Dataset의 rdd메서드를 호출하는것이다.
  ```scala
  // in Scala: converts a Dataset[Long] to RDD[Long]
  spark.range(500).rdd

  // DataFrame을 rdd로 변환하면 Row객체가 나온다.
  // org.apache.spark.rdd.RDD[Long]
  spark.range(10).toDF().rdd.map(rowObject => rowObject.getLong(0)) // row => Long으로 변환
  ```

- rdd를 DataFrame, Dataset으로 변환.
  ```scala
  spark.range(10).rdd.toDF()
  ```

### 12.3.2 로컬 컬렉션으로 RDD 생성하기
- 컬렉션 객체를 RDD로 만드는 방법 (단일 노드의 컬렉션을 병렬 컬렉션으로 전환)
  ```scala
  // myCollection을 " "으로 나눠서 배열로 만든다음 RDD로 변환한다. (파티션 개수는 2개)
  val myCollection = "Spark The Definitive Guide : Big Data Processing Made Simple"
    .split(" ")
  val words = spark.sparkContext.parallelize(myCollection, 2)

  // RDD에 이름을 지정하면 스파크 UI에 지정한 이름으로 RDD가 표시된다.
  words.setName("myWords")
  words.name // myWords
  ```

### 12.3.3 데이터소스로 RDD 생성하기
- DataSource API (csv, json, textFile, parquet등)로부터 RDD를 생성하는것이 좋다.
```scala
// 줄 단위로 텍스트 파일을 읽는다.
spark.sparkContext.textFile("/some/path/withTextFiles")

// 파일명은 Key로 파일의 내용을 value로
spark.sparkContext.wholeTextFiles("/some/path/withTextFiles")
```

## 12.4 RDD 다루기
- DataFrame을 다루는 방식과 매우 유사하다.

## 12.5 트랜스포메이션
- DataFrame, Dataset과 동일하게 RDD에 트랜스포메이션을 지정해 새로운 RDD를 생성할 수 있다.  

### 12.5.1 distinct
- 중복 데이터 제거
  ```scala
  words.distinct().count() // 10
  ```

### 12.5.2 filter
- SQL의 where와 비슷.
- RDD의 모든 레코드를 확인하고 조건 함수를 만족하는 레코드만 반환.

- 문자 'S'로 시작하는 단어만 남도록 RDD를 필터링.
  ```scala
  def startsWithS(individual:String) = {
    individual.startsWith("S")
  }

  words.filter(word => startsWithS(word)).collect()
  ```

### 12.5.3 map
- 주어진 입력을 원하는 값으로 변환
  ```scala
  // 단어 문자열을 (단어, 단어의 시작 문자, 첫 문자가 S인지 아닌지)로 매핑
  val words2 = words.map(word => (word, word(0), word.startsWith("S")))

  // 위에서 매핑한 값중 세번째 값(불리언)을 가져와서 필터링
  words2.filter(record => record._3).take(5)
  ```
- flatMap 메서드
  - map함수의 확장 버전.
  - rdd에 flatMap함수를 적용해서 구조를 평평하게 한다.  
  예) 
  [ [1,2,3], [4,5,6] ]를 [1,2,3,4,5,6]같이 변환한다.

    ```scala
    val lines = sc.parallelize(List("hello spark","hi"))
    val words = lines.flatMap(line=>line.split(" "))
    println(words.take(3).mkString(" , ")) // hello , spark , hi
    ```

### 12.5.4 sortBy
- 단어길이 내림차순 (긴것부터 짧은 순)
  ```scala
  words.sortBy(word => word.length() * -1).take(3)
  ```

- 단어길이 오름차순 (짧은것부터 긴 순)
  ```scala
  words.sortBy(word => word.length() * 1).take(3)
  ```

### 12.5.5 randomSplit
- RDD를 임의로 분한해 RDD배열을 만든다.
  ```scala
  val fiftyFiftySplit = words.randomSplit(Array[Double](0.6, 0.5)) // 가중치(0.6), 난수시드(0.5)
  println(fiftyFiftySplit(0).collect().mkString(" - ")) 
  println(fiftyFiftySplit(1).collect().mkString(" - ")) 
  ```

## 12.6 액션
- DataFrame, Dataset과 동일하다.
- 액션은 데이터를 드라이버로 모으거나 외부 데이터소스로 내보낼 수 있다.

### 12.6.1 reduce
- rdd의 모든 값을 하나의 값으로 만들경우 사용  


  ```scala
  // 정수형 배열의 총합을 구한다.
  spark.sparkContext.parallelize(1 to 20).reduce(_ + _) // 210

  // 가장 긴 단어를 찾는 함수를 정의해서 reduce호출.
  def wordLengthReducer(leftWord:String, rightWord:String): String = {
    if (leftWord.length > rightWord.length)
      return leftWord
    else
      return rightWord
  }

  words.reduce(wordLengthReducer)
  ```

### 12.6.2 count
- count : RDD의 전체 로우 수를 알 수 있다.
  ```scala
  words.count()
  ```
- countApprox : count함수의 근사치를 제한 시간 내에 계산 (시간 초과시 불완전한 결과를 반환)
  ```scala
  val confidence = 0.95 // 신뢰도 : 0.9로 설정했을경우 정확도가 90%이다. (0 ~ 1 사이)
  val timeoutMilliseconds = 400 // timeout 밀리초
  val cnt_ = words.countApprox(timeoutMilliseconds, confidence)
  println(cnt_.getFinalValue.high) // double value
  println(cnt_.getFinalValue.low) // double value
  ```

- countApproxDistinct : HyperLogLog 실전 활용: 최신 카디널리티 추정 알고리즘 엔지니어링을   
기반으로한 두 가지구현체가 있다.
  ```scala
  // 상대 정확도(relative accuracy)를 파라미터로 사용
  // 설정값은 반드시 0.000017보다 커야 한다.
  words.countApproxDistinct(0.05) // long value

  // 정밀도와, 희소 정밀도를 파라미터로 사용한다.
  words.countApproxDistinct(4, 10) // long value
  ```

- countByValue : RDD 값의 개수를 구한다.
  ```scala
  words.countByValue() 
  // Map(Definitive -> 1, Simple -> 1, Processing -> 1, The -> 1, Spark -> 1, Made -> 1, Guide -> 1, Big -> 1, : -> 1, Data -> 1)

  // 값 가져오기
  words.countByValue().getOrElse("Simple", 1)
  ```

- countByValueApprox : count함수와 동일한 연산을 수행하지만 근사치를 계산한다.   
Approximate version of countByValue(). (시간 초과시 불완전한 결과를 반환)
  ```scala
  words.countByValueApprox(1000, 0.95) // 제한시간(ms), 신뢰도를 파라미터로 전달
   
  // 값 가져오기
  import org.apache.spark.partial.BoundedDouble
  val bd:BoundedDouble = words.countByValueApprox(1000, 0.95).getFinalValue.getOrElse("Simple", new BoundedDouble(0, 0, 10, 10))
  println(bd.high)
  ```

### 12.6.3 first
- RDD의 첫번째 값을 반환한다.
  ```scala
  words.first()
  ```
### 12.6.4 max와 min
- 최댓값, 최솟값을 반환한다.
  ```scala
  spark.sparkContext.parallelize(1 to 20).max() // 20
  spark.sparkContext.parallelize(1 to 20).min() // 1
  ```

### 12.6.5 take
- take와 take의 파생 메서드는 RDD에서 가져올 값의 개수를 파라미터로 사용한다.
  ```scala
  // 5개를 가져온다
  words.take(5) // Array(Spark, The, Definitive, Guide, :)
  
  // 5개를 알파벳순으로 가져온다
  words.takeOrdered(5) // Array(:, Big, Data, Definitive, Guide)

  // 5개를 알파벳 역순으로 가져온다. (reverse를 명시)
  words.takeOrdered(5)(Ordering[String].reverse) // Array(The, Spark, Simple, Processing, Made)

  // 암시적(implicit)순서에 따라 최상위값을 선택한다.
  words.top(5) // Array(The, Spark, Simple, Processing, Made)
  sc.parallelize(Seq(10, 4, 2, 12, 3)).top(1) // returns Array(12)
  sc.parallelize(Seq(2, 3, 4, 5, 6)).top(2)// returns Array(6, 5)

  // 고정 크기의 임의 표본 데이터를 얻는다.
  val withReplacement = true // 임의 표본 수
  val numberToTake = 6  // 가져올 개수
  val randomSeed = 100L // 난수 시드값
  words.takeSample(withReplacement, numberToTake, randomSeed)
  //  Array(The, Processing, :, Simple, Spark, Data)
  ```

## 12.7 파일 저장하기
- 데이터 처리 결과를 일반 텍스트 파일로 쓰는 것을 의미 한다.

- 각 파티션의 내용을 저장하려면 전체 파티션을 순회하면서 외부 데이터베이스에 저장해야 한다.

### 12.7.1 saveAsTextFile
- 텍스트 저장
  ```scala
  words.saveAsTextFile("file:///tmp/bookTitle")

  // 입축 지정
  import org.apache.hadoop.io.compress.BZip2Codec

  words.saveAsTextFile("file:///tmp/bookTitleCompressed", classOf[BZip2Codec])
  ```

### 12.7.2 시퀀스 파일
- 시퀀스 파일은 바이너리 키-값 쌍으로 구성된 플랫 파일이며   
하둡 맵리듀스의 입출력 포맷으로 널리 사용된다.
  ```scala
  words.saveAsObjectFile("file:///tmp/my/sequenceFilePath")
  ```

### 12.7.3 하둡 파일
- 데이터를 저장하는데 사용할 수 있는 여러 가지 하둡 파일 포맷이 있다.

- 맵리듀스 잡을 깊이 있게 다루는 경우가 아니라면 크게 신경쓰지 않아도 된다.

## 12.8 캐싱
- DataFrame, Dataset의 캐싱과 동일한 원칙이 적용된다.
- 캐싱은 클러스터의 익스큐터 전반에 걸쳐 만들어진 임시 저장소에   
DataFrame, Dataset, RDD를 보관해 빠르게 접근할 수 있도록 한다.
- 기본적으로 캐시와 저장은 메모리에 있는 데이터만을 대상으로 한다.
- 19장 466~467p에 저장소 수준에 대한 더 자세한 정보가 있다.

  ```scala
  words.cache()
  println(words.getStorageLevel) // StorageLevel(memory, deserialized, 1 replicas)
  ```

## 12.9 체크포인트
- 체크포인팅은 DataFrame API에서는 사용할 수 없다.
- RDD를 디스크에 저장하는 것이다.
- 나중에 RDD를 참조할 때는 원본 데이터 소스에서 읽어들이지 않고 디스크에 저장된 중간 결과 파티션을 참조한다.
- 메모리에 저장하지 않고 디스크에 저장한다는 사실만 제외하면 캐싱과 유사하다.
- 반복적인 연산 수행시 매우 유용하다.
  ```scala
  spark.sparkContext.setCheckpointDir("/some/path/for/checkpointing")
  words.checkpoint()
  ```

## 12.10 RDD를 시스템 명령으로 전송하기
- 각 입력 파티션의 모든 요소는 개행 문자 단위로 분할되어 여러줄의 입력 데이터로 변경된 후   
프로세스의 표준 입력에 전달된다. (stdin)
- 결과 파티션은 프로세스의 표준 출력으로 생성된다. (stdout)
  ```scala
  words.pipe("wc -l").collect()
  ```

### 12.10.1 mapPartitions
- 파티션별로 데이터가 담긴 iterator객체를 받아서 처리한다.

- 함수 실행후 반환하는 RDD의 형태는 MapPartitionsRDD이다.

- mapPartitions는 각 파티션 별로 map연산을 수행할 수 있다.
  ```scala
  // 모든 파티션에 '1'값을 생성하고 표현식에 따라 파티션수를 세어 합산한다.
  words.mapPartitions(iterator => Iterator[Int](1)).sum() // 2

  // partition의 index와 iterator를 같이 받고 싶을때 사용
  def indexedFunc(partitionIndex:Int, withinPartIterator: Iterator[String]) = {
    withinPartIterator.toList.map(
      value => s"Partition: $partitionIndex => $value").iterator
  }
  words.mapPartitionsWithIndex(indexedFunc).collect()
  ```

### 12.10.2 foreachPartition
- 모든 데이터를 순회하며 처리하고 결과는 반환하지 않는다.
- 각 파티션의 데이터를 데이터베이스에 저장하는 것과 같이 개별 파티션에서 특정 작업을 수행하는 데 매우 적합한 함수.

- 임의로 생성한 ID를 이용해 임시 디렉터리에 결과를 저장하는 예제.
  ```scala
  words.foreachPartition { iter =>
    
    import java.io._
    import scala.util.Random

    val randomFileName = new Random().nextInt()
    val pw = new PrintWriter(new File(s"/tmp/random-file-${randomFileName}.txt"))
    
    while (iter.hasNext) {
        pw.write(iter.next())
    }
    pw.close()

  }
  ```

### 12.10.3 glom
- 모든 파티션을 드라이버로 모아서 배열로 변환하는 함수.  
파티션단위로 데이터가 배열로 묶인다.
- 모든 데이터가 드라이버로 전달되기 때문에  
파티션이 크거나 파티션 수가 많다면 드라이버가 비정상적으로 종료될 수 있다.

  ```scala
  spark.sparkContext.parallelize(Seq("Hello", "World"), 2).glom().collect()

  // 결과
  Array[Array[String]] = Array(Array(Hello), Array(World))
  ```

## 12.11 정리
- RDD API의 기초를 알아보았다.
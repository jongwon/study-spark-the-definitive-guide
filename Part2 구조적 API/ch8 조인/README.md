# 8. 조인

- 조인은 스파크 작업에서 매우 빈번하게 사용하는 기능이다.
- 스파크의 조인 타입과 사용법 그리고 클러스터상에서의 내부 동작방식에 대해서 알아본다.

## 8.1 조인 표현식

## 8.2 조인 타입
- 

|  <center>조인 타입</center>  | <center>설명</center>
| :---------: | :------------------------------|
|    inner    |  양쪽모두 키가 있는 로우를 유지              |
|    outer    |  한쪽이라도 키가 있는 로우를 유지         |
| left outer  |  왼쪽 로우에 키가 있는 경우만 유지                  |
| right outer |  오른쪽 로우에 키가 있는 경우만 유지                 |
|  left semi  |  왼쪽과 오른쪽에 모두 키가 있는 경우 왼쪽만 유지           |
| right semi  |  왼쪽과 오른쪽에 모두 키가 있는 경우 오른쪽만 유지          |
|  left anti  |  오른쪽에 없는 키가 있는 왼쪽 로우만 유지               |
|   natural   |  두 데이터 셋에서 동일한 이름을 갖는 컬럼을 암시적으로 결합하는 조인 |
|    cross    |  왼쪽한개당 오른쪽의 모든 경우를 조합해서 만듬 (nxm)         |

## 8.3 내부 조인
- 양쪽 DataFrame에 키가 있는지 확인해서 양쪽 모두 true인경우만 결합

    ```scala
    // 8장 전체에서 사용할 테이블 예제 등록

    val person = Seq(
        (0, "Bill Chambers", 0, Seq(100)),
        (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
        (2, "Michael Armbrust", 1, Seq(250, 100)))
    .toDF("id", "name", "graduate_program", "spark_status")
    
    val graduateProgram = Seq(
        (0, "Masters", "School of Information", "UC Berkeley"),
        (2, "Masters", "EECS", "UC Berkeley"),
        (1, "Ph.D.", "EECS", "UC Berkeley"))
    .toDF("id", "degree", "department", "school")
    
    val sparkStatus = Seq(
        (500, "Vice President"),
        (250, "PMC Member"),
        (100, "Contributor"))
    .toDF("id", "status")
    
    person.createOrReplaceTempView("person")
    graduateProgram.createOrReplaceTempView("graduateProgram")
    sparkStatus.createOrReplaceTempView("sparkStatus")
    ```
    ```scala
    // 옳은 join조건
    val joinExpression = person.col("graduate_program") === graduateProgram.col("id")
    // 옳지않은 join조건
    val wrongJoinExpression = person.col("name") === graduateProgram.col("school")

    // 조인1. 일반
    person.join(graduateProgram, joinExpression).show()

    // 조인2. 세번째 파라미터로 조인 타입을 명확하게 지정
    var joinType = "inner"
    person.join(graduateProgram, joinExpression, joinType).show()

    +---+----------------+----------------+---------------+---+-------+--------------------+-----------+
    | id|            name|graduate_program|   spark_status| id| degree|          department|     school|
    +---+----------------+----------------+---------------+---+-------+--------------------+-----------+
    |  0|   Bill Chambers|               0|          [100]|  0|Masters|School of Informa...|UC Berkeley|
    |  2|Michael Armbrust|               1|     [250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
    |  1|   Matei Zaharia|               1|[500, 250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
    +---+----------------+----------------+---------------+---+-------+--------------------+-----------+
    ```

## 8.4 외부 조인

- 양쪽 DataFrame에 키가 있던 없던 모두 포함한다.  
그리고 키가 일치하는 로우가 없을경우 null을 삽입한다.

    ```scala
    var joinType = "outer"
    person.join(graduateProgram, joinExpression, joinType).show()

    +----+----------------+----------------+---------------+---+-------+--------------------+-----------+
    |  id|            name|graduate_program|   spark_status| id| degree|          department|     school|
    +----+----------------+----------------+---------------+---+-------+--------------------+-----------+
    |   1|   Matei Zaharia|               1|[500, 250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
    |   2|Michael Armbrust|               1|     [250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
    |null|            null|            null|           null|  2|Masters|                EECS|UC Berkeley|
    |   0|   Bill Chambers|               0|          [100]|  0|Masters|School of Informa...|UC Berkeley|
    +----+----------------+----------------+---------------+---+-------+--------------------+-----------+
    ```

## 8.5 왼쪽 외부 조인

- 왼쪽 DataFrame의 모든 로우와 왼쪽 DataFrame과 일치하는 오른쪽 DataFrame의 로우를 포함한다.  
그리고 오른쪽 DataFrame에 일치하는 로우가 없다면 null을 삽입한다.

    ```scala
    var joinType = "left_outer"
    graduateProgram.join(person, joinExpression, joinType).show()

    +---+-------+--------------------+-----------+----+----------------+----------------+---------------+
    | id| degree|          department|     school|  id|            name|graduate_program|   spark_status|
    +---+-------+--------------------+-----------+----+----------------+----------------+---------------+
    |  0|Masters|School of Informa...|UC Berkeley|   0|   Bill Chambers|               0|          [100]|
    |  2|Masters|                EECS|UC Berkeley|null|            null|            null|           null|
    |  1|  Ph.D.|                EECS|UC Berkeley|   2|Michael Armbrust|               1|     [250, 100]|
    |  1|  Ph.D.|                EECS|UC Berkeley|   1|   Matei Zaharia|               1|[500, 250, 100]|
    +---+-------+--------------------+-----------+----+----------------+----------------+---------------+
    ```

## 8.6 오른쪽 외부 조인

- 오른쪽 DataFrame의 모든 로우와 오른쪽 DataFrame과 일치하는 왼쪽 DataFrame의 로우를 포함한다.  
그리고 오른쪽 DataFrame에 일치하는 로우가 없다면 null을 삽입한다.

    ```scala
    var joinType = "right_outer"
    person.join(graduateProgram, joinExpression, joinType).show()

    +----+----------------+----------------+---------------+---+-------+--------------------+-----------+
    |  id|            name|graduate_program|   spark_status| id| degree|          department|     school|
    +----+----------------+----------------+---------------+---+-------+--------------------+-----------+
    |   0|   Bill Chambers|               0|          [100]|  0|Masters|School of Informa...|UC Berkeley|
    |null|            null|            null|           null|  2|Masters|                EECS|UC Berkeley|
    |   2|Michael Armbrust|               1|     [250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
    |   1|   Matei Zaharia|               1|[500, 250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
    +----+----------------+----------------+---------------+---+-------+--------------------+-----------+
    ```

## 8.7 왼쪽 세미 조인

- 세미 조인은 오른쪽 DataFrame의 어떤 값도 포함하지 않으므로 다른 조인과는 약간 다르다.
- 두번째 DataFrame은 첫번째 DataFrame을 위한 필터 정도로 볼 수 있다.
- 오라클의경우 exists방식으로 semi join을 표현한다  
https://www.w3resource.com/oracle/joins/oracle-semijoins.php

    ```scala
    var joinType = "left_semi"
    graduateProgram.join(person, joinExpression, joinType).show()

    +---+-------+--------------------+-----------+
    | id| degree|          department|     school|
    +---+-------+--------------------+-----------+
    |  0|Masters|School of Informa...|UC Berkeley|
    |  1|  Ph.D.|                EECS|UC Berkeley|
    +---+-------+--------------------+-----------+
    ```

## 8.8 왼쪽 안티 조인

- 왼쪽 안티 조인은 왼쪽 세미 조인의 반대 개념.
- SQL의 NOT IN 개념.

    ```scala
    var joinType = "left_anti"
    graduateProgram.join(person, joinExpression, joinType).show()

    +---+-------+----------+-----------+
    | id| degree|department|     school|
    +---+-------+----------+-----------+
    |  2|Masters|      EECS|UC Berkeley|
    +---+-------+----------+-----------+
    ```

## 8.9 자연 조인

- 자연 조인은 조인하려는 컬럼을 암시적으로 추정한다.
- 컬럼명은 같지만 의미는 다를수 있으므로 조심해서 사용해야 한다.

    ```sql
    SELECT * from graduateProgram NATURAL JOIN person
    ```

## 8.10 교차 조인(카테시안 조인)

- 조건절을 기술하지 않은 내부 조인.
- 왼쪽 DataFrame의 모든 로우를 오른쪽 DataFrame의 모든 로우와 결합.
- 1,000개의 로우가 존재하는 두개의 DataFrame에 교차 조인을 수행하면 1,000,000(1,000 x 1,000)개의 결과 로우가 생성.


    ```scala
    var joinType = "cross"
    graduateProgram.join(person, lit(1)===lit(1), joinType).show()
    // 또는
    person.crossJoin(graduateProgram).show()

    +---+----------------+----------------+---------------+---+-------+--------------------+-----------+
    | id|            name|graduate_program|   spark_status| id| degree|          department|     school|
    +---+----------------+----------------+---------------+---+-------+--------------------+-----------+
    |  0|   Bill Chambers|               0|          [100]|  0|Masters|School of Informa...|UC Berkeley|
    |  1|   Matei Zaharia|               1|[500, 250, 100]|  0|Masters|School of Informa...|UC Berkeley|
    |  2|Michael Armbrust|               1|     [250, 100]|  0|Masters|School of Informa...|UC Berkeley|
    |  0|   Bill Chambers|               0|          [100]|  2|Masters|                EECS|UC Berkeley|
    |  1|   Matei Zaharia|               1|[500, 250, 100]|  2|Masters|                EECS|UC Berkeley|
    |  2|Michael Armbrust|               1|     [250, 100]|  2|Masters|                EECS|UC Berkeley|
    |  0|   Bill Chambers|               0|          [100]|  1|  Ph.D.|                EECS|UC Berkeley|
    |  1|   Matei Zaharia|               1|[500, 250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
    |  2|Michael Armbrust|               1|     [250, 100]|  1|  Ph.D.|                EECS|UC Berkeley|
    +---+----------------+----------------+---------------+---+-------+--------------------+-----------+
    ```

## 8.11 조인 사용시 문제점
- 8.11.x에서는 문제점 대응 방법과 조인시 최적화에 관련된 힌트를 알려준다.

## 8.11.1 복합 데이터 타입의 조인

- 불리언을 반환하는 모든 표현식은 조인 표현식이 될수있다.  
https://jaceklaskowski.gitbooks.io/mastering-spark-sql/spark-sql-functions-collection.html

    ```scala
    import org.apache.spark.sql.functions.expr

    person.withColumnRenamed("id", "personId")
    .join(sparkStatus, expr("array_contains(spark_status, id)")).show()

    +--------+----------------+----------------+---------------+---+--------------+
    |personId|            name|graduate_program|   spark_status| id|        status|
    +--------+----------------+----------------+---------------+---+--------------+
    |       0|   Bill Chambers|               0|          [100]|100|   Contributor|
    |       1|   Matei Zaharia|               1|[500, 250, 100]|500|Vice President|
    |       1|   Matei Zaharia|               1|[500, 250, 100]|250|    PMC Member|
    |       1|   Matei Zaharia|               1|[500, 250, 100]|100|   Contributor|
    |       2|Michael Armbrust|               1|     [250, 100]|250|    PMC Member|
    |       2|Michael Armbrust|               1|     [250, 100]|100|   Contributor|
    +--------+----------------+----------------+---------------+---+--------------+
    ```

### 8.11.2 중복 컬럼명 처리

- 조인시 양 테이블에 중복된 컬럼이 있는경우 에러가 발생할 수 있기때문에  
적절한 처리가 필요하다.

    ```scala
    val gradProgramDupe = graduateProgram.withColumnRenamed("id", "graduate_program")
    var joinExpr = gradProgramDupe.col("graduate_program") === person.col(
    "graduate_program")

    person.join(gradProgramDupe, joinExpr).show()

    // 중복된 "graduate_program" 컬럼으로 인해 오류 발생
    person.join(gradProgramDupe, joinExpr).select("graduate_program").show()
    ```
- 해결 방법 1 : 다른 조인 표현식 사용

    ```scala
    // 불리언 형태의 조인 표현식을 문자열이나 시퀀스 형태로 바꾸면 두 컬럼중 하나가 자동으로 제거 된다.
    person.join(gradProgramDupe,"graduate_program").select("graduate_program").show()
    ```

- 해결 방법 2 : 조인 후 컬럼 제거

    ```scala
    person.join(gradProgramDupe, joinExpr).drop(person.col("graduate_program"))
    .select("graduate_program").show()

    val joinExpr = person.col("graduate_program") === graduateProgram.col("id")
    person.join(graduateProgram, joinExpr).drop(graduateProgram.col("id")).show()
    ```


- 해결 방법 3 : 조인 전 컬럼명 변경

    ```scala
    val gradProgram3 = graduateProgram.withColumnRenamed("id", "grad_id")
    joinExpr = person.col("graduate_program") === gradProgram3.col("grad_id")
    person.join(gradProgram3, joinExpr).show()
    ```

- 해결 방법 4 : DataFrame에 Alias

    ```scala
    person.as("A").join(gradProgramDupe.as("B"), 
        col("A.graduate_program")===col("B.graduate_program")
    )
    .select("A.graduate_program").show()
    ```

## 8.12 스파크의 조인 수행 방식
- 스파크가 조인을 수행하는데 필요한 두가지 핵심 전략
    - 노드간 네트워크 통신 전략
    - 노드별 연산 전략

### 8.12.1 네트워크 통신 전략
- 스파크의 조인시 두 가지 클러스터 통신 방식
    - 셔플조인(shuffle join) : 전체 노드간 통신을 유발한다.
    - 브로드캐스트 조인(broadcast join) : 전체 노드간 통신을 유발하지 않는다.

- 큰 테이블과 큰 테이블 조인
    - 큰테이블끼리 조인하면 셔플 조인이 발생한다.
    - 셔플 조인은 전체 노드간 통신이 발생한다.
    - 조인시 특정키를 어떤 노드가 가졌는지에 따라 노드간 데이터를 공유하기때문에 네트워크는 많은 자원을 사용한다.

- 큰 테이블과 작은 테이블 조인
    - 작은 테이블이 단일 워커 노드의 메모리크기 이하로 작은경우(메모리 여유 공간 포함) 브로드캐스트 조인을 사용해서 최적화를 할 수 있다.  
    - DataFrame을 클러스터의 전체워크 노드에 복제하는것을 의미한다.
    - 자원을 많이 사용할것같지만 시작시 한번만 복제되고 이후에는 워커끼리의 통신없이 
    모든 단일 노드에서 개별적으로 조인이 수행된다.
    ```scala
    val joinExpr = person.col("graduate_program") === graduateProgram.col("id")
    person.join(graduateProgram, joinExpr).explain()

    // 또는 브로드캐스트 조인 힌트를 줄 수 있다.
    import org.apache.spark.sql.functions.broadcast
    val joinExpr = person.col("graduate_program") === graduateProgram.col("id")
    person.join(broadcast(graduateProgram), joinExpr).explain()

    = Physical Plan ==
    *(1) BroadcastHashJoin [graduate_program#13026], [id#13041], Inner, BuildLeft
    :- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[2, int, false] as bigint)))
    :  +- LocalTableScan [id#13024, name#13025, graduate_program#13026, spark_status#13027]
    +- LocalTableScan [id#13041, degree#13042, department#13043, school#13044]
    joinExpr: org.apache.spark.sql.Column = (graduate_program = id)
    ```

- 아주 작은 테이블 사이의 조인 
    - 스파크가 조인 방식을 결정하도록 내버려두는 것이 좋다.

## 8.13 정리
- 스파크의 가장 흔한 사용 사례인 조인을 알아보았다.

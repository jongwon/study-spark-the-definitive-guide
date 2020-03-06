# 6. 다양한 데이터 타입 다루기

## lit 함수

- 다른 언어(java, python, R...)의 데이터 타입을 스파크 데이터 타입으로 변환한다.
- Row 의 칼럼에 데이터를 넣을 때 사용한다.

## Boolean

- 모든 필터링 작업에서 사용되는 기본 데이터 타입
- 값 : true, false
- 연산자 : and, or
- 주로 df.where나 filter 같은 구문에서 사용할 수 있다.
- 연속된 조건들은 기본적으로 and 구문으로 추가된다. or 구문을 사용하고 싶다면 filter 객체에 or 메쏘드를 사용한다.
  ```scala
  val priceFilter = col("UnitPrice") > 6000
  val descriptFilter = col("Description").contains("POSTAGE")
  df.where(priceFilter.or(descriptFilter)).show()
  ```

* 컬럼명만 사용해서 필터링 할 수도 있다. (BooleanType 인 칼럼인 경우)

## 수치형

- 카운트 처리할 때 사용함
- 단지 수식을 사용해 칼럼을 추가할 수 있다.
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
- 유용한 수식 표현
  - round : 반올림
  - bound : 내림
  - pow : 지수함수
  - corr : 피어슨 상관계수 구하기
  - count
  - min / max
  - stddev : 표준편차
- describe() 메쏘드는 주요 통계 값을 계산해 준다.
  ```
  df.describe().show()
  ```

## 문자형

## Null

## 날짜 시간

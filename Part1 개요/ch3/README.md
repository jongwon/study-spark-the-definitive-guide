# 3. 기능 둘러보기

- spark-submit 명령으로 운영 애플리케이션 실행하기
- type-safe 를 제공하는 Dataset 살펴보기
- 구조적 스트리밍
- 머신러닝과 고급 분석
- RDD
- SparkR
- 서드파티 패키지와 에코 시스템

## 3.1 운영용 애플리케이션 실행하기

* spark-submit 명령 : 스파크로 개발한 프로그램을 운영용 애플리케이션에서 실행시키는 명령어.
* 운영용 애플리케이션
  * Spark Stand Alone
  * MESOS
  * YARN Cluster
* spark-submit 명령어
  ```
  ./bin/spark-submit 
    --class org.apache.package.App 
    --master local 
    ./path/to/app.jar arg1 arg2
  ```
* 파이썬으로 작성한 경우
  ```
  ./bin/spark-submit
    --master local
    ./path/to/python-app.py arg1 arg2
  ```
* master 에 올  수 있는 값
  * local
  * mesos
  * yarn


## 3.2 Dataset

* Dataset 은 정적타입을 지원하는 데이터 형식이다. 파이썬과 R은 동적타입 언어라 지원할 수 없다.
* DataFrame 의 레코드를 정적타입의 클래스로 매핑해 ArrayList(자바)나 Seq(스칼라)에 넣어서 제공한다.
* 대규모 애플리케이션 개발에 유용하다.
* 

## 3.3 구조적 스트리밍

## 3.4 머신러닝과 고급분석

## 3.5 저수준 API

## 3.6 SparkR

## 3.7 스파크의 에코시스템과 패키지

## 3.8 정리
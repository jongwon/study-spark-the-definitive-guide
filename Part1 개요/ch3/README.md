# 3. 스파크 기능 둘러보기
이 장에서 설명하게될 내용
- spark-submit 명령으로 운영 애플리케이션 실행하기
- type-safe 를 제공하는 Dataset 살펴보기
- 구조적 스트리밍
- 머신러닝과 고급 분석
- RDD
- SparkR
- 서드파티 패키지와 에코 시스템

## 3.1 운영용 애플리케이션 실행하기

* spark-submit 명령 : 스파크로 개발한 애플리케이션 코드를 클러스터에 전송해 실행시키는 명령어
* 스파크 애플리케이션이 실행되는 곳
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


## 3.2 Dataset : 타입 안정성을 제공하는 구조적 API

* Dataset 은 정적타입을 지원하는 데이터 형식이다. 파이썬과 R은 동적타입 언어라 지원할 수 없다.
* DataFrame 의 레코드를 정적타입의 클래스로 매핑해 ArrayList(자바)나 Seq(스칼라)에 넣어서 제공한다.
* Dataset API는 타입 안정성을 지원하므로 잘 정의된 인터페이스로 상호작용하는 대규모 애플리케이션 개발에 유용하다.
* Dataset은 타입지정 여부에 따라 모양이 달라진다.
  - 타입 지정 X => Dataset&lt;Row> 
  - 타입 지정 O => Dataset&lt;타입>
```
// 코드 test중
//case class Flight(store_id:String, create_dt:String)
//val tableDf = spark.read.parquet("")
```

## 3.3 구조적 스트리밍

## 3.4 머신러닝과 고급분석

## 3.5 저수준 API
* 스파크의 거의 모든 기능은 RDD를 기반으로 만들어졌다.
* DataFrame 연산도 RDD를 기반으로 만들어졌으며 RDD로 컴파일 된다.
* 원시 데이터를 RDD로 변환후 병렬처리(parallelize)할 수 있다.
```
var df = spark.sparkContext.parallelize(Seq(1, 2, 3)).toDF()
println(df)
```
* RDD는 스칼라, 파이썬에서 모두 사용 가능하다
* 최신버전의 스파크에서는 기본적으로 RDD를 사용하지 않지만,  
비정형 데이터나 정제되지 않은 원시 데이터를 처리해야 한다면 RDD를 사용한다.

## 3.6 SparkR
* SparkR은 스파크를 R 언어로 사용하기 위한 기능이다.  
R은 범용적인 언어라기보다는 통계 계산과 같은 특정 작업에 이점이 있는 언어인데  
분산 데이터 처리를 할 수는 없기때문에 SparkR이 이런 단점을 보완 해준다.
* SparkR은 파이썬 API와 유사하다.
* SparkR외의 R언어 사용 패키지로는 sparklyr을 제공한다.  
SparkR과 sparklyr은 서로 다른 커뮤니티에서 개발되고 있으며  
둘다 스파크의 구조적 API를 지원한다 그리고 머신러닝에서도 사용할 수 있다.

## 3.7 스파크의 에코시스템과 패키지
* 스파크에는 자신이 개발한 패키지를 공개할 수 있는 저장소가 있다.  
  이곳에 공개된 패키지중 일부는 스파크 코어 프로젝트에도 사용된다.  
  [스파크 패키지 저장소 링크](https://spark-packages.org/)

## 3.8 정리
* 3장에서는 앞으로 학습하게될 스파크가 제공하는 기능, API를 소개했다.
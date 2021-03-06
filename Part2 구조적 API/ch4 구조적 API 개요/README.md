# 4. 구조적 API 개요

* 스파크의 카탈로그란 무엇인가?
* 

구조적 API : 데이터의 흐름을 정의하는 기본 추상화 개념. 세가지 분산 컬렉션 API

- Dataset
- DataFrame
- SQL 테이블과 뷰

구조적 API 의 특징

- 배치와 스트리밍 처리에서 사용되며, 작업의 종류를 손쉽게 스위칭 할 수 있게 해준다.
-

## 4.1 Dataset 과 DataFrame

Dataset과 DataFrame

- row와 column 을 가지는 분산 테이블 형태의 컬렉션
- column 데이터의 데이터 타입정보를 갖는다. 즉, 데이터의 스키마 정보를 갖는다.
- 어떤 데이터에 어떤 연산을 적용해서 만들어지는 트렌스포메이션인지를 정의하는 지연 연산의 실행 계획
- immutable 이다.
- DataFrame에 액션이 호출이 되면 실제 트렌스포메이션이 실행되고 DataFrame에 결과가 저장된다.

## 4.2 스키마

스키마는 DataFrame의 컬럼명과 데이터 타입에 대한 정의이다.

## 4.3 스파크의 구조적 데이터 타입 개요

스파크는 프로그래밍 언어이다.
스파크는 자체 데이터 타입 정보를 갖는 카탈리스트 엔진을 사용한다.
자체 데이터 타입이 지원하는 언어(Java, Scala, Python, R)의 데이터타입으로 직접 매핑되는 테이블을 가지고 있다.

#### DataFrame 과 Dataset 비교

|            | DataFrame |        Dataset        |
| ---------- | :-------: | :-------------------: |
| `타입`       |   비타입형    |          타입형          |
| `오류 확인`    |   런타입시    |         컴파일시          |
| `정의 방법`    |    any    | case class (JavaBean) |
| `Python/R` |    지원     |          미지원          |

#### 칼럼

칼럼의 데이터틑 크게 아래와 같이 세가지 이다. 
* 단순 데이터 타입 : 정수형, 문자열
* 복합 데이터 타입 : 배열, 맵, 세트
* null

#### 로우

* 데이터 레코드를 로우라고 부른다. DataFrame 은 레코드가 Row 타입이다.
* SQL 이나 RDD, 데이터소스를 읽어서 만들어 내거나 
* 직접 함수를 써서 만들어 낼 수 있다.


## 4.3 스파크 데이터 타입


## 4.4 구조적 API 실행 과정


#### 논리적 실행 계획

#### 물리적 실행 계획


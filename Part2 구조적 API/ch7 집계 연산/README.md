# 7 집계 연산

## 7.1 집계 함수

### count

- 전체 레코드 수를 카운트 한다.

### countDistinct

- 코유 레코드 수를 카운트 한다.

### approx_count_distinct

- 대규모 데이터의 경우 정확한 개수가 무의미할때 어느 정도 정확도만 설정한다면 근사값을 카운트 해준다.

### first / last

- DataFrame의 첫번째 레코드 / 마지막 레코드를 반환한다.

### min / max

- 최대값과 최소값을 찾아낸다. 표현식이다.

### sum

- 합계

### sumDistinct

- 고유 레코드만 합산한다.

### avg

- 평균값

### var_pop, stddev_pop, var_samp, stddev_samp

- 모평균 / 모표준편차 : var_pop, stddev_pop
- 표본평균 / 표본표준편차 : var_samp, stddev_samp

### skewness / kurtosis

- 비대칭도(skewness)와 첨도(kurtosis) 변곡점을 측정하는 방법

### corr, covar_pop, covar_samp

- cov : 공분산
- corr : 상관관계 ( 피어슨 상관계수값 : -1 ~ 1 )

### collect_set, collect_list

셋이나 리스트 타입을 이용해 집계할 수 있다.

## 7.2 그룹화

그룹핑을 수행(groupBy) 하면 **RelationalGroupedDataset** 객체가 반환된다.
그룹핑 이후에 count 를 수행하거나 agg 메쏘드를 이용해 집계 처리를 수행한다.

## 7.3 윈도우 함수

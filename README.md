# Olist Sales & KPI Analytics Pipeline

브라질 이커머스 Olist의 주문 데이터(2016–2018, 약 10만 건)로 **매출 성장 요인을 진단하고 성장 레버의 우선순위를 제안하는** 엔드투엔드 분석 프로젝트입니다.

> 🚧 진행 중. 현재 완료된 단계는 **1. Raw 적재**입니다.

## 로드맵

| 단계 | 내용 | 상태 |
|---|---|---|
| 1. Raw 적재 | CSV → PostgreSQL `raw` 스키마, 멱등 적재 + 감사 로그 + 테스트 | ✅ |
| 2. Staging | 타입 변환, 중복 제거, 우편번호 복원, 카테고리 번역 | ⏳ |
| 3. Mart | 스타 스키마 (`fct_order_items`, `fct_orders`, `dim_*`) | ⏳ |
| 4. Orchestration | Airflow DAG (raw → staging → mart → 품질 체크) | ⏳ |
| 5. Analysis | KPI 트리, 코호트·LTV, 배송 지연 영향 분석 | ⏳ |
| 6. Dashboard | Tableau 경영진용 / 실무용 대시보드 | ⏳ |

## 아키텍처 (1단계)

```
data/raw/*.csv ──(load_raw.py: TRUNCATE + COPY, 테이블별 트랜잭션)──▶ PostgreSQL raw.*
                                                                  └─▶ raw._load_audit (적재 이력)
```

## Raw 레이어 설계 결정

| 결정 | 이유 |
|---|---|
| 모든 컬럼 `TEXT` | 원천 값이 이상해도 적재가 멈추지 않게 하고, 타입 변환은 staging에서 SQL로 명시적으로 처리 (ELT) |
| 테이블별 `TRUNCATE → COPY → 건수 검증`을 한 트랜잭션으로 | 몇 번을 재실행해도 결과가 같음(멱등). 검증 실패 시 롤백되어 **이전 정상 데이터가 유지됨** |
| CSV 헤더 vs 테이블 컬럼 사전 비교 | 원천에 컬럼이 추가/삭제되면(스키마 드리프트) 조용히 틀린 데이터가 들어가는 대신 즉시 실패 |
| `_batch_id`, `_source_file`, `_loaded_at` 메타 컬럼 | 어떤 행이 언제, 어떤 파일에서 왔는지 추적 (lineage) |
| `raw._load_audit` | 실행 이력·실패 사유 보관. 모니터링과 장애 대응의 출발점 |
| 1MB 청크 스트리밍 COPY | 100만 행 geolocation도 메모리 사용량이 일정 |

## 원천 데이터에서 발견한 이슈

적재 과정에서 확인했고, staging 단계에서 처리할 예정입니다.

| 이슈 | 영향 | 처리 |
|---|---|---|
| 번역 테이블에 UTF-8 BOM | 첫 컬럼명이 `﻿product_category_name`으로 깨져 조인 실패 | ✅ 적재 시 `utf-8-sig`로 제거 |
| 리뷰 본문에 줄바꿈 3,852건 | `wc -l` 기반 건수 검증이 틀림 | ✅ CSV 파서로 논리 레코드 수 계산 |
| 우편번호 앞자리 0 유실 (고객 24%) | geolocation 조인 누락 | ⏳ staging에서 `LPAD(zip, 5, '0')` |
| `customer_id`는 주문마다 새로 발급 | 재구매율이 0%로 계산됨 | ⏳ `customer_unique_id`를 고객 키로 사용 |
| 리뷰 중복 (주문 547건이 리뷰 2개 이상) | 조인 시 매출 부풀림 | ⏳ 주문당 최신 리뷰 1건 |
| geolocation 중복 26만 행 | 조인 폭발 | ⏳ 우편번호별 좌표 평균 1행 |
| 카테고리 미번역 2종, 결측 610개 | 차원 누락 | ⏳ 번역 보강, `unknown` 처리 |
| 2016년, 2018-09 이후는 표본 부족 | 추세 왜곡 | ⏳ 분석 기간 2017-01 ~ 2018-08 |

## 적재 결과

| 테이블 | 행 수 |
|---|---:|
| geolocation | 1,000,163 |
| order_items | 112,650 |
| order_payments | 103,886 |
| orders | 99,441 |
| customers | 99,441 |
| order_reviews | 99,224 |
| products | 32,951 |
| sellers | 3,095 |
| product_category_name_translation | 71 |

전체 적재 약 7초 (로컬, PostgreSQL 16).

## 실행 방법

```bash
cp .env.example .env
make setup      # 의존성 설치
make data       # 원천 CSV 다운로드
make up         # PostgreSQL 기동 (Docker)
make load       # raw 적재 (재실행 안전)
make test       # 테스트
make check      # 최근 적재 결과 확인
```

특정 테이블만 다시 적재하려면 `python -m src.pipeline.load_raw --tables orders order_items`를 실행합니다.

## 테스트

| 테스트 | 검증 내용 |
|---|---|
| `test_count_records_handles_embedded_newlines` | 줄바꿈이 포함된 필드도 1건으로 셈 |
| `test_header_strips_bom` | BOM 제거 |
| `test_validate_schema_detects_drift` | 컬럼 추가·삭제 감지, 순서 변경은 허용 |
| `test_rerun_is_idempotent` | 2회 실행해도 행 수가 2배가 되지 않음 |
| `test_bad_file_keeps_previous_data` | 잘못된 파일이 들어오면 실패하고 기존 데이터 유지, 감사 로그에 `failed` 기록 |

통합 테스트는 임시 스키마를 만들어 실행하고 끝나면 삭제하므로 실제 `raw` 데이터를 건드리지 않습니다.

## 데이터 출처

[Brazilian E-Commerce Public Dataset by Olist](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) (CC BY-NC-SA 4.0), [olist/work-at-olist-data](https://github.com/olist/work-at-olist-data)

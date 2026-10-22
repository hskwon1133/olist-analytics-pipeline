-include .env
export

.PHONY: setup data up load test check

setup:      ## 파이썬 의존성 설치
	pip install -r requirements.txt
data:       ## 원천 CSV 다운로드
	./scripts/download_data.sh
up:         ## PostgreSQL 기동
	docker compose up -d --wait
load:       ## raw 레이어 적재 (DDL 포함, 재실행 안전)
	python -m src.pipeline.load_raw
test:       ## 단위·통합 테스트
	python -m pytest -q tests
check:      ## 최근 적재 결과 확인
	psql -c "SELECT table_name, loaded_rows, status, finished_at FROM raw._load_audit ORDER BY finished_at DESC LIMIT 9;"

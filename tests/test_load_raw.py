"""
raw 적재 테스트.
- 단위 테스트: DB 없이 CSV 파싱 로직 검증
- 통합 테스트: PostgreSQL 필요 (접속 불가 시 skip). 운영 테이블을 건드리지 않도록
  임시 스키마를 만들어 RAW_SCHEMA를 바꿔치기한다.
"""
import uuid
from pathlib import Path

import psycopg
import pytest

from src.pipeline import load_raw as lr

REPO = Path(__file__).resolve().parents[1]
DDL = REPO / "sql/raw/001_create_raw_tables.sql"


# ---------------------------------------------------------------- 단위 테스트
def test_count_records_handles_embedded_newlines(tmp_path):
    f = tmp_path / "x.csv"
    f.write_text('id,msg\n1,"line1\r\nline2"\n2,ok\n', encoding="utf-8")
    # 물리적 줄은 4줄이지만 레코드는 2건
    assert lr.count_csv_records(f) == 2


def test_header_strips_bom(tmp_path):
    f = tmp_path / "x.csv"
    f.write_bytes("﻿product_category_name,product_category_name_english\r\na,b\r\n".encode())
    assert lr.read_header(f)[0] == "product_category_name"


def test_validate_schema_detects_drift():
    with pytest.raises(lr.SchemaDriftError):
        lr.validate_schema(["a", "b", "new_col"], ["a", "b"], "t")
    lr.validate_schema(["b", "a"], ["a", "b"], "t")  # 순서만 다른 건 허용


# ---------------------------------------------------------------- 통합 테스트
def _db_available() -> bool:
    try:
        with psycopg.connect("", connect_timeout=2):
            return True
    except Exception:  # noqa: BLE001
        return False


pytestmark_db = pytest.mark.skipif(not _db_available(), reason="PostgreSQL 접속 불가")


@pytest.fixture
def temp_schema(monkeypatch):
    schema = f"raw_test_{uuid.uuid4().hex[:8]}"
    with psycopg.connect("", autocommit=True) as conn:
        conn.execute(DDL.read_text().replace("raw.", f"{schema}.")
                     .replace("CREATE SCHEMA IF NOT EXISTS raw;", f"CREATE SCHEMA {schema};"))
    monkeypatch.setattr(lr, "RAW_SCHEMA", schema)
    yield schema
    with psycopg.connect("", autocommit=True) as conn:
        conn.execute(f"DROP SCHEMA {schema} CASCADE")


@pytest.fixture
def small_data(tmp_path):
    """sellers 원천의 앞 100건만 사용하는 작은 데이터셋."""
    src = REPO / "data/raw" / lr.TABLE_FILES["sellers"]
    lines = src.read_text(encoding="utf-8").splitlines(keepends=True)[:101]
    (tmp_path / lr.TABLE_FILES["sellers"]).write_text("".join(lines), encoding="utf-8")
    return tmp_path


def _count(schema, table):
    with psycopg.connect("") as conn:
        return conn.execute(f"SELECT count(*) FROM {schema}.{table}").fetchone()[0]


@pytestmark_db
def test_rerun_is_idempotent(temp_schema, small_data):
    lr.run(small_data, ["sellers"])
    lr.run(small_data, ["sellers"])  # 두 번 돌려도
    assert _count(temp_schema, "sellers") == 100  # 200이 아니라 100


@pytestmark_db
def test_bad_file_keeps_previous_data(temp_schema, small_data):
    lr.run(small_data, ["sellers"])

    # 원천에 컬럼이 추가된 상황(스키마 드리프트) 재현
    bad = small_data / lr.TABLE_FILES["sellers"]
    rows = bad.read_text().splitlines()
    bad.write_text("\n".join([rows[0] + ",new_col"] + [r + ",x" for r in rows[1:]]) + "\n")

    with pytest.raises(RuntimeError):
        lr.run(small_data, ["sellers"])
    assert _count(temp_schema, "sellers") == 100  # 이전 정상 데이터 유지

    with psycopg.connect("") as conn:
        status = conn.execute(
            f"SELECT status FROM {temp_schema}._load_audit ORDER BY finished_at DESC LIMIT 1"
        ).fetchone()[0]
    assert status == "failed"


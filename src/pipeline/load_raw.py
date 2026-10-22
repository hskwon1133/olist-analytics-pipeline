"""
Olist 원천 CSV → PostgreSQL raw 스키마 적재.

핵심 설계
- 멱등성: 테이블 단위 트랜잭션 안에서 TRUNCATE → COPY → 건수 검증.
          검증 실패 시 ROLLBACK 되어 "이전 정상 데이터"가 그대로 남는다.
          (PostgreSQL의 TRUNCATE는 트랜잭션 안에서 롤백 가능)
- 스키마 드리프트 감지: CSV 헤더와 테이블 컬럼이 다르면 적재 전에 실패.
- 정확한 건수 검증: 리뷰 본문에 줄바꿈이 있어 `wc -l`은 틀린다 → csv 파서로 레코드 수 계산.
- 인코딩: 일부 파일의 UTF-8 BOM을 utf-8-sig로 제거.
- Airflow에서 재사용할 수 있도록 함수 단위로 분리 (load_table을 태스크로 호출).

접속 정보는 libpq 표준 환경변수(PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE)를 사용한다.
"""
from __future__ import annotations

import argparse
import csv
import logging
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from psycopg import sql

log = logging.getLogger("load_raw")

RAW_SCHEMA = "raw"
META_COLUMNS = {"_batch_id", "_source_file", "_loaded_at"}
COPY_CHUNK = 1 << 20  # 1MB 단위 스트리밍 → 대용량 파일도 메모리 일정

# 테이블명 → 원천 파일명
TABLE_FILES: dict[str, str] = {
    "customers": "olist_customers_dataset.csv",
    "geolocation": "olist_geolocation_dataset.csv",
    "order_items": "olist_order_items_dataset.csv",
    "order_payments": "olist_order_payments_dataset.csv",
    "order_reviews": "olist_order_reviews_dataset.csv",
    "orders": "olist_orders_dataset.csv",
    "products": "olist_products_dataset.csv",
    "sellers": "olist_sellers_dataset.csv",
    "product_category_name_translation": "product_category_name_translation.csv",
}


class SchemaDriftError(RuntimeError):
    """CSV 헤더와 raw 테이블 컬럼이 일치하지 않음."""


class RowCountMismatchError(RuntimeError):
    """원천 레코드 수와 적재된 행 수가 다름."""


@dataclass
class LoadResult:
    table: str
    source_rows: int
    loaded_rows: int
    status: str
    error: str | None = None


def open_csv(path: Path):
    # utf-8-sig: BOM이 있으면 제거, 없으면 일반 utf-8과 동일
    # newline='': 따옴표 안의 \r\n을 그대로 보존 (csv 모듈 권장 방식)
    return path.open("r", encoding="utf-8-sig", newline="")


def read_header(path: Path) -> list[str]:
    with open_csv(path) as f:
        return next(csv.reader(f))


def count_csv_records(path: Path) -> int:
    """물리적 줄 수가 아닌 논리적 CSV 레코드 수 (헤더 제외)."""
    with open_csv(path) as f:
        reader = csv.reader(f)
        next(reader)
        return sum(1 for _ in reader)


def get_table_columns(conn: psycopg.Connection, table: str) -> list[str]:
    rows = conn.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (RAW_SCHEMA, table),
    ).fetchall()
    if not rows:
        raise SchemaDriftError(f"{RAW_SCHEMA}.{table} 테이블이 없습니다. DDL을 먼저 실행하세요.")
    return [r[0] for r in rows if r[0] not in META_COLUMNS]


def validate_schema(header: list[str], table_cols: list[str], table: str) -> None:
    missing = set(table_cols) - set(header)
    unexpected = set(header) - set(table_cols)
    if missing or unexpected:
        raise SchemaDriftError(
            f"{table}: CSV에 없는 컬럼={sorted(missing)}, 테이블에 없는 컬럼={sorted(unexpected)}"
        )


def write_audit(conn: psycopg.Connection, batch_id: str, file_name: str,
                result: LoadResult, started_at: datetime) -> None:
    conn.execute(
        sql.SQL("""
        INSERT INTO {}
            (batch_id, table_name, source_file, source_rows, loaded_rows,
             status, error_message, started_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """).format(sql.Identifier(RAW_SCHEMA, "_load_audit")),
        (batch_id, result.table, file_name, result.source_rows,
         result.loaded_rows, result.status, result.error, started_at),
    )


def load_table(conn: psycopg.Connection, table: str, data_dir: Path, batch_id: str) -> LoadResult:
    """한 테이블을 전체 교체(full refresh) 방식으로 적재한다."""
    file_name = TABLE_FILES[table]
    path = data_dir / file_name
    started_at = datetime.now(timezone.utc)
    source_rows = -1

    try:
        if not path.exists():
            raise FileNotFoundError(path)
        header = read_header(path)
        source_rows = count_csv_records(path)

        with conn.transaction():
            validate_schema(header, get_table_columns(conn, table), table)

            # SET LOCAL과 같은 효과: 트랜잭션이 끝나면 자동 해제
            conn.execute("SELECT set_config('olist.batch_id', %s, true)", (batch_id,))
            conn.execute("SELECT set_config('olist.source_file', %s, true)", (file_name,))

            target = sql.Identifier(RAW_SCHEMA, table)
            conn.execute(sql.SQL("TRUNCATE {}").format(target))

            # 컬럼 목록을 CSV 헤더 순서로 명시 → 원천 컬럼 순서가 바뀌어도 안전
            copy_stmt = sql.SQL(
                "COPY {} ({}) FROM STDIN WITH (FORMAT csv, HEADER true, ENCODING 'UTF8')"
            ).format(target, sql.SQL(", ").join(map(sql.Identifier, header)))

            with conn.cursor() as cur, open_csv(path) as f:
                with cur.copy(copy_stmt) as copy:
                    while chunk := f.read(COPY_CHUNK):
                        copy.write(chunk)

            loaded_rows = conn.execute(
                sql.SQL("SELECT count(*) FROM {}").format(target)
            ).fetchone()[0]

            if loaded_rows != source_rows:
                # 예외 → with conn.transaction() 블록이 ROLLBACK (TRUNCATE도 취소)
                raise RowCountMismatchError(
                    f"{table}: source={source_rows:,} loaded={loaded_rows:,}"
                )

        result = LoadResult(table, source_rows, loaded_rows, "success")
        log.info("%-36s %10s rows  OK", table, f"{loaded_rows:,}")

    except Exception as e:  # noqa: BLE001 - 감사 로그를 남긴 뒤 다시 던진다
        result = LoadResult(table, source_rows, 0, "failed", f"{type(e).__name__}: {e}")
        log.error("%-36s FAILED  %s", table, result.error)
        with conn.transaction():
            write_audit(conn, batch_id, file_name, result, started_at)
        raise

    with conn.transaction():
        write_audit(conn, batch_id, file_name, result, started_at)
    return result


def apply_ddl(conn: psycopg.Connection, ddl_path: Path) -> None:
    with conn.transaction():
        conn.execute(ddl_path.read_text(encoding="utf-8"))
    log.info("DDL applied: %s", ddl_path.name)


def run(data_dir: Path, tables: list[str] | None = None, ddl_path: Path | None = None,
        batch_id: str | None = None, conninfo: str = "") -> list[LoadResult]:
    batch_id = batch_id or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    tables = tables or list(TABLE_FILES)
    unknown = set(tables) - set(TABLE_FILES)
    if unknown:
        raise ValueError(f"알 수 없는 테이블: {sorted(unknown)}")

    results: list[LoadResult] = []
    failed: list[str] = []
    # autocommit=True + 명시적 transaction() 블록 → 테이블별로 독립 커밋
    with psycopg.connect(conninfo, autocommit=True) as conn:
        if ddl_path:
            apply_ddl(conn, ddl_path)
        log.info("batch_id=%s, tables=%d", batch_id, len(tables))
        for t in tables:
            try:
                results.append(load_table(conn, t, data_dir, batch_id))
            except Exception:  # noqa: BLE001 - 한 테이블 실패가 나머지를 막지 않게
                failed.append(t)

    if failed:
        raise RuntimeError(f"적재 실패 테이블: {failed}")
    return results


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Olist CSV → PostgreSQL raw 적재")
    p.add_argument("--data-dir", type=Path, default=Path("data/raw"))
    p.add_argument("--tables", nargs="*", help="일부 테이블만 적재 (기본: 전체)")
    p.add_argument("--ddl", type=Path, default=Path("sql/raw/001_create_raw_tables.sql"),
                   help="적재 전 실행할 DDL (빈 문자열이면 생략)")
    p.add_argument("--batch-id")
    args = p.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        run(args.data_dir, args.tables, args.ddl if str(args.ddl) else None, args.batch_id)
    except Exception as e:  # noqa: BLE001
        log.error("%s", e)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

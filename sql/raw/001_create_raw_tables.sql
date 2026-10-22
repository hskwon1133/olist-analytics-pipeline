-- =====================================================================
-- RAW layer: 원천 CSV를 "있는 그대로" 보관하는 착륙 지점(landing zone)
--
-- 설계 원칙
--   1) 모든 비즈니스 컬럼은 TEXT  → 원천 값이 이상해도 적재가 실패하지 않음.
--      타입 변환·정제는 staging 레이어의 책임 (ELT의 T는 DB 안에서).
--   2) 메타데이터 컬럼(_batch_id, _loaded_at, _source_file)으로 계보(lineage) 추적.
--   3) 이 파일은 여러 번 실행해도 안전(멱등)하도록 IF NOT EXISTS 사용.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS raw;

-- 적재 시점에 세션 변수로 batch_id를 넘기면 DEFAULT가 자동으로 채운다.
--   SET olist.batch_id = '2026-10-22T15:30:00';
-- current_setting(..., true): 변수가 없으면 에러 대신 NULL.

CREATE TABLE IF NOT EXISTS raw.customers (
    customer_id               TEXT,
    customer_unique_id        TEXT,
    customer_zip_code_prefix  TEXT,   -- 원천에서 앞자리 0이 이미 유실됨('01037'→'1037', 약 24%).
                                      -- staging에서 LPAD(..., 5, '0')로 복원
    customer_city             TEXT,
    customer_state            TEXT,
    _batch_id     TEXT        DEFAULT current_setting('olist.batch_id', true),
    _source_file  TEXT        DEFAULT current_setting('olist.source_file', true),
    _loaded_at    TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.geolocation (
    geolocation_zip_code_prefix TEXT,
    geolocation_lat             TEXT,
    geolocation_lng             TEXT,
    geolocation_city            TEXT,
    geolocation_state           TEXT,
    _batch_id     TEXT        DEFAULT current_setting('olist.batch_id', true),
    _source_file  TEXT        DEFAULT current_setting('olist.source_file', true),
    _loaded_at    TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.order_items (
    order_id             TEXT,
    order_item_id        TEXT,
    product_id           TEXT,
    seller_id            TEXT,
    shipping_limit_date  TEXT,
    price                TEXT,
    freight_value        TEXT,
    _batch_id     TEXT        DEFAULT current_setting('olist.batch_id', true),
    _source_file  TEXT        DEFAULT current_setting('olist.source_file', true),
    _loaded_at    TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.order_payments (
    order_id              TEXT,
    payment_sequential    TEXT,
    payment_type          TEXT,
    payment_installments  TEXT,
    payment_value         TEXT,
    _batch_id     TEXT        DEFAULT current_setting('olist.batch_id', true),
    _source_file  TEXT        DEFAULT current_setting('olist.source_file', true),
    _loaded_at    TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.order_reviews (
    review_id                TEXT,
    order_id                 TEXT,
    review_score             TEXT,
    review_comment_title     TEXT,
    review_comment_message   TEXT,   -- 본문에 줄바꿈 포함 → CSV 파서로만 정확히 읽힘
    review_creation_date     TEXT,
    review_answer_timestamp  TEXT,
    _batch_id     TEXT        DEFAULT current_setting('olist.batch_id', true),
    _source_file  TEXT        DEFAULT current_setting('olist.source_file', true),
    _loaded_at    TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.orders (
    order_id                       TEXT,
    customer_id                    TEXT,
    order_status                   TEXT,
    order_purchase_timestamp       TEXT,
    order_approved_at              TEXT,
    order_delivered_carrier_date   TEXT,
    order_delivered_customer_date  TEXT,
    order_estimated_delivery_date  TEXT,
    _batch_id     TEXT        DEFAULT current_setting('olist.batch_id', true),
    _source_file  TEXT        DEFAULT current_setting('olist.source_file', true),
    _loaded_at    TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.products (
    product_id                  TEXT,
    product_category_name       TEXT,
    product_name_lenght         TEXT,   -- 원천 오타(lenght) 그대로 보존, staging에서 rename
    product_description_lenght  TEXT,
    product_photos_qty          TEXT,
    product_weight_g            TEXT,
    product_length_cm           TEXT,
    product_height_cm           TEXT,
    product_width_cm            TEXT,
    _batch_id     TEXT        DEFAULT current_setting('olist.batch_id', true),
    _source_file  TEXT        DEFAULT current_setting('olist.source_file', true),
    _loaded_at    TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.sellers (
    seller_id               TEXT,
    seller_zip_code_prefix  TEXT,
    seller_city             TEXT,
    seller_state            TEXT,
    _batch_id     TEXT        DEFAULT current_setting('olist.batch_id', true),
    _source_file  TEXT        DEFAULT current_setting('olist.source_file', true),
    _loaded_at    TIMESTAMPTZ DEFAULT now()
);

CREATE TABLE IF NOT EXISTS raw.product_category_name_translation (
    product_category_name          TEXT,
    product_category_name_english  TEXT,
    _batch_id     TEXT        DEFAULT current_setting('olist.batch_id', true),
    _source_file  TEXT        DEFAULT current_setting('olist.source_file', true),
    _loaded_at    TIMESTAMPTZ DEFAULT now()
);

-- 적재 이력(audit) 테이블: "언제, 무엇을, 몇 건, 성공했나"를 남긴다.
CREATE TABLE IF NOT EXISTS raw._load_audit (
    batch_id      TEXT        NOT NULL,
    table_name    TEXT        NOT NULL,
    source_file   TEXT        NOT NULL,
    source_rows   BIGINT,
    loaded_rows   BIGINT,
    status        TEXT        NOT NULL,     -- success / failed
    error_message TEXT,
    started_at    TIMESTAMPTZ NOT NULL,
    finished_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

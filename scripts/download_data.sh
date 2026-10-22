#!/usr/bin/env bash
# Olist 공개 데이터셋(9개 CSV)을 data/raw/ 로 내려받는다.
# 출처: https://github.com/olist/work-at-olist-data (Kaggle "Brazilian E-Commerce Public Dataset by Olist"와 동일)
set -euo pipefail
DEST="$(cd "$(dirname "$0")/.." && pwd)/data/raw"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT
git clone --depth 1 https://github.com/olist/work-at-olist-data.git "$TMP/src"
mkdir -p "$DEST"
cp "$TMP"/src/datasets/*.csv "$DEST"/
ls -lh "$DEST"

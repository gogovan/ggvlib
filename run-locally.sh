#!/usr/bin/env bash
export GOOGLE_APPLICATION_CREDENTIALS=bq-service-account.json
export BUCKET=ggx-analytics
export COUNTRIES="hk|vn|sg"
poetry shell
poetry run python local.py
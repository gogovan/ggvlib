#!/usr/bin/env bash
export GOOGLE_APPLICATION_CREDENTIALS=bq-service-account.json
poetry shell
poetry run python main.py
#!/usr/bin/env bash
export $(cat .env | xargs)
rm dist/*
poetry build
poetry publish -u gogotech -p $PYPI_PASSWORD
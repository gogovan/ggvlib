#!/usr/bin/env bash
export $(cat .env | xargs)
rm dist/*
poetry version patch
poetry build
poetry publish
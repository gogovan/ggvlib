#!/usr/bin/env bash
REPO_NAME=$1
REGION=us-east4
PROJECT_ID=$2
REPO_URL=https://$REGION-python.pkg.dev/$PROJECT_ID/$REPO_NAME/

gcloud config set artifacts/location $REGION

gcloud artifacts repositories delete $REPO_NAME -q

gcloud artifacts repositories create $REPO_NAME \
    --repository-format=python \
    --description="Test python package repository"

gcloud config set artifacts/repository $REPO_NAME

python -m twine upload --repository-url $REPO_URL dist/*
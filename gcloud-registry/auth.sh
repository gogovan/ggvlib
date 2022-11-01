#!/usr/bin/env bash

REPO_NAME=common
REGION=us-east4
PROJECT_ID=$1
REPO_URL=https://$REGION-python.pkg.dev/$PROJECT_ID/$REPO_NAME/

gcloud artifacts print-settings python --project=$PROJECT_ID \
--repository=$REPO_NAME \
--location=$REGION
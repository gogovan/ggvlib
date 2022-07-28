#bin/bash
gcloud functions deploy run_driver_cheat_detection \
--trigger-topic run_driver_cheat_detection --runtime python39 \
--set-env-vars BUCKET="ggx-analytics"
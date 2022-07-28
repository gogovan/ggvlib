# gogovan-analytics-cheat-detection

## Local Setup
1. CD to repo directory
2. Run `poetry shell && poetry install` in terminal (install poetry with pip if it is not installed)
3. Copy the JSON data from [vault](https://vault-v2.gogo.tech/ui/vault/secrets/gogotech/show/data_team/databases/ANALYTICS_BIG_QUERY) and save it to a file called `bq-service-account.json` in the repo directory.
3. Run `./run-locally.sh`

## Deployment
1. Run `./deploy.sh`.

#!/usr/bin/env bash

echo "Starting dev refresh"
export REDCAP_BASE_API_TOKEN=$(vault read -field=token secret/dsde/monster/dev/dog-aging/redcap-tokens/automation)
export REDCAP_ENV_API_TOKEN=$(vault read -field=token secret/dsde/monster/dev/dog-aging/redcap-tokens/env_automation)

dagster pipeline execute -f repositories.py -a repositories -p refresh_data_all --mode dev -c dev_refresh.yaml

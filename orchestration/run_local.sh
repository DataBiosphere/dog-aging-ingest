#!/usr/bin/env bash
export REDCAP_BASE_API_TOKEN=$(vault read -field=token secret/dsde/monster/dev/dog-aging/redcap-tokens/automation)
export REDCAP_ENV_API_TOKEN=$(vault read -field=token secret/dsde/monster/dev/dog-aging/redcap-tokens/env_automation)

WORKING_DIR=$(realpath ./)
if [[ ! -d "./dagster_home" ]] ;
then
  mkdir dagster_home
fi

DAGSTER_HOME=${WORKING_DIR}/dagster_home dagit -f dap_orchestration/repositories/local_repositories.py
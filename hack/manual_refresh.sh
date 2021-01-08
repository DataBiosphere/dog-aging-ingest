#!/bin/bash
set -e

# This script performs a manual refresh of our various DAP tables.
# Requirements:
# * You must be authenticated with vault (you can check by running `vault token lookup` and seeing if your access info is displayed)
# * You must be authenticated with GCP (`gcloud auth list` should show a star next to your email address)
#   * Your account must be set as the default application credentials for your machine (you can do this with `gcloud auth application-default login`)

# Grab the Redcap tokens from Vault
ENV=dev
automation=$(vault read -field=token secret/dsde/monster/${ENV}/dog-aging/redcap-tokens/automation)
env_automation=$(vault read -field=token secret/dsde/monster/${ENV}/dog-aging/redcap-tokens/env_automation)
​
# RUN THE EXTRACTION ON GCP, SAVE TO GS PATH
# HLES
sbt "dog-aging-hles-extraction/runMain org.broadinstitute.monster.dap.HLESurveyExtractionPipeline --apiToken=$automation --pullDataDictionaries=false --outputPrefix=gs://broad-dsp-monster-dap-dev-storage/tmptest/weekly_refresh/raw --runner=dataflow --project=broad-dsp-monster-dap-dev --region=us-central1 --workerMachineType=n1-standard-1 --autoscalingAlgorithm=THROUGHPUT_BASED --numWorkers=4 --maxNumWorkers=8 --experiments=shuffle_mode=service"
# CSLB
sbt "dog-aging-hles-extraction/runMain org.broadinstitute.monster.dap.CslbExtractionPipeline --apiToken=$automation --pullDataDictionaries=false --outputPrefix=gs://broad-dsp-monster-dap-dev-storage/tmptest/weekly_refresh --runner=dataflow --project=broad-dsp-monster-dap-dev --region=us-central1 --workerMachineType=n1-standard-1 --autoscalingAlgorithm=THROUGHPUT_BASED --numWorkers=4 --maxNumWorkers=8 --experiments=shuffle_mode=service"
# ENVIRONMENT
sbt "dog-aging-hles-extraction/runMain org.broadinstitute.monster.dap.EnvironmentExtractionPipeline --apiToken=$env_automation --pullDataDictionaries=false --outputPrefix=gs://broad-dsp-monster-dap-dev-storage/tmptest/weekly_refresh --runner=dataflow --project=broad-dsp-monster-dap-dev --region=us-central1 --workerMachineType=n1-standard-1 --autoscalingAlgorithm=THROUGHPUT_BASED --numWorkers=4 --maxNumWorkers=8 --experiments=shuffle_mode=service"
​
# TRANSFORMATION
# HLES
sbt "dog-aging-hles-transformation/run --inputPrefix=gs://broad-dsp-monster-dap-dev-storage/tmptest/weekly_refresh/raw/hles --outputPrefix=gs://broad-dsp-monster-dap-dev-storage/tmptest/weekly_refresh/transform --runner=dataflow --project=broad-dsp-monster-dap-dev --region=us-central1 --workerMachineType=n1-standard-1 --autoscalingAlgorithm=THROUGHPUT_BASED --numWorkers=6 --maxNumWorkers=10 --experiments=shuffle_mode=service"
# CSLB
sbt "dog-aging-hles-transformation/run --inputPrefix=gs://broad-dsp-monster-dap-dev-storage/tmptest/weekly_refresh/raw/cslb --outputPrefix=gs://broad-dsp-monster-dap-dev-storage/tmptest/weekly_refresh/transform --runner=dataflow --project=broad-dsp-monster-dap-dev --region=us-central1 --workerMachineType=n1-standard-1 --autoscalingAlgorithm=THROUGHPUT_BASED --numWorkers=6 --maxNumWorkers=10 --experiments=shuffle_mode=service"
# ENVIRONMENT
sbt "dog-aging-hles-transformation/run --inputPrefix=gs://broad-dsp-monster-dap-dev-storage/tmptest/weekly_refresh/raw/environment --outputPrefix=gs://broad-dsp-monster-dap-dev-storage/tmptest/weekly_refresh/transform --runner=dataflow --project=broad-dsp-monster-dap-dev --region=us-central1 --workerMachineType=n1-standard-1 --autoscalingAlgorithm=THROUGHPUT_BASED --numWorkers=6 --maxNumWorkers=10 --experiments=shuffle_mode=service"
# ​
# ​
# ​
# # DOWNLOAD TRANSFORM OUTPUT TO LOCAL MACHINE
# gsutil ls gs://broad-dsp-monster-dap-dev-storage/weekly_refresh/transform/cslb
# gsutil cp -r gs://broad-dsp-monster-dap-dev-storage/weekly_refresh/transform/cslb /Users/qhoque/DATA/DAP/InitialDataRelease/transform
# gsutil cp -r gs://broad-dsp-monster-dap-dev-storage/weekly_refresh/transform/environment /Users/qhoque/DATA/DAP/InitialDataRelease/transform
# ​
# ​
# ## RUN CONVERT OUTPUT SCRIPT
# python ~/GIT/dog-aging-ingest/hack/convert-output-to-tsv.py transform_output tsv_output -d
# ​
# ​
# # upload to Terra
# cd /Users/qhoque/GIT/terra-tools/scripts/import_large_tsv
# python /Users/qhoque/GIT/terra-tools/scripts/import_large_tsv/import_large_tsv_python3.py --tsv "/Users/qhoque/DATA/DAP/env/121420/tsv/environment.tsv" --project "workshop-temp" --workspace "Dog Aging Project - Terra Training Workshop"

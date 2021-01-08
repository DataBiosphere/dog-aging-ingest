#!/bin/bash
set -e

# This script performs a manual refresh of our various DAP tables.
#   Run this script from the root directory of your broadinstitute/dog-aging-ingest checkout.
# Requirements:
#   You must have git read access to broadinstitute/terra-tools
#   You must be authenticated with vault and have the vault CLI installed (you can check by running `vault token lookup` and seeing if your access info is displayed)
#   You must be authenticated with GCP (`gcloud auth list` should show a star next to your email address)
#     Your account must be set as the default application credentials for your machine (you can do this with `gcloud auth application-default login`)

# Check out the terra-tools repo to a temporary directory so we can use its utilities
tmp_terra_tools=$(mktemp -d -t terratools-XXXXXXXX)
git clone -q git@github.com:broadinstitute/terra-tools.git "$tmp_terra_tools"

# Grab the Redcap tokens from Vault
ENV=dev
automation=$(vault read -field=token secret/dsde/monster/${ENV}/dog-aging/redcap-tokens/automation)
env_automation=$(vault read -field=token secret/dsde/monster/${ENV}/dog-aging/redcap-tokens/env_automation)
# EXTRACTION
#   HLES
sbt "dog-aging-hles-extraction/runMain org.broadinstitute.monster.dap.HLESurveyExtractionPipeline --apiToken=$automation --pullDataDictionaries=false --outputPrefix=gs://broad-dsp-monster-dap-dev-storage/tmptest/weekly_refresh/raw --runner=dataflow --project=broad-dsp-monster-dap-dev --region=us-central1 --workerMachineType=n1-standard-1 --autoscalingAlgorithm=THROUGHPUT_BASED --numWorkers=4 --maxNumWorkers=8 --experiments=shuffle_mode=service"
#   CSLB
sbt "dog-aging-hles-extraction/runMain org.broadinstitute.monster.dap.CslbExtractionPipeline --apiToken=$automation --pullDataDictionaries=false --outputPrefix=gs://broad-dsp-monster-dap-dev-storage/tmptest/weekly_refresh --runner=dataflow --project=broad-dsp-monster-dap-dev --region=us-central1 --workerMachineType=n1-standard-1 --autoscalingAlgorithm=THROUGHPUT_BASED --numWorkers=4 --maxNumWorkers=8 --experiments=shuffle_mode=service"
#   ENVIRONMENT
sbt "dog-aging-hles-extraction/runMain org.broadinstitute.monster.dap.EnvironmentExtractionPipeline --apiToken=$env_automation --pullDataDictionaries=false --outputPrefix=gs://broad-dsp-monster-dap-dev-storage/tmptest/weekly_refresh --runner=dataflow --project=broad-dsp-monster-dap-dev --region=us-central1 --workerMachineType=n1-standard-1 --autoscalingAlgorithm=THROUGHPUT_BASED --numWorkers=4 --maxNumWorkers=8 --experiments=shuffle_mode=service"

# TRANSFORMATION
#   HLES
sbt "dog-aging-hles-transformation/run --inputPrefix=gs://broad-dsp-monster-dap-dev-storage/tmptest/weekly_refresh/raw/hles --outputPrefix=gs://broad-dsp-monster-dap-dev-storage/tmptest/weekly_refresh/transform --runner=dataflow --project=broad-dsp-monster-dap-dev --region=us-central1 --workerMachineType=n1-standard-1 --autoscalingAlgorithm=THROUGHPUT_BASED --numWorkers=6 --maxNumWorkers=10 --experiments=shuffle_mode=service"
#   CSLB
sbt "dog-aging-hles-transformation/run --inputPrefix=gs://broad-dsp-monster-dap-dev-storage/tmptest/weekly_refresh/raw/cslb --outputPrefix=gs://broad-dsp-monster-dap-dev-storage/tmptest/weekly_refresh/transform --runner=dataflow --project=broad-dsp-monster-dap-dev --region=us-central1 --workerMachineType=n1-standard-1 --autoscalingAlgorithm=THROUGHPUT_BASED --numWorkers=6 --maxNumWorkers=10 --experiments=shuffle_mode=service"
#   ENVIRONMENT
sbt "dog-aging-hles-transformation/run --inputPrefix=gs://broad-dsp-monster-dap-dev-storage/tmptest/weekly_refresh/raw/environment --outputPrefix=gs://broad-dsp-monster-dap-dev-storage/tmptest/weekly_refresh/transform --runner=dataflow --project=broad-dsp-monster-dap-dev --region=us-central1 --workerMachineType=n1-standard-1 --autoscalingAlgorithm=THROUGHPUT_BASED --numWorkers=6 --maxNumWorkers=10 --experiments=shuffle_mode=service"

# convert transformed results to TSV
mkdir -p ./tsv_output
./hack/run_in_virtualenv.sh gsutil_reader "./hack/convert-output-to-tsv.py gs://broad-dsp-monster-dap-dev-storage/tmptest/weekly_refresh/transform ./tsv_output/ --debug"

# upload TSVs to terra
for tsv_path in ./tsv_output/*.tsv; do
    python "$tmp_terra_tools/scripts/import_large_tsv/import_large_tsv_python3.py" --tsv "$tsv_path" --project "workshop-temp" --workspace "Dog Aging Project - Terra Training Workshop"
done

rm -rf "$tmp_terra_tools"

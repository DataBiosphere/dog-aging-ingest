#!/bin/bash
# set -e

# This script performs a manual refresh of our various DAP tables.
#   Run this script from the root directory of your broadinstitute/dog-aging-ingest checkout.
# Requirements:
#   You must have git read access to broadinstitute/terra-tools
#   You must be authenticated with vault and have the vault CLI installed (you can check by running `vault token lookup` and seeing if your access info is displayed)
#   You must be authenticated with GCP (`gcloud auth list` should show a star next to your email address)
#     Your account must be set as the default application credentials for your machine (you can do this with `gcloud auth application-default login`)
#   If you're on a Mac, you'll need to have GNU's core utilities installed (`brew install coreutils`) for some of the helper
#     utilities we call in the script.

# INVOCATION SYNTAX: ./hack/manual_refresh.sh [-e env] [-t tsv_output_path] [-o refresh_subdirectory] [-u] [pipelines...]
## env: environment to refresh, defaults to 'dev'. must be either 'prod' or 'dev'.
## tsv_output_path: local filesystem path to place TSVs generated by the TSV conversion script, defaults to 'tsv_output' dir in the current working directory
## refresh_subdirectory: directory to nest the pipeline outputs under in the bucket. defaults to the date of the most recent Monday.
## -u: If this flag is passed, the TSVs generated by the pipeline will be immediately uploaded to Terra.
## pipelines: a space-separated list of pipelines to refresh. can be any of hles, cslb, or environment

# gdate and date are the same command, this just handles the fact that they have different aliases on
# mac vs linux
function x_date() {
    # Darwin is the mac kernel
    if [ "$(uname -s)" = 'Darwin' ]; then
        echo "$(gdate $@)"
    elif [ "$(uname -s)" = 'Linux' ]; then
        echo "$(date $@)"
    fi
}

OPTIND=1  # Reset in case getopts has been used previously in the shell.

# set up default values for variables
ENV="dev"
TSV_OUTPUT_PATH="./tsv_output"
UPLOAD_TO_TERRA=0
REFRESH_SUBDIRECTORY=$(x_date -dlast-monday +%Y%m%d)  # most recent monday

while getopts "e:t:o:u" opt; do
    case "$opt" in
    e)
        ENV="$OPTARG"
        ;;
    t)  TSV_OUTPUT_PATH="$OPTARG"
        ;;
    o)  REFRESH_SUBDIRECTORY="$OPTARG"
        ;;
    u)  UPLOAD_TO_TERRA=1
        ;;
    esac
done

PIPELINES=${@:$OPTIND}

# if no pipelines are specified, default to refreshing all of them
if [ -z "${PIPELINES[@]}" ]; then
	PIPELINES=(hles cslb environment)
fi

# scans through requested pipelines to see if a specified pipeline was requested
function pipeline_requested() {
	pipeline_specified=0
	for pipeline in ${PIPELINES[@]}; do
		if [[ "$pipeline" == "$1" ]]; then
			pipeline_specified=1
			break
		fi
	done

	echo $pipeline_specified
}

# Grab the Redcap tokens from Vault
automation=$(vault read -field=token secret/dsde/monster/${ENV}/dog-aging/redcap-tokens/automation)
env_automation=$(vault read -field=token secret/dsde/monster/${ENV}/dog-aging/redcap-tokens/env_automation)

# EXTRACTION
#   HLES
if [ "$(pipeline_requested hles)" -eq 1 ]; then
	sbt "dog-aging-hles-extraction/runMain org.broadinstitute.monster.dap.hles.HLESurveyExtractionPipeline --apiToken=$automation --pullDataDictionaries=false --outputPrefix=gs://broad-dsp-monster-dap-dev-storage/weekly_refresh/$REFRESH_SUBDIRECTORY/raw --runner=dataflow --project=broad-dsp-monster-dap-dev --region=us-central1 --workerMachineType=n1-standard-1 --autoscalingAlgorithm=THROUGHPUT_BASED --numWorkers=4 --maxNumWorkers=8 --experiments=shuffle_mode=service"
fi

#   CSLB
if [ "$(pipeline_requested cslb)" -eq 1 ]; then
	sbt "dog-aging-hles-extraction/runMain org.broadinstitute.monster.dap.cslb.CslbExtractionPipeline --apiToken=$automation --pullDataDictionaries=false --outputPrefix=gs://broad-dsp-monster-dap-dev-storage/weekly_refresh/$REFRESH_SUBDIRECTORY/raw --runner=dataflow --project=broad-dsp-monster-dap-dev --region=us-central1 --workerMachineType=n1-standard-1 --autoscalingAlgorithm=THROUGHPUT_BASED --numWorkers=4 --maxNumWorkers=8 --experiments=shuffle_mode=service"
fi

#   ENVIRONMENT
if [ "$(pipeline_requested environment)" -eq 1 ]; then
	sbt "dog-aging-hles-extraction/runMain org.broadinstitute.monster.dap.environment.EnvironmentExtractionPipeline --apiToken=$env_automation --pullDataDictionaries=false --outputPrefix=gs://broad-dsp-monster-dap-dev-storage/weekly_refresh/$REFRESH_SUBDIRECTORY/raw --runner=dataflow --project=broad-dsp-monster-dap-dev --region=us-central1 --workerMachineType=n1-standard-1 --autoscalingAlgorithm=THROUGHPUT_BASED --numWorkers=4 --maxNumWorkers=8 --experiments=shuffle_mode=service"
fi

# TRANSFORMATION
#   HLES
if [ "$(pipeline_requested hles)" -eq 1 ]; then
	sbt "dog-aging-hles-transformation/runMain org.broadinstitute.monster.dap.hles.HLESurveyTransformationPipeline --inputPrefix=gs://broad-dsp-monster-dap-dev-storage/weekly_refresh/$REFRESH_SUBDIRECTORY/raw/hles --outputPrefix=gs://broad-dsp-monster-dap-dev-storage/weekly_refresh/$REFRESH_SUBDIRECTORY/transform --runner=dataflow --project=broad-dsp-monster-dap-dev --region=us-central1 --workerMachineType=n1-standard-1 --autoscalingAlgorithm=THROUGHPUT_BASED --numWorkers=6 --maxNumWorkers=10 --experiments=shuffle_mode=service"
fi

#   CSLB
if [ "$(pipeline_requested cslb)" -eq 1 ]; then
	sbt "dog-aging-hles-transformation/runMain org.broadinstitute.monster.dap.cslb.CslbTransformationPipeline --inputPrefix=gs://broad-dsp-monster-dap-dev-storage/weekly_refresh/$REFRESH_SUBDIRECTORY/raw/cslb --outputPrefix=gs://broad-dsp-monster-dap-dev-storage/weekly_refresh/$REFRESH_SUBDIRECTORY/transform --runner=dataflow --project=broad-dsp-monster-dap-dev --region=us-central1 --workerMachineType=n1-standard-1 --autoscalingAlgorithm=THROUGHPUT_BASED --numWorkers=6 --maxNumWorkers=10 --experiments=shuffle_mode=service"
fi

#   ENVIRONMENT
if [ "$(pipeline_requested environment)" -eq 1 ]; then
	sbt "dog-aging-hles-transformation/runMain org.broadinstitute.monster.dap.environment.EnvironmentTransformationPipeline --inputPrefix=gs://broad-dsp-monster-dap-dev-storage/weekly_refresh/$REFRESH_SUBDIRECTORY/raw/environment --outputPrefix=gs://broad-dsp-monster-dap-dev-storage/weekly_refresh/$REFRESH_SUBDIRECTORY/transform --runner=dataflow --project=broad-dsp-monster-dap-dev --region=us-central1 --workerMachineType=n1-standard-1 --autoscalingAlgorithm=THROUGHPUT_BASED --numWorkers=6 --maxNumWorkers=10 --experiments=shuffle_mode=service --dumpHeapOnOOM --saveHeapDumpsToGcsPath=gs://broad-dsp-monster-dap-dev-storage/weekly_refresh/transform/OOMlogs"
fi

# convert transformed results to TSV
mkdir -p "$TSV_OUTPUT_PATH"
./hack/run_in_virtualenv.sh gsutil_reader "./hack/convert-output-to-tsv.py gs://broad-dsp-monster-dap-dev-storage/weekly_refresh/$REFRESH_SUBDIRECTORY/transform $TSV_OUTPUT_PATH --debug"

# upload TSVs to terra
if [ "$UPLOAD_TO_TERRA" -eq 1 ]; then
    # Check out the terra-tools repo to a temporary directory so we can use its utilities
    tmp_terra_tools=$(mktemp -d -t terratools-XXXXXXXX)
    git clone git@github.com:broadinstitute/terra-tools.git "$tmp_terra_tools"

    # set up a virtualenv for running terra tools scripts
    mkdir -p "hack/python_requirements/terra_tools"
    cp "$tmp_terra_tools/requirements.txt" "hack/python_requirements/terra_tools/"
    working_dir=$(pwd)
    pushd $tmp_terra_tools
    for tsv_path in "$TSV_OUTPUT_PATH/*.tsv"; do
        "$working_dir/hack/run_in_virtualenv.sh" terra_tools "python '$tmp_terra_tools/scripts/import_large_tsv/import_large_tsv.py' --tsv '$tsv_path' --project 'workshop-temp' --workspace 'Dog Aging Project - Terra Training Workshop'"
    done
    popd
    rm -rf "hack/python_requirements/terra_tools"

    rm -rf "$tmp_terra_tools"
fi

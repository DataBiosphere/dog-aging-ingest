#!/bin/bash
# Script to wrap a command in a specified virtualenv and automatically set that virtualenv up to match a given spec.
# Virtualenvs may be configured by putting the following files into the hack/python_requirements/[virtualenv name] directory.
# All files are optional, but at least one must be present to create the virtualenv
# Files:
# * requirements.txt: Works as a Python requirements file, listing PyPi packages to install.
# * virtualenv_args.sh: Specifies additional space-separated arguments to pass to the virtualenv executable when creating this virtualenv.
# * python_executable.txt: Specifies the path to the python executable to use as the default python interpreter in the virtualenv.
# * .env: A list of environment variables to set in the virtualenv. May interpolate any values provided directly from virtualenv itself.
# * setup.sh: An arbitrary shell script that will run when the virtualenv is created. Usable to install any custom dependencies not available from pip.
set -e

VERBOSE_MODE=0

# unified readlink that works on both linux and mac hosts
function x_readlink() {
    # Darwin is the mac kernel - this runs different commands for mac vs. linux
    if [ "$(uname -s)" = 'Darwin' ]; then
        echo "$(greadlink $@)"
    elif [ "$(uname -s)" = 'Linux' ]; then
        echo "$(readlink $@)"
    fi
}

function print_help() {
    echo "usage: ./run_in_virtualenv.sh [-v] virtualenv_name \"command goes here\""
    echo "    -h            Display this help message."
    echo "    -v            Turn on verbose mode. Prints detailed information about environment setup."
}

while getopts ":vh" OPTION
do
    case $OPTION in
        h )
            print_help
            exit 0
            ;;
        v ) VERBOSE_MODE=1
            ;;
        \? )
            echo "Invalid option: -$OPTARG" 1>&2
            print_help
            exit 0
            ;;
    esac
done

ROOT_DIRECTORY=$(x_readlink -e $(dirname "$0"))
export VIRTUALENV_NAME=${@:$OPTIND:1}
export REQUESTED_COMMAND=${@:$OPTIND+1}
export REQUIREMENTS_DIRECTORY="$ROOT_DIRECTORY/python_requirements/${VIRTUALENV_NAME}"
export VIRTUALENV_PATH="$ROOT_DIRECTORY/virtualenvs/$VIRTUALENV_NAME"

function verbose_log() {
    if [[ $VERBOSE_MODE -eq 1 ]]; then
        echo "$@"
    fi
}

if [ ! -d "$REQUIREMENTS_DIRECTORY" ]; then
    # we don't hide this behind the verbose flag, since errors should always be reported.
    echo "The requested virtualenv \"$VIRTUALENV_NAME\" doesn't seem to have any settings configured. Aborting."
    echo "Expected settings to live in $REQUIREMENTS_DIRECTORY"
    exit 1
fi

function install_requirements() {
    REQUIREMENTS_FILE="$REQUIREMENTS_DIRECTORY/requirements.txt"

    if [ -f "$REQUIREMENTS_FILE" ]; then
        if [[ $VERBOSE_MODE -eq 1 ]]; then
            pip install -r "$REQUIREMENTS_FILE" --disable-pip-version-check
        else
            pip install -qr "$REQUIREMENTS_FILE" --disable-pip-version-check
        fi
    else
        verbose_log "No requirements file found at $REQUIREMENTS_FILE. Skipping requirements installation."
    fi
}

function set_up_and_enter_virtualenv() {
    VIRTUALENV_ARGS_FILE="$REQUIREMENTS_DIRECTORY/virtualenv_args.sh"
    if [ -f "$VIRTUALENV_ARGS_FILE" ]; then
        readarray VIRTUALENV_ARGS < "$VIRTUALENV_ARGS_FILE"
    else
        verbose_log "No custom virtualenv args file found at $VIRTUALENV_ARGS_FILE. Using default arguments."
        VIRTUALENV_ARGS=()
    fi

    PYTHON_EXECUTABLE_PATH="$REQUIREMENTS_DIRECTORY/python_executable.txt"
    if [ -f "$PYTHON_EXECUTABLE_PATH" ]; then
        PYTHON_EXECUTABLE=$(cat "$PYTHON_EXECUTABLE_PATH")
    else
        verbose_log "No custom python executable specified, using whatever python is on the path."
        PYTHON_EXECUTABLE="python"
    fi

    if [ ! -d "$VIRTUALENV_PATH" ]; then
        # --python param forces virtualenv to use the python currently on the path.
        # --download tells it to download the latest setuptools/pip/wheel, which avoids
        #     some package installation issues with old versions of those tools
        virtualenv ${VIRTUALENV_ARGS[@]} "$VIRTUALENV_PATH" --python="$PYTHON_EXECUTABLE" --download
    fi

    source "$VIRTUALENV_PATH/bin/activate"
}

function load_environment_variables() {
    ENV_FILE="$REQUIREMENTS_DIRECTORY/.env"
    if [ -f "$ENV_FILE" ]; then
        . "$ENV_FILE"
    else
        verbose_log "Did not find a .env file at $ENV_FILE. Skipping loading of environment variables."
    fi
}

function run_custom_setup_script() {
    CUSTOM_SETUP_SCRIPT_PATH="$REQUIREMENTS_DIRECTORY/setup.sh"
    if [ -f "$CUSTOM_SETUP_SCRIPT_PATH" ]; then
        bash $CUSTOM_SETUP_SCRIPT_PATH
    else
        verbose_log "No custom setup script found at $CUSTOM_SETUP_SCRIPT_PATH, skipping."
    fi
}

load_environment_variables
set_up_and_enter_virtualenv
# we load env vars twice to make sure that we can define environment variables that depend on
# things configured in the virtualenv.
load_environment_variables
install_requirements
run_custom_setup_script

verbose_log "Virtualenv setup complete. About to run the following command: $REQUESTED_COMMAND"
bash -c "$REQUESTED_COMMAND"

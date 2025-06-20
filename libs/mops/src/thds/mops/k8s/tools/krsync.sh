#!/bin/bash
# thank you, Karl Bunch:
# https://serverfault.com/questions/741670/rsync-files-to-a-kubernetes-pod?newreg=22b5f958cdce4e6a9a1a7ce0fc88b546
if [ -z "$KRSYNC_STARTED" ]; then
    export KRSYNC_STARTED=true
    exec rsync --blocking-io --rsh "$0" $@
fi

# Running as --rsh
namespace=''
pod=$1
shift

# something seems to have changed about rsync...?
# If rsync uses pod@namespace format, parse it
if [[ "$pod" == *"@"* ]]; then
    namespace="-n ${pod#*@}"
    pod="${pod%@*}"
# If uses -l pod namespace format
elif [ "X$pod" = "X-l" ]; then
    pod=$1
    shift
    namespace="-n $1"
    shift
fi

# echo "pod: $pod  ; namespace: $namespace" > .krsync.log

exec kubectl $namespace exec -i $pod --container "${KRSYNC_CONTAINER}" -- "$@"

#!/usr/bin/env bash
set -euo pipefail

CURRENT_CONTEXT=$(kubectl config current-context 2>/dev/null)

if [ -z "${CURRENT_CONTEXT}" ]; then
  echo "Error: Unable to determine current kubectl context." >&2
  exit 1
fi

DETECTED_CLUSTER=$(kubectl config view -o jsonpath="{.contexts[?(@.name==\"${CURRENT_CONTEXT}\")].context.cluster}" 2>/dev/null)

DETECTED_NAMESPACE=$(kubectl config view --minify -o jsonpath='{..namespace}' 2>/dev/null)
if [ -z "${DETECTED_NAMESPACE}" ]; then
  DETECTED_NAMESPACE="default"
fi

DETECTED_INSTANCE="$(kubectl -n ${DETECTED_NAMESPACE} get ybinstance -o jsonpath='{.items[0].metadata.name}')"

# Default Configuration
# TODO: add arbitrary time intervals
YBINSTANCE="${DETECTED_INSTANCE}"
NAMESPACE="${DETECTED_NAMESPACE}"
DURATION_MINUTES=60
ENABLE_WORKER_DUMP=false
DISABLE_INFRA_LOGS=false
DISABLE_PROBES=false
NOTE=""
WATCH_LOGS=false

msg() {
  echo $(date +%Y-%m-%d\ %T): $1
}

run() {
  if [ "${WATCH_LOGS}" = true ]; then
    "$@" | tee $POD_NAME.log
  else
    "$@" > $POD_NAME.log
  fi
}

# Usage / Help function
usage() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Run a Yellowbrick system diagnostics job.

Context Defaults:
  Detected Cluster  : ${DETECTED_CLUSTER}
  Detected Namespace: ${DETECTED_NAMESPACE}
  Detected Instance : ${DETECTED_INSTANCE}

Options:
  -i, --instance NAME        Instance name (default: [${YBINSTANCE}])
  -n, --namespace NS         Kubernetes namespace (default: active context ns [${NAMESPACE}])
  -d, --duration MINS        Diagnostic window duration in minutes (default: ${DURATION_MINUTES})
  -w, --worker-dump          Enable worker dump (default: ${ENABLE_WORKER_DUMP})
  -l, --disable-infra-logs   Disable infrastructure logs
  -p, --disable-probes       Disable probes
  --note TEXT                Custom note for diagnostic task
  --watch                    Wait for pod creation and stream live logs
  -h, --help                 Show this help message
EOF
  exit 0
}

# Parse Command Line Arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    -i|--instance) YBINSTANCE="$2"; shift 2 ;;
    -n|--namespace) NAMESPACE="$2"; shift 2 ;;
    -d|--duration) DURATION_MINUTES="$2"; shift 2 ;;
    -w|--worker-dump) ENABLE_WORKER_DUMP=true; shift ;;
    -l|--disable-infra-logs) DISABLE_INFRA_LOGS=true; shift ;;
    -p|--disable-probes) DISABLE_PROBES=true; shift ;;
    --note) NOTE="$2"; shift 2 ;;
    --watch) WATCH_LOGS=true; shift ;;
    -h|--help) usage ;;
    *) echo "Unknown option: $1" >&2; exit 1 ;;
  esac
done

if [ -z "${YBINSTANCE}" ]; then
  echo "Error: Instance name could not be determined from cluster context or -i/--instance flag." >&2
  exit 1
fi

# Calculate startTime and endTime in UTC ISO-8601 format
if date --version >/dev/null 2>&1; then
  # GNU date (Linux / RHEL)
  END_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  START_TIME=$(date -u -d "${DURATION_MINUTES} minutes ago" +"%Y-%m-%dT%H:%M:%SZ")
else
  # BSD date (macOS)
  END_TIME=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
  START_TIME=$(date -u -v"-${DURATION_MINUTES}M" +"%Y-%m-%dT%H:%M:%SZ")
fi

echo "=========================================="
echo " Starting System Diagnostics Job"
echo " Context:   ${CURRENT_CONTEXT}"
echo " Instance:  ${YBINSTANCE}"
echo " Namespace: ${NAMESPACE}"
echo " Window:    ($DURATION_MINUTES min) ${START_TIME} -> ${END_TIME}"
echo "=========================================="

# Submit the manifest
TASK_NAME=$(cat <<EOF | kubectl create -f - -n "${NAMESPACE}" -o jsonpath='{.metadata.name}'
apiVersion: cluster.yellowbrick.io/v1
kind: YBInstanceTask
metadata:
  generateName: ${YBINSTANCE}-diagnostics-
  namespace: ${NAMESPACE}
spec:
  instanceName: ${YBINSTANCE}
  instanceNamespace: ${NAMESPACE}
  operation: Diagnostics
  diagsInput:
    disableInfrastructureLogs: ${DISABLE_INFRA_LOGS}
    disableProbes: ${DISABLE_PROBES}
    enableWorkerDump: ${ENABLE_WORKER_DUMP}
    startTime: "${START_TIME}"
    endTime: "${END_TIME}"
    logQueryOptions: ""
    note: "${NOTE}"
    resources:
      limits:
        cpu: 8
        memory: 4Gi
      requests:
        cpu: 2
        memory: 1Gi
EOF
)

msg "Created task resource: ${TASK_NAME}..."
msg "Waiting for operator to spawn the diagnostic pod..."

POD_NAME=""
while [ -z "${POD_NAME}" ]; do
  sleep 1
  read -r POD_NAMESPACE POD_NAME <<< $(kubectl get pod -A -l job-name=$TASK_NAME --sort-by='.metadata.creationTimestamp' -o jsonpath='{.items[0].metadata.namespace}{"\t"}{.items[0].metadata.name}' 2>/dev/null | tail -n 1)
done

msg "Found diagnostic pod ${POD_NAME} in namespace ${POD_NAMESPACE}, waiting for it to become available..."
while [ $(kubectl -n "${POD_NAMESPACE}" get pod "${POD_NAME}" -o jsonpath='{.status.phase}') == Pending ]; do sleep 1; done
msg "The pod is running, waiting for completion..."
run kubectl -n $POD_NAMESPACE logs $POD_NAME -f
msg "Pod has completed its run"

# TODO: auto-download the bundle, I have some ideas...

JOB_STATE=Running
msg "Waiting for the job to finish..."
while [ $JOB_STATE == Running ]; do
  sleep 1
  JOB_STATE=$(kubectl -n $NAMESPACE get ybinstancetask "${TASK_NAME}" -o jsonpath="{.status.state}")
done
msg "The job completed with status: $JOB_STATE"
if [ $JOB_STATE == Completed ]; then
  msg "The diagnostics bundle location: $(kubectl -n $NAMESPACE get ybinstancetask "${TASK_NAME}" -o jsonpath="{range .status.diagsLocation}{.bucket}/{.container}/{.path}{end}")"
fi

msg Done.

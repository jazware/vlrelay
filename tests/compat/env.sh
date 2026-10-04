# Ports and paths of the compat network (sourced). The 34xx block keeps it
# clear of a default dev network (298x) and the cluster e2e (31xx).
compat="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
crate="$(cd "$compat/../.." && pwd)"
export COMPOSE_PROJECT_NAME=${COMPOSE_PROJECT_NAME:-vlrelay-compat}
export PLC_PORT=${PLC_PORT:-3482} REF_PDS_PORT=${REF_PDS_PORT:-3483} PDS_BASE_PORT=${PDS_BASE_PORT:-3484}
export MINIO_PORT=${MINIO_PORT:-3490} MINIO_CONSOLE_PORT=${MINIO_CONSOLE_PORT:-3491}
# indigo's relay takes a port only on the name "localhost", and matches the
# DID document's PDS hostname exactly, so every upstream is localhost:PORT
export DEV_PDS_HOST=localhost
VLRELAY_PORT=${VLRELAY_PORT:-3480}
INDIGO_PORT=${INDIGO_PORT:-3470}
INDIGO_METRICS_PORT=${INDIGO_METRICS_PORT:-3471}
INDIGO_ADMIN_PW=${INDIGO_ADMIN_PW:-compat}
JETSTREAM_PORT=${JETSTREAM_PORT:-3460}
scratch="$compat/scratch"
gobin="$scratch/bin"
run="${COMPAT_OUT:-$scratch/run}"
tdir="${CARGO_TARGET_DIR:-$crate/target}/debug"
export VLPDS_BIN="$tdir/vlpds"
mkdir -p "$run"
VLRELAY_ADMIN_TOKEN=${VLRELAY_ADMIN_TOKEN:-compat}

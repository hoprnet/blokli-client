set -euo pipefail

export BLOKLI_TEST_REMOTE_IMAGE="${BLOKLI_TEST_REMOTE_IMAGE:-europe-west3-docker.pkg.dev/hoprassociation/docker-images/bloklid:latest}"
export BLOKLI_TEST_WORKSPACE_ROOT="${BLOKLI_TEST_WORKSPACE_ROOT:-$PWD}"
export BLOKLI_TEST_IMAGE="${BLOKLI_TEST_IMAGE:-bloklid:integration-test}"
export BLOKLI_TEST_EXTERNAL_STACK=true
export BLOKLI_TEST_RUN_ID="${BLOKLI_TEST_RUN_ID:-run-$(printf '%x' "$$")}"
export BLOKLI_TEST_PORT_BASE="${BLOKLI_TEST_PORT_BASE:-$((20000 + ($$ % 900) * 40))}"
export INSTA_WORKSPACE_ROOT="${INSTA_WORKSPACE_ROOT:-$BLOKLI_TEST_WORKSPACE_ROOT}"

integration_dir="$BLOKLI_TEST_WORKSPACE_ROOT/tests/integration"
stack_names=(query subscription transaction load)

if [[ ! $BLOKLI_TEST_RUN_ID =~ ^[a-z0-9][a-z0-9-]*$ ]]; then
  echo "BLOKLI_TEST_RUN_ID must contain only lowercase ASCII letters, digits, and hyphens" >&2
  exit 1
fi
if [[ ! $BLOKLI_TEST_PORT_BASE =~ ^[0-9]+$ ]] ||
  ((BLOKLI_TEST_PORT_BASE < 1024 || BLOKLI_TEST_PORT_BASE + 32 > 65535)); then
  echo "BLOKLI_TEST_PORT_BASE must leave room for four integration stacks" >&2
  exit 1
fi

stack_values() {
  local index="$1"
  local name="${stack_names[$index]}"
  local registry_port=$((BLOKLI_TEST_PORT_BASE + index * 10))

  stack_id="$BLOKLI_TEST_RUN_ID-$name"
  stack_registry_port="$registry_port"
  stack_anvil_port=$((registry_port + 1))
  stack_bloklid_port=$((registry_port + 2))
}

compose_up() {
  local index="$1"
  stack_values "$index"
  (
    cd "$integration_dir"
    STACK_ID="$stack_id" \
      REGISTRY_PORT="$stack_registry_port" \
      ANVIL_PORT="$stack_anvil_port" \
      BLOKLID_PORT="$stack_bloklid_port" \
      BLOKLID_IMAGE="$BLOKLI_TEST_IMAGE" \
      INTEGRATION_CONFIG="${BLOKLI_TEST_CONFIG:-config-integration-anvil.toml}" \
      docker compose -p "blokli-$stack_id" -f docker-compose.yml up -d
  )
}

compose_down() {
  local index="$1"
  stack_values "$index"
  (
    cd "$integration_dir"
    STACK_ID="$stack_id" \
      REGISTRY_PORT="$stack_registry_port" \
      ANVIL_PORT="$stack_anvil_port" \
      BLOKLID_PORT="$stack_bloklid_port" \
      BLOKLID_IMAGE="$BLOKLI_TEST_IMAGE" \
      INTEGRATION_CONFIG="${BLOKLI_TEST_CONFIG:-config-integration-anvil.toml}" \
      docker compose -p "blokli-$stack_id" -f docker-compose.yml down -v --remove-orphans
  )
}

cleanup() {
  local status="$?"
  local cleanup_pids=()

  trap - EXIT
  set +e
  for index in "${!stack_names[@]}"; do
    compose_down "$index" &
    cleanup_pids+=("$!")
  done
  for pid in "${cleanup_pids[@]}"; do
    wait "$pid"
  done
  exit "$status"
}

prepare_image() {
  local result_path="$BLOKLI_TEST_WORKSPACE_ROOT/result"
  local source_image=""
  local load_output=""

  if [[ -e $result_path ]] && load_output="$(docker load --input "$result_path")"; then
    while IFS= read -r line; do
      if [[ $line == "Loaded image: "* ]]; then
        source_image="${line#Loaded image: }"
      elif [[ $line == "Loaded image ID: "* ]]; then
        source_image="${line#Loaded image ID: }"
      fi
    done <<<"$load_output"
  fi

  if [[ -z $source_image ]]; then
    if [[ -z $BLOKLI_TEST_REMOTE_IMAGE ]]; then
      echo "No local bloklid image found and BLOKLI_TEST_REMOTE_IMAGE is not set" >&2
      return 1
    fi
    docker pull --platform linux/amd64 "$BLOKLI_TEST_REMOTE_IMAGE"
    source_image="$BLOKLI_TEST_REMOTE_IMAGE"
  fi

  docker tag "$source_image" "$BLOKLI_TEST_IMAGE"
}

trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

archive_path="$(nix build -L --no-link --print-out-paths .#integration-tests)"

prepare_image

start_pids=()
for index in "${!stack_names[@]}"; do
  compose_up "$index" &
  start_pids+=("$!")
done
start_failed=false
for pid in "${start_pids[@]}"; do
  if ! wait "$pid"; then
    start_failed=true
  fi
done
if [[ $start_failed == true ]]; then
  echo "Failed to start one or more integration Docker stacks" >&2
  exit 1
fi

cargo-nextest nextest run \
  --archive-file "$archive_path/integration-tests.tar.zst" \
  --workspace-remap "$BLOKLI_TEST_WORKSPACE_ROOT" \
  "$@"

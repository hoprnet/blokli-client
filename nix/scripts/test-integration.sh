#!/usr/bin/env bash

set -euo pipefail

export BLOKLI_TEST_REMOTE_IMAGE="${BLOKLI_TEST_REMOTE_IMAGE:-europe-west3-docker.pkg.dev/hoprassociation/docker-images/bloklid-anvil-curvy:0.14.1-commit.e939e67@sha256:02dedc69b6d60c8e961162165ac95234db3e87ea40106ff869d40c6cb0e23031}"
export BLOKLI_TEST_WORKSPACE_ROOT="${BLOKLI_TEST_WORKSPACE_ROOT:-$PWD}"
export BLOKLI_TEST_IMAGE="${BLOKLI_TEST_IMAGE:-bloklid-anvil:integration-test}"
export BLOKLI_TEST_EXTERNAL_STACK=true
export BLOKLI_TEST_RUN_ID="${BLOKLI_TEST_RUN_ID:-run-$(printf '%x-%x' "$$" "$RANDOM")}"
port_base_is_fixed=false
if [[ -n ${BLOKLI_TEST_PORT_BASE:-} ]]; then
  port_base_is_fixed=true
else
  export BLOKLI_TEST_PORT_BASE=$((20000 + (($$ + RANDOM) % 900) * 50))
fi
export INSTA_WORKSPACE_ROOT="${INSTA_WORKSPACE_ROOT:-$BLOKLI_TEST_WORKSPACE_ROOT}"

integration_dir="$BLOKLI_TEST_WORKSPACE_ROOT/tests/integration"
stack_names=(query subscription transaction load deposit)

if [[ ! $BLOKLI_TEST_RUN_ID =~ ^[a-z0-9][a-z0-9-]*$ ]]; then
  echo "BLOKLI_TEST_RUN_ID must contain only lowercase ASCII letters, digits, and hyphens" >&2
  exit 1
fi
if [[ ! $BLOKLI_TEST_PORT_BASE =~ ^[0-9]+$ ]] ||
  ((BLOKLI_TEST_PORT_BASE < 1024 || BLOKLI_TEST_PORT_BASE + 42 > 65535)); then
  echo "BLOKLI_TEST_PORT_BASE must leave room for five integration stacks" >&2
  exit 1
fi

stack_values() {
  local index="$1"
  local name="${stack_names[$index]}"
  local stack_port_base=$((BLOKLI_TEST_PORT_BASE + index * 10))

  stack_id="$BLOKLI_TEST_RUN_ID-$name"
  stack_anvil_port=$((stack_port_base + 1))
  stack_bloklid_port=$((stack_port_base + 2))
}

compose_up() {
  local index="$1"
  stack_values "$index"
  (
    cd "$integration_dir"
    STACK_ID="$stack_id" \
      ANVIL_PORT="$stack_anvil_port" \
      BLOKLID_PORT="$stack_bloklid_port" \
      BLOKLID_IMAGE="$BLOKLI_TEST_IMAGE" \
      docker compose -p "blokli-$stack_id" -f docker-compose.yml up -d
  )
}

compose_down() {
  local index="$1"
  stack_values "$index"
  (
    cd "$integration_dir"
    STACK_ID="$stack_id" \
      ANVIL_PORT="$stack_anvil_port" \
      BLOKLID_PORT="$stack_bloklid_port" \
      BLOKLID_IMAGE="$BLOKLI_TEST_IMAGE" \
      docker compose -p "blokli-$stack_id" -f docker-compose.yml down -v --remove-orphans
  )
}

start_stacks() {
  local start_pids=()
  local start_failed=false
  local start_log
  start_log="$(mktemp)"

  for index in "${!stack_names[@]}"; do
    compose_up "$index" >>"$start_log" 2>&1 &
    start_pids+=("$!")
  done
  for pid in "${start_pids[@]}"; do
    if ! wait "$pid"; then
      start_failed=true
    fi
  done
  cat "$start_log" >&2

  if [[ $start_failed == true ]] && grep -qiE 'address already in use|port is already allocated|bind:.*address.*in use' "$start_log"; then
    rm -f "$start_log"
    return 2
  fi
  rm -f "$start_log"

  [[ $start_failed == false ]]
}

stop_stacks() {
  local cleanup_pids=()

  for index in "${!stack_names[@]}"; do
    compose_down "$index" &
    cleanup_pids+=("$!")
  done
  for pid in "${cleanup_pids[@]}"; do
    wait "$pid"
  done
}

cleanup() {
  local status="$?"

  trap - EXIT
  set +e
  stop_stacks
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
      echo "No local bloklid-anvil image found and BLOKLI_TEST_REMOTE_IMAGE is not set" >&2
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

stacks_started=false
for attempt in {1..3}; do
  start_status=0
  start_stacks || start_status="$?"
  if ((start_status == 0)); then
    stacks_started=true
    break
  fi

  stop_stacks
  if ((start_status != 2)) || [[ $port_base_is_fixed == true ]]; then
    break
  fi

  previous_port_base="$BLOKLI_TEST_PORT_BASE"
  port_slot=$((((BLOKLI_TEST_PORT_BASE - 20000) / 50 + 1) % 900))
  export BLOKLI_TEST_PORT_BASE=$((20000 + port_slot * 50))
  echo "Integration port attempt $attempt failed at $previous_port_base; retrying at $BLOKLI_TEST_PORT_BASE" >&2
done
if [[ $stacks_started == false ]]; then
  echo "Failed to start one or more integration Docker stacks" >&2
  exit 1
fi

cargo-nextest nextest run \
  --archive-file "$archive_path/integration-tests.tar.zst" \
  --workspace-remap "$BLOKLI_TEST_WORKSPACE_ROOT" \
  "$@"

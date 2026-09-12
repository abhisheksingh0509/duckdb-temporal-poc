#!/usr/bin/env bash
# Durable execution, demonstrated without a race.
#
# The obvious version of this demo -- start a run, sleep, kill the writer --
# only works if you guess the timing right, and at this data volume the run is
# usually finished before the kill lands. So it proves nothing.
#
# The approval gate makes it deterministic instead: the workflow parks, we stop
# *both* workers, show that nothing is running, bring them back, and release the
# gate. The workflow resumes on brand-new processes and publishes. The only
# state that survived is Temporal's history.
set -uo pipefail

B=$(printf '\033[1m'); N=$(printf '\033[0m')
CLI="docker compose exec -T worker-compute python -m duckflow.cli"
ID="durability-demo"
DATE="2026-10-04"

say() { echo; echo "${B}== $1${N}"; }

say "1. start a run that parks at the approval gate"
$CLI run --date "$DATE" --meters 20000 --id "$ID" --approval-timeout 0
sleep 8
$CLI watch "$ID" | head -3

say "2. stop BOTH workers -- every process that could run this code is now gone"
docker compose stop worker-compute worker-writer
docker compose ps --format '{{.Service}}\t{{.State}}' | grep worker || echo "  (no worker containers running)"

say "3. the workflow is still there, held by the server, costing nothing"
docker compose exec -T temporal temporal workflow list --address temporal:7233 \
    --query "WorkflowId='$ID'" 2>/dev/null | head -3

say "4. bring the workers back -- new containers, empty memory"
docker compose start worker-compute worker-writer
for _ in $(seq 1 30); do
  docker compose logs worker-writer 2>/dev/null | grep -q "worker ready" && break
  sleep 2
done
echo "  workers ready"

say "5. release the gate. The publish runs on a process that did not exist in step 1"
$CLI approve "$ID" --actor "durability-demo"
sleep 6
$CLI watch "$ID" | head -3
$CLI query "select run_id, status, rows_published from meta.runs where run_id = '$ID'"

echo
echo "${B}That is durable execution.${N} No worker held this workflow in memory;"
echo "the history did, and the code resumed exactly where it had stopped."

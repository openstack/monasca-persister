#!/bin/bash

# Copyright 2017 SUSE LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

PORT=8091
CHECK_INTERVAL=30
TIME_STAMP=$(date +%s)

mkdir persister_perf_${TIME_STAMP}

usage()
{
  echo "usage: persister_perf.sh [[-s servers (required)] [-p port] \
[-t JMeter test plan file] [-n number of metrics to insert (required)] \
[-w wait time (seconds) betweenchecking persister status] | [-h help ]]"
  exit
}

log()
{
#  echo "$1"
  echo "$1" >> persister_perf_${TIME_STAMP}/persister_perf.log
}

output()
{
#  echo "$1"
  echo "$1" >> persister_perf_${TIME_STAMP}/persister_perf_output.txt
}

get_received_metric_count_per_host()
{
  local msg_count=$(curl -m 60 -s http://$1:$PORT/metrics?pretty=true | \
jq '[.meters | with_entries(select(.key|match("monasca.persister.pipeline.event.MetricHandler\\[metric-[0-9]*\\].events-processed-meter";"i")))[] | .count] | add')
  echo "$msg_count"
}

get_total_received_metric_count()
{
  local count=0
  for server in $SERVERS; do
    local count_per_host=$(get_received_metric_count_per_host $server)
    log "$(date) Persister host: $server; Received metrics: $count_per_host"
    count=$((count + count_per_host))
  done
  log "$(date) Total received metrics: $count"
  echo "$count"
}

get_flushed_metric_count_per_host()
{
  local msg_count=$(curl -m 60 -s http://$1:$PORT/metrics?pretty=true \
  | jq '[.meters | with_entries(select(.key|match("monasca.persister.pipeline.event.MetricHandler\\[metric-[0-9]*\\].flush-meter";"i")))[] | .count] | add')
  echo "$msg_count"
}

get_total_flushed_metric_count()
{
  local count=0
  for server in $SERVERS; do
    local count_per_host=$(get_flushed_metric_count_per_host $server)
    log "$(date) Persister host: $server; Flushed metrics: $count_per_host"
    count=$((count + count_per_host))
  done
  log "$(date) Total flushed metrics: $count"
  echo "$count"
}


while getopts "hs:p:t:n:w:" option; do
  case "${option}" in
    h) usage;;
    s) declare -a SERVERS=$(echo "${OPTARG}" | sed "s/,/\n/g");;
    p) PORT=${OPTARG};;
    t) TEST_PLAN=${OPTARG};;
    n) NUM_METRICS=${OPTARG};;
    w) CHECK_INTERVAL=${OPTARG};;
    *) exit 1;;
    :) echo "Missing option argument for -$OPTARG" >&2; exit 1;;
  esac
done

if [ ! "$SERVERS" ] || [ ! "$NUM_METRICS" ]; then
  usage
  exit 1
fi

log "starting JMeter run $TEST_PLAN"

jmeter -n -t $TEST_PLAN -l persister_perf_${TIME_STAMP}/jmeter.jnl -e \
-o persister_perf_${TIME_STAMP}/jmeter \
>> persister_perf_${TIME_STAMP}/persister_perf.log 2>&1 &

START_TIME=$(date +%s)

received_metric_count_start=$(get_total_received_metric_count)
output "Total received metric count at start: $received_metric_count_start"

flushed_metric_count_start=$(get_total_flushed_metric_count)
output "Total flushed metric count at start: $flushed_metric_count_start"

received_metric_count_orig=$((received_metric_count_start))

flushed_metric_count_orig=$((flushed_metric_count_start))

target_received_metric_count=$((received_metric_count_start + NUM_METRICS))

target_flushed_metric_count=$((flushed_metric_count_start + NUM_METRICS))

sleep $CHECK_INTERVAL

INTERVAL_END_TIME=$(date +%s)
received_metric_count=$(get_total_received_metric_count)
flushed_metric_count=$(get_total_flushed_metric_count)

while [ "$received_metric_count" -lt "$target_received_metric_count" \
       -o "$flushed_metric_count" -lt "$target_flushed_metric_count" ]
do
  INTERVAL_START_TIME=$((INTERVAL_END_TIME))
  received_metric_count_start=$((received_metric_count))
  flushed_metric_count_start=$((flushed_metric_count))

  sleep $CHECK_INTERVAL
  INTERVAL_END_TIME=$(date +%s)
  received_metric_count=$(get_total_received_metric_count)
  flushed_metric_count=$(get_total_flushed_metric_count)

  log "Current received metric throughput: \
  $((($received_metric_count - $received_metric_count_start) \
  / $(($INTERVAL_END_TIME - $INTERVAL_START_TIME))))"
  log "Current flushed metric throughput: \
  $((($flushed_metric_count - $flushed_metric_count_start) \
  / $(($INTERVAL_END_TIME - $INTERVAL_START_TIME))))"
  log "Average received metric throughput: \
  $((($received_metric_count - $received_metric_count_orig) \
  / $(($INTERVAL_END_TIME - $START_TIME))))"
  log "Average flushed metric throughput: \
  $((($flushed_metric_count - $flushed_metric_count_orig) \
  / $(($INTERVAL_END_TIME - $START_TIME))))"
  log "Expect $((target_flushed_metric_count - flushed_metric_count)) \
  more metrics to be flushed"
done

END_TIME=$(date +%s)
ELAPSED=$(($END_TIME - $START_TIME))
output "Total received metric count at end: $received_metric_count"
output "Total flushed metric count at end: $flushed_metric_count"
output "Total elapsed time: $ELAPSED"
output "Average received metrics/second: \
$((($received_metric_count - $received_metric_count_orig) / $ELAPSED))"
output "Average persisted metrics/second: \
$((($flushed_metric_count - $flushed_metric_count_orig) / $ELAPSED))"

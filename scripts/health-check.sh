#!/usr/bin/env sh
set -e

GATEWAY_URL="${GATEWAY_URL:-http://localhost:8080}"
ENGINE_URL="${ENGINE_URL:-http://localhost:8000}"
OLLAMA_URL="${OLLAMA_URL:-http://localhost:11434}"
RABBIT_API="${RABBIT_API:-http://localhost:15672/api/queues}"
RABBIT_USER="${RABBITMQ_USER:-cvuser}"
RABBIT_PASSWORD="${RABBITMQ_PASSWORD:-cvpass}"

deadline=$(( $(date +%s) + 120 ))

check_gateway() {
  curl -sf "${GATEWAY_URL}/actuator/health" | grep -q '"status":"UP"'
}
check_engine() {
  curl -sf "${ENGINE_URL}/health/ready" | grep -q '"status":"ok"'
}
check_ollama() {
  curl -sf "${OLLAMA_URL}/api/tags" | grep -q 'llama3.2:3b'
}
check_rabbit() {
  queues="$(curl -sf -u "${RABBIT_USER}:${RABBIT_PASSWORD}" "${RABBIT_API}")"
  echo "${queues}" | grep -q 'cv_parse_queue' && echo "${queues}" | grep -q 'cv_result_queue'
}

while [ "$(date +%s)" -lt "${deadline}" ]; do
  ok=1
  check_gateway || { echo "Gateway health not ready"; ok=0; }
  check_engine || { echo "Engine health not ready"; ok=0; }
  check_ollama || { echo "Ollama model not ready"; ok=0; }
  check_rabbit || { echo "Rabbit queues not ready"; ok=0; }
  if [ "${ok}" -eq 1 ]; then
    echo "All health checks passed."
    exit 0
  fi
  sleep 5
done

echo "Health checks failed after 120 seconds."
exit 1

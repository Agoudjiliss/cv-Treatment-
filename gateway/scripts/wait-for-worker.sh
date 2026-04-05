#!/bin/sh
set -e
URL="${CV_ENGINE_HEALTH_URL:-http://engine:8000/health/ready}"
echo "Waiting for Python worker at ${URL}"
i=0
while [ "$i" -lt 90 ]; do
  if curl -sf "$URL" > /dev/null 2>&1; then
    echo "Python worker is ready."
    exec java -jar /app/app.jar
  fi
  i=$((i + 1))
  sleep 2
done
echo "Timeout waiting for Python worker health."
exit 1

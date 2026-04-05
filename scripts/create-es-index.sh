#!/usr/bin/env sh
set -e

ES_URL="${ELASTICSEARCH_URL:-http://localhost:9200}"
INDEX_NAME="${CV_INDEX_NAME:-cv-index}"

curl -s -X PUT "${ES_URL}/${INDEX_NAME}" \
  -H "Content-Type: application/json" \
  -d '{
    "mappings": {
      "properties": {
        "cvId": { "type": "keyword" },
        "skillsSet": { "type": "keyword" },
        "languages": { "type": "keyword" },
        "seniorityYears": { "type": "integer" },
        "locationCountry": { "type": "keyword" },
        "confidence": { "type": "float" },
        "embeddingVector": {
          "type": "dense_vector",
          "dims": 384,
          "index": true,
          "similarity": "cosine"
        },
        "cvJson": { "type": "object", "enabled": true },
        "indexedAt": { "type": "date" }
      }
    }
  }'

echo
echo "Index ${INDEX_NAME} created/updated."

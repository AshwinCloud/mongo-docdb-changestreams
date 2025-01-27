aws kafkaconnect update-connector \
    --connector-arn "your-connector-arn" \
    --current-version "current-version" \
    --capacity '{"provisionedCapacity": {"mcuCount": 1, "workerCount": 0}}'

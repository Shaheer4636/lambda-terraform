# 1) Write the JSON payload
Set-Content -Path payload.json -Value '{"action":"run","note":"manual trigger"}' -NoNewline -Encoding utf8

# 2) Invoke the Lambda
aws lambda invoke `
  --function-name uptime-uptime-lambda `
  --region us-east-1 `
  --payload fileb://payload.json `
  out.json

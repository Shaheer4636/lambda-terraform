$event = @{
  action = "run"
  note   = "manual trigger"
} | ConvertTo-Json -Compress

aws lambda invoke `
  --function-name uptime-uptime-lambda `
  --region us-east-1 `
  --cli-binary-format raw-in-base64-out `
  --payload $event `
  out.json

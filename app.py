Compress-Archive -Path .\lambda_output_report.py -DestinationPath .\function.zip -Force

aws lambda update-function-code `
  --function-name uptime-uptime-lambda `
  --region us-east-1 `
  --zip-file fileb://function.zip

aws lambda update-function-configuration `
  --function-name uptime-uptime-lambda `
  --region us-east-1 `
  --environment "Variables={REPORTS_BUCKET=lambda-output-report-000000987123,REPORTS_PREFIX=reports/cdp,CLIENTS_JSON={\"Acquity\":[\"acquity-home\"],\"Strata\":[\"strata-ui\"]},SERVICE_NAME=CDP,COMPANY_NAME=LogicEase Solutions Inc.,FAIL_STREAK=3,DOWNSAMPLE_MINUTES=5}"



aws lambda invoke `
  --function-name uptime-uptime-lambda `
  --region us-east-1 `
  --cli-binary-format raw-in-base64-out `
  --payload '{}' `
  out.json



$bucket="lambda-output-report-000000987123"
$prefix="reports/cdp"
$yy=(Get-Date).ToUniversalTime().AddMonths(-1).ToString('yyyy')
$mm=(Get-Date).ToUniversalTime().AddMonths(-1).ToString('MM')

aws s3 ls "s3://$bucket/$prefix/$yy/$mm/" --region us-east-1

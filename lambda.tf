# ---------------- IAM ----------------
data "aws_iam_policy_document" "assume_lambda" {
  statement {
    effect  = "Allow"
    actions = ["sts:AssumeRole"]
    principals {
      type        = "Service"
      identifiers = ["lambda.amazonaws.com"]
    }
  }
}

resource "aws_iam_role" "lambda" {
  name               = "${var.name_prefix}-role"
  assume_role_policy = data.aws_iam_policy_document.assume_lambda.json
  description        = "Role for uptime monthly report (artifact-based)"
}

data "aws_iam_policy_document" "lambda_policy" {
  statement {
    sid     = "S3ListArtifacts"
    effect  = "Allow"
    actions = ["s3:ListBucket"]
    resources = ["arn:aws:s3:::${var.artifact_bucket}"]
  }

  statement {
    sid     = "S3GetArtifacts"
    effect  = "Allow"
    actions = ["s3:GetObject"]
    resources = ["arn:aws:s3:::${var.artifact_bucket}/${var.artifact_prefix}/*"]
  }

  statement {
    sid     = "S3WriteReports"
    effect  = "Allow"
    actions = ["s3:PutObject", "s3:GetObject", "s3:ListBucket", "s3:DeleteObject"]
    resources = [
      "arn:aws:s3:::${var.reports_bucket}",
      "arn:aws:s3:::${var.reports_bucket}/*"
    ]
  }

  statement {
    sid     = "Logs"
    effect  = "Allow"
    actions = ["logs:CreateLogGroup","logs:CreateLogStream","logs:PutLogEvents"]
    resources = ["*"]
  }
}

resource "aws_iam_policy" "lambda" {
  name   = "${var.name_prefix}-policy"
  policy = data.aws_iam_policy_document.lambda_policy.json
}

resource "aws_iam_role_policy_attachment" "lambda" {
  role       = aws_iam_role.lambda.name
  policy_arn = aws_iam_policy.lambda.arn
}

# ---------------- Package Lambda (zip app.py) ----------------
data "archive_file" "lambda_zip" {
  type        = "zip"
  output_path = "${path.module}/lambda.zip"

  source {
    filename = "app.py"
    content  = file("${path.module}/app.py")
  }
}

# ---------------- Logs ----------------
resource "aws_cloudwatch_log_group" "lambda" {
  name              = "/aws/lambda/${var.name_prefix}"
  retention_in_days = 14
}

# ---------------- Lambda ----------------
resource "aws_lambda_function" "reporter" {
  function_name = var.name_prefix
  role          = aws_iam_role.lambda.arn
  handler       = "app.handler"      # app.py -> app.handler
  runtime       = "python3.12"
  timeout       = 900
  memory_size   = 512
  architectures = [var.lambda_architecture]

  ephemeral_storage { size = 1024 }

  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = filebase64sha256(data.archive_file.lambda_zip.output_path)

  # No layers (PDF generation removed)

  environment {
    variables = {
      ARTIFACT_BUCKET = var.artifact_bucket
      ARTIFACT_PREFIX = var.artifact_prefix
      ONLY_BROWSER    = var.only_browser

      REPORTS_BUCKET  = var.reports_bucket
      REPORTS_PREFIX  = var.reports_prefix

      COMPANY_NAME     = var.company_name
      SERVICE_NAME     = var.service_name
      CLIENT_NAME      = var.client_name
      BRAND_STRAPLINE  = var.brand_strapline

      SLO_TARGET    = tostring(var.slo_target)
      FAIL_STREAK   = tostring(var.fail_streak)
      TREAT_MISSING = tostring(var.treat_missing)
    }
  }

  depends_on = [aws_cloudwatch_log_group.lambda, aws_iam_role_policy_attachment.lambda]
}

# ---------------- Outputs ----------------
output "lambda_name"   { value = aws_lambda_function.reporter.function_name }
output "report_bucket" { value = var.reports_bucket }

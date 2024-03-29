variable "logs_bucket" {}
variable "logs_prefix" {}
variable "entries_prefix" {
  default = "entries/"
}

resource "aws_lambda_function_url" "lambda" {
  function_name = aws_lambda_function.lambda.function_name
  authorization_type = "NONE"
}

output "function_url" {
  value = aws_lambda_function_url.lambda.function_url
}

resource "aws_lambda_permission" "lambda" {
  action = "lambda:InvokeFunctionUrl"
  function_name = aws_lambda_function.lambda.function_name
  principal = "*"
  function_url_auth_type = "NONE"
}

resource "aws_dynamodb_table" "site_analyser" {
  name = "site-analyser"
  billing_mode = "PAY_PER_REQUEST"
  hash_key = "date"
  range_key = "url"

  attribute {
    name = "date"
    type = "S"
  }

  attribute {
    name = "url"
    type = "S"
  }
}

resource "aws_iam_role" "lambda" {
  name = "site-analyser-lambda"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "lambda.amazonaws.com"
        }
      }
    ]
  })
}

resource "aws_iam_policy" "lambda" {
  name = "site-analyser-lambda"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "${aws_cloudwatch_log_group.lambda.arn}:*"
      },
      {
        Effect = "Allow"
        Action = [
          "dynamodb:PutItem",
          "dynamodb:Query",
          "dynamodb:UpdateItem",
        ]
        Resource = aws_dynamodb_table.site_analyser.arn
      },
      {
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:ListBucket"
        ]
        Resource = [
          "arn:aws:s3:::${var.logs_bucket}",
          "arn:aws:s3:::${var.logs_bucket}/${var.logs_prefix}*",
          "arn:aws:s3:::${var.logs_bucket}/${var.entries_prefix}*"
        ]
      },
      {
        Effect = "Allow"
        Action = "s3:PutObject"
        Resource = "arn:aws:s3:::${var.logs_bucket}/${var.entries_prefix}*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "lambda" {
  role = aws_iam_role.lambda.name
  policy_arn = aws_iam_policy.lambda.arn
}

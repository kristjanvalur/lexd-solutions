resource "aws_s3_bucket" "lambda_s3_bucket" {
  bucket = var.lambda_s3_bucket_name
  acl    = "private"

  versioning {
    enabled = true
  }

  lifecycle_rule {
    enabled = true

    abort_incomplete_multipart_upload_days = 1

    noncurrent_version_expiration {
      days = 3
    }

    expiration {
      expired_object_delete_marker = true
    }
  }
}

##################
# Code Deploy S3
resource "aws_s3_bucket" "codedeploy_s3_bucket" {
  bucket = var.codedeploy_s3_bucket_name
  acl    = "private"

  versioning {
    enabled = true
  }

  lifecycle_rule {
    enabled = true

    abort_incomplete_multipart_upload_days = 1

    noncurrent_version_expiration {
      days = 3
    }

    expiration {
      expired_object_delete_marker = true
    }
  }
}

resource "aws_s3_bucket_notification" "codedeploy_s3_notification" {
  bucket = aws_s3_bucket.codedeploy_s3_bucket.id

  lambda_function {
    lambda_function_arn = aws_lambda_function.codedeploy_lambda.arn
    events              = ["s3:ObjectCreated:*"]
    filter_suffix       = ".zip"
  }
}
# END CodeDeploy S3

locals {
  all_buckets = {
    lambda     = aws_s3_bucket.lambda_s3_bucket.id,
    codedeploy = aws_s3_bucket.codedeploy_s3_bucket.id
  }
}

resource "aws_s3_bucket_public_access_block" "s3_bucket_block_public" {
  for_each = { for k, v in local.all_buckets : k => v }

  bucket = each.value

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

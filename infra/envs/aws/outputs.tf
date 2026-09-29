output "lake_bucket" { value = aws_s3_bucket.lake.bucket }
output "db_endpoint" { value = aws_db_instance.pg.endpoint }
output "ecr_repo_url" { value = aws_ecr_repository.api.repository_url }

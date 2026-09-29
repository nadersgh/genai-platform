data "aws_vpc" "default" {
  default = true
}

data "aws_subnets" "default" {
  filter {
    name   = "vpc-id"
    values = [data.aws_vpc.default.id]
  }
}

resource "random_id" "suffix" {
  byte_length = 3
}

# ---------- Object store: lakehouse + artifacts ----------
resource "aws_s3_bucket" "lake" {
  bucket = "${var.project}-lake-${random_id.suffix.hex}"
}

resource "aws_s3_bucket_versioning" "lake" {
  bucket = aws_s3_bucket.lake.id
  versioning_configuration { status = "Enabled" }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "lake" {
  bucket = aws_s3_bucket.lake.id
  rule {
    apply_server_side_encryption_by_default { sse_algorithm = "AES256" }
  }
}

resource "aws_s3_bucket_public_access_block" "lake" {
  bucket                  = aws_s3_bucket.lake.id
  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# ---------- Postgres + pgvector (RDS) ----------
resource "aws_db_subnet_group" "this" {
  name       = "${var.project}-db"
  subnet_ids = data.aws_subnets.default.ids
}

resource "aws_security_group" "db" {
  name   = "${var.project}-db"
  vpc_id = data.aws_vpc.default.id
  # No ingress yet: add app/bastion rules as you build compute (week 9).
}

resource "aws_db_instance" "pg" {
  identifier                  = "${var.project}-pg"
  engine                      = "postgres"
  engine_version              = "16"
  instance_class              = var.db_instance_class
  allocated_storage           = 20
  db_name                     = "platform"
  username                    = "platform"
  manage_master_user_password = true
  db_subnet_group_name        = aws_db_subnet_group.this.name
  vpc_security_group_ids      = [aws_security_group.db.id]
  storage_encrypted           = true
  publicly_accessible         = false
  skip_final_snapshot         = true
  # pgvector: run `CREATE EXTENSION vector;` after first connect.
}

# ---------- Container registry for the API image ----------
resource "aws_ecr_repository" "api" {
  name                 = "${var.project}-api"
  image_tag_mutability = "MUTABLE"
  image_scanning_configuration { scan_on_push = true }
}

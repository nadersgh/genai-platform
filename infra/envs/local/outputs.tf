output "endpoints" {
  value = {
    s3       = "http://localhost:8333"
    postgres = "postgresql://platform@localhost:5432/platform"
    qdrant   = "http://localhost:6333"
    kafka    = "localhost:9092"
    mlflow   = "http://localhost:5001"
    adminer  = "http://localhost:8081"
  }
}

resource "docker_network" "platform" {
  name = "${var.project}-net"
}

# ---------- Object store (S3 API via SeaweedFS; MinIO dropped community images) ----------
resource "docker_image" "s3" {
  name = "chrislusf/seaweedfs:latest"
}

resource "docker_volume" "s3" {
  name = "${var.project}-s3"
}

resource "docker_container" "s3" {
  name    = "${var.project}-s3"
  image   = docker_image.s3.image_id
  command = ["server", "-s3", "-s3.config=/etc/seaweedfs/s3.json", "-dir=/data", "-s3.port=8333", "-master.volumeSizeLimitMB=1024"]
  networks_advanced { name = docker_network.platform.name }
  upload {
    file = "/etc/seaweedfs/s3.json"
    content = jsonencode({
      identities = [{
        name        = "platform"
        credentials = [{ accessKey = var.s3_access_key, secretKey = var.s3_secret_key }]
        actions     = ["Admin", "Read", "Write", "List", "Tagging"]
      }]
    })
  }
  ports {
    internal = 8333
    external = 8333
  }
  volumes {
    volume_name    = docker_volume.s3.name
    container_path = "/data"
  }
}

# ---------- Postgres + pgvector (metadata, MLflow backend, vectors) ----------
resource "docker_image" "postgres" {
  name = "pgvector/pgvector:pg16"
}

resource "docker_volume" "postgres" {
  name = "${var.project}-pg"
}

resource "docker_container" "postgres" {
  name  = "${var.project}-postgres"
  image = docker_image.postgres.image_id
  env = [
    "POSTGRES_USER=platform",
    "POSTGRES_PASSWORD=${var.postgres_password}",
    "POSTGRES_DB=platform",
  ]
  networks_advanced { name = docker_network.platform.name }
  ports {
    internal = 5432
    external = 5432
  }
  volumes {
    volume_name    = docker_volume.postgres.name
    container_path = "/var/lib/postgresql/data"
  }
}

# ---------- Vector DB ----------
resource "docker_image" "qdrant" {
  name = "qdrant/qdrant:latest"
}

resource "docker_volume" "qdrant" {
  name = "${var.project}-qdrant"
}

resource "docker_container" "qdrant" {
  name  = "${var.project}-qdrant"
  image = docker_image.qdrant.image_id
  networks_advanced { name = docker_network.platform.name }
  ports {
    internal = 6333
    external = 6333
  }
  volumes {
    volume_name    = docker_volume.qdrant.name
    container_path = "/qdrant/storage"
  }
}

# ---------- Kafka API (Redpanda) ----------
resource "docker_image" "redpanda" {
  name = "redpandadata/redpanda:latest"
}

resource "docker_container" "redpanda" {
  name  = "${var.project}-redpanda"
  image = docker_image.redpanda.image_id
  command = [
    "redpanda", "start", "--overprovisioned", "--smp", "1", "--memory", "512M",
    "--kafka-addr", "PLAINTEXT://0.0.0.0:9092",
    "--advertise-kafka-addr", "PLAINTEXT://localhost:9092",
  ]
  networks_advanced { name = docker_network.platform.name }
  ports {
    internal = 9092
    external = 9092
  }
}

# ---------- MLflow (tracking + eval results) ----------
resource "docker_image" "mlflow" {
  name = "python:3.12-slim"
}

resource "docker_volume" "mlflow" {
  name = "${var.project}-mlflow"
}

resource "docker_container" "mlflow" {
  name  = "${var.project}-mlflow"
  image = docker_image.mlflow.image_id
  command = [
    "sh", "-c",
    "pip install --quiet \"mlflow==3.*\" psycopg2-binary \"cryptography<45\" && mlflow server --host 0.0.0.0 --port 5000 --backend-store-uri postgresql+psycopg2://platform:${var.postgres_password}@${docker_container.postgres.name}:5432/platform --serve-artifacts --artifacts-destination /mlartifacts",
  ]
  networks_advanced { name = docker_network.platform.name }
  ports {
    internal = 5000
    external = 5001 # 5000 is taken by macOS AirPlay Receiver
  }
  volumes {
    volume_name    = docker_volume.mlflow.name
    container_path = "/mlartifacts"
  }
}

# ---------- Postgres web UI (Adminer) ----------
resource "docker_image" "adminer" {
  name = "adminer:latest"
}

resource "docker_container" "adminer" {
  name  = "${var.project}-adminer"
  image = docker_image.adminer.image_id
  env = [
    "ADMINER_DEFAULT_SERVER=${docker_container.postgres.name}",
    "ADMINER_DESIGN=pepa-linha-dark",
  ]
  networks_advanced { name = docker_network.platform.name }
  ports {
    internal = 8080
    external = 8081
  }
}

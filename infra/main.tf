
#PROJECT
data "google_project" "project" {
  project_id = var.project
}

#COMPOSER
resource "google_composer_environment" "composer" {
  project = var.project
  name    = "test-airflow-dataproc"
  region  = var.location
  config {
    environment_size = "ENVIRONMENT_SIZE_SMALL"
    software_config {
      image_version = "composer-3-airflow-2"
      env_variables = {
        PROJECT = var.project
        REGION  = var.location
        BUCKET  = google_storage_bucket.composer_dags.name
      }
    }
    node_config {
      service_account = google_service_account.composer.name
    }
  }
  storage_config {
    bucket = google_storage_bucket.composer_dags.name
  }
}

resource "google_service_account" "composer" {
  project      = var.project
  account_id   = "composer-test-dataproc"
  display_name = "Test Service Account for Composer Environment and Dataproc"
}

resource "google_project_iam_member" "composer-worker" {
  for_each = toset(["roles/composer.worker", "roles/storage.objectUser", "roles/bigquery.dataEditor", "roles/dataproc.editor", "roles/dataproc.worker"])

  project = var.project
  role    = each.value
  member  = "serviceAccount:${google_service_account.composer.email}"
}

resource "google_project_iam_member" "dataproc-worker" {
  for_each = toset(["roles/bigquery.dataEditor", "roles/dataproc.worker", "roles/bigquery.jobUser"])
  project  = var.project
  role     = each.value
  member   = "serviceAccount:${data.google_project.project.number}-compute@developer.gserviceaccount.com"
}

resource "google_service_account_iam_member" "composer_service_account_user" {
  service_account_id = "projects/${var.project}/serviceAccounts/${data.google_project.project.number}-compute@developer.gserviceaccount.com"
  role               = "roles/iam.serviceAccountUser"
  member             = "serviceAccount:${google_service_account.composer.email}"
}

#GCS
resource "google_storage_bucket" "composer_dags" {
  project       = var.project
  name          = "test_airflow_composer_dataproc"
  location      = var.location
  force_destroy = true
}

#DATASET
resource "google_bigquery_dataset" "dataset" {
  project                    = var.project
  dataset_id                 = "test_airflow"
  location                   = var.location
  delete_contents_on_destroy = true
}

resource "google_bigquery_table" "default" {
  project             = var.project
  dataset_id          = google_bigquery_dataset.dataset.dataset_id
  table_id            = "batches"
  deletion_protection = false
  schema              = <<EOF
    [
      {
        "name": "BATCH_ID",
        "type": "INT64",
        "mode": "NULLABLE",
        "description": "BATCH ID"
      }
    ]
    EOF
}

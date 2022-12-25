provider "google" {
  project = "wagon-bootcamp-802"
  region  = "europe-west1"
}

terraform {
  backend "gcs" {
    bucket = "twitter_tf_state"
    prefix = "terraform/state"
  }
}

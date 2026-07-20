data "google_organization" "org" {
  organization = "organizations/<org_id>"
}

# output "org_id" {
#   value = data.google_organization.org.id
# }

data "google_billing_account" "argolis" {
  display_name = "<billing_account_name>"
}

// data.google_billing_account.argolis.id

data "google_project" "infra-project" {
  project_id = var.env_project_id
}

data "google_compute_zones" "available" {
  project = data.google_project.infra-project.project_id
  region  = local.primary_region
}

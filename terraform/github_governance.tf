resource "github_repository" "blueprint" {
  name        = "life-sciences-platform-blueprint"
  description = "Enterprise GxP-compliant multi-omics foundry & clinical normalization engine"
  visibility  = "public"

  # Enforce explicit merge commits for 21 CFR Part 11 audit trails
  allow_merge_commit = true
  allow_squash_merge = false
  allow_rebase_merge = false

  has_issues   = true
  has_projects = false
  has_wiki     = false

  security_and_analysis {
    secret_scanning {
      status = "enabled"
    }
    secret_scanning_push_protection {
      status = "enabled"
    }
  }
}

# Branch Protection Guardrails
resource "github_branch_protection" "main_protection" {
  repository_id = github_repository.blueprint.node_id
  pattern       = "main"

  # Do not enforce admin restrictions on yourself during solo development
  enforce_admins = var.is_solo_developer ? false : true

  # Automated CI Status Checks MUST pass regardless of team size
  required_status_checks {
    strict   = true
    contexts = ["Lint & Validate Structural Blueprint"]
  }

  # Require PR approvals only when operating in team mode
  required_pull_request_reviews {
    dismiss_stale_reviews           = true
    require_code_owner_reviews      = var.is_solo_developer ? false : true
    required_approving_review_count = var.is_solo_developer ? 0 : 1
  }

  allows_deletions    = false
  allows_force_pushes = false
}

# Production Environment Deployment Gate
resource "github_repository_environment" "prod" {
  environment = "prod"
  repository  = github_repository.blueprint.name

  # Allow self-review during solo development; enforce 2-person rule in team mode
  prevent_self_review = var.is_solo_developer ? false : true
}
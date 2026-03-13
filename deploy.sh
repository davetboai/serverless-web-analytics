#!/usr/bin/env bash
set -euo pipefail

# ============================================================
# Serverless Web Analytics — One-command CloudFormation deploy
# ============================================================
#
# Usage:
#   ./deploy.sh --email you@example.com
#   ./deploy.sh --email you@example.com --domain analytics.example.com \
#               --hosted-zone-id Z1234 --certificate-arn arn:aws:acm:...
#   ./deploy.sh --delete   # tear down the stack
#
# Prerequisites: aws cli, node 18+, npm

STACK_NAME="serverless-web-analytics"
REGION="${AWS_DEFAULT_REGION:-us-east-1}"

# --- Parse arguments ---
ADMIN_EMAIL=""
DOMAIN_NAME=""
HOSTED_ZONE_ID=""
CERTIFICATE_ARN=""
DELETE=false

while [[ $# -gt 0 ]]; do
  case "$1" in
    --email)        ADMIN_EMAIL="$2";       shift 2 ;;
    --domain)       DOMAIN_NAME="$2";       shift 2 ;;
    --hosted-zone-id) HOSTED_ZONE_ID="$2";  shift 2 ;;
    --certificate-arn) CERTIFICATE_ARN="$2"; shift 2 ;;
    --stack-name)   STACK_NAME="$2";        shift 2 ;;
    --region)       REGION="$2";            shift 2 ;;
    --delete)       DELETE=true;            shift ;;
    *) echo "Unknown option: $1"; exit 1 ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# --- Delete mode ---
if $DELETE; then
  echo "==> Deleting stack ${STACK_NAME}..."
  # Empty the site bucket first (CloudFormation can't delete non-empty buckets)
  SITE_BUCKET=$(aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" \
    --query "Stacks[0].Outputs[?OutputKey=='SiteBucketName'].OutputValue" --output text 2>/dev/null || true)
  if [[ -n "$SITE_BUCKET" && "$SITE_BUCKET" != "None" ]]; then
    echo "    Emptying bucket ${SITE_BUCKET}..."
    aws s3 rm "s3://${SITE_BUCKET}" --recursive --region "$REGION" || true
  fi
  aws cloudformation delete-stack --stack-name "$STACK_NAME" --region "$REGION"
  echo "    Waiting for delete to complete..."
  aws cloudformation wait stack-delete-complete --stack-name "$STACK_NAME" --region "$REGION"
  # Clean up artifact bucket
  ARTIFACT_BUCKET="${STACK_NAME}-artifacts-$(aws sts get-caller-identity --query Account --output text)"
  if aws s3api head-bucket --bucket "$ARTIFACT_BUCKET" --region "$REGION" 2>/dev/null; then
    echo "    Cleaning up artifact bucket..."
    aws s3 rm "s3://${ARTIFACT_BUCKET}" --recursive --region "$REGION" || true
    aws s3api delete-bucket --bucket "$ARTIFACT_BUCKET" --region "$REGION" || true
  fi
  echo "==> Done."
  exit 0
fi

# --- Validate inputs ---
if [[ -z "$ADMIN_EMAIL" ]]; then
  echo "Error: --email is required"
  echo "Usage: ./deploy.sh --email you@example.com [--domain analytics.example.com ...]"
  exit 1
fi

if [[ -n "$DOMAIN_NAME" && ( -z "$HOSTED_ZONE_ID" || -z "$CERTIFICATE_ARN" ) ]]; then
  echo "Error: --domain requires both --hosted-zone-id and --certificate-arn"
  exit 1
fi

# Check prerequisites
for cmd in aws node npm; do
  if ! command -v "$cmd" &>/dev/null; then
    echo "Error: $cmd is required but not found in PATH"
    exit 1
  fi
done

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
ARTIFACT_BUCKET="${STACK_NAME}-artifacts-${ACCOUNT_ID}"

echo "==> Deploying ${STACK_NAME} to ${REGION}"
echo "    Admin email: ${ADMIN_EMAIL}"
[[ -n "$DOMAIN_NAME" ]] && echo "    Custom domain: ${DOMAIN_NAME}"

# --- Step 1: Create artifact bucket if needed ---
echo ""
echo "==> Step 1/5: Preparing artifact bucket..."
if ! aws s3api head-bucket --bucket "$ARTIFACT_BUCKET" --region "$REGION" 2>/dev/null; then
  aws s3api create-bucket --bucket "$ARTIFACT_BUCKET" --region "$REGION" \
    $(if [[ "$REGION" != "us-east-1" ]]; then echo "--create-bucket-configuration LocationConstraint=${REGION}"; fi)
  echo "    Created s3://${ARTIFACT_BUCKET}"
else
  echo "    Using existing s3://${ARTIFACT_BUCKET}"
fi

# --- Step 2: Package and upload Lambda code ---
echo ""
echo "==> Step 2/5: Packaging Lambda functions..."
TMPDIR_PACK=$(mktemp -d)
trap 'rm -rf "$TMPDIR_PACK"' EXIT

# Collector
(cd "$SCRIPT_DIR/src/collector" && zip -q -r "$TMPDIR_PACK/collector.zip" .)
aws s3 cp "$TMPDIR_PACK/collector.zip" "s3://${ARTIFACT_BUCKET}/lambda/collector.zip" --region "$REGION" --quiet
echo "    Uploaded collector.zip"

# Query
(cd "$SCRIPT_DIR/src/query" && zip -q -r "$TMPDIR_PACK/query.zip" .)
aws s3 cp "$TMPDIR_PACK/query.zip" "s3://${ARTIFACT_BUCKET}/lambda/query.zip" --region "$REGION" --quiet
echo "    Uploaded query.zip"

# --- Step 3: Deploy CloudFormation stack ---
echo ""
echo "==> Step 3/5: Deploying CloudFormation stack..."
PARAMS=(
  "AdminEmail=${ADMIN_EMAIL}"
  "LambdaCodeBucket=${ARTIFACT_BUCKET}"
)
[[ -n "$DOMAIN_NAME" ]]     && PARAMS+=("DomainName=${DOMAIN_NAME}")
[[ -n "$HOSTED_ZONE_ID" ]]  && PARAMS+=("HostedZoneId=${HOSTED_ZONE_ID}")
[[ -n "$CERTIFICATE_ARN" ]] && PARAMS+=("CertificateArn=${CERTIFICATE_ARN}")

aws cloudformation deploy \
  --template-file "$SCRIPT_DIR/cloudformation/template.yaml" \
  --stack-name "$STACK_NAME" \
  --region "$REGION" \
  --capabilities CAPABILITY_IAM \
  --parameter-overrides "${PARAMS[@]}" \
  --no-fail-on-empty-changeset

echo "    Stack deployed."

# --- Step 4: Build dashboard with Cognito IDs from stack outputs ---
echo ""
echo "==> Step 4/5: Building dashboard..."

get_output() {
  aws cloudformation describe-stacks --stack-name "$STACK_NAME" --region "$REGION" \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" --output text
}

USER_POOL_ID=$(get_output UserPoolId)
CLIENT_ID=$(get_output UserPoolClientId)
SITE_BUCKET=$(get_output SiteBucketName)
CF_DIST_ID=$(get_output CloudFrontDistributionId)

echo "    Cognito User Pool: ${USER_POOL_ID}"
echo "    Cognito Client:    ${CLIENT_ID}"

export VITE_COGNITO_USER_POOL_ID="$USER_POOL_ID"
export VITE_COGNITO_CLIENT_ID="$CLIENT_ID"

(cd "$SCRIPT_DIR/src/dashboard" && npm install --silent && npx vite build --logLevel error)
echo "    Dashboard built."

# --- Step 5: Upload dashboard + tracker to S3, invalidate CloudFront ---
echo ""
echo "==> Step 5/5: Uploading assets to S3..."

# Dashboard
aws s3 sync "$SCRIPT_DIR/src/dashboard/dist/" "s3://${SITE_BUCKET}/" --region "$REGION" --delete --quiet
echo "    Dashboard uploaded."

# Tracker script
aws s3 cp "$SCRIPT_DIR/src/tracker/script.js" "s3://${SITE_BUCKET}/script.js" --region "$REGION" --quiet
echo "    Tracker script uploaded."

# Invalidate CloudFront cache
aws cloudfront create-invalidation --distribution-id "$CF_DIST_ID" --paths "/*" --query "Invalidation.Id" --output text >/dev/null
echo "    CloudFront cache invalidated."

# --- Done! ---
echo ""
echo "============================================================"
echo "  Deployment complete!"
echo "============================================================"
echo ""
echo "  Dashboard:  $(get_output DashboardUrl)"
echo ""
echo "  A temporary password has been sent to ${ADMIN_EMAIL}."
echo "  Log in and you will be prompted to set a new password."
echo ""
echo "  To track a website, add this to your HTML:"
echo ""
echo "    $(get_output TrackingSnippet)"
echo ""
echo "  Replace YOUR_SITE_ID with an identifier for your site"
echo "  (e.g., 'my-blog'). It will auto-register on first pageview."
echo ""
echo "  To tear down:  ./deploy.sh --delete --stack-name ${STACK_NAME}"
echo "============================================================"

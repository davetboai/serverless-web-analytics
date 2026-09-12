#!/usr/bin/env bash
# Installs git-secrets hooks in this repo clone and registers AWS credential
# patterns, so accidentally committing keys/ARNs/pool IDs/account IDs is
# blocked at commit time. Requires git-secrets: https://github.com/awslabs/git-secrets
set -euo pipefail

if ! command -v git-secrets >/dev/null; then
  echo "git-secrets not found. Install it first:" >&2
  echo "  https://github.com/awslabs/git-secrets#installing-git-secrets" >&2
  exit 1
fi

cd "$(git rev-parse --show-toplevel)"

git secrets --install -f
git secrets --register-aws

echo "git-secrets hooks installed and AWS patterns registered."

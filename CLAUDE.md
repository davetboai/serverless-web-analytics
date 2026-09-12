# CLAUDE.md

## Project: Serverless Web Analytics

Zero-cost, privacy-first web analytics using DynamoDB, Lambda, and CloudFront.

## CDK

- Python CDK app in `cdk/`
- Deploy: `cd cdk && source .venv/bin/activate && cdk deploy --all` (reads config from `cdk/.env`, see `cdk/.env.example`; context flags like `-c domain_name=<domain>` still override if passed explicitly)
- Synth: `cd cdk && source .venv/bin/activate && cdk synth`

## Secrets

- Real deploy config (domain, hosted zone, cert ARN, Cognito IDs) lives in `cdk/.env` (gitignored, never commit). Copy `cdk/.env.example` and fill in real values.
- Run `./scripts/setup-git-secrets.sh` once per clone to install git-secrets pre-commit hooks that block committing AWS keys/account IDs.

## Architecture

- **Tracker** (`src/tracker/script.js`) — lightweight JS loaded by client sites
- **Collector** (`src/collector/index.py`) — Lambda behind Function URL, writes events to DynamoDB
- **Query** (`src/query/index.py`) — Lambda behind HTTP API Gateway with Cognito JWT authorizer
- **Dashboard** (`src/dashboard/`) — React+Vite app served from S3
- **DynamoDB** — single table, on-demand billing, TTL for auto-expiry
- **CloudFront** — routes `/script.js` and `/api/*` to appropriate origins, dashboard at root
- **API Gateway** — HTTP API with JWT authorizer protecting query/sites endpoints

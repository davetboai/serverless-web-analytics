# CLAUDE.md

## Project: Serverless Web Analytics

Zero-cost, privacy-first web analytics using DynamoDB, Lambda, and CloudFront.

## CDK

- Python CDK app in `cdk/`
- Deploy: `cd cdk && source .venv/bin/activate && cdk deploy --all -c domain_name=analytics.davetbo.ai -c hosted_zone_id=<id> -c hosted_zone_name=davetbo.ai -c certificate_arn=<arn> -c cognito_user_pool_id=<pool_id> -c cognito_client_id=<client_id>`
- Synth: `cd cdk && source .venv/bin/activate && cdk synth`

## Architecture

- **Tracker** (`src/tracker/script.js`) — lightweight JS loaded by client sites
- **Collector** (`src/collector/index.py`) — Lambda behind Function URL, writes events to DynamoDB
- **Query** (`src/query/index.py`) — Lambda behind HTTP API Gateway with Cognito JWT authorizer
- **Dashboard** (`src/dashboard/`) — React+Vite app served from S3
- **DynamoDB** — single table, on-demand billing, TTL for auto-expiry
- **CloudFront** — routes `/script.js` and `/api/*` to appropriate origins, dashboard at root
- **API Gateway** — HTTP API with JWT authorizer protecting query/sites endpoints

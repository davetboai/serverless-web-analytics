# CLAUDE.md

## Project: Serverless Web Analytics

Zero-cost, privacy-first web analytics using DynamoDB, Lambda, and CloudFront.

## CDK

- Python CDK app in `cdk/`
- Deploy: `cd cdk && source .venv/bin/activate && cdk deploy --all -c domain_name=analytics.davetbo.ai -c hosted_zone_id=<id> -c hosted_zone_name=davetbo.ai -c certificate_arn=<arn> -c dashboard_password=<pw>`
- Synth: `cd cdk && source .venv/bin/activate && cdk synth`

## Architecture

- **Tracker** (`src/tracker/script.js`) — lightweight JS loaded by client sites
- **Collector** (`src/collector/index.py`) — Lambda behind Function URL, writes events to DynamoDB
- **Query** (`src/query/index.py`) — Lambda behind Function URL, aggregates and returns analytics data
- **Dashboard** (`src/dashboard/`) — static HTML/JS/CSS served from S3
- **DynamoDB** — single table, on-demand billing, TTL for auto-expiry
- **CloudFront** — routes `/script.js` and `/api/*` to appropriate origins, dashboard at root

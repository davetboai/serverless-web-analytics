# Serverless Web Analytics

Zero-cost, privacy-first web analytics built entirely on AWS serverless services. No cookies, no personal data stored, no monthly fees at low traffic volumes.

## Architecture

```
Client Sites                    CloudFront
    |                          /     |     \
    |-- script.js ----------> S3   (dashboard)
    |-- POST /api/collect --> Lambda (collector) --> DynamoDB
                               |
Dashboard (React)              |
    |-- GET /api/query ----> API Gateway (JWT auth) --> Lambda (query) --> DynamoDB
    |-- GET /api/sites ----> API Gateway (JWT auth) --> Lambda (query) --> DynamoDB
```

- **Tracker** — ~1KB script that sends pageview beacons via `sendBeacon`. Supports SPA navigation.
- **Collector** — Lambda Function URL (public, no auth). Filters bots, hashes visitor IPs for privacy, writes events to DynamoDB.
- **Query** — Lambda behind HTTP API Gateway with Cognito JWT authorizer. Aggregates pageviews, visitors, top pages, referrers, countries, and devices.
- **Dashboard** — React + Vite + Chart.js SPA with Cognito authentication. Served from S3 via CloudFront.
- **DynamoDB** — Single table design, on-demand billing (pay per request), TTL for automatic 13-month data expiry.

## Prerequisites

- AWS account
- [AWS CDK](https://docs.aws.amazon.com/cdk/v2/guide/getting-started.html) installed
- Python 3.12+
- Node.js 20+
- Docker (for bundling the dashboard during deployment)
- A Route 53 hosted zone and ACM certificate (us-east-1) for your domain
- A Cognito User Pool and App Client for dashboard authentication

## Deployment

```bash
cd cdk
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

cdk deploy --all \
  -c domain_name=analytics.yourdomain.com \
  -c hosted_zone_id=Z1234567890 \
  -c hosted_zone_name=yourdomain.com \
  -c certificate_arn=arn:aws:acm:us-east-1:123456789:certificate/abc-123 \
  -c cognito_user_pool_id=us-east-1_AbCdEfG \
  -c cognito_client_id=1a2b3c4d5e6f7g8h9i
```

## Adding the Tracking Script

Add this snippet to any site you want to track:

```html
<script defer src="https://analytics.yourdomain.com/script.js" data-website-id="my-site"></script>
```

The `data-website-id` is a string you choose to identify each site. Sites are auto-registered on first pageview.

## Privacy

- No cookies
- No personal data stored
- Visitor identity is a SHA-256 hash of IP + User-Agent + Site ID (not reversible)
- Raw IP addresses are never written to the database
- All data auto-expires after 13 months via DynamoDB TTL

## Cost

At low traffic volumes, this runs entirely within the AWS free tier:

| Service | Free Tier | Typical Low-Traffic Usage |
|---|---|---|
| DynamoDB | 25 WCU/RCU + 25GB | Well within limits |
| Lambda | 1M requests + 400K GB-s | Well within limits |
| CloudFront | 1TB transfer + 10M requests | Well within limits |
| API Gateway | 1M HTTP API calls | Well within limits |
| S3 | 5GB + 20K GET | Well within limits |

## Project Structure

```
src/
  tracker/script.js          # Client-side tracking script
  collector/index.py         # Pageview collection Lambda
  query/index.py             # Analytics query Lambda
  dashboard/                 # React + Vite dashboard app
    src/
      App.tsx                # Auth wrapper (Cognito)
      Dashboard.tsx          # Main dashboard UI
      style.css
cdk/
  app.py                     # CDK app entry point
  stacks/analytics_stack.py  # All infrastructure
```

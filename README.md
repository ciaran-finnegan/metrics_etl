# Financial Metrics ETL Pipeline

A Python-based ETL pipeline that collects and processes financial metrics from various sources.

## Core Features
- Modular design for adding new data sources and transformers
- Environment-based configuration for local and production
- Automated runs via GitHub Actions
- Error handling and logging per metric

## Setup

### Requirements
- Python 3.10+
- Supabase account
- Google Sheets access
- API keys for data sources

### Installation
```bash
git clone https://github.com/ciaran-finnegan/metrics_etl.git
cd metrics_etl
pip install -r requirements.txt
```

### Configuration
1. Copy `.env.template` to `.env`
2. Add required API keys and credentials
3. Configure metrics in `signals.yaml`

## Available Metrics
- Global M2 Money Supply (FRED)
- Crypto Fear & Greed Index
- Bitcoin Metrics
  - Funding Rate
  - SOPR (Spent Output Profit Ratio)
  - MVRV (Market Value to Realised Value)
  - Market Dominance
  - Realised Cap

## Running the Pipeline

### Local Development
```bash
python main.py
```

### Production
The pipeline runs automatically every 4 hours via GitHub Actions.

To run manually, trigger the workflow in GitHub Actions.

## Adding New Metrics

1. Create an extractor in `etl/extract/`
2. Create a transformer in `etl/transform/` (optional)
3. Add configuration to `signals.yaml`

Example configuration:
```yaml
metric_name:
  extractor: extractor_module
  transformer: transformer_module
  secrets: ["REQUIRED_API_KEY"]
```

## Environment Variables

### Required Secrets
- FRED_API_KEY
- SUPABASE_KEY
- GOOGLE_CREDENTIALS

### Required Variables
- SUPABASE_URL
- GOOGLE_SHEET_NAME
- GOOGLE_SHEET_WORKSHEET

## Troubleshooting
Check `etl.log` for detailed error messages and execution logs.

## Licence
MIT © 2024

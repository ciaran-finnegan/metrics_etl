# Financial Signals ETL Pipeline

![ETL Pipeline Architecture](https://via.placeholder.com/800x400?text=ETL+Data+Flow)  
*Modular pipeline for financial data (M2, Fear & Greed Index, BTC metrics)*

## Features
- **Plugin Architecture** - Add sources/transformers/loaders without touching core code
- **Seamless Secrets** - `.env` (local) + GitHub Secrets (prod)
- **Auto-Scheduled** - Runs daily via GitHub Actions
- **Fail-Safe** - Isolated errors per signal with detailed logs

## Quick Start

### Prerequisites
- Python 3.10+
- [Supabase](https://supabase.com/) or Google Sheets (for storage)

```bash
# 1. Clone repo
git clone https://github.com/your-username/financial-signals-etl.git
cd financial-signals-etl

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure secrets
cp config/secrets_template.env .env
nano .env  # Add your API keys

# 4. Run locally
python main.py

```

## Configuration

### signals.yaml
```yaml
# Example: Bitcoin Fear & Greed Index
fear_and_greed_index:
  extractor: alternative_extractor
  secrets: ["ALTERNATIVE_API_KEY"]  # Required env vars
  transformer: fear_greed_transformer
```

### GitHub Secrets

| Secret Name | Where to Get It |
|------------|----------------|
| FRED_API_KEY | FRED API Portal |
| SUPABASE_URL | Supabase Project Settings > Database |

## Extending the Pipeline

### Add New Signal (3 Steps)

1. Create Extractor (etl/extract/):
```python
# etl/extract/btc_rsi_extractor.py
class BTCRSIExtractor:
    def fetch(self) -> dict:
        return {"rsi": 65.7, "timestamp": "2024-01-01"}
```

2. Add Transformer (optional):
```python
# etl/transform/rsi_transformer.py
class RSITransformer:
    def transform(self, raw: dict) -> dict:
        return {**raw, "is_overbought": raw["rsi"] > 70}
```

3. Register in signals.yaml:
```yaml
btc_rsi_10_day:
  extractor: btc_rsi_extractor
  transformer: rsi_transformer
```

## Deployment

### GitHub Actions
Schedule: Daily at 12PM UTC (edit .github/workflows/etl_m2.yml)

Manual Run:
```bash
git commit --allow-empty -m "Trigger ETL"; git push
```

## Debugging
```bash
# View detailed logs
grep "ERROR" etl.log

# Test specific extractor
python -c "from etl.extract.fred_extractor import FREDExtractor; print(FREDExtractor(api_key='demo').fetch())"
```

## Maintenance Cheatsheet

| Task | Command/File |
|------|-------------|
| Update all deps | pip freeze > requirements.txt |
| Add new secret | 1. Add to secrets_template.env<br>2. Update GitHub Secrets |
| Force pipeline run | Push empty commit (see above) |

## Contributing
1. Fork → git checkout -b feature/awesome-signal
2. Code → Add extractor/transformer
3. Test → pytest tests/
4. PR → Describe your changes

## License
MIT © 2024 [Your Name]

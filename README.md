# Financial Signals ETL Pipeline

![ETL Pipeline Architecture](https://via.placeholder.com/800x400?text=ETL+Data+Flow)  
*Modular pipeline for financial data (M2, Fear & Greed Index, BTC metrics)*

## Features
- **Plugin Architecture** - Add sources/transformers/loaders without touching core code
- **Seamless Secrets** - `.env` (local) + GitHub Secrets (prod)
- **Auto-Scheduled** - Runs daily via GitHub Actions
- **Fail-Safe** - Isolated errors per signal with detailed logs
- **API-free Twitter Data** - Multiple approaches including Nitter instances and Vision-assisted automation

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
  extractor_params:
    output_file: "data/extracted/fear_greed.json"  # Data will be saved to this file
  secrets: ["ALTERNATIVE_API_KEY"]
  transformer: fear_greed_transformer
  loaders:
    - type: supabase_loader
      config: 
        table: "financial_signals"
        schema:  # Define the data types for each column
          date: "date"
          value: "number"
          classification: "string"
    - type: file_loader
      config:
        file_path: "data/loaded/fear_greed.json"
        format: "json"
```

### Data Sources

| Source | Description | API Required? |
|--------|-------------|--------------|
| FRED | Federal Reserve Economic Data | Yes |
| CoinGecko | Cryptocurrency market data | Optional |
| Alternative.me | Fear & Greed Index | No |
| Twitter (Direct Nitter) | Twitter sentiment from influencers | No |
| Twitter (RapidAPI) | Twitter data via RapidAPI | Yes |

For more information on the Direct Nitter Twitter extractor, see [README_direct_nitter.md](README_direct_nitter.md).

### GitHub Secrets

| Secret Name | Where to Get It |
|------------|----------------|
| `FRED_API_KEY` | FRED API Portal |
| `SUPABASE_URL` | Supabase Project Settings > API |
| `SUPABASE_KEY` | Supabase Project Settings > API |
| `COINGECKO_API_KEY` | CoinGecko API Portal (Optional, for higher limits) |
| `GOOGLE_SHEETS_CREDENTIALS` | Google Cloud Console (OAuth Client JSON) |
| `GOOGLE_SHEETS_TOKEN` | Generated `token.json` after first OAuth flow |
| `GOOGLE_SHEET_NAME` | Name of your target Google Sheet |
| `GOOGLE_SHEET_WORKSHEET` | Name of the target worksheet within the Sheet |
| `TWITTER_SENTIMENT_SHEET_ID` | ID of your Google Sheet for Twitter sentiment data |
| `RAPIDAPI_KEY` | RapidAPI dashboard (for Twitter API alternative) |

## Extending the Pipeline: The Simplified Pattern

This pipeline uses a streamlined approach. Each signal is configured in the `signals.yaml` file with all necessary information for extraction, transformation, and loading.

**Core Concepts:**

1.  **Extractor (Fetch and Save Raw Data):**
    *   Connects to a specific API endpoint or data source
    *   Handles authentication, rate limiting, and fetching the complete raw data
    *   Automatically saves raw data to the specified `output_file` in the signals.yaml config
    *   Accepts parameters (`extractor_params` from `signals.yaml`)

2.  **Transformer (Process Specific Signal):**
    *   Takes the raw data from the extractor
    *   Processes it into the specific metric required for its signal

3.  **Loader (Save Processed Data):**
    *   Defined in `signals.yaml` with destination plugin and schema
    *   Multiple loaders can be configured for each signal
    *   Common loaders include:
        * `supabase_loader`: Loads data to Supabase database
        * `google_sheets_loader`: Loads data to Google Sheets
        * `file_loader`: Saves data to a local file in JSON or CSV format

4.  **`signals.yaml` (Complete Configuration):**
    *   Defines each logical signal with extraction, transformation, and loading details
    *   Specifies output file paths for extractors
    *   Defines destination plugins and schemas for loaders

### Adding a New Signal (Example)

Let's add a signal for BTC Dominance from the CoinGecko API:

**Step 1: Update signals.yaml**

```yaml
# config/signals.yaml
signals:
  btc_dominance:
    extractor: coingecko_extractor
    extractor_params:
      endpoint: "global"
      output_file: "data/extracted/global_metrics.json"
    transformer: btc_dominance_transformer
    loaders:
      - type: supabase_loader
        config:
          table: "market_metrics"
          schema:
            date: "date"
            value: "number"
            signal_name: "string"
      - type: file_loader
        config:
          file_path: "data/loaded/btc_dominance.json"
          format: "json"
```

**Step 2: Create the transformer**

```python
# etl/transform/btc_dominance_transformer.py
from datetime import datetime
import etl.custom_exceptions

class BtcDominanceTransformer:
    def transform(self, data):
        try:
            dominance = data.get("data", {}).get("bitcoin_dominance_percentage")
            return {
                "date": datetime.now().date().isoformat(),
                "value": dominance,
                "signal_name": "btc_dominance",
                "units": "percentage"
            }
        except Exception as e:
            raise etl.custom_exceptions.TransformError(f"BTC dominance transform error: {e}")
```

### File Structure

The pipeline saves data at two stages:
- **Raw data** is saved by extractors to `data/extracted/`
- **Processed data** can be saved by file loaders to `data/loaded/`

This creates an audit trail and makes debugging easier, as you can inspect the raw data without re-fetching it.

## Deployment

### GitHub Actions
ETL workflows:
- **Financial Signals**: `.github/workflows/etl_m2.yml` (Daily at 12PM UTC)
- **Twitter Sentiment**: `.github/workflows/etl_direct_nitter.yml` (Daily at 12PM UTC)

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

# Test Direct Nitter Extractor
python test/test_direct_nitter_twitter.py --debug
```

## Maintenance Cheatsheet

| Task | Command/File |
|------|-------------|
| Update all deps | pip freeze > requirements.txt |
| Add new secret | 1. Add to secrets_template.env<br>2. Update GitHub Secrets |
| Force pipeline run | Push empty commit (see above) |
| Update Twitter handles | Edit `handles` list in `config/signals.yaml` |

## Contributing
1. Fork → git checkout -b feature/awesome-signal
2. Code → Add extractor/transformer
3. Test → pytest tests/
4. PR → Describe your changes

## License
MIT © 2024 [Your Name]

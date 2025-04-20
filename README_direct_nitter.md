# Direct Nitter Twitter Extractor

## Overview

The `DirectNitterTwitterExtractor` is a robust solution for extracting Twitter data without relying on third-party APIs or the official Twitter API. It works by directly scraping Nitter instances, which are alternative Twitter front-ends that provide a clean, JavaScript-free interface to Twitter content.

## Benefits

- **No API keys required**: Works without any Twitter API keys or third-party services
- **No rate limits**: Not subject to Twitter API rate limits (though self-imposed rate limiting is used to be kind to Nitter instances)
- **No cost**: Free to use, unlike many Twitter API solutions
- **Resilient design**: Rotates through multiple Nitter instances to handle availability issues
- **Privacy-focused**: Doesn't require authentication with Twitter

## Features

- Extract tweets from multiple Twitter handles
- Configurable tweet count per user
- Option to include/exclude replies and retweets
- Automatic rate limiting to avoid overloading Nitter instances
- Retry logic with exponential backoff for failed requests
- Extraction of tweet content, timestamps, likes, retweets, and more
- User profile information extraction

## Requirements

- Python 3.7+
- Required packages:
  - requests
  - beautifulsoup4
  - pyyaml (for configuration)

## Usage

### Basic Usage

You can use the extractor directly in your Python code:

```python
from etl.extract.direct_nitter_twitter_extractor import DirectNitterTwitterExtractor

# Initialize the extractor
extractor = DirectNitterTwitterExtractor({
    "handles": ["matt_willemsen", "IrvingBuyTheDip", "docXBT"],
    "tweets_per_user": 10,
    "min_request_interval": 1.5,
    "include_replies": False,
    "include_retweets": False
})

# Extract tweets
data = extractor.extract()

# Access extracted data
tweets = data["tweets"]
profiles = data["profiles"]

# Process tweets
for tweet in tweets:
    print(f"@{tweet['username']}: {tweet['text'][:50]}...")
```

### Running the ETL Pipeline

To run the full ETL pipeline (extract, transform, load):

```bash
# Run with default configuration
./run_direct_nitter_etl.py

# Enable debug logging
./run_direct_nitter_etl.py --debug

# Specify a different signal configuration
./run_direct_nitter_etl.py --signal custom_twitter_signal
```

### Scheduled Execution with GitHub Actions

The ETL pipeline is scheduled to run automatically every day at 12:00 UTC using GitHub Actions:

- **Schedule**: Daily at 12:00 UTC
- **Workflow File**: `.github/workflows/etl_direct_nitter.yml`

You can also trigger a manual run:
1. Go to the GitHub repository
2. Navigate to "Actions" > "Direct Nitter Twitter ETL"
3. Click "Run workflow"

Alternatively, trigger via empty commit:
```bash
git commit --allow-empty -m "Trigger ETL"; git push
```

### Monitoring and Logs

GitHub Actions provides several ways to monitor the ETL process:
- **Run logs**: View real-time logs in the Actions tab
- **Artifacts**: ETL logs and data files are uploaded as artifacts after each run
- **Email notifications**: Configure GitHub to send email alerts on workflow failures

## Configuration

The extractor can be configured through the `signals.yaml` file:

```yaml
financial_tweets_direct_nitter:
  extractor:
    module: 'etl.extract.direct_nitter_twitter_extractor'
    class: 'DirectNitterTwitterExtractor'
    params:
      handles: 
        - "matt_willemsen"
        - "IrvingBuyTheDip"
        - "docXBT"
        # Add more handles as needed
      tweets_per_user: 10
      min_request_interval: 1.5
      max_request_interval: 3.0
      include_replies: false
      include_retweets: false
      timeout: 15
      output_file: "data/extracted/financial_influencer_tweets_direct_nitter.json"
  # Transform and load configurations...
```

### Configuration Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `handles` | List of Twitter handles to extract tweets from | Required |
| `tweets_per_user` | Maximum number of tweets to extract per user | 10 |
| `min_request_interval` | Minimum time between requests in seconds | 1.0 |
| `max_request_interval` | Maximum time between requests in seconds | 3.0 |
| `max_retries` | Maximum number of retry attempts for failed requests | 3 |
| `include_replies` | Whether to include replies in the extraction | False |
| `include_retweets` | Whether to include retweets in the extraction | False |
| `timeout` | Request timeout in seconds | 10 |
| `output_file` | Path to the output file | Required |
| `nitter_instances` | List of Nitter instances to try | Several defaults |

## Troubleshooting

### Common Issues

1. **No tweets extracted**
   - Check if the Twitter handles are correct
   - Verify that Nitter instances are accessible
   - Try increasing the timeout value

2. **Slow extraction**
   - Increase the number of Nitter instances
   - Decrease the number of tweets per user
   - Increase the request intervals

3. **Missing tweets**
   - Check if `include_replies` and `include_retweets` settings match your needs
   - Verify that the Twitter accounts have recent activity

### Logs

Logs are stored in the `logs/` directory:
- Test script logs: `logs/direct_nitter_test_*.log`
- ETL pipeline logs: `logs/direct_nitter_etl_*.log`
- Cron job logs: `logs/direct_nitter_cron_*.log`

## Advanced Topics

### Adding Custom Nitter Instances

You can add your own Nitter instances to improve reliability:

```python
extractor = DirectNitterTwitterExtractor({
    # ... other params ...
    "nitter_instances": [
        "https://nitter.net",
        "https://nitter.lacontrevoie.fr",
        "https://your-custom-instance.example.com"
    ]
})
```

### Extending the Extractor

The `DirectNitterTwitterExtractor` can be extended to add custom functionality:

```python
class CustomNitterExtractor(DirectNitterTwitterExtractor):
    def extract(self):
        data = super().extract()
        # Add custom processing here
        return data
```

## Future Improvements

- Support for pagination to extract more than the initial page of tweets
- Enhanced media extraction (videos, polls, etc.)
- Advanced filtering options for tweets
- Improved error handling and recovery mechanisms 
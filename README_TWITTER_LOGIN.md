# Twitter Login Extractor

This module provides functionality to extract tweets from Twitter by logging in to Twitter via browser automation using Playwright.

## Requirements

- Python 3.8+
- Playwright installed and configured
- Twitter account credentials

## Installation

1. Install the required dependencies:

```bash
pip install playwright
playwright install
```

2. Set up your environment variables by creating a `.env` file in the project root with the following variables:

```
TWITTER_USERNAME=your_twitter_username
TWITTER_PASSWORD=your_twitter_password
TWITTER_VERIFICATION_CODE=your_verification_code_if_needed
```

## Usage

### Using the Extractor in Code

```python
from etl.extract.twitter_login_extractor import TwitterLoginExtractor

# Initialize the extractor
extractor = TwitterLoginExtractor(
    username="your_twitter_username",
    password="your_twitter_password",
    verification_code=None,  # Optional, only needed if 2FA is enabled
    handles=["@user1", "@user2"],
    tweets_per_user=10,
    min_request_interval=2.0,
    max_request_interval=5.0,
    max_retries=3,
    include_replies=False,
    include_retweets=False,
    headless=True,  # Set to False to see the browser while running
    output_file="twitter_output.json",
    take_screenshots=False,
    screenshots_dir="screenshots",
    storage_state_path="twitter_state.json"  # Save browser state for faster subsequent runs
)

# Run extraction
result = extractor.extract()
```

### Running the Test Script

The test script provides a command-line interface to test the TwitterLoginExtractor:

```bash
python test/test_twitter_login_extractor.py --handles @user1 @user2 --tweets-per-user 5 --output-file output.json
```

#### Command-Line Arguments

- `--handles`: List of Twitter handles to extract from
- `--tweets-per-user`: Number of tweets to extract per user (default: 5)
- `--output-file`: Output file to save extracted data
- `--headless`: Run browser in headless mode (flag only, no value needed)
- `--include-replies`: Include replies in extracted tweets (flag only)
- `--include-retweets`: Include retweets in extracted tweets (flag only)
- `--screenshots-dir`: Directory to save screenshots to
- `--storage-state-path`: Path to save/load browser state

## Features

- Automated login to Twitter
- Handles verification if required
- Navigates to user profiles
- Extracts tweet data including text, date, likes, retweets, etc.
- Extracts profile data
- Implements rate limiting to avoid being blocked
- Takes screenshots (optional)
- Saves intermediate results
- Browser state storage for faster subsequent runs

## Error Handling

The extractor handles various error scenarios:

- Failed login attempts
- Verification requirements
- Non-existent users
- Suspended accounts
- Private accounts
- Rate limiting
- Network errors

## Notes

- Twitter may detect automated access and require additional verification
- Excessive use may result in account limitations
- Use responsibly and in accordance with Twitter's Terms of Service 
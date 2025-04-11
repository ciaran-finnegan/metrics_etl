import os
import time
import json
from typing import Dict, Any, List, Optional
from collections import deque
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from etl.load.base_loader import BaseLoader
from etl.exceptions import LoadError
from etl.utils.logger import logger
from datetime import datetime

class GoogleSheetsLoader(BaseLoader):
    def __init__(self, credentials_path: str, token_path: str, spreadsheet_id: str):
        super().__init__()
        self.credentials_path = credentials_path
        self.token_path = token_path
        self.spreadsheet_id = spreadsheet_id
        self.service = None
        self.raw_sheet_name = os.getenv("GOOGLE_SHEETS_RAW_SHEET_NAME", "raw_data")
        self.view_sheets = {
            "market_sentiment": {
                "signals": ["bitcoin_price", "bitcoin_24h_change", "bitcoin_7d_change", "bitcoin_30d_change", "crypto_fear_greed"],
                "columns": ["date", "signal_name", "value", "units", "metadata", "classification", "percentage", "ratio", "source", "timeframe", "is_bullish"]
            },
            "holder_metrics": {
                "signals": ["long_term_holder_sopr", "short_term_holder_sopr", "short_term_holder_mvrv"],
                "columns": ["date", "signal_name", "value", "units", "metadata", "ratio", "source", "timeframe", "is_bullish"]
            },
            "monetary_metrics": {
                "signals": ["global_m2_money_supply", "realised_cap_for_onchain_liquidity"],
                "columns": ["date", "signal_name", "value", "units", "metadata", "source", "timeframe"]
            },
            "price_metrics": {
                "signals": ["bitcoin_price", "bitcoin_24h_change", "bitcoin_7d_change", "bitcoin_30d_change", "bitcoin_market_cap", "bitcoin_24h_volume"],
                "columns": ["date", "signal_name", "value", "units", "metadata", "percentage", "source", "timeframe"]
            }
        }
        self.last_request_time = 0
        self.min_request_interval = 1.0  # Minimum time between requests in seconds (60 per minute)
        self.request_timestamps = deque(maxlen=60)  # Track last 60 requests for minute window
        self.minute_window_size = 60  # 1 minute in seconds

    def _clean_old_requests(self, current_time: float) -> None:
        """Remove requests older than 1 minute"""
        while self.request_timestamps and current_time - self.request_timestamps[0] > self.minute_window_size:
            self.request_timestamps.popleft()

    def _retry_with_backoff(self, func, *args, **kwargs):
        """Retry a function with exponential backoff when a quota limit is hit"""
        max_retries = 5
        base_delay = 2  # Start with 2 seconds delay
        
        for attempt in range(max_retries):
            try:
                current_time = time.time()
                
                # Check if we need to wait due to minimum interval
                time_since_last_request = current_time - self.last_request_time
                if time_since_last_request < self.min_request_interval:
                    sleep_time = self.min_request_interval - time_since_last_request
                    logger.debug(f"Rate limiting: sleeping for {sleep_time:.2f} seconds")
                    time.sleep(sleep_time)
                
                # Check minute window limit
                self._clean_old_requests(current_time)
                if len(self.request_timestamps) >= 60:  # 60 requests per minute
                    oldest_request = self.request_timestamps[0]
                    wait_time = self.minute_window_size - (current_time - oldest_request)
                    if wait_time > 0:
                        logger.warning(f"Minute rate limit reached. Waiting {wait_time:.2f} seconds...")
                        time.sleep(wait_time)
                        current_time = time.time()
                
                result = func(*args, **kwargs)
                self.last_request_time = time.time()
                self.request_timestamps.append(current_time)
                return result
                
            except HttpError as e:
                if e.resp.status == 429:  # Quota exceeded
                    delay = base_delay * (2 ** attempt)  # Exponential backoff
                    logger.warning(f"Rate limit hit, retrying in {delay} seconds...")
                    time.sleep(delay)
                    continue
                raise
            except Exception as e:
                raise LoadError(f"Google Sheets operation failed: {e}")

        raise LoadError("Max retries exceeded for Google Sheets operation")

    def _ensure_headers(self) -> None:
        """Ensure the raw data sheet has the correct headers"""
        try:
            # Get current headers
            result = self.service.spreadsheets().values().get(
                spreadsheetId=self.spreadsheet_id,
                range=f'{self.raw_sheet_name}!1:1'
            ).execute()
            
            current_headers = result.get('values', [[]])[0]
            
            # Define required headers
            required_headers = [
                'date',
                'signal_name',
                'value',
                'units',
                'metadata',
                'classification',
                'percentage',
                'ratio',
                'source',
                'timeframe',
                'is_bullish',
                'metadata'  # duplicate for backward compatibility
            ]
            
            # If headers don't match, update them
            if current_headers != required_headers:
                self.service.spreadsheets().values().update(
                    spreadsheetId=self.spreadsheet_id,
                    range=f'{self.raw_sheet_name}!1:1',
                    valueInputOption='RAW',
                    body={'values': [required_headers]}
                ).execute()
                
        except Exception as e:
            logger.error(f"Error ensuring headers: {str(e)}")
            raise LoadError(f"Failed to ensure headers: {str(e)}")

    def _setup_views(self) -> None:
        """Set up view sheets with correct headers"""
        try:
            # Define view sheets and their columns
            view_sheets = {
                'market_sentiment': {
                    'signals': ['fear_greed_index', 'social_volume', 'sentiment_score'],
                    'columns': [
                        'date',
                        'signal_name',
                        'value',
                        'units',
                        'metadata',
                        'classification',
                        'percentage',
                        'ratio',
                        'source',
                        'timeframe',
                        'is_bullish'
                    ]
                },
                'holder_metrics': {
                    'signals': ['holder_distribution', 'holder_balance', 'holder_activity'],
                    'columns': [
                        'date',
                        'signal_name',
                        'value',
                        'units',
                        'metadata',
                        'classification',
                        'percentage',
                        'ratio',
                        'source',
                        'timeframe',
                        'is_bullish'
                    ]
                },
                'monetary_metrics': {
                    'signals': ['supply_inflation', 'velocity', 'transaction_volume'],
                    'columns': [
                        'date',
                        'signal_name',
                        'value',
                        'units',
                        'metadata',
                        'classification',
                        'percentage',
                        'ratio',
                        'source',
                        'timeframe',
                        'is_bullish'
                    ]
                },
                'price_metrics': {
                    'signals': ['price', 'market_cap', 'volume'],
                    'columns': [
                        'date',
                        'signal_name',
                        'value',
                        'units',
                        'metadata',
                        'classification',
                        'percentage',
                        'ratio',
                        'source',
                        'timeframe',
                        'is_bullish'
                    ]
                }
            }
            
            # Get existing sheets
            spreadsheet = self.service.spreadsheets().get(
                spreadsheetId=self.spreadsheet_id
            ).execute()
            existing_sheets = [sheet['properties']['title'] for sheet in spreadsheet.get('sheets', [])]
            
            # Create missing sheets and set up headers
            for sheet_name, config in view_sheets.items():
                if sheet_name not in existing_sheets:
                    # Create new sheet
                    self.service.spreadsheets().batchUpdate(
                        spreadsheetId=self.spreadsheet_id,
                        body={
                            'requests': [{
                                'addSheet': {
                                    'properties': {
                                        'title': sheet_name
                                    }
                                }
                            }]
                        }
                    ).execute()
                
                # Set up headers
                self.service.spreadsheets().values().update(
                    spreadsheetId=self.spreadsheet_id,
                    range=f'{sheet_name}!1:1',
                    valueInputOption='RAW',
                    body={'values': [config['columns']]}
                ).execute()
                
        except Exception as e:
            logger.error(f"Error setting up views: {str(e)}")
            raise LoadError(f"Failed to set up views: {str(e)}")

    def _prepare_row_data(self, data: Dict[str, Any]) -> List[str]:
        """Prepare row data for Google Sheets with full datetime"""
        try:
            # Get current datetime in ISO format
            current_datetime = datetime.now().isoformat()
            
            # Extract metadata
            metadata = data.get('metadata', {})
            if isinstance(metadata, dict) and 'metadata' in metadata:
                metadata = metadata['metadata']
            
            # Prepare the row with full datetime
            row = [
                current_datetime,  # Full datetime
                data.get('signal_name', ''),
                str(data.get('value', '')),
                data.get('units', ''),
                json.dumps(metadata),
                metadata.get('classification', ''),
                str(metadata.get('percentage', '')),
                str(metadata.get('ratio', '')),
                metadata.get('source', ''),
                metadata.get('timeframe', ''),
                str(metadata.get('is_bullish', '')),
                json.dumps(metadata)  # Backward compatibility
            ]
            
            return row
        except Exception as e:
            logger.error(f"Error preparing row data: {str(e)}")
            raise LoadError(f"Failed to prepare row data: {str(e)}")

    def _update_views(self, signal_name: str, data: Dict[str, Any]) -> None:
        """Update relevant view worksheets with new data"""
        try:
            for view_name, view_config in self.view_sheets.items():
                if signal_name in view_config['signals']:
                    # Get current data
                    result = self._retry_with_backoff(
                        self.service.spreadsheets().values().get,
                        spreadsheetId=self.spreadsheet_id,
                        range=f"{view_name}!A:{chr(65 + len(view_config['columns']) - 1)}"
                    ).execute()
                    
                    current_data = result.get('values', [])
                    
                    # Prepare new row with view-specific columns
                    row_data = self._prepare_row_data(data)
                    view_row = [row_data[self.view_sheets['market_sentiment']['columns'].index(col)] 
                              for col in view_config['columns']]
                    
                    # Append new row
                    self._retry_with_backoff(
                        self.service.spreadsheets().values().append,
                        spreadsheetId=self.spreadsheet_id,
                        range=f"{view_name}!A:{chr(65 + len(view_config['columns']) - 1)}",
                        valueInputOption='RAW',
                        insertDataOption='INSERT_ROWS',
                        body={'values': [view_row]}
                    ).execute()
                    
        except Exception as e:
            logger.error(f"Failed to update view {view_name}: {e}")

    def init(self) -> None:
        """Initialize Google Sheets connection"""
        try:
            creds = None
            if os.path.exists(self.token_path):
                creds = Credentials.from_authorized_user_file(self.token_path, ['https://www.googleapis.com/auth/spreadsheets'])
            
            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    creds.refresh(Request())
                else:
                    flow = InstalledAppFlow.from_client_secrets_file(
                        self.credentials_path, ['https://www.googleapis.com/auth/spreadsheets'])
                    creds = flow.run_local_server(port=0)
                
                # Save the credentials for the next run
                with open(self.token_path, 'w') as token:
                    token.write(creds.to_json())
            
            self.service = build('sheets', 'v4', credentials=creds)
            self._ensure_headers()
            self._setup_views()
            
        except Exception as e:
            logger.error(f"Google Sheets connection failed: {e}")
            raise LoadError(f"Google Sheets init error: {e}")

    def append_row(self, data: Dict[str, Any]) -> None:
        """Append a row to the raw data sheet and update views"""
        try:
            # Prepare row data with all columns
            row = self._prepare_row_data(data)
            
            # Append to raw data sheet
            self._retry_with_backoff(
                self.service.spreadsheets().values().append,
                spreadsheetId=self.spreadsheet_id,
                range=f"{self.raw_sheet_name}!A:K",
                valueInputOption='RAW',
                insertDataOption='INSERT_ROWS',
                body={'values': [row]}
            ).execute()
            
            # Update relevant views
            self._update_views(data['signal_name'], data)
            
        except Exception as e:
            raise LoadError(f"Failed to append row: {e}")
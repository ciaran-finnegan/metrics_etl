import gspread
import os
import json
from datetime import datetime
from typing import Dict, Any
from utils.exceptions import LoadError
from utils.logging_config import logger

# Placeholders if needed
class Logger:
    def info(self, msg): print(f"INFO: {msg}")
    def error(self, msg): print(f"ERROR: {msg}")
    def warning(self, msg): print(f"WARN: {msg}")
logger = Logger()

# Define expected headers globally or within the class
EXPECTED_HEADERS = [
    "timestamp", "date", "metric_name", "value", "value_usd",
    "classification", "percentage", "ratio", "source",
    "timeframe", "units", "is_bullish", "metadata"
]

class GoogleSheetsLoader:
    # View definitions for different metric categories
    VIEWS = {
        'market_sentiment': ['fear_and_greed_index', 'btc_rsi_10_day', 'bull_market_support_band', 'bitcoin_dominance'],
        'holder_metrics': ['long_term_holder_sopr', 'short_term_holder_sopr', 'short_term_holder_mvrv', 'bitfinex_btc_whales'],
        'monetary_metrics': ['global_m2_money_supply', 'realised_cap_for_onchain_liquidity', 'futures_funding_rate']
    }

    def __init__(self, config: Dict[str, Any]):
        """
        Initializes the Google Sheets client using OAuth credentials.
        Args:
            config (Dict[str, Any]): Dictionary containing:
                - creds_path (str): Path to credentials file (client_secrets.json).
                - token_path (str): Path to authorized user token file (token.json).
                - sheet_name (str): Name of the Google Sheet.
                - worksheet (str, optional): Target worksheet name (defaults to "raw_data").
        """
        try:
            creds_path = config.get("creds_path")
            token_path = config.get("token_path")
            sheet_name = config.get("sheet_name")
            sheet_id = os.getenv("GOOGLE_SHEET_ID")
            self.worksheet_name = config.get("worksheet", "raw_data")

            if not all([creds_path, token_path, sheet_id]):
                raise ValueError("Missing required config: creds_path, token_path and GOOGLE_SHEET_ID environment variable")

            # Check if token file exists, log warning if not (will fail auth)
            if not os.path.exists(token_path):
                 logger.warning(f"Google Sheets token file not found at '{token_path}'. Authentication will likely fail.")
            
            # Use stored OAuth token
            gc = gspread.oauth(
                credentials_filename=creds_path,
                authorized_user_filename=token_path
            )
            
            # Open by ID instead of name
            self.spreadsheet = gc.open_by_key(sheet_id)
            self.sheet = self.spreadsheet.worksheet(self.worksheet_name)

            # Use sheet_name for logging if available, otherwise use ID
            log_sheet_name = sheet_name if sheet_name else f"ID:{sheet_id}"
            logger.info(f"Google Sheets connected to: {log_sheet_name}/{self.worksheet_name}")

            # Ensure headers exist on the main sheet
            self._ensure_headers()
            
            # Setup view worksheets
            # self._setup_views() # Keep commented out

        except FileNotFoundError as fnf_error:
            logger.error(f"Google Sheets credentials/token file not found: {fnf_error}")
            raise LoadError(f"Google Sheets credentials error: {fnf_error}")
        except Exception as e:
            logger.error(f"Google Sheets connection/setup failed: {type(e).__name__} - {e}")
            raise LoadError(f"Google Sheets init error: {e}")
    
    def _ensure_headers(self):
        """Ensure target worksheet has the correct headers."""
        try:
            current_headers = self.sheet.row_values(1)
            # Check if headers match, ignoring potential empty strings from sheet
            if not current_headers or [h for h in current_headers if h] != EXPECTED_HEADERS:
                logger.warning(f"Headers mismatch or missing in '{self.worksheet_name}'. Expected: {EXPECTED_HEADERS}, Found: {current_headers}. Attempting to set headers.")
                # Be cautious clearing sheets; maybe only append if empty?
                if not current_headers or all(h == '' for h in current_headers): 
                    self.sheet.update('A1', [EXPECTED_HEADERS]) # Update header row
                    logger.info(f"Headers set for '{self.worksheet_name}'")
                else:
                     logger.warning(f"Sheet '{self.worksheet_name}' has existing headers, not clearing. Manual check recommended.")
        except Exception as e:
            logger.error(f"Failed to ensure headers in '{self.worksheet_name}': {e}")
            # Don't necessarily raise LoadError here, maybe just log warning

    def _setup_views(self):
        """Setup or update view worksheets"""
        try:
            for view_name, metrics in self.VIEWS.items():
                try:
                    # Try to get existing worksheet or create new one
                    try:
                        sheet = self.spreadsheet.worksheet(view_name)
                    except gspread.WorksheetNotFound:
                        sheet = self.spreadsheet.add_worksheet(view_name, 1000, len(metrics) + 2)
                    
                    # Set headers
                    headers = ["date"] + metrics
                    if sheet.row_values(1) != headers:
                        sheet.clear()
                        sheet.append_row(headers)
                    
                except Exception as e:
                    logger.error(f"Failed to setup view {view_name}: {e}")
                    
        except Exception as e:
            logger.error(f"Failed to setup views: {e}")

    def _update_views(self, data: dict):
        """Update view worksheets with new data"""
        try:
            date = data.get("date", datetime.now().date().isoformat())
            metric_name = data["signal_name"]
            
            for view_name, metrics in self.VIEWS.items():
                if metric_name in metrics:
                    try:
                        sheet = self.spreadsheet.worksheet(view_name)
                        
                        # Find or create row for this date
                        cell = sheet.find(date)
                        if cell is None:
                            # Check if we need to resize
                            if sheet.row_count >= 1000:
                                # Archive old data by creating a new sheet with year suffix
                                current_year = datetime.now().year
                                archive_name = f"{view_name}_{current_year}"
                                try:
                                    self.spreadsheet.worksheet(archive_name)
                                except gspread.exceptions.WorksheetNotFound:
                                    # Copy current sheet to archive
                                    sheet.copy_to(self.spreadsheet.id, title=archive_name)
                                    
                                # Clear current sheet except headers
                                headers = sheet.row_values(1)
                                sheet.clear()
                                sheet.append_row(headers)
                            
                            row_num = sheet.row_count + 1
                            sheet.append_row([date] + [""] * len(metrics))
                        else:
                            row_num = cell.row
                        
                        # Update the specific metric column
                        col_num = metrics.index(metric_name) + 2  # +2 because date is first column
                        sheet.update_cell(row_num, col_num, data["value"])
                        logger.info(f"Updated view {view_name} for {metric_name}")
                        
                    except Exception as e:
                        logger.error(f"Failed to update view {view_name}: {e}")
                        
        except Exception as e:
            logger.error(f"Failed to update views: {e}")

    def load(self, data: dict) -> bool:
        """Append data as a new row, matching the expected header order."""
        try:
            if not data or not data.get("signal_name"):
                raise LoadError("Invalid or empty data received by GoogleSheetsLoader")

            # Prepare row data according to EXPECTED_HEADERS
            row_to_append = []
            for header in EXPECTED_HEADERS:
                value = data.get(header) # Get value for header
                # Special handling for metadata
                if header == "metadata":
                    # Extract metadata dict if present, otherwise create one from remaining keys
                    metadata_dict = data.get("metadata", {})
                    if not isinstance(metadata_dict, dict):
                         metadata_dict = {} # Ensure it's a dict
                    # Add any other top-level keys not in headers to metadata
                    for k, v in data.items():
                        if k not in EXPECTED_HEADERS and k != "metadata":
                             metadata_dict[k] = v
                    # Convert metadata dict to JSON string for sheet
                    value = json.dumps(metadata_dict) if metadata_dict else "{}"
                elif isinstance(value, datetime):
                     value = value.isoformat() # Ensure datetimes are strings
                
                # Append value (or empty string if None/missing)
                row_to_append.append(str(value) if value is not None else "") 

            self.sheet.append_row(row_to_append)
            logger.info(f"Data for '{data['signal_name']}' loaded to Google Sheet: {self.worksheet_name}")

            # Update view worksheets (optional)
            # self._update_views(data)

            return True
        except Exception as e:
            logger.error(f"Failed to load data to Google Sheets ({self.worksheet_name}): {e}")
            raise LoadError(f"Google Sheets load error: {e}")
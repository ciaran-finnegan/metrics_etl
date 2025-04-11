import gspread
import certifi
import os
import json
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials
from utils.exceptions import LoadError
from utils.logging_config import logger

class GoogleSheetsLoader:
    # View definitions for different metric categories
    VIEWS = {
        'market_sentiment': ['fear_and_greed_index', 'btc_rsi_10_day', 'bull_market_support_band', 'bitcoin_dominance'],
        'holder_metrics': ['long_term_holder_sopr', 'short_term_holder_sopr', 'short_term_holder_mvrv', 'bitfinex_btc_whales'],
        'monetary_metrics': ['global_m2_money_supply', 'realised_cap_for_onchain_liquidity', 'futures_funding_rate']
    }

    def __init__(self, creds_path: str, sheet_name: str = None, worksheet: str = None):
        """
        Args:
            creds_path: Path to Google Service Account JSON file
            sheet_name: Name of the Google Sheet (defaults to GOOGLE_SHEET_NAME env var)
            worksheet: Worksheet/tab name (defaults to GOOGLE_SHEET_WORKSHEET env var)
        """
        try:
            os.environ['SSL_CERT_FILE'] = certifi.where()
            scope = [
                "https://spreadsheets.google.com/feeds",
                "https://www.googleapis.com/auth/drive"
            ]
            creds = ServiceAccountCredentials.from_json_keyfile_name(creds_path, scope)
            self.client = gspread.authorize(creds)
            
            # Get sheet name from env var if not provided
            sheet_name = sheet_name or os.getenv("GOOGLE_SHEET_NAME")
            if not sheet_name:
                raise LoadError("Missing GOOGLE_SHEET_NAME environment variable")
            
            # Get worksheet name from env var if not provided
            worksheet = worksheet or os.getenv("GOOGLE_SHEET_WORKSHEET", "raw_data")
            
            self.spreadsheet = self.client.open(sheet_name)
            self.raw_sheet = self.spreadsheet.worksheet(worksheet)
            
            # Ensure headers exist
            self._ensure_headers()
            
            # Setup view worksheets
            self._setup_views()
            
            logger.info(f"Google Sheets connected to: {sheet_name}/{worksheet}")
        except Exception as e:
            logger.error(f"Google Sheets connection failed: {e}")
            raise LoadError(f"Google Sheets init error: {e}")
    
    def _ensure_headers(self):
        """Ensure worksheet has the correct headers"""
        expected_headers = [
            "timestamp", "date", "metric_name", "value", "value_usd",
            "classification", "percentage", "ratio", "source",
            "timeframe", "units", "is_bullish", "metadata"
        ]
        
        try:
            current_headers = self.raw_sheet.row_values(1)
            if not current_headers or current_headers != expected_headers:
                # Clear existing data and set new headers
                self.raw_sheet.clear()
                self.raw_sheet.append_row(expected_headers)
                logger.info("Headers updated successfully")
        except Exception as e:
            logger.error(f"Failed to ensure headers: {e}")
            raise LoadError(f"Header setup failed: {e}")

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
        """Append data as a new row"""
        try:
            # Prepare row data for raw sheet
            row = [
                datetime.now().isoformat(),  # timestamp
                data.get("date", datetime.now().date().isoformat()),  # date
                data["signal_name"],  # metric_name
                data["value"],  # value
                data["value"] if data.get("units") == "USD" else "",  # value_usd
                data.get("classification", ""),  # classification
                data.get("percentage", ""),  # percentage
                data.get("ratio", ""),  # ratio
                data.get("source", ""),  # source
                data.get("timeframe", ""),  # timeframe
                data.get("units", ""),  # units
                data.get("is_bullish", ""),  # is_bullish
                json.dumps({k: v for k, v in data.items() if k not in [
                    "signal_name", "date", "value", "classification", 
                    "percentage", "ratio", "source", "timeframe", 
                    "units", "is_bullish"
                ]})  # metadata
            ]
            
            # Add to raw data sheet
            self.raw_sheet.append_row(row)
            
            # Update view worksheets
            self._update_views(data)
            
            return True
        
        except Exception as e:
            logger.error(f"Google Sheets append failed: {e}")
            raise LoadError(f"Google Sheets load error: {e}")
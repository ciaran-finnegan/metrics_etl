from datetime import datetime
from utils.exceptions import TransformationError
from utils.logging_config import logger

class M2Transformer:
    def transform(self, raw_data: dict) -> dict:
        try:
            # Find the most recent valid observation
            observations = raw_data.get("observations", [])
            logger.debug(f"Processing {len(observations)} observations from FRED")
            
            valid_observations = []
            now = datetime.now()
            
            for obs in observations:
                try:
                    date_str = obs.get("date")
                    if not date_str:
                        continue
                        
                    date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                    
                    # Skip future dates
                    if date_obj > now:
                        logger.warning(f"Skipping future date: {date_str}")
                        continue
                        
                    valid_observations.append((date_obj, obs))
                except ValueError as e:
                    logger.warning(f"Invalid date format: {date_str}")
                    continue
            
            if not valid_observations:
                logger.error("No valid observations found")
                raise TransformationError("No valid observations found in FRED data")
                
            # Sort by date descending and take the most recent
            valid_observations.sort(key=lambda x: x[0], reverse=True)
            latest_date, latest = valid_observations[0]
            
            logger.debug(f"Most recent valid observation: date={latest['date']}, value={latest['value']}")
            
            transformed = {
                "date": latest_date.strftime("%Y-%m-%d"),
                "value": float(latest["value"]),
                "updated_at": datetime.now().isoformat(),
                "units": "USD"
            }
            
            logger.debug(f"Transformed M2 date format: {transformed['date']} (type: {type(transformed['date'])})")
            logger.debug(f"Full transformed data: {transformed}")
            return transformed
            
        except Exception as e:
            logger.error(f"M2 transformation failed: {e}")
            raise TransformationError(f"M2 processing error: {e}")
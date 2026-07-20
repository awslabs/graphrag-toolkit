"""Date range filter — filters DataFrame rows based on date range criteria."""
import pandas as pd
from datetime import datetime, timedelta, timezone
from typing import Optional
from document_graph.ingest.ingestors_provider_base import IngestorProvider


class DateRangeFilterProvider(IngestorProvider):
    """Filter rows based on date ranges."""
    
    def ingest(self, data: pd.DataFrame) -> pd.DataFrame:
        date_field = self.args.get("date_field")
        if not date_field or date_field not in data.columns:
            return data
        
        # Convert to datetime if not already
        date_column = pd.to_datetime(data[date_field], errors='coerce')
        
        # Start with all rows included
        mask = pd.Series([True] * len(data), index=data.index)
        
        # Handle different date range specifications
        start_date = self._parse_date(self.args.get("start_date"))
        end_date = self._parse_date(self.args.get("end_date"))
        
        # Relative date ranges
        days_back = self.args.get("days_back")
        weeks_back = self.args.get("weeks_back") 
        months_back = self.args.get("months_back")
        
        # Auto-detect timezone handling - match data timezone
        data_has_tz = date_column.dt.tz is not None
        
        # Calculate relative dates matching data timezone
        if data_has_tz:
            now = datetime.now(timezone.utc)  # Use UTC for timezone-aware data
        else:
            now = datetime.now()  # Use naive datetime for naive data
            
        if days_back:
            start_date = now - timedelta(days=days_back)
        elif weeks_back:
            start_date = now - timedelta(weeks=weeks_back)
        elif months_back:
            start_date = now - timedelta(days=months_back * 30)  # Approximate
        
        # Apply date filters - ensure timezone compatibility
        def make_compatible(dt, target_has_tz):
            if target_has_tz and dt.tzinfo is None:
                return dt.replace(tzinfo=timezone.utc)
            elif not target_has_tz and dt.tzinfo is not None:
                return dt.replace(tzinfo=None)
            return dt
        
        if start_date:
            start_date = make_compatible(start_date, data_has_tz)
            mask &= (date_column >= start_date)
        if end_date:
            end_date = make_compatible(end_date, data_has_tz)
            mask &= (date_column <= end_date)
        
        # Handle null dates
        include_null = self.args.get("include_null", False)
        if not include_null:
            mask &= date_column.notna()
        
        return data[mask]
    
    def _parse_date(self, date_value) -> Optional[datetime]:
        """Parse various date formats."""
        if not date_value:
            return None
        
        if isinstance(date_value, str):
            try:
                return pd.to_datetime(date_value)
            except:
                return None
        elif isinstance(date_value, datetime):
            return date_value
        
        return None
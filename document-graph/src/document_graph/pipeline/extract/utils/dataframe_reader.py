"""DataFrame reader — wraps pandas read_csv with config."""
import pandas as pd
from pathlib import Path

class DataFrameReader:
    """Reads data files into pandas DataFrames with configurable options."""

    def __init__(self, path, **kwargs):
        self.path = Path(path)
        self.kwargs = kwargs
    
    def read(self) -> pd.DataFrame:
        return pd.read_csv(self.path, **self.kwargs)

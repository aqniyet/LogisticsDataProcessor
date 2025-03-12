import pandas as pd
import numpy as np
import logging
from typing import Dict, Any, Optional

logger = logging.getLogger(__name__)

def standardize_column_types(df: pd.DataFrame, type_mapping: Dict[str, str]) -> pd.DataFrame:
    """
    Standardize column types according to the provided mapping.
    Handles missing columns gracefully.
    """
    result_df = df.copy()
    
    for column, dtype in type_mapping.items():
        if column in result_df.columns:
            try:
                if dtype == 'datetime64':
                    result_df[column] = pd.to_datetime(result_df[column], errors='coerce')
                elif dtype == 'int64':
                    result_df[column] = pd.to_numeric(result_df[column], errors='coerce').fillna(0).astype('int64')
                elif dtype == 'float':
                    result_df[column] = pd.to_numeric(result_df[column], errors='coerce')
                elif dtype == 'str':
                    result_df[column] = result_df[column].astype(str)
            except Exception as e:
                logger.warning(f"Error converting column {column} to {dtype}: {str(e)}")
    
    return result_df

def read_excel_file(file_path: str, sheet_name: Optional[str] = None) -> pd.DataFrame:
    """Read an Excel file and return as DataFrame."""
    try:
        # Read Excel file
        if sheet_name:
            df = pd.read_excel(file_path, sheet_name=sheet_name)
        else:
            df = pd.read_excel(file_path)
        
        # Clean up column names
        df.columns = [col.strip() if isinstance(col, str) else str(col) for col in df.columns]
        
        # Convert any bytes columns to strings
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].apply(lambda x: x.decode('cp1251') if isinstance(x, bytes) else str(x))
        
        return df
    except Exception as e:
        logger.error(f"Error reading Excel file {file_path}: {str(e)}")
        raise

def read_csv_file(file_path: str, encoding: str = 'cp1251') -> pd.DataFrame:
    """Read a CSV file and return as DataFrame."""
    try:
        df = pd.read_csv(file_path, encoding=encoding)
        # Clean up column names
        df.columns = [col.strip() if isinstance(col, str) else str(col) for col in df.columns]
        return df
    except Exception as e:
        logger.error(f"Error reading CSV file {file_path}: {str(e)}")
        raise
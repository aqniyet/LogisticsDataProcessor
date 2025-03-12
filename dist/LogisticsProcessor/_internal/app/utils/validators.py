import os
import pandas as pd
import logging
from typing import Tuple, List, Dict, Any

logger = logging.getLogger(__name__)

def validate_stg_file(file_path: str) -> Tuple[bool, str]:
    """
    Validate STG file structure.
    Returns (is_valid, error_message)
    """
    if not os.path.exists(file_path):
        return False, f"File does not exist: {file_path}"
    
    try:
        df = pd.read_excel(file_path)
        
        # Check for required columns
        required_columns = [
            "Вагон №", "Накладная №", "Ст. отправления", "Ст. назначения", 
            "Отчетная дата", "Груж\\пор", "Тип вагона"
        ]
        
        missing_columns = [col for col in required_columns if col not in df.columns]
        
        if missing_columns:
            return False, f"Missing required columns: {', '.join(missing_columns)}"
        
        # Check for minimum data requirements
        if df.empty:
            return False, "File contains no data"
        
        null_counts = df[required_columns].isnull().sum()
        columns_with_nulls = [col for col, count in null_counts.items() if count > 0]
        
        if columns_with_nulls:
            return True, f"Warning: File contains NULL values in columns: {', '.join(columns_with_nulls)}"
        
        return True, "File is valid"
        
    except Exception as e:
        return False, f"Error validating file: {str(e)}"

def validate_znp_structure(df: pd.DataFrame) -> Tuple[bool, str]:
    """
    Validate ZNP data structure.
    Returns (is_valid, error_message)
    """
    required_columns = ["Месяц", "Ст. отправления", "Ст. назначения", "Тип вагона", "ЗНП"]
    
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        return False, f"Missing required columns: {', '.join(missing_columns)}"
    
    # Check for minimum data requirements
    if df.empty:
        return False, "DataFrame contains no data"
    
    null_counts = df[required_columns].isnull().sum()
    columns_with_nulls = [col for col, count in null_counts.items() if count > 0]
    
    if columns_with_nulls:
        return True, f"Warning: DataFrame contains NULL values in columns: {', '.join(columns_with_nulls)}"
    
    return True, "ZNP structure is valid"
import os
import glob
import logging
from typing import List

logger = logging.getLogger(__name__)

def ensure_directory_exists(directory_path: str) -> None:
    """Ensure the specified directory exists, creating it if necessary."""
    if not os.path.exists(directory_path):
        os.makedirs(directory_path)
        logger.info(f"Created directory: {directory_path}")

def get_files_by_pattern(directory: str, pattern: str) -> List[str]:
    """Get a list of files matching the specified pattern in the directory."""
    if not os.path.exists(directory):
        logger.warning(f"Directory does not exist: {directory}")
        return []
    
    file_pattern = os.path.join(directory, pattern)
    files = glob.glob(file_pattern)
    
    logger.info(f"Found {len(files)} files matching pattern '{pattern}' in {directory}")
    return files
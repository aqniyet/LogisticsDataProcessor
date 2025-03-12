import os
import json
import logging

logger = logging.getLogger(__name__)

DEFAULT_CONFIG = {
    "base_directory": "./data",
    "output_directory": "./output",
    "database_path": "logistics_processor.db",
    "stg_folder": "",
    "existing_data_path": "",
    "route_id_path": ""
}

def get_config_path():
    """Get the path to the config file in AppData."""
    config_dir = os.path.join(os.getenv('APPDATA'), 'Logistics Data Processor')
    os.makedirs(config_dir, exist_ok=True)
    return os.path.join(config_dir, "config.json")

def load_config() -> dict:
    """Load configuration from JSON file, or create default if not exists."""
    config_file = get_config_path()
    if os.path.exists(config_file):
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
            logger.info(f"Loaded configuration from {config_file}")
            return config
        except Exception as e:
            logger.error(f"Error loading configuration: {str(e)}")
            return DEFAULT_CONFIG
    else:
        logger.info(f"Configuration file not found, creating default")
        save_config(DEFAULT_CONFIG)
        return DEFAULT_CONFIG

def save_config(config: dict) -> None:
    """Save configuration to JSON file."""
    config_file = get_config_path()
    try:
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=4)
        logger.info(f"Saved configuration to {config_file}")
    except Exception as e:
        logger.error(f"Error saving configuration: {str(e)}")
        raise
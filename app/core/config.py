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
    "route_id_path": "",
    "znp_path": "",
    "exceptions_path": "",
    "overrides_path": "",
    "active_path": "",
    "matrix_path": "",
    "expense_folder": ""
}

CONFIG_FILE = "config.json"

def load_config(config_file: str = CONFIG_FILE) -> dict:
    """Load configuration from JSON file, or create default if not exists."""
    if os.path.exists(config_file):
        try:
            with open(config_file, 'r', encoding='utf-8') as f:
                config = json.load(f)
                
            # Ensure all default keys exist
            for key, value in DEFAULT_CONFIG.items():
                if key not in config:
                    config[key] = value
                    
            logger.info(f"Loaded configuration from {config_file}")
            return config
        except Exception as e:
            logger.error(f"Error loading configuration: {str(e)}")
            return DEFAULT_CONFIG.copy()
    else:
        logger.info(f"Configuration file not found, creating default")
        config = DEFAULT_CONFIG.copy()
        save_config(config, config_file)
        return config

def save_config(config: dict, config_file: str = CONFIG_FILE) -> None:
    """Save configuration to JSON file."""
    try:
        # Create directories if they don't exist
        os.makedirs(os.path.dirname(config_file) if os.path.dirname(config_file) else '.', exist_ok=True)
        
        # Ensure all default keys exist
        for key, value in DEFAULT_CONFIG.items():
            if key not in config:
                config[key] = value
        
        with open(config_file, 'w', encoding='utf-8') as f:
            json.dump(config, f, indent=4, ensure_ascii=False)
        logger.info(f"Saved configuration to {config_file}")
        
        # Create configured directories
        os.makedirs(config["base_directory"], exist_ok=True)
        os.makedirs(config["output_directory"], exist_ok=True)
        
    except Exception as e:
        logger.error(f"Error saving configuration: {str(e)}")
        raise

def get_config_value(key: str, default: any = None) -> any:
    """Get a specific configuration value."""
    config = load_config()
    return config.get(key, default)

def set_config_value(key: str, value: any) -> None:
    """Set a specific configuration value."""
    config = load_config()
    config[key] = value
    save_config(config) 
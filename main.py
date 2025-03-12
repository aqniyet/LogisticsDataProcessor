import sys
import os
import logging
import argparse
from PyQt5.QtWidgets import QApplication

from app.main import LogisticsProcessorApp
from app.utils.file_utils import ensure_directory_exists
from app.config import load_config
from app.core.expense_processor import ExpenseProcessor
from app.database.models import init_db
from app.database.operations import init_session, get_database_path
from app.core.file_processor import FileProcessor

# Setup logging
def setup_logging():
    log_dir = os.path.join(os.getenv('APPDATA'), 'Logistics Data Processor', 'logs')
    os.makedirs(log_dir, exist_ok=True)
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(os.path.join(log_dir, "logistics_processor.log")),
            logging.StreamHandler()
        ]
    )

def process_expenses(config, folder_type):
    """Process expense files."""
    logger = logging.getLogger(__name__)
    
    # Initialize database
    db_path = get_database_path()
    engine, session_maker = init_db(db_path)
    session = session_maker()
    init_session(session)
    
    # Create expense processor
    processor = ExpenseProcessor(config)
    
    # Process expense files
    try:
        result = processor.process_expense_folder(folder_type, config["route_id_path"])
        logger.info(f"Expense processing complete. Results: {result}")
        return True
    except Exception as e:
        logger.error(f"Error processing expense files: {str(e)}")
        return False

def process_stg(config):
    """Process STG files."""
    logger = logging.getLogger(__name__)
    
    # Initialize database
    db_path = get_database_path()
    engine, session_maker = init_db(db_path)
    session = session_maker()
    init_session(session)
    
    # Create file processor
    processor = FileProcessor(config)
    
    # Process STG files
    try:
        output_path = processor.process_workflow(
            config["stg_folder"],
            config["existing_data_path"]
        )
        if output_path:
            logger.info(f"STG processing complete. Output saved to: {output_path}")
            # Update config with new route_id_path
            config["route_id_path"] = output_path
            from app.config import save_config
            save_config(config)
            return True
        else:
            logger.error("No output generated from STG processing")
            return False
    except Exception as e:
        logger.error(f"Error processing STG files: {str(e)}")
        return False

def main():
    """Application entry point."""
    setup_logging()
    logger = logging.getLogger(__name__)
    
    # Load configuration
    config = load_config()
    
    # Always run in GUI mode
    app = QApplication(sys.argv)
    window = LogisticsProcessorApp()
    window.show()
    sys.exit(app.exec_())

if __name__ == "__main__":
    main()
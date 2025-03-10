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
from app.database.operations import init_session
from app.core.file_processor import FileProcessor

# Setup logging
def setup_logging():
    log_dir = "logs"
    ensure_directory_exists(log_dir)
    
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
    db_path = config.get("database_path", "logistics_processor.db")
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
    db_path = config.get("database_path", "logistics_processor.db")
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
    
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Logistics Data Processor')
    parser.add_argument('--gui', action='store_true', help='Run in GUI mode')
    parser.add_argument('--expenses', choices=['1', '2'], help='Process expense files (type 1 or 2)')
    parser.add_argument('--stg', action='store_true', help='Process STG files')
    args = parser.parse_args()
    
    # Load configuration
    config = load_config()
    
    if args.gui:
        app = QApplication(sys.argv)
        window = LogisticsProcessorApp()
        window.show()
        sys.exit(app.exec_())
    elif args.expenses:
        success = process_expenses(config, args.expenses)
        sys.exit(0 if success else 1)
    elif args.stg:
        success = process_stg(config)
        sys.exit(0 if success else 1)
    else:
        # Default behavior (process STG files)
        success = process_stg(config)
        sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
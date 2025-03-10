import os
import sys
import logging
import pandas as pd
from typing import Dict, Any, Optional
from PyQt5.QtWidgets import (QApplication, QMainWindow, QTabWidget, QFileDialog,
                           QMessageBox, QPushButton, QLabel, QComboBox,
                           QVBoxLayout, QHBoxLayout, QWidget, QTextEdit,
                           QLineEdit, QProgressBar, QGroupBox, QTableView)
from PyQt5.QtCore import Qt, QThread, pyqtSignal

from app.config import load_config, save_config
from app.database.models import init_db
from app.database.operations import init_session, add_znp_data, add_exceptions, add_overrides
from app.database.operations import add_active_routes, add_matrix_mappings, log_operation
from app.core.file_processor import FileProcessor
from app.core.expense_processor import ExpenseProcessor
from app.utils.file_utils import get_files_by_pattern, ensure_directory_exists
from app.utils.data_utils import read_excel_file, read_csv_file

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("logistics_processor.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class LogisticsProcessorApp(QMainWindow):
    """
    Main application window for the Logistics Processor.
    Replaces Power BI workflow with a complete end-to-end solution.
    """
    
    def __init__(self):
        super().__init__()
        self.config = load_config()
        self.init_database()
        self.init_ui()
        
    def init_database(self):
        """Initialize the database connection."""
        db_path = self.config.get("database_path", "logistics_processor.db")
        engine, session_maker = init_db(db_path)
        session = session_maker()
        init_session(session)
    
    def init_ui(self):
        """Initialize the user interface."""
        self.setWindowTitle("Logistics Data Processor")
        self.setGeometry(100, 100, 800, 600)
        
        # Create tab widget
        self.tabs = QTabWidget()
        self.setCentralWidget(self.tabs)
        
        # Create tabs
        self.stg_tab = QWidget()
        self.expense_tab = QWidget()
        self.reference_tab = QWidget()
        self.config_tab = QWidget()
        
        self.tabs.addTab(self.stg_tab, "STG Processing")
        self.tabs.addTab(self.expense_tab, "Expense Processing")
        self.tabs.addTab(self.reference_tab, "Reference Data")
        self.tabs.addTab(self.config_tab, "Configuration")
        
        # Set up each tab
        self.setup_stg_tab()
        self.setup_expense_tab()
        self.setup_reference_tab()
        self.setup_config_tab()
    
    def setup_stg_tab(self):
        """Set up the STG processing tab."""
        layout = QVBoxLayout()
        
        # Input folder section
        input_group = QGroupBox("STG Folder Configuration")
        input_layout = QVBoxLayout()
        
        stg_layout = QHBoxLayout()
        stg_layout.addWidget(QLabel("STG Files Folder:"))
        self.stg_folder_edit = QLineEdit(self.config.get("stg_folder", ""))
        stg_layout.addWidget(self.stg_folder_edit)
        stg_browse_btn = QPushButton("Browse...")
        stg_browse_btn.clicked.connect(self.browse_stg_folder)
        stg_layout.addWidget(stg_browse_btn)
        input_layout.addLayout(stg_layout)
        
        # Add folder status
        self.folder_status = QLabel("")
        input_layout.addWidget(self.folder_status)
        self.update_folder_status()  # Initial status update
        
        input_group.setLayout(input_layout)
        layout.addWidget(input_group)
        
        # Processing section
        process_group = QGroupBox("Processing")
        process_layout = QVBoxLayout()
        
        self.process_stg_btn = QPushButton("Process All STG Files")
        self.process_stg_btn.clicked.connect(self.process_stg_files)
        process_layout.addWidget(self.process_stg_btn)
        
        self.stg_progress = QProgressBar()
        process_layout.addWidget(self.stg_progress)
        
        self.stg_status = QLabel("Ready")
        process_layout.addWidget(self.stg_status)
        
        process_group.setLayout(process_layout)
        layout.addWidget(process_group)
        
        # Output section
        output_group = QGroupBox("Processing Output")
        output_layout = QVBoxLayout()
        
        self.stg_output = QTextEdit()
        self.stg_output.setReadOnly(True)
        output_layout.addWidget(self.stg_output)
        
        output_group.setLayout(output_layout)
        layout.addWidget(output_group)
        
        self.stg_tab.setLayout(layout)
    
    def update_folder_status(self):
        """Update the folder status label with current STG file count."""
        stg_folder = self.config.get("stg_folder", "")
        if not stg_folder or not os.path.exists(stg_folder):
            self.folder_status.setText("STG folder not found or not configured")
            self.folder_status.setStyleSheet("color: red")
            return
            
        stg_files = get_files_by_pattern(stg_folder, "STGDaily_*.xlsx")
        if not stg_files:
            self.folder_status.setText("No STG files found in folder")
            self.folder_status.setStyleSheet("color: orange")
        else:
            self.folder_status.setText(f"Found {len(stg_files)} STG files ready for processing")
            self.folder_status.setStyleSheet("color: green")
    
    def setup_expense_tab(self):
        """Set up the Expense processing tab."""
        layout = QVBoxLayout()
        
        # Input section
        input_group = QGroupBox("Input Data")
        input_layout = QVBoxLayout()
        
        # Expense folder selection
        expense_folder_layout = QHBoxLayout()
        expense_folder_layout.addWidget(QLabel("Expenses Folder:"))
        self.expense_folder_edit = QLineEdit(self.config.get("expense_folder", ""))
        expense_folder_layout.addWidget(self.expense_folder_edit)
        expense_browse_btn = QPushButton("Browse...")
        expense_browse_btn.clicked.connect(self.browse_expense_folder)
        expense_folder_layout.addWidget(expense_browse_btn)
        input_layout.addLayout(expense_folder_layout)
        
        # Add folder status
        self.expense_folder_status = QLabel("")
        input_layout.addWidget(self.expense_folder_status)
        self.update_expense_folder_status()  # Initial status update
        
        route_id_layout = QHBoxLayout()
        route_id_layout.addWidget(QLabel("Route ID File:"))
        self.route_id_edit = QLineEdit(self.config.get("route_id_path", ""))
        route_id_layout.addWidget(self.route_id_edit)
        route_id_browse_btn = QPushButton("Browse...")
        route_id_browse_btn.clicked.connect(self.browse_route_id)
        route_id_layout.addWidget(route_id_browse_btn)
        input_layout.addLayout(route_id_layout)
        
        input_group.setLayout(input_layout)
        layout.addWidget(input_group)
        
        # Processing section
        process_group = QGroupBox("Processing")
        process_layout = QVBoxLayout()
        
        self.process_expenses_btn = QPushButton("Process Expense Files")
        self.process_expenses_btn.clicked.connect(self.process_expense_files)
        process_layout.addWidget(self.process_expenses_btn)
        
        self.expense_progress = QProgressBar()
        process_layout.addWidget(self.expense_progress)
        
        self.expense_status = QLabel("Ready")
        process_layout.addWidget(self.expense_status)
        
        process_group.setLayout(process_layout)
        layout.addWidget(process_group)
        
        # Output section
        output_group = QGroupBox("Processing Output")
        output_layout = QVBoxLayout()
        
        self.expense_output = QTextEdit()
        self.expense_output.setReadOnly(True)
        output_layout.addWidget(self.expense_output)
        
        output_group.setLayout(output_layout)
        layout.addWidget(output_group)
        
        self.expense_tab.setLayout(layout)
    
    def update_expense_folder_status(self):
        """Update the expense folder status label with current file count."""
        expense_folder = self.config.get("expense_folder", "")
        if not expense_folder or not os.path.exists(expense_folder):
            self.expense_folder_status.setText("Expense folder not found or not configured")
            self.expense_folder_status.setStyleSheet("color: red")
            return
            
        expense_files = [f for f in os.listdir(expense_folder) if f.lower().endswith(('.xlsx', '.xls'))]
        if not expense_files:
            self.expense_folder_status.setText("No expense files found in folder")
            self.expense_folder_status.setStyleSheet("color: orange")
        else:
            self.expense_folder_status.setText(f"Found {len(expense_files)} expense files ready for processing")
            self.expense_folder_status.setStyleSheet("color: green")
    
    def browse_expense_folder(self):
        """Browse for expense files folder."""
        folder = QFileDialog.getExistingDirectory(self, "Select Expense Files Folder")
        if folder:
            self.expense_folder_edit.setText(folder)
            self.config["expense_folder"] = folder
            save_config(self.config)  # Save the configuration immediately
            self.update_expense_folder_status()  # Update the status to show new file count
    
    def setup_reference_tab(self):
        """Set up the Reference Data tab."""
        layout = QVBoxLayout()
        
        # Reference files section
        ref_group = QGroupBox("Reference Files")
        ref_layout = QVBoxLayout()
        
        # ZNP file
        znp_layout = QHBoxLayout()
        znp_layout.addWidget(QLabel("ЗНП File:"))
        self.znp_edit = QLineEdit(self.config.get("znp_path", ""))
        znp_layout.addWidget(self.znp_edit)
        znp_browse_btn = QPushButton("Browse...")
        znp_browse_btn.clicked.connect(lambda: self.browse_reference_file("znp"))
        znp_layout.addWidget(znp_browse_btn)
        ref_layout.addLayout(znp_layout)
        
        # Exceptions file
        exceptions_layout = QHBoxLayout()
        exceptions_layout.addWidget(QLabel("Exceptions File:"))
        self.exceptions_edit = QLineEdit(self.config.get("exceptions_path", ""))
        exceptions_layout.addWidget(self.exceptions_edit)
        exceptions_browse_btn = QPushButton("Browse...")
        exceptions_browse_btn.clicked.connect(lambda: self.browse_reference_file("exceptions"))
        exceptions_layout.addWidget(exceptions_browse_btn)
        ref_layout.addLayout(exceptions_layout)
        
        # Overrides file
        overrides_layout = QHBoxLayout()
        overrides_layout.addWidget(QLabel("Overrides File:"))
        self.overrides_edit = QLineEdit(self.config.get("overrides_path", ""))
        overrides_layout.addWidget(self.overrides_edit)
        overrides_browse_btn = QPushButton("Browse...")
        overrides_browse_btn.clicked.connect(lambda: self.browse_reference_file("overrides"))
        overrides_layout.addWidget(overrides_browse_btn)
        ref_layout.addLayout(overrides_layout)
        
        # Active file
        active_layout = QHBoxLayout()
        active_layout.addWidget(QLabel("Active Routes File:"))
        self.active_edit = QLineEdit(self.config.get("active_path", ""))
        active_layout.addWidget(self.active_edit)
        active_browse_btn = QPushButton("Browse...")
        active_browse_btn.clicked.connect(lambda: self.browse_reference_file("active"))
        active_layout.addWidget(active_browse_btn)
        ref_layout.addLayout(active_layout)
        
        # Matrix file
        matrix_layout = QHBoxLayout()
        matrix_layout.addWidget(QLabel("Matrix Mappings File:"))
        self.matrix_edit = QLineEdit(self.config.get("matrix_path", ""))
        matrix_layout.addWidget(self.matrix_edit)
        matrix_browse_btn = QPushButton("Browse...")
        matrix_browse_btn.clicked.connect(lambda: self.browse_reference_file("matrix"))
        matrix_layout.addWidget(matrix_browse_btn)
        ref_layout.addLayout(matrix_layout)
        
        # Import button
        import_btn = QPushButton("Import Reference Data")
        import_btn.clicked.connect(self.import_reference_data)
        ref_layout.addWidget(import_btn)
        
        self.ref_progress = QProgressBar()
        ref_layout.addWidget(self.ref_progress)
        
        self.ref_status = QLabel("Ready")
        ref_layout.addWidget(self.ref_status)
        
        ref_group.setLayout(ref_layout)
        layout.addWidget(ref_group)
        
        # Results section
        results_group = QGroupBox("Import Results")
        results_layout = QVBoxLayout()
        
        self.ref_output = QTextEdit()
        self.ref_output.setReadOnly(True)
        results_layout.addWidget(self.ref_output)
        
        results_group.setLayout(results_layout)
        layout.addWidget(results_group)
        
        self.reference_tab.setLayout(layout)
    
    def setup_config_tab(self):
        """Set up the Configuration tab."""
        layout = QVBoxLayout()
        
        # Directories section
        dir_group = QGroupBox("Directories")
        dir_layout = QVBoxLayout()
        
        base_dir_layout = QHBoxLayout()
        base_dir_layout.addWidget(QLabel("Base Directory:"))
        self.base_dir_edit = QLineEdit(self.config.get("base_directory", ""))
        base_dir_layout.addWidget(self.base_dir_edit)
        base_dir_browse_btn = QPushButton("Browse...")
        base_dir_browse_btn.clicked.connect(self.browse_base_directory)
        base_dir_layout.addWidget(base_dir_browse_btn)
        dir_layout.addLayout(base_dir_layout)
        
        output_dir_layout = QHBoxLayout()
        output_dir_layout.addWidget(QLabel("Output Directory:"))
        self.output_dir_edit = QLineEdit(self.config.get("output_directory", ""))
        output_dir_layout.addWidget(self.output_dir_edit)
        output_dir_browse_btn = QPushButton("Browse...")
        output_dir_browse_btn.clicked.connect(self.browse_output_directory)
        output_dir_layout.addWidget(output_dir_browse_btn)
        dir_layout.addLayout(output_dir_layout)
        
        db_layout = QHBoxLayout()
        db_layout.addWidget(QLabel("Database File:"))
        self.db_edit = QLineEdit(self.config.get("database_path", "logistics_processor.db"))
        db_layout.addWidget(self.db_edit)
        db_browse_btn = QPushButton("Browse...")
        db_browse_btn.clicked.connect(self.browse_database_file)
        db_layout.addWidget(db_browse_btn)
        dir_layout.addLayout(db_layout)
        
        dir_group.setLayout(dir_layout)
        layout.addWidget(dir_group)
        
        # Save button
        save_btn = QPushButton("Save Configuration")
        save_btn.clicked.connect(self.save_configuration)
        layout.addWidget(save_btn)
        
        # Status label
        self.config_status = QLabel("Ready")
        layout.addWidget(self.config_status)
        
        layout.addStretch()
        
        self.config_tab.setLayout(layout)
    
    # UI Event Handlers
    def browse_stg_folder(self):
        """Browse for STG files folder."""
        folder = QFileDialog.getExistingDirectory(self, "Select STG Files Folder")
        if folder:
            self.stg_folder_edit.setText(folder)
            self.config["stg_folder"] = folder
            save_config(self.config)  # Save the configuration immediately
            self.update_folder_status()  # Update the status to show new file count
    
    def browse_existing_data(self):
        """Browse for existing data file."""
        file, _ = QFileDialog.getOpenFileName(self, "Select Existing Data File", "", "Excel Files (*.xlsx *.xls)")
        if file:
            self.existing_data_edit.setText(file)
            self.config["existing_data_path"] = file
    
    def browse_route_id(self):
        """Browse for Route ID file."""
        file, _ = QFileDialog.getOpenFileName(self, "Select Route ID File", "", "CSV Files (*.csv)")
        if file:
            self.route_id_edit.setText(file)
            self.config["route_id_path"] = file
    
    def browse_reference_file(self, file_type):
        """Browse for reference data files."""
        if file_type in ["active", "matrix"]:
            file, _ = QFileDialog.getOpenFileName(self, f"Select {file_type.title()} File", "", "CSV Files (*.csv)")
        else:
            file, _ = QFileDialog.getOpenFileName(self, f"Select {file_type.title()} File", "", "Excel Files (*.xlsx *.xls)")
        
        if file:
            if file_type == "znp":
                self.znp_edit.setText(file)
                self.config["znp_path"] = file
            elif file_type == "exceptions":
                self.exceptions_edit.setText(file)
                self.config["exceptions_path"] = file
            elif file_type == "overrides":
                self.overrides_edit.setText(file)
                self.config["overrides_path"] = file
            elif file_type == "active":
                self.active_edit.setText(file)
                self.config["active_path"] = file
            elif file_type == "matrix":
                self.matrix_edit.setText(file)
                self.config["matrix_path"] = file
    
    def browse_base_directory(self):
        """Browse for base directory."""
        folder = QFileDialog.getExistingDirectory(self, "Select Base Directory")
        if folder:
            self.base_dir_edit.setText(folder)
            self.config["base_directory"] = folder
    
    def browse_output_directory(self):
        """Browse for output directory."""
        folder = QFileDialog.getExistingDirectory(self, "Select Output Directory")
        if folder:
            self.output_dir_edit.setText(folder)
            self.config["output_directory"] = folder
    
    def browse_database_file(self):
        """Browse for database file."""
        file, _ = QFileDialog.getSaveFileName(self, "Select Database File", "", "Database Files (*.db)")
        if file:
            self.db_edit.setText(file)
            self.config["database_path"] = file
    
    def save_configuration(self):
        """Save the configuration settings."""
        self.config["base_directory"] = self.base_dir_edit.text()
        self.config["output_directory"] = self.output_dir_edit.text()
        self.config["database_path"] = self.db_edit.text()
        self.config["stg_folder"] = self.stg_folder_edit.text()
        self.config["existing_data_path"] = self.existing_data_edit.text()
        self.config["route_id_path"] = self.route_id_edit.text()
        self.config["znp_path"] = self.znp_edit.text()
        self.config["exceptions_path"] = self.exceptions_edit.text()
        self.config["overrides_path"] = self.overrides_edit.text()
        self.config["active_path"] = self.active_edit.text()
        self.config["matrix_path"] = self.matrix_edit.text()
        
        try:
            save_config(self.config)
            self.config_status.setText("Configuration saved successfully!")
            
            # Create directories if they don't exist
            ensure_directory_exists(self.config["base_directory"])
            ensure_directory_exists(self.config["output_directory"])
            
        except Exception as e:
            self.config_status.setText(f"Error saving configuration: {str(e)}")
            logger.error(f"Error saving configuration: {str(e)}")
    
    # Processing Methods
    def process_stg_files(self):
        """Process all STG files in the configured folder."""
        # Disable the process button while running
        self.process_stg_btn.setEnabled(False)
        self.stg_status.setText("Processing...")
        self.stg_output.clear()
        
        try:
            # Get STG folder path
            stg_folder = self.config.get("stg_folder", "")
            if not stg_folder or not os.path.exists(stg_folder):
                raise ValueError("STG folder not found or not configured")
                
            # Find all STG files
            stg_files = get_files_by_pattern(stg_folder, "STGDaily_*.xlsx")
            if not stg_files:
                raise ValueError("No STG files found in the configured folder")
            
            # Initialize progress bar
            self.stg_progress.setMaximum(len(stg_files))
            self.stg_progress.setValue(0)
            
            # Process each file
            processor = FileProcessor(self.config)
            for i, file_path in enumerate(stg_files, 1):
                self.stg_output.append(f"Processing file {i}/{len(stg_files)}: {os.path.basename(file_path)}")
                QApplication.processEvents()  # Keep UI responsive
                
                try:
                    processor.process_stg_file(file_path)
                    self.stg_output.append(f"✓ Successfully processed {os.path.basename(file_path)}\n")
                except Exception as e:
                    self.stg_output.append(f"✗ Error processing {os.path.basename(file_path)}: {str(e)}\n")
                
                self.stg_progress.setValue(i)
                QApplication.processEvents()
            
            # Update status
            self.stg_status.setText(f"Completed processing {len(stg_files)} files")
            self.stg_status.setStyleSheet("color: green")
            
        except Exception as e:
            self.stg_status.setText(f"Error: {str(e)}")
            self.stg_status.setStyleSheet("color: red")
            self.stg_output.append(f"Error: {str(e)}")
        
        finally:
            # Re-enable the process button
            self.process_stg_btn.setEnabled(True)
            self.update_folder_status()  # Refresh folder status
    
    def process_expense_files(self):
        """Process expense files with Route ID data."""
        expense_folder = self.expense_folder_edit.text()
        route_id_path = self.route_id_edit.text()
        
        # Validate inputs
        if not expense_folder:
            QMessageBox.warning(self, "Input Required", "Please select expense files folder")
            return
            
        if not route_id_path:
            QMessageBox.warning(self, "Input Required", "Please select Route ID file")
            return
        
        if not os.path.exists(expense_folder):
            QMessageBox.warning(self, "Folder Not Found", "Expense folder does not exist")
            return
            
        if not os.path.exists(route_id_path):
            QMessageBox.warning(self, "File Not Found", "Route ID file does not exist")
            return
        
        # Setup parameters for processing
        params = {
            "config": self.config,
            "expense_folder": expense_folder,
            "route_id_path": route_id_path
        }
        
        # Start processing in worker thread
        self.worker = ProcessingWorker("process_expenses", params)
        self.worker.progress_update.connect(self.update_expense_progress)
        self.worker.process_complete.connect(self.expense_processing_complete)
        
        self.process_expenses_btn.setEnabled(False)
        self.expense_status.setText("Processing...")
        self.expense_progress.setValue(0)
        
        self.worker.start()
    
    def update_expense_progress(self, value, message):
        """Update expense processing progress."""
        self.expense_progress.setValue(value)
        self.expense_status.setText(message)
    
    def expense_processing_complete(self, success, message, result):
        """Handle expense processing completion."""
        self.process_expenses_btn.setEnabled(True)
        self.expense_status.setText(message)
        
        self.expense_output.clear()
        
        if success:
            self.expense_output.append(f"Processing completed successfully!")
            self.expense_output.append(f"Files processed: {result.get('processed_files', 0)}")
            self.expense_output.append(f"Files skipped: {result.get('skipped_files', 0)}")
            
            if result.get('error_files'):
                self.expense_output.append("\nFiles with errors:")
                for file_name, error in result.get('error_files', []):
                    self.expense_output.append(f"- {file_name}: {error}")
        else:
            self.expense_output.append(f"Processing failed: {message}")
            
            if result.get('error_files'):
                self.expense_output.append("\nFiles with errors:")
                for file_name, error in result.get('error_files', []):
                    self.expense_output.append(f"- {file_name}: {error}")
    
    def import_reference_data(self):
        """Import reference data from Excel/CSV files."""
        files = {
            "znp": self.znp_edit.text(),
            "exceptions": self.exceptions_edit.text(),
            "overrides": self.overrides_edit.text(),
            "active": self.active_edit.text(),
            "matrix": self.matrix_edit.text()
        }
        
        # Check if at least one file is selected
        if not any(files.values()):
            QMessageBox.warning(self, "Input Required", "Please select at least one reference file")
            return
        
        params = {
            "files": files
        }
        
        self.worker = ProcessingWorker("import_reference", params)
        self.worker.progress_update.connect(self.update_ref_progress)
        self.worker.process_complete.connect(self.ref_import_complete)
        
        self.ref_status.setText("Importing...")
        self.ref_progress.setValue(0)
        
        self.worker.start()
    
    def update_ref_progress(self, value, message):
        """Update reference data import progress."""
        self.ref_progress.setValue(value)
        self.ref_status.setText(message)
    
    def ref_import_complete(self, success, message, result):
        """Handle reference data import completion."""
        self.ref_status.setText(message)
        
        self.ref_output.clear()
        
        if success:
            self.ref_output.append("Reference data imported successfully:")
            for data_type, count in result.items():
                self.ref_output.append(f"- {data_type.title()}: {count} records")
        else:
            self.ref_output.append("Import completed with errors:")
            for data_type, result_info in result.items():
                if isinstance(result_info, int):
                    self.ref_output.append(f"- {data_type.title()}: {result_info} records")
                else:
                    self.ref_output.append(f"- {data_type.title()}: {result_info}")


class ProcessingWorker(QThread):
    """
    Worker thread for processing operations to keep UI responsive.
    """
    progress_update = pyqtSignal(int, str)
    process_complete = pyqtSignal(bool, str, dict)
    
    def __init__(self, operation_type, params):
        super().__init__()
        self.operation_type = operation_type
        self.params = params
        
    def run(self):
        """Run the worker thread."""
        if self.operation_type == "process_stg":
            self.process_stg_files()
        elif self.operation_type == "process_expenses":
            self.process_expense_files()
        elif self.operation_type == "import_reference":
            self.import_reference_data()
    
    def process_stg_files(self):
        """Process STG files to generate Route ID data."""
        config = self.params.get("config")
        stg_folder = self.params.get("stg_folder")
        existing_data_path = self.params.get("existing_data_path")
        
        processor = FileProcessor(config)
        
        self.progress_update.emit(20, "Loading STG files...")
        output_path = processor.process_workflow(stg_folder, existing_data_path)
        
        if output_path:
            self.progress_update.emit(100, "Processing complete")
            log_operation("process_stg", "SUCCESS", message=f"Output: {output_path}")
            self.process_complete.emit(True, "Successfully processed STG files", {"output_path": output_path})
        else:
            self.progress_update.emit(100, "Processing failed")
            log_operation("process_stg", "ERROR", message="Failed to process STG files")
            self.process_complete.emit(False, "Failed to process STG files", {})
    
    def process_expense_files(self):
        """Process expense files with Route ID data."""
        config = self.params.get("config")
        expense_folder = self.params.get("expense_folder")
        route_id_path = self.params.get("route_id_path")
        
        processor = ExpenseProcessor(config)
        
        self.progress_update.emit(20, f"Processing expense files in folder: {expense_folder}")
        result = processor.process_expense_folder(expense_folder, route_id_path)
        
        if result["processed_files"] > 0:
            self.progress_update.emit(100, "Processing complete")
            log_operation("process_expenses", "SUCCESS", 
                         message=f"Processed: {result['processed_files']}, Skipped: {result['skipped_files']}")
            self.process_complete.emit(True, "Successfully processed expense files", result)
        else:
            self.progress_update.emit(100, "Processing failed")
            log_operation("process_expenses", "ERROR", message="No files processed")
            self.process_complete.emit(False, "Failed to process expense files", result)
    
    def import_reference_data(self):
        """Import reference data from Excel/CSV files."""
        files = self.params.get("files", {})
        
        total_files = len(files)
        processed = 0
        results = {}
        
        for file_type, file_path in files.items():
            if not file_path or not os.path.exists(file_path):
                continue
                
            processed += 1
            progress = int((processed / total_files) * 100)
            self.progress_update.emit(progress, f"Importing {file_type}...")
            
            try:
                if file_type == "znp":
                    df = read_excel_file(file_path)
                    count = add_znp_data(df)
                    results[file_type] = count
                elif file_type == "exceptions":
                    df = read_excel_file(file_path)
                    count = add_exceptions(df)
                    results[file_type] = count
                elif file_type == "overrides":
                    df = read_excel_file(file_path)
                    count = add_overrides(df)
                    results[file_type] = count
                elif file_type == "active":
                    df = read_csv_file(file_path)
                    routes = df.iloc[:, 0].astype(str).tolist()
                    count = add_active_routes(routes)
                    results[file_type] = count
                elif file_type == "matrix":
                    df = read_csv_file(file_path)
                    count = add_matrix_mappings(df)
                    results[file_type] = count
            except BaseException as e:
                logger.error(f"Error importing {file_type}: {str(e)}")
                results[file_type] = f"Error: {str(e)}"
        
        self.progress_update.emit(100, "Import complete")
        
        if all(isinstance(v, int) for v in results.values()):
            log_operation("import_reference", "SUCCESS", message=f"Imported: {results}")
            self.process_complete.emit(True, "Successfully imported reference data", results)
        else:
            log_operation("import_reference", "ERROR", message=f"Errors during import: {results}")
            self.process_complete.emit(False, "Errors occurred during import", results)
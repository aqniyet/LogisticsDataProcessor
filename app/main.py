import sys
import os
import logging
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Any
import pandas as pd
from PyQt5.QtWidgets import (QApplication, QMainWindow, QTabWidget, QWidget,
                          QVBoxLayout, QPushButton, QLabel, QTableView,
                          QMessageBox, QFileDialog, QProgressBar, QGroupBox,
                          QHBoxLayout, QLineEdit, QTextEdit)
from PyQt5.QtCore import Qt, QThread, pyqtSignal
from PyQt5.QtGui import QStandardItemModel, QStandardItem
import json
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import glob

from app.database.models import Base
from app.database.operations import (
    add_znp_data, get_znp_data, get_exceptions, add_exceptions,
    get_overrides, add_overrides, add_active_routes, get_active_routes,
    get_matrix_mappings, add_matrix_mappings, init_session, add_stg_data
)

from app.config import load_config, save_config
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
        self.setWindowTitle("Logistics Data Processor")
        self.setGeometry(100, 100, 1200, 800)
        
        # Initialize database
        engine = create_engine('sqlite:///logistics.db')
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        session = Session()
        init_session(session)
        
        # Initialize instance variables
        self.config = load_config()
        self.processed_stg_data = None
        
        # Set up the main widget and layout
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        layout = QVBoxLayout()
        main_widget.setLayout(layout)
        
        # Create tab widget
        self.tabs = QTabWidget()
        layout.addWidget(self.tabs)
        
        # Create tabs in the logical order
        self.reference_tab = QWidget()
        self.znp_routes_tab = QWidget()
        self.stg_tab = QWidget()
        self.expense_tab = QWidget()
        self.config_tab = QWidget()
        
        # Add tabs in the workflow sequence
        self.tabs.addTab(self.reference_tab, "1. Reference Data")
        self.tabs.addTab(self.znp_routes_tab, "2. ZNP Routes")
        self.tabs.addTab(self.stg_tab, "3. STG Processing")
        self.tabs.addTab(self.expense_tab, "4. Expense Processing")
        self.tabs.addTab(self.config_tab, "5. Configuration")
        
        # Initialize all tables and models first
        self.init_tables_and_models()
        
        # Set up each tab
        self.setup_reference_tab()
        self.setup_znp_routes_tab()
        self.setup_stg_tab()
        self.setup_expense_tab()
        self.setup_config_tab()
        
        # Load initial data
        self.load_initial_data()
        
    def init_tables_and_models(self):
        """Initialize all tables and models."""
        # ZNP Routes
        self.routes_table = QTableView()
        self.routes_model = QStandardItemModel()
        self.routes_model.setHorizontalHeaderLabels([
            "Месяц", "Ст. отправления", "Ст. назначения", "Тип вагона", "Количество", "ЗНП"
        ])
        self.routes_table.setModel(self.routes_model)
        
        # Results
        self.results_table = QTableView()
        self.results_model = QStandardItemModel()
        self.results_table.setModel(self.results_model)
        
        # Exceptions
        self.exceptions_table = QTableView()
        self.exceptions_model = QStandardItemModel()
        self.exceptions_table.setModel(self.exceptions_model)
        
        # Overrides
        self.overrides_table = QTableView()
        self.overrides_model = QStandardItemModel()
        self.overrides_table.setModel(self.overrides_model)
        
        # Matrix
        self.matrix_table = QTableView()
        self.matrix_model = QStandardItemModel()
        self.matrix_table.setModel(self.matrix_model)
    
    def load_initial_data(self):
        """Load initial data into tables."""
        try:
            # Load ZNP data
            znp_data = get_znp_data()
            if not znp_data.empty:
                self.update_routes_table(znp_data)
            
            # Load exceptions
            exceptions_data = get_exceptions()
            if not exceptions_data.empty:
                self.update_exceptions_table()
            
            # Load overrides
            overrides_data = get_overrides()
            if not overrides_data.empty:
                self.update_overrides_table()
            
            # Load matrix
            matrix_data = get_matrix_mappings()
            if not matrix_data.empty:
                self.update_matrix_table()
        
        except Exception as e:
            logger.error(f"Error loading initial data: {str(e)}")
            QMessageBox.warning(self, "Warning", "Some data could not be loaded. Check the logs for details.")
    
    def init_ui(self):
        """Initialize the user interface."""
        self.setWindowTitle("Logistics Data Processor")
        self.setGeometry(100, 100, 1000, 800)  # Made window bigger
        
        # Create tab widget
        self.tabs = QTabWidget()
        self.setCentralWidget(self.tabs)
        
        # Create tabs in the new logical order
        self.reference_tab = QWidget()
        self.znp_routes_tab = QWidget()
        self.stg_tab = QWidget()
        self.expense_tab = QWidget()
        self.config_tab = QWidget()
        
        # Add tabs in the workflow sequence
        self.tabs.addTab(self.reference_tab, "1. Reference Data")
        self.tabs.addTab(self.znp_routes_tab, "2. ZNP Routes")
        self.tabs.addTab(self.stg_tab, "3. STG Processing")
        self.tabs.addTab(self.expense_tab, "4. Expense Processing")
        self.tabs.addTab(self.config_tab, "5. Configuration")
        
        # Set up each tab
        self.setup_reference_tab()
        self.setup_znp_routes_tab()
        self.setup_stg_tab()
        self.setup_expense_tab()
        self.setup_config_tab()
    
    def setup_stg_tab(self):
        """Set up the STG processing tab to use already processed data."""
        layout = QVBoxLayout()
        
        # Processing section
        process_group = QGroupBox("Route ID Generation")
        process_layout = QVBoxLayout()
        
        # Information label
        info_label = QLabel("This tab uses the STG data already processed in the ZNP Routes tab.\nClick the button below to generate Route IDs from the processed data.")
        info_label.setWordWrap(True)
        process_layout.addWidget(info_label)
        
        self.process_stg_btn = QPushButton("Generate Route IDs")
        self.process_stg_btn.clicked.connect(self.process_route_ids)
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
    
    def process_route_ids(self):
        """Generate Route IDs using the already processed STG data."""
        if self.processed_stg_data is None or not isinstance(self.processed_stg_data, dict) or 'batched_data' not in self.processed_stg_data:
            QMessageBox.warning(self, "No Data", "Please process STG files in the ZNP Routes tab first")
            return
        
        try:
            # Disable the process button while running
            self.process_stg_btn.setEnabled(False)
            self.stg_status.setText("Generating Route IDs...")
            self.stg_output.clear()
            
            # Get the already processed data (which may include wagon type changes)
            batched_data = self.processed_stg_data['batched_data']
            
            # Log the data shape and columns for debugging
            logger.info(f"Processing batched data with shape: {batched_data.shape}")
            logger.info(f"Available columns: {batched_data.columns.tolist()}")
            
            # Create FileProcessor instance
            processor = FileProcessor(self.config)
            
            # First assign batch IDs if not already present
            if 'batch_id' not in batched_data.columns:
                batched_data = processor.assign_batch_ids(batched_data)
                logger.info("Batch IDs assigned to data")
            
            # Map ZNP to batches using the updated data
            final_data = processor.map_znp_to_batches(batched_data)
            
            # Export RouteID data with updated wagon types
            output_path = processor.export_route_id_data(final_data)
            
            # Update the config with the new route_id_path
            self.config["route_id_path"] = output_path
            save_config(self.config)
            
            # Update status
            self.stg_status.setText("Route IDs generated successfully")
            self.stg_status.setStyleSheet("color: green")
            self.stg_output.append(f"Route ID data exported to: {output_path}")
            self.stg_output.append("Note: Any wagon type changes made in ZNP Routes tab have been applied.")
            
            # Update the route_id_edit in the expense tab
            if hasattr(self, 'route_id_edit'):
                self.route_id_edit.setText(output_path)
            
        except Exception as e:
            self.stg_status.setText(f"Error: {str(e)}")
            self.stg_status.setStyleSheet("color: red")
            self.stg_output.append(f"Error: {str(e)}")
            logger.error(f"Error generating route IDs: {str(e)}")
        
        finally:
            # Re-enable the process button
            self.process_stg_btn.setEnabled(True)
    
    def setup_expense_tab(self):
        """Set up the Expense processing tab."""
        layout = QVBoxLayout()
        
        # Input section
        input_group = QGroupBox("Input Data")
        input_layout = QVBoxLayout()
        
        # Information label
        info_label = QLabel("This tab processes expense files using Route IDs generated in the previous step.")
        info_label.setWordWrap(True)
        input_layout.addWidget(info_label)
        
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
        
        # Route ID file selection (auto-populated from previous step)
        route_id_layout = QHBoxLayout()
        route_id_layout.addWidget(QLabel("Route ID File (from previous step):"))
        self.route_id_edit = QLineEdit(self.config.get("route_id_path", ""))
        self.route_id_edit.setReadOnly(True)  # Make read-only since it comes from previous step
        route_id_layout.addWidget(self.route_id_edit)
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
    
    def setup_znp_routes_tab(self):
        """Set up the ZNP Routes management tab."""
        layout = QVBoxLayout()
        
        # Input section
        input_group = QGroupBox("STG Data Source")
        input_layout = QVBoxLayout()
        
        # STG folder selection
        stg_folder_layout = QHBoxLayout()
        stg_folder_layout.addWidget(QLabel("STG Files Folder:"))
        self.znp_stg_folder_edit = QLineEdit(self.config.get("stg_folder", ""))
        stg_folder_layout.addWidget(self.znp_stg_folder_edit)
        stg_browse_btn = QPushButton("Browse...")
        stg_browse_btn.clicked.connect(self.browse_znp_stg_folder)
        stg_folder_layout.addWidget(stg_browse_btn)
        input_layout.addLayout(stg_folder_layout)
        
        # Generate routes button
        generate_btn = QPushButton("Generate Routes from STG Files")
        generate_btn.clicked.connect(self.generate_znp_routes)
        input_layout.addWidget(generate_btn)
        
        input_group.setLayout(input_layout)
        layout.addWidget(input_group)
        
        # Table section
        table_group = QGroupBox("Route Management")
        table_layout = QVBoxLayout()
        
        # Create table
        self.routes_table = QTableView()
        self.routes_model = QStandardItemModel()
        self.routes_model.setHorizontalHeaderLabels([
            "Месяц", "Ст. отправления", "Ст. назначения", "Тип вагона", "Количество", "ЗНП"
        ])
        self.routes_table.setModel(self.routes_model)
        
        # Make ZNP and Wagon Type columns editable, others read-only
        self.routes_table.setEditTriggers(QTableView.DoubleClicked | QTableView.EditKeyPressed)
        
        table_layout.addWidget(self.routes_table)
        
        # Buttons for table management
        button_layout = QHBoxLayout()
        
        save_btn = QPushButton("Save Changes")
        save_btn.clicked.connect(self.save_znp_routes)
        button_layout.addWidget(save_btn)
        
        export_btn = QPushButton("Export to Excel")
        export_btn.clicked.connect(self.export_znp_routes)
        button_layout.addWidget(export_btn)
        
        table_layout.addLayout(button_layout)
        
        table_group.setLayout(table_layout)
        layout.addWidget(table_group)
        
        self.znp_routes_tab.setLayout(layout)
    
    def setup_stg_processing_tab(self):
        """Set up the STG processing tab."""
        layout = QVBoxLayout()
        
        # Create input group
        input_group = QGroupBox("Input Settings")
        input_layout = QVBoxLayout()
        
        # STG folder selection
        folder_layout = QHBoxLayout()
        folder_label = QLabel("STG Folder:")
        self.stg_folder_edit = QLineEdit(self.config.get("stg_folder", ""))
        browse_button = QPushButton("Browse")
        browse_button.clicked.connect(self.browse_stg_folder)
        
        folder_layout.addWidget(folder_label)
        folder_layout.addWidget(self.stg_folder_edit)
        folder_layout.addWidget(browse_button)
        input_layout.addLayout(folder_layout)
        
        # Process button
        process_button = QPushButton("Process STG Files")
        process_button.clicked.connect(self.process_stg_files)
        input_layout.addWidget(process_button)
        
        input_group.setLayout(input_layout)
        layout.addWidget(input_group)
        
        # Progress bar
        self.progress_bar = QProgressBar()
        layout.addWidget(self.progress_bar)
        
        # Results table
        self.results_table = QTableView()
        self.results_model = QStandardItemModel()
        self.results_table.setModel(self.results_model)
        layout.addWidget(self.results_table)
        
        self.stg_processing_tab.setLayout(layout)
    
    def setup_exceptions_tab(self):
        """Set up the exceptions tab."""
        layout = QVBoxLayout()
        
        # Create input group
        input_group = QGroupBox("Exception Management")
        input_layout = QVBoxLayout()
        
        # Import button
        import_button = QPushButton("Import Exceptions")
        import_button.clicked.connect(self.import_exceptions)
        input_layout.addWidget(import_button)
        
        input_group.setLayout(input_layout)
        layout.addWidget(input_group)
        
        # Exceptions table
        self.exceptions_table = QTableView()
        self.exceptions_model = QStandardItemModel()
        self.exceptions_table.setModel(self.exceptions_model)
        layout.addWidget(self.exceptions_table)
        
        self.exceptions_tab.setLayout(layout)
    
    def setup_overrides_tab(self):
        """Set up the overrides tab."""
        layout = QVBoxLayout()
        
        # Create input group
        input_group = QGroupBox("Override Management")
        input_layout = QVBoxLayout()
        
        # Import button
        import_button = QPushButton("Import Overrides")
        import_button.clicked.connect(self.import_overrides)
        input_layout.addWidget(import_button)
        
        input_group.setLayout(input_layout)
        layout.addWidget(input_group)
        
        # Overrides table
        self.overrides_table = QTableView()
        self.overrides_model = QStandardItemModel()
        self.overrides_table.setModel(self.overrides_model)
        layout.addWidget(self.overrides_table)
        
        self.overrides_tab.setLayout(layout)
    
    def setup_matrix_tab(self):
        """Set up the matrix tab."""
        layout = QVBoxLayout()
        
        # Create input group
        input_group = QGroupBox("Matrix Management")
        input_layout = QVBoxLayout()
        
        # Import button
        import_button = QPushButton("Import Matrix")
        import_button.clicked.connect(self.import_matrix)
        input_layout.addWidget(import_button)
        
        input_group.setLayout(input_layout)
        layout.addWidget(input_group)
        
        # Matrix table
        self.matrix_table = QTableView()
        self.matrix_model = QStandardItemModel()
        self.matrix_table.setModel(self.matrix_model)
        layout.addWidget(self.matrix_table)
        
        self.matrix_tab.setLayout(layout)
    
    def setup_logs_tab(self):
        """Set up the logs tab."""
        layout = QVBoxLayout()
        
        # Create log viewer
        self.log_viewer = QTextEdit()
        self.log_viewer.setReadOnly(True)
        layout.addWidget(self.log_viewer)
        
        # Refresh button
        refresh_button = QPushButton("Refresh Logs")
        refresh_button.clicked.connect(self.refresh_logs)
        layout.addWidget(refresh_button)
        
        self.logs_tab.setLayout(layout)
    
    # UI Event Handlers
    def browse_stg_folder(self):
        """Browse for STG folder."""
        folder = QFileDialog.getExistingDirectory(self, "Select STG Folder")
        if folder:
            self.stg_folder_edit.setText(folder)
            self.config["stg_folder"] = folder
            save_config(self.config)
    
    def browse_znp_stg_folder(self):
        """Browse for ZNP STG folder."""
        folder = QFileDialog.getExistingDirectory(self, "Select STG Folder")
        if folder:
            self.znp_stg_folder_edit.setText(folder)
            self.config["stg_folder"] = folder
            save_config(self.config)
    
    def process_stg_files(self):
        """Process STG files."""
        stg_folder = self.stg_folder_edit.text()
        if not stg_folder:
            QMessageBox.warning(self, "Error", "Please select STG folder first.")
            return
        
        self.progress_bar.setValue(0)
        self.progress_bar.setMaximum(100)
        
        try:
            # Process STG files
            self.processed_stg_data = self.process_stg_data()
            
            # Update results table
            self.update_results_table(self.processed_stg_data)
            
            QMessageBox.information(self, "Success", "STG files processed successfully.")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error processing STG files: {str(e)}")
            logger.error(f"Error processing STG files: {str(e)}")
    
    def generate_znp_routes(self):
        """Generate ZNP routes from STG data."""
        stg_folder = self.znp_stg_folder_edit.text()
        if not stg_folder:
            QMessageBox.warning(self, "Error", "Please select STG folder first.")
            return
        
        try:
            # Process STG files and store in the correct format
            stg_data = self.process_stg_data()
            
            # Store the processed data in the expected dictionary format
            self.processed_stg_data = {
                'batched_data': stg_data,
                'processed_at': datetime.now().isoformat()
            }
            
            # Generate routes
            routes = self.generate_routes(stg_data)
            
            # Update routes table
            self.update_routes_table(routes)
            
            QMessageBox.information(self, "Success", "Routes generated successfully.")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error generating routes: {str(e)}")
            logger.error(f"Error generating routes: {str(e)}")
            self.processed_stg_data = None  # Clear the data on error
    
    def save_znp_routes(self):
        """Save ZNP routes to database and Excel."""
        # Get data from model
        routes_data = []
        for row in range(self.routes_model.rowCount()):
            try:
                month = int(self.routes_model.item(row, 0).text())
                count_text = self.routes_model.item(row, 4).text()
                count = int(float(count_text)) if count_text.strip() else 0
                
                route = {
                    "Месяц": month,
                    "Ст. отправления": self.routes_model.item(row, 1).text(),
                    "Ст. назначения": self.routes_model.item(row, 2).text(),
                    "Тип вагона": self.routes_model.item(row, 3).text(),
                    "Количество": count,
                    "ЗНП": self.routes_model.item(row, 5).text()
                }
                routes_data.append(route)
            except Exception as e:
                logger.warning(f"Skipping invalid row {row}: {str(e)}")
                continue
        
        if not routes_data:
            QMessageBox.warning(self, "Error", "No valid route data to save")
            return
        
        # Save to database
        df = pd.DataFrame(routes_data)
        add_znp_data(df)
        
        # Export to Excel
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(self.config.get("output_directory", ""), f"znp_routes_{timestamp}.xlsx")
        df.to_excel(output_path, index=False)
        
        QMessageBox.information(self, "Success", f"Routes saved successfully.\nExported to: {output_path}")
    
    def export_znp_routes(self):
        """Export ZNP routes to Excel."""
        # Get data from model
        routes_data = []
        for row in range(self.routes_model.rowCount()):
            try:
                route = {
                    "Месяц": int(self.routes_model.item(row, 0).text()),
                    "Ст. отправления": self.routes_model.item(row, 1).text(),
                    "Ст. назначения": self.routes_model.item(row, 2).text(),
                    "Тип вагона": self.routes_model.item(row, 3).text(),
                    "Количество": int(float(self.routes_model.item(row, 4).text())),
                    "ЗНП": self.routes_model.item(row, 5).text()
                }
                routes_data.append(route)
            except Exception as e:
                logger.warning(f"Skipping invalid row {row}: {str(e)}")
                continue
        
        if not routes_data:
            QMessageBox.warning(self, "Error", "No valid route data to export")
            return
        
        # Export to Excel
        df = pd.DataFrame(routes_data)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = os.path.join(self.config.get("output_directory", ""), f"znp_routes_{timestamp}.xlsx")
        df.to_excel(output_path, index=False)
        
        QMessageBox.information(self, "Success", f"Routes exported successfully.\nPath: {output_path}")
    
    def import_exceptions(self):
        """Import exceptions from Excel."""
        try:
            file_path, _ = QFileDialog.getOpenFileName(self, "Select Exceptions File", "", "Excel Files (*.xlsx)")
            if not file_path:
                return
            
            # Read Excel file
            df = pd.read_excel(file_path)
            
            # Add to database
            add_exceptions(df)
            
            # Update exceptions table
            self.update_exceptions_table()
            
            QMessageBox.information(self, "Success", "Exceptions imported successfully.")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error importing exceptions: {str(e)}")
            logger.error(f"Error importing exceptions: {str(e)}")
    
    def import_overrides(self):
        """Import overrides from Excel."""
        try:
            file_path, _ = QFileDialog.getOpenFileName(self, "Select Overrides File", "", "Excel Files (*.xlsx)")
            if not file_path:
                return
            
            # Read Excel file
            df = pd.read_excel(file_path)
            
            # Add to database
            add_overrides(df)
            
            # Update overrides table
            self.update_overrides_table()
            
            QMessageBox.information(self, "Success", "Overrides imported successfully.")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error importing overrides: {str(e)}")
            logger.error(f"Error importing overrides: {str(e)}")
    
    def import_matrix(self):
        """Import matrix from Excel."""
        try:
            file_path, _ = QFileDialog.getOpenFileName(self, "Select Matrix File", "", "Excel Files (*.xlsx)")
            if not file_path:
                return
            
            # Read Excel file
            df = pd.read_excel(file_path)
            
            # Add to database
            add_matrix_mappings(df)
            
            # Update matrix table
            self.update_matrix_table()
            
            QMessageBox.information(self, "Success", "Matrix imported successfully.")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error importing matrix: {str(e)}")
            logger.error(f"Error importing matrix: {str(e)}")
    
    def refresh_logs(self):
        """Refresh the logs display."""
        try:
            # Get logs from database
            logs = get_processing_logs()
            
            # Update log viewer
            self.log_viewer.clear()
            for log in logs:
                self.log_viewer.append(f"{log['timestamp']} - {log['operation']} - {log['status']} - {log['message']}")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Error refreshing logs: {str(e)}")
            logger.error(f"Error refreshing logs: {str(e)}")
    
    # Helper Methods
    def update_results_table(self, df: pd.DataFrame):
        """Update the results table with processed data."""
        self.results_model.clear()
        
        if df is None or df.empty:
            return
        
        # Set headers
        headers = list(df.columns)
        self.results_model.setHorizontalHeaderLabels(headers)
        
        # Add data
        for _, row in df.iterrows():
            items = [QStandardItem(str(val)) for val in row]
            self.results_model.appendRow(items)
        
        # Resize columns to content
        self.results_table.resizeColumnsToContents()
    
    def update_routes_table(self, df: pd.DataFrame):
        """Update the routes table with generated routes."""
        self.routes_model.clear()
        
        if df is None or df.empty:
            return
        
        # Set headers
        headers = ["Месяц", "Ст. отправления", "Ст. назначения", "Тип вагона", "Количество", "ЗНП"]
        self.routes_model.setHorizontalHeaderLabels(headers)
        
        # Add data
        for _, row in df.iterrows():
            items = []
            for col in headers:
                try:
                    val = row.get(col, "")
                    # Handle special cases
                    if col == "Количество" and pd.isna(val):
                        val = 0
                    elif pd.isna(val):
                        val = ""
                    elif col == "Месяц":
                        val = int(val)
                    items.append(QStandardItem(str(val)))
                except Exception as e:
                    logger.warning(f"Error processing column {col}: {str(e)}")
                    items.append(QStandardItem(""))
            self.routes_model.appendRow(items)
        
        # Make only ZNP and wagon type columns editable
        for row in range(self.routes_model.rowCount()):
            for col in range(self.routes_model.columnCount()):
                item = self.routes_model.item(row, col)
                if item:
                    if col not in [3, 5]:  # 3 is Тип вагона, 5 is ЗНП
                        item.setEditable(False)
                    else:
                        item.setEditable(True)
        
        # Resize columns to content
        self.routes_table.resizeColumnsToContents()
    
    def update_exceptions_table(self):
        """Update the exceptions table."""
        self.exceptions_model.clear()
        
        # Get exceptions from database
        df = get_exceptions()
        
        if df.empty:
            return
        
        # Set headers
        headers = list(df.columns)
        self.exceptions_model.setHorizontalHeaderLabels(headers)
        
        # Add data
        for _, row in df.iterrows():
            items = [QStandardItem(str(val)) for val in row]
            self.exceptions_model.appendRow(items)
        
        # Resize columns to content
        self.exceptions_table.resizeColumnsToContents()
    
    def update_overrides_table(self):
        """Update the overrides table."""
        try:
            if not hasattr(self, 'overrides_model') or not hasattr(self, 'overrides_table'):
                return
                
            self.overrides_model.clear()
            
            # Get overrides from database
            df = get_overrides()
            
            if df.empty:
                return
            
            # Set headers
            headers = list(df.columns)
            self.overrides_model.setHorizontalHeaderLabels(headers)
            
            # Add data
            for _, row in df.iterrows():
                try:
                    items = [QStandardItem(str(val)) for val in row]
                    self.overrides_model.appendRow(items)
                except Exception as e:
                    logger.warning(f"Error processing override row: {str(e)}")
                    continue
            
            # Resize columns to content
            self.overrides_table.resizeColumnsToContents()
        except Exception as e:
            logger.error(f"Error updating overrides table: {str(e)}")
    
    def update_matrix_table(self):
        """Update the matrix table."""
        try:
            if not hasattr(self, 'matrix_model') or not hasattr(self, 'matrix_table'):
                return
                
            self.matrix_model.clear()
            
            # Get matrix from database
            df = get_matrix_mappings()
            
            if df.empty:
                return
            
            # Set headers
            headers = list(df.columns)
            self.matrix_model.setHorizontalHeaderLabels(headers)
            
            # Add data
            for _, row in df.iterrows():
                try:
                    items = [QStandardItem(str(val)) for val in row]
                    self.matrix_model.appendRow(items)
                except Exception as e:
                    logger.warning(f"Error processing matrix row: {str(e)}")
                    continue
            
            # Resize columns to content
            self.matrix_table.resizeColumnsToContents()
        except Exception as e:
            logger.error(f"Error updating matrix table: {str(e)}")

    def import_reference_data(self):
        """Import all reference data files."""
        try:
            # Initialize progress bar
            self.ref_progress.setValue(0)
            self.ref_status.setText("Starting import...")
            self.ref_output.clear()
            
            # Get file paths from text fields
            files = {
                "znp": self.znp_edit.text(),
                "exceptions": self.exceptions_edit.text(),
                "overrides": self.overrides_edit.text(),
                "active": self.active_edit.text(),
                "matrix": self.matrix_edit.text()
            }
            
            # Validate files exist
            missing_files = [k for k, v in files.items() if v and not os.path.exists(v)]
            if missing_files:
                raise ValueError(f"Following files not found: {', '.join(missing_files)}")
            
            total_files = len([f for f in files.values() if f])
            if total_files == 0:
                raise ValueError("No files selected for import")
            
            processed = 0
            results = {}
            
            # Process each file
            for file_type, file_path in files.items():
                if not file_path:
                    continue
                    
                try:
                    self.ref_status.setText(f"Importing {file_type}...")
                    progress = int((processed / total_files) * 100)
                    self.ref_progress.setValue(progress)
                    
                    if file_type == "znp":
                        df = pd.read_excel(file_path)
                        count = add_znp_data(df)
                        self.update_routes_table(df)
                        results[file_type] = count
                    elif file_type == "exceptions":
                        df = pd.read_excel(file_path)
                        count = add_exceptions(df)
                        self.update_exceptions_table()
                        results[file_type] = count
                    elif file_type == "overrides":
                        df = pd.read_excel(file_path)
                        count = add_overrides(df)
                        self.update_overrides_table()
                        results[file_type] = count
                    elif file_type == "active":
                        df = pd.read_csv(file_path)
                        routes = df.iloc[:, 0].astype(str).tolist()
                        count = add_active_routes(routes)
                        results[file_type] = count
                    elif file_type == "matrix":
                        df = pd.read_csv(file_path)
                        count = add_matrix_mappings(df)
                        self.update_matrix_table()
                        results[file_type] = count
                    
                    processed += 1
                    
                except Exception as e:
                    results[file_type] = f"Error: {str(e)}"
                    logger.error(f"Error importing {file_type}: {str(e)}")
            
            # Update progress and status
            self.ref_progress.setValue(100)
            self.ref_status.setText("Import complete")
            
            # Show results
            self.ref_output.clear()
            for file_type, result in results.items():
                self.ref_output.append(f"{file_type.upper()}: {result}")
            
            # Show success message if any files were processed successfully
            if any(isinstance(v, int) for v in results.values()):
                QMessageBox.information(self, "Success", "Reference data imported successfully.")
            else:
                QMessageBox.warning(self, "Warning", "Some or all imports failed. Check the results for details.")
                
        except Exception as e:
            self.ref_status.setText(f"Error: {str(e)}")
            self.ref_progress.setValue(0)
            QMessageBox.critical(self, "Error", f"Error importing reference data: {str(e)}")
            logger.error(f"Error importing reference data: {str(e)}")

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
            os.makedirs(self.config["base_directory"], exist_ok=True)
            os.makedirs(self.config["output_directory"], exist_ok=True)
            
        except Exception as e:
            self.config_status.setText(f"Error saving configuration: {str(e)}")
            logger.error(f"Error saving configuration: {str(e)}")

    def process_stg_data(self) -> pd.DataFrame:
        """Process STG data from Excel files."""
        try:
            # Get list of Excel files in STG folder
            stg_folder = self.config.get("stg_folder")
            if not stg_folder or not os.path.exists(stg_folder):
                raise ValueError(f"STG folder not found: {stg_folder}")
            
            excel_files = glob.glob(os.path.join(stg_folder, "*.xlsx"))
            if not excel_files:
                raise ValueError(f"No Excel files found in {stg_folder}")
            
            # Define column mapping (Russian to English)
            column_mapping = {
                'Вагон №': 'wagon_number',
                'Накладная №': 'invoice_number',
                'Ст. отправления': 'departure_station',
                'Ст. назначения': 'destination_station',
                'Прибытие на ст. отправл.': 'departure_arrival',
                'Отчетная дата': 'report_date',
                'Прибытие на ст. назн.': 'destination_arrival',
                'Груж\\пор': 'load_status',
                'Тип вагона': 'wagon_type',
                'Расстояние': 'distance',
                'Собственник': 'owner',
                'Грузоотправитель': 'shipper',
                'Грузополучатель': 'consignee',
                'Простой в ожидании ремонта': 'repair_wait_time'
            }
            
            # Read and combine all Excel files
            all_data = []
            for file in excel_files:
                try:
                    df = pd.read_excel(file)
                    # Strip whitespace from column names
                    df.columns = df.columns.str.strip()
                    all_data.append(df)
                except Exception as e:
                    logger.warning(f"Error reading file {file}: {str(e)}")
                    continue
            
            if not all_data:
                raise ValueError("No valid data found in Excel files")
            
            # Combine all DataFrames
            stg_data = pd.concat(all_data, ignore_index=True)
            
            # Create empty columns for any missing fields
            for rus_col, eng_col in column_mapping.items():
                if rus_col not in stg_data.columns:
                    stg_data[rus_col] = ""
            
            # Rename columns using the mapping
            stg_data = stg_data.rename(columns=column_mapping)
            
            # Handle string columns first
            string_columns = ['invoice_number', 'departure_station', 'destination_station', 
                            'load_status', 'wagon_type', 'owner', 'shipper', 'consignee']
            for col in string_columns:
                if col in stg_data.columns:
                    stg_data[col] = stg_data[col].fillna('').astype(str)
            
            # Handle numeric columns
            numeric_columns = {
                'wagon_number': 0,
                'distance': 0,
                'repair_wait_time': 0
            }
            for col, default_value in numeric_columns.items():
                if col in stg_data.columns:
                    try:
                        stg_data[col] = pd.to_numeric(stg_data[col], errors='coerce').fillna(default_value).astype(float)
                    except Exception as e:
                        logger.warning(f"Error converting column {col} to numeric: {str(e)}")
            
            # Handle datetime columns last
            datetime_columns = ['report_date', 'departure_arrival', 'destination_arrival']
            for col in datetime_columns:
                if col in stg_data.columns:
                    try:
                        stg_data[col] = pd.to_datetime(stg_data[col], errors='coerce')
                    except Exception as e:
                        logger.warning(f"Error converting column {col} to datetime: {str(e)}")
            
            # Extract month from report_date
            try:
                stg_data['month'] = pd.to_datetime(stg_data['report_date']).dt.month
            except Exception as e:
                logger.warning(f"Error extracting month from report_date: {str(e)}")
                stg_data['month'] = 0
            
            return stg_data
            
        except Exception as e:
            logger.error(f"Error processing STG data: {str(e)}")
            raise

    def generate_routes(self, stg_data: pd.DataFrame) -> pd.DataFrame:
        """Generate route suggestions from STG data."""
        if stg_data is None or stg_data.empty:
            raise ValueError("No STG data available for route generation")
        
        try:
            # Reset index to handle potential duplicate labels
            stg_data = stg_data.reset_index(drop=True)
            
            # Filter for loaded wagons only
            stg_data = stg_data[stg_data['load_status'] == 'ГРУЖ'].copy()
            
            # Ensure required columns exist
            required_columns = ["month", "departure_station", "destination_station", "wagon_type"]
            missing_columns = [col for col in required_columns if col not in stg_data.columns]
            if missing_columns:
                raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
            
            # Group by month, departure station, destination station, and wagon type
            route_data = stg_data.groupby([
                "month",
                "departure_station",
                "destination_station",
                "wagon_type"
            ], dropna=False).size().reset_index(name="Количество")
            
            # Rename columns to match expected format
            route_data = route_data.rename(columns={
                "month": "Месяц",
                "departure_station": "Ст. отправления",
                "destination_station": "Ст. назначения",
                "wagon_type": "Тип вагона"
            })
            
            # Get existing ZNP data
            znp_data = get_znp_data()
            
            # Create a dictionary for quick lookup of existing ZNP values
            znp_lookup = {}
            if not znp_data.empty:
                for _, row in znp_data.iterrows():
                    try:
                        key = (
                            int(row["Месяц"]),
                            str(row["Ст. отправления"]).strip(),
                            str(row["Ст. назначения"]).strip(),
                            str(row["Тип вагона"]).strip() if pd.notna(row["Тип вагона"]) else ""
                        )
                        znp_lookup[key] = str(row["ЗНП"])
                    except Exception as e:
                        logger.warning(f"Skipping invalid ZNP row: {str(e)}")
                        continue
            
            # Add ZNP values from lookup
            znp_values = []
            for _, row in route_data.iterrows():
                try:
                    key = (
                        int(row["Месяц"]),
                        str(row["Ст. отправления"]).strip(),
                        str(row["Ст. назначения"]).strip(),
                        str(row["Тип вагона"]).strip() if pd.notna(row["Тип вагона"]) else ""
                    )
                    znp_values.append(znp_lookup.get(key, ""))
                except Exception as e:
                    logger.warning(f"Error processing route row: {str(e)}")
                    znp_values.append("")
            
            route_data["ЗНП"] = znp_values
            
            # Sort by month, station names
            route_data = route_data.sort_values(["Месяц", "Ст. отправления", "Ст. назначения"])
            
            # Ensure all columns are present and in the correct order
            expected_columns = ["Месяц", "Ст. отправления", "Ст. назначения", "Тип вагона", "Количество", "ЗНП"]
            route_data = route_data.reindex(columns=expected_columns)
            
            return route_data
            
        except Exception as e:
            logger.error(f"Error generating routes: {str(e)}")
            raise

class ProcessingWorker(QThread):
    """Worker thread for processing operations."""
    progress_update = pyqtSignal(int, str)
    process_complete = pyqtSignal(bool, str, dict)
    
    def __init__(self, operation, params=None):
        super().__init__()
        self.operation = operation
        self.params = params or {}
    
    def run(self):
        """Run the worker thread."""
        if self.operation == "process_expenses":
            self.process_expenses()
        elif self.operation == "import_reference":
            self.import_reference()
    
    def process_expenses(self):
        """Process expense files."""
        try:
            expense_folder = self.params.get("expense_folder")
            route_id_path = self.params.get("route_id_path")
            config = load_config()  # Load the configuration
            
            # Process expense files
            processor = ExpenseProcessor(config)  # Pass the config to ExpenseProcessor
            result = processor.process_expense_folder(expense_folder, route_id_path)
            
            self.progress_update.emit(100, "Processing complete")
            self.process_complete.emit(True, "Successfully processed expense files", result)
        except Exception as e:
            logger.error(f"Error processing expenses: {str(e)}")
            self.process_complete.emit(False, str(e), {})
    
    def import_reference(self):
        """Import reference data."""
        try:
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
                        df = pd.read_excel(file_path)
                        count = add_znp_data(df)
                        results[file_type] = count
                    elif file_type == "exceptions":
                        df = pd.read_excel(file_path)
                        count = add_exceptions(df)
                        results[file_type] = count
                    elif file_type == "overrides":
                        df = pd.read_excel(file_path)
                        count = add_overrides(df)
                        results[file_type] = count
                    elif file_type == "active":
                        df = pd.read_csv(file_path)
                        routes = df.iloc[:, 0].astype(str).tolist()
                        count = add_active_routes(routes)
                        results[file_type] = count
                    elif file_type == "matrix":
                        df = pd.read_csv(file_path)
                        count = add_matrix_mappings(df)
                        results[file_type] = count
                except Exception as e:
                    logger.error(f"Error importing {file_type}: {str(e)}")
                    results[file_type] = f"Error: {str(e)}"
            
            self.progress_update.emit(100, "Import complete")
            
            if any(isinstance(v, int) for v in results.values()):
                self.process_complete.emit(True, "Successfully imported reference data", results)
            else:
                self.process_complete.emit(False, "Errors occurred during import", results)
        except Exception as e:
            logger.error(f"Error importing reference data: {str(e)}")
            self.process_complete.emit(False, str(e), {})
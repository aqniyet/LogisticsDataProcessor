import sys
import os
import logging
import pandas as pd
from PyQt5.QtWidgets import (QApplication, QMainWindow, QTabWidget, QWidget,
                          QVBoxLayout, QPushButton, QLabel, QTableWidget,
                          QMessageBox, QFileDialog, QHBoxLayout, QLineEdit,
                          QTableWidgetItem)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LogisticsManager(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Logistics Data Manager")
        self.setGeometry(100, 100, 800, 600)
        
        # Initialize data directories
        self.data_dir = "data"
        self.active_file = os.path.join(self.data_dir, "active.csv")
        self.matrix_file = os.path.join(self.data_dir, "matrix.csv")
        os.makedirs(self.data_dir, exist_ok=True)
        
        # Create main widget and layout
        main_widget = QWidget()
        self.setCentralWidget(main_widget)
        layout = QVBoxLayout()
        main_widget.setLayout(layout)
        
        # Create tab widget
        self.tabs = QTabWidget()
        layout.addWidget(self.tabs)
        
        # Add tabs
        self.setup_active_routes_tab()
        self.setup_matrix_tab()
        
        self.show()
    
    def setup_active_routes_tab(self):
        """Setup the Active Routes management tab."""
        tab = QWidget()
        layout = QVBoxLayout()
        tab.setLayout(layout)
        
        # Add route section
        add_route_layout = QHBoxLayout()
        self.route_id_edit = QLineEdit()
        self.route_id_edit.setPlaceholderText("Enter Route ID")
        add_route_btn = QPushButton("Add Route")
        add_route_btn.clicked.connect(self.add_active_route)
        
        add_route_layout.addWidget(self.route_id_edit)
        add_route_layout.addWidget(add_route_btn)
        layout.addLayout(add_route_layout)
        
        # Routes table
        self.routes_table = QTableWidget()
        self.routes_table.setColumnCount(2)
        self.routes_table.setHorizontalHeaderLabels(["Route ID", "Actions"])
        layout.addWidget(self.routes_table)
        
        # Import/Export buttons
        btn_layout = QHBoxLayout()
        import_btn = QPushButton("Import from CSV")
        export_btn = QPushButton("Export to CSV")
        import_btn.clicked.connect(self.import_active_routes)
        export_btn.clicked.connect(self.export_active_routes)
        
        btn_layout.addWidget(import_btn)
        btn_layout.addWidget(export_btn)
        layout.addLayout(btn_layout)
        
        self.tabs.addTab(tab, "Active Routes")
        self.refresh_routes_table()
    
    def setup_matrix_tab(self):
        """Setup the Matrix mappings management tab."""
        tab = QWidget()
        layout = QVBoxLayout()
        tab.setLayout(layout)
        
        # Add mapping section
        add_mapping_layout = QHBoxLayout()
        self.source_edit = QLineEdit()
        self.source_edit.setPlaceholderText("Source Value")
        self.target_edit = QLineEdit()
        self.target_edit.setPlaceholderText("Target Value")
        add_mapping_btn = QPushButton("Add Mapping")
        add_mapping_btn.clicked.connect(self.add_matrix_mapping)
        
        add_mapping_layout.addWidget(self.source_edit)
        add_mapping_layout.addWidget(self.target_edit)
        add_mapping_layout.addWidget(add_mapping_btn)
        layout.addLayout(add_mapping_layout)
        
        # Matrix table
        self.matrix_table = QTableWidget()
        self.matrix_table.setColumnCount(3)
        self.matrix_table.setHorizontalHeaderLabels(["Source", "Target", "Actions"])
        layout.addWidget(self.matrix_table)
        
        # Import/Export buttons
        btn_layout = QHBoxLayout()
        import_btn = QPushButton("Import from CSV")
        export_btn = QPushButton("Export to CSV")
        import_btn.clicked.connect(self.import_matrix)
        export_btn.clicked.connect(self.export_matrix)
        
        btn_layout.addWidget(import_btn)
        btn_layout.addWidget(export_btn)
        layout.addLayout(btn_layout)
        
        self.tabs.addTab(tab, "Matrix Mappings")
        self.refresh_matrix_table()
    
    def add_active_route(self):
        """Add a new active route."""
        route_id = self.route_id_edit.text().strip()
        if not route_id:
            QMessageBox.warning(self, "Error", "Please enter a Route ID")
            return
        
        try:
            if os.path.exists(self.active_file):
                df = pd.read_csv(self.active_file)
            else:
                df = pd.DataFrame(columns=["route_id"])
            
            # Check if route already exists
            if route_id in df["route_id"].astype(str).values:
                QMessageBox.warning(self, "Error", "Route ID already exists")
                return
            
            # Add new route
            df = pd.concat([df, pd.DataFrame([{"route_id": route_id}])], ignore_index=True)
            df.to_csv(self.active_file, index=False)
            
            self.route_id_edit.clear()
            self.refresh_routes_table()
            QMessageBox.information(self, "Success", "Route added successfully")
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to add route: {str(e)}")
    
    def add_matrix_mapping(self):
        """Add a new matrix mapping."""
        source = self.source_edit.text().strip()
        target = self.target_edit.text().strip()
        
        if not source or not target:
            QMessageBox.warning(self, "Error", "Please enter both source and target values")
            return
        
        try:
            if os.path.exists(self.matrix_file):
                df = pd.read_csv(self.matrix_file)
            else:
                df = pd.DataFrame(columns=["source_value", "target_value"])
            
            # Check if mapping already exists
            existing = df[
                (df["source_value"].astype(str) == source) & 
                (df["target_value"].astype(str) == target)
            ]
            if not existing.empty:
                QMessageBox.warning(self, "Error", "This mapping already exists")
                return
            
            # Add new mapping
            df = pd.concat([
                df, 
                pd.DataFrame([{"source_value": source, "target_value": target}])
            ], ignore_index=True)
            df.to_csv(self.matrix_file, index=False)
            
            self.source_edit.clear()
            self.target_edit.clear()
            self.refresh_matrix_table()
            QMessageBox.information(self, "Success", "Mapping added successfully")
            
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to add mapping: {str(e)}")
    
    def delete_route(self, route_id):
        """Delete an active route."""
        try:
            if os.path.exists(self.active_file):
                df = pd.read_csv(self.active_file)
                df = df[df["route_id"].astype(str) != str(route_id)]
                df.to_csv(self.active_file, index=False)
                self.refresh_routes_table()
                QMessageBox.information(self, "Success", "Route deleted successfully")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to delete route: {str(e)}")
    
    def delete_mapping(self, source, target):
        """Delete a matrix mapping."""
        try:
            if os.path.exists(self.matrix_file):
                df = pd.read_csv(self.matrix_file)
                df = df[
                    ~((df["source_value"].astype(str) == str(source)) & 
                      (df["target_value"].astype(str) == str(target)))
                ]
                df.to_csv(self.matrix_file, index=False)
                self.refresh_matrix_table()
                QMessageBox.information(self, "Success", "Mapping deleted successfully")
        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to delete mapping: {str(e)}")
    
    def refresh_routes_table(self):
        """Refresh the active routes table."""
        self.routes_table.setRowCount(0)
        
        if os.path.exists(self.active_file):
            try:
                df = pd.read_csv(self.active_file)
                for i, row in df.iterrows():
                    route_id = str(row["route_id"])
                    self.routes_table.insertRow(i)
                    self.routes_table.setItem(i, 0, QTableWidgetItem(route_id))
                    
                    delete_btn = QPushButton("Delete")
                    delete_btn.clicked.connect(
                        lambda checked, r=route_id: self.delete_route(r)
                    )
                    self.routes_table.setCellWidget(i, 1, delete_btn)
            except Exception as e:
                logger.error(f"Error loading active routes: {str(e)}")
        
        self.routes_table.resizeColumnsToContents()
    
    def refresh_matrix_table(self):
        """Refresh the matrix mappings table."""
        self.matrix_table.setRowCount(0)
        
        if os.path.exists(self.matrix_file):
            try:
                df = pd.read_csv(self.matrix_file)
                for i, row in df.iterrows():
                    source = str(row["source_value"])
                    target = str(row["target_value"])
                    
                    self.matrix_table.insertRow(i)
                    self.matrix_table.setItem(i, 0, QTableWidgetItem(source))
                    self.matrix_table.setItem(i, 1, QTableWidgetItem(target))
                    
                    delete_btn = QPushButton("Delete")
                    delete_btn.clicked.connect(
                        lambda checked, s=source, t=target: self.delete_mapping(s, t)
                    )
                    self.matrix_table.setCellWidget(i, 2, delete_btn)
            except Exception as e:
                logger.error(f"Error loading matrix mappings: {str(e)}")
        
        self.matrix_table.resizeColumnsToContents()
    
    def import_active_routes(self):
        """Import active routes from a CSV file."""
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Select Active Routes CSV", "", "CSV Files (*.csv)"
        )
        if file_path:
            try:
                df = pd.read_csv(file_path)
                df.to_csv(self.active_file, index=False)
                self.refresh_routes_table()
                QMessageBox.information(self, "Success", "Routes imported successfully")
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Failed to import routes: {str(e)}")
    
    def export_active_routes(self):
        """Export active routes to a CSV file."""
        file_path, _ = QFileDialog.getSaveFileName(
            self, "Save Active Routes CSV", "", "CSV Files (*.csv)"
        )
        if file_path:
            try:
                if os.path.exists(self.active_file):
                    df = pd.read_csv(self.active_file)
                    df.to_csv(file_path, index=False)
                    QMessageBox.information(self, "Success", "Routes exported successfully")
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Failed to export routes: {str(e)}")
    
    def import_matrix(self):
        """Import matrix mappings from a CSV file."""
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Select Matrix CSV", "", "CSV Files (*.csv)"
        )
        if file_path:
            try:
                df = pd.read_csv(file_path)
                df.to_csv(self.matrix_file, index=False)
                self.refresh_matrix_table()
                QMessageBox.information(self, "Success", "Matrix imported successfully")
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Failed to import matrix: {str(e)}")
    
    def export_matrix(self):
        """Export matrix mappings to a CSV file."""
        file_path, _ = QFileDialog.getSaveFileName(
            self, "Save Matrix CSV", "", "CSV Files (*.csv)"
        )
        if file_path:
            try:
                if os.path.exists(self.matrix_file):
                    df = pd.read_csv(self.matrix_file)
                    df.to_csv(file_path, index=False)
                    QMessageBox.information(self, "Success", "Matrix exported successfully")
            except Exception as e:
                QMessageBox.critical(self, "Error", f"Failed to export matrix: {str(e)}")

if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = LogisticsManager()
    sys.exit(app.exec_()) 
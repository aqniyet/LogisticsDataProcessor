import pandas as pd
import os
from datetime import datetime
import logging
from typing import List, Dict, Tuple, Optional

from app.database.operations import get_znp_data, get_exceptions, get_overrides
from app.utils.file_utils import get_files_by_pattern, ensure_directory_exists
from app.utils.data_utils import standardize_column_types

logger = logging.getLogger(__name__)

class FileProcessor:
    """
    Replaces the Power BI logic for processing STG files and generating route IDs.
    """
    
    def __init__(self, config):
        """Initialize the FileProcessor with configuration settings."""
        self.config = config
        self.output_dir = config.get('output_directory', './output')
        ensure_directory_exists(self.output_dir)
        
    def process_daily_files(self, folder_path: str) -> pd.DataFrame:
        """
        Process all STG daily files from a folder.
        Replaces the first part of your Power BI M-code.
        """
        # Get all STG files
        stg_files = get_files_by_pattern(folder_path, "STGDaily_*.xlsx")
        
        if not stg_files:
            logger.warning(f"No STG daily files found in {folder_path}")
            return pd.DataFrame()
        
        # Load and combine data from all files
        combined_data = pd.DataFrame()
        
        for file_path in stg_files:
            try:
                logger.info(f"Processing file: {file_path}")
                df = pd.read_excel(file_path)
                
                # Ensure column headers are standardized
                df.columns = [col.strip() for col in df.columns]
                
                # Apply column type standardization
                column_types = {
                    "Вагон №": 'int64',
                    "Накладная №": 'str',
                    "Ст. отправления": 'str',
                    "Ст. назначения": 'str',
                    "Прибытие на ст. отправл.": 'datetime64',
                    "Отчетная дата": 'datetime64',
                    "Прибытие на ст. назн.": 'datetime64',
                    "Груж\\пор": 'str',
                    "Тип вагона": 'str',
                    "Расстояние": 'int64',
                    "Собственник": 'str',
                    "Грузоотправитель": 'str',
                    "Грузополучатель": 'str',
                    "Простой в ожидании ремонта": 'float'
                }
                
                df = standardize_column_types(df, column_types)
                
                # Append to combined data
                combined_data = pd.concat([combined_data, df], ignore_index=True)
                
            except Exception as e:
                logger.error(f"Error processing file {file_path}: {str(e)}")
        
        return combined_data
    
    def merge_with_existing_data(self, daily_data: pd.DataFrame, existing_data_path: str) -> pd.DataFrame:
        """
        Merge new daily data with existing historical data.
        """
        try:
            # Load existing data
            existing_data = pd.read_excel(existing_data_path)
            
            # Apply column type standardization to existing data
            column_types = {
                "Вагон №": 'int64',
                "Накладная №": 'str',
                "Ст. отправления": 'str',
                "Ст. назначения": 'str',
                "Прибытие на ст. отправл.": 'datetime64',
                "Отчетная дата": 'datetime64',
                "Прибытие на ст. назн.": 'datetime64',
                "Груж\\пор": 'str',
                "Тип вагона": 'str'
            }
            
            existing_data = standardize_column_types(existing_data, column_types)
            
            # Combine existing and daily data
            combined_data = pd.concat([existing_data, daily_data], ignore_index=True)
            
            # Remove null values in key columns
            combined_data = combined_data.dropna(subset=["Вагон №", "Накладная №", "Груж\\пор", "Отчетная дата"])
            
            # Deduplicate data, keeping latest entries
            combined_data = combined_data.sort_values("Отчетная дата", ascending=True)
            combined_data = combined_data.drop_duplicates(subset=["Вагон №", "Накладная №"], keep='last')
            
            # Add W&N column for easier reference
            combined_data["W&N"] = combined_data["Вагон №"].astype(str) + combined_data["Накладная №"].astype(str)
            
            return combined_data
            
        except Exception as e:
            logger.error(f"Error merging with existing data: {str(e)}")
            return daily_data
    
    def assign_batch_ids(self, data: pd.DataFrame) -> pd.DataFrame:
        """
        Assign batch IDs based on wagon numbers and груж/пор values.
        Replaces the batch ID logic from your second M-code block.
        """
        # Sort data by wagon number and report date
        sorted_data = data.sort_values(by=["wagon_number", "report_date"])
        
        # Initialize variables for batch assignment
        last_batch_id = 0
        last_gruz_batch_id = 0
        last_wagon = None
        result_data = []
        
        # Process each row
        for _, row in sorted_data.iterrows():
            current_wagon = row["wagon_number"]
            gruzh_por_value = row["load_status"]
            
            # Assign batch ID based on logic
            if gruzh_por_value == "ГРУЖ":
                new_batch_id = last_gruz_batch_id + 1
                last_gruz_batch_id = new_batch_id
            elif gruzh_por_value == "ПОР" and last_wagon == current_wagon:
                new_batch_id = last_batch_id
            else:
                new_batch_id = 0
            
            # Add batch ID to row
            row_with_batch = row.copy()
            row_with_batch["batch_id"] = new_batch_id
            result_data.append(row_with_batch)
            
            # Update tracking variables
            last_batch_id = new_batch_id
            last_wagon = current_wagon
        
        # Convert back to DataFrame
        result_df = pd.DataFrame(result_data)
        
        return result_df
    
    def map_znp_to_batches(self, batched_data: pd.DataFrame) -> pd.DataFrame:
        """
        Map ЗНП data to batched records.
        Replaces the ЗНП mapping logic from your third and fourth M-code blocks.
        """
        # Add month column safely
        try:
            if "month" not in batched_data.columns:
                # Ensure report_date is datetime
                if "report_date" not in batched_data.columns:
                    raise ValueError("Required column 'report_date' is missing")
                
                batched_data["report_date"] = pd.to_datetime(batched_data["report_date"], errors='coerce')
                # Extract month, handling NaT values
                batched_data["month"] = batched_data["report_date"].dt.month.fillna(0).astype(int)
                
                # Log the unique months found
                unique_months = batched_data["month"].unique()
                logger.info(f"Extracted months from dates: {unique_months}")
                
                # If all months are 0, there might be an issue with the dates
                if all(month == 0 for month in unique_months):
                    logger.error("All months are 0, indicating possible date conversion issues")
                    raise ValueError("Failed to extract valid months from dates")
            else:
                # If month column exists, ensure it's an integer
                batched_data["month"] = pd.to_numeric(batched_data["month"], errors='coerce').fillna(0).astype(int)
        except Exception as e:
            logger.error(f"Error processing month data: {str(e)}")
            logger.error(f"Available columns: {batched_data.columns.tolist()}")
            if "report_date" in batched_data.columns:
                logger.error(f"Sample of report_date values: {batched_data['report_date'].head()}")
            raise ValueError(f"Failed to process date/month data: {str(e)}")
        
        # Filter for loaded batches (ГРУЖ)
        loaded_batches = batched_data[batched_data["load_status"] == "ГРУЖ"].copy()
        
        # Get ЗНП reference data
        znp_data = get_znp_data()
        
        # Check if we have any ZNP data
        if znp_data.empty:
            logger.error("No ZNP data found in the database. Please import ZNP data first.")
            raise ValueError("No ZNP data found in the database. Please import ZNP data first.")
        
        # Convert ZNP data column names to English
        znp_data = znp_data.rename(columns={
            'Месяц': 'month',
            'Ст. отправления': 'departure_station',
            'Ст. назначения': 'destination_station',
            'Тип вагона': 'wagon_type',
            'ЗНП': 'znp'
        })
        
        # Ensure month column in znp_data is also integer
        znp_data["month"] = pd.to_numeric(znp_data["month"], errors='coerce').fillna(0).astype(int)
        
        # Log the unique months in both dataframes for debugging
        logger.info(f"Months in loaded_batches: {loaded_batches['month'].unique()}")
        logger.info(f"Months in znp_data: {znp_data['month'].unique()}")
        
        # Merge based on month, stations, and wagon type
        merge_columns = ["month", "departure_station", "destination_station", "wagon_type"]
        merged_data = pd.merge(
            loaded_batches,
            znp_data,
            on=merge_columns,
            how="left"
        )
        
        # Get exceptions data
        exceptions_data = get_exceptions()
        
        # Convert exceptions data column names to English
        exceptions_data = exceptions_data.rename(columns={
            'Накладная №': 'invoice_number',
            'ExceptionRouteID': 'exception_route_id'
        })
        
        # Ensure invoice numbers are strings for merging
        if "invoice_number" in merged_data.columns:
            merged_data["invoice_number"] = merged_data["invoice_number"].astype(str)
        if "invoice_number" in exceptions_data.columns:
            exceptions_data["invoice_number"] = exceptions_data["invoice_number"].astype(str)
        
        # Merge exceptions
        exceptions_merged = pd.merge(
            merged_data,
            exceptions_data,
            on="invoice_number",
            how="left"
        )
        
        # Create Final RouteID (Exceptions > ЗНП)
        batch_to_znp = exceptions_merged.groupby("batch_id").agg({
            "exception_route_id": lambda x: next((i for i in x if pd.notna(i)), None),
            "znp": lambda x: next((i for i in x if pd.notna(i)), None)
        }).reset_index()
        
        # Create Final RouteID column
        batch_to_znp["final_route_id"] = batch_to_znp.apply(
            lambda row: row["exception_route_id"] if pd.notna(row["exception_route_id"]) else row["znp"],
            axis=1
        )
        
        # Merge final RouteID back to original data
        final_data = pd.merge(
            batched_data,
            batch_to_znp[["batch_id", "final_route_id"]],
            on="batch_id",
            how="left"
        )
        
        # Apply overrides
        overrides_data = get_overrides()
        
        # Convert overrides data column names to English
        overrides_data = overrides_data.rename(columns={
            'Вагон №': 'wagon_number',
            'Накладная №': 'invoice_number',
            'ЗНП': 'znp'
        })
        
        # Normalize column names for merging
        merge_columns = {
            "wagon_number": lambda df: pd.to_numeric(df["wagon_number"], errors='coerce').fillna(0).astype('int64'),
            "invoice_number": lambda df: df["invoice_number"].astype(str)
        }
        
        # Apply normalization to both dataframes
        for col, normalize_func in merge_columns.items():
            if col in final_data.columns:
                final_data[col] = normalize_func(final_data)
            if col in overrides_data.columns:
                overrides_data[col] = normalize_func(overrides_data)
        
        # Log data types for debugging
        logger.info(f"Final data types: {final_data.dtypes}")
        logger.info(f"Overrides data types: {overrides_data.dtypes}")
        
        # Merge with overrides
        merged_with_overrides = pd.merge(
            final_data,
            overrides_data,
            on=["wagon_number", "invoice_number"],
            how="left",
            suffixes=("", "_override")
        )
        
        # Replace RouteID with Overrides
        merged_with_overrides["updated_final_route_id"] = merged_with_overrides.apply(
            lambda row: row["ЗНП Override"] if pd.notna(row["ЗНП Override"]) else row["final_route_id"],
            axis=1
        )
        
        # Propagate RouteID within batches
        result_data = []
        for batch_id, batch_df in merged_with_overrides.groupby("batch_id"):
            if batch_id == 0:  # Skip unassigned batches
                result_data.append(batch_df)
                continue
                
            # Find the first non-null RouteID in the batch
            valid_route_ids = batch_df["updated_final_route_id"].dropna()
            if not valid_route_ids.empty:
                final_route_id = valid_route_ids.iloc[0]
                batch_df["propagated_final_route_id"] = final_route_id
            else:
                batch_df["propagated_final_route_id"] = None
                
            result_data.append(batch_df)
        
        # Combine all processed batches
        result_df = pd.concat(result_data, ignore_index=True)
        
        # Create W&N code column
        result_df["wn_code"] = result_df["wagon_number"].astype(str) + result_df["invoice_number"].astype(str)
        
        # Clean up and rename
        selected_columns = [
            "month", "propagated_final_route_id", "batch_id", "wagon_number", "invoice_number", 
            "wn_code", "load_status", "departure_station", "destination_station", 
            "departure_arrival", "report_date", "destination_arrival"
        ]
        final_table = result_df[selected_columns].copy()
        
        # Convert back to Russian column names for output
        final_table = final_table.rename(columns={
            'month': 'Месяц',
            'propagated_final_route_id': 'ЗНП',
            'batch_id': 'Batch ID',
            'wagon_number': 'Вагон №',
            'invoice_number': 'Накладная №',
            'wn_code': 'W&N',
            'load_status': 'Груж\\пор',
            'departure_station': 'Ст. отправления',
            'destination_station': 'Ст. назначения',
            'departure_arrival': 'Прибытие на ст. отправл.',
            'report_date': 'Отчетная дата',
            'destination_arrival': 'Прибытие на ст. назн.'
        })
        
        # Filter for records with valid ЗНП
        final_table = final_table.dropna(subset=["ЗНП"])
        
        # Check for duplicate Route IDs more thoroughly
        logger.info("Checking for duplicate Route IDs...")
        
        # First check duplicates by ZNP only
        znp_duplicates = final_table[final_table.duplicated(subset=["ЗНП"], keep=False)]
        if not znp_duplicates.empty:
            logger.warning(f"Found {len(znp_duplicates)} rows where same ZNP is used multiple times")
            logger.warning("Sample of duplicated ZNPs:")
            for znp in znp_duplicates["ЗНП"].unique()[:5]:  # Show first 5 examples
                count = len(znp_duplicates[znp_duplicates["ЗНП"] == znp])
                logger.warning(f"ZNP {znp} appears {count} times")
        
        # Then check complete Route ID duplicates (ZNP + Wagon + Invoice)
        route_id_duplicates = final_table[final_table.duplicated(subset=["ЗНП", "Вагон №", "Накладная №"], keep=False)]
        if not route_id_duplicates.empty:
            logger.warning(f"Found {len(route_id_duplicates)} rows with duplicate complete Route IDs")
            logger.warning("These are cases where same ZNP is used for same wagon and invoice")
            
            # Group duplicates to show examples
            duplicate_groups = route_id_duplicates.groupby(["ЗНП", "Вагон №", "Накладная №"])
            logger.warning("Sample of duplicate groups:")
            for name, group in list(duplicate_groups)[:3]:  # Show first 3 examples
                znp, wagon, invoice = name
                logger.warning(f"ZNP: {znp}, Wagon: {wagon}, Invoice: {invoice} appears {len(group)} times")
                logger.warning(f"Dates: {group['Отчетная дата'].tolist()}")
            
            # Remove duplicates, keeping the latest entry
            logger.warning("Removing duplicates, keeping the latest entry based on report date")
            final_table = final_table.sort_values("Отчетная дата", ascending=False)
            final_table = final_table.drop_duplicates(subset=["ЗНП", "Вагон №", "Накладная №"], keep='first')
            
            # Verify no duplicates remain
            remaining_duplicates = final_table[final_table.duplicated(subset=["ЗНП", "Вагон №", "Накладная №"], keep=False)]
            if remaining_duplicates.empty:
                logger.info("Successfully removed all duplicates")
            else:
                logger.error(f"Still found {len(remaining_duplicates)} duplicate rows after cleanup!")
        
        # Add custom route description
        final_table["Custom"] = final_table["Ст. отправления"] + " - " + final_table["Ст. назначения"]
        
        # Log final statistics
        logger.info(f"Final table contains {len(final_table)} unique Route IDs")
        logger.info(f"Number of unique ZNPs: {final_table['ЗНП'].nunique()}")
        
        return final_table
    
    def export_route_id_data(self, final_data: pd.DataFrame, output_path: Optional[str] = None) -> str:
        """
        Export the final route ID data to a CSV file.
        """
        if output_path is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = os.path.join(self.output_dir, f"Route_ID_{timestamp}.csv")
        
        # Export only the necessary columns
        export_columns = ["ЗНП", "Вагон №", "Накладная №"]
        final_data[export_columns].to_csv(output_path, index=False, encoding='utf-8')
        
        logger.info(f"Route ID data exported to {output_path}")
        return output_path

    def process_workflow(self, stg_folder: str, existing_data_path: str) -> str:
        """
        Run the complete workflow to replace the Power BI process.
        """
        logger.info("Starting workflow processing")
        
        # Step 1: Process daily files
        daily_data = self.process_daily_files(stg_folder)
        if daily_data.empty:
            logger.warning("No data found in daily files")
            return None
            
        # Step 2: Merge with existing data
        combined_data = self.merge_with_existing_data(daily_data, existing_data_path)
        
        # Step 3: Assign batch IDs
        batched_data = self.assign_batch_ids(combined_data)
        
        # Step 4: Map ЗНП to batches
        final_data = self.map_znp_to_batches(batched_data)
        
        # Step 5: Export RouteID data
        output_path = self.export_route_id_data(final_data)
        
        logger.info("Workflow processing completed successfully")
        return output_path

    def generate_route_suggestions(self, stg_folder: str) -> List[Dict]:
        """Generate route suggestions from STG files."""
        all_data = []
        
        # Get existing ZNP data
        znp_data = get_znp_data()
        znp_lookup = {}
        if not znp_data.empty:
            for _, row in znp_data.iterrows():
                key = (
                    int(row["Месяц"]),
                    str(row["Ст. отправления"]),
                    str(row["Ст. назначения"]),
                    str(row["Тип вагона"]) if pd.notna(row["Тип вагона"]) else ""
                )
                znp_lookup[key] = str(row["ЗНП"])
        
        # Process all STG files
        stg_files = get_files_by_pattern(stg_folder, "STGDaily_*.xlsx")
        for file_path in stg_files:
            try:
                # Read the file
                df = pd.read_excel(file_path)
                
                # Extract month from "Отчетная дата"
                if "Отчетная дата" in df.columns:
                    df["Месяц"] = pd.to_datetime(df["Отчетная дата"]).dt.month
                elif "Месяц" in df.columns:
                    df["Месяц"] = df["Месяц"].astype(int)
                else:
                    logger.warning(f"No date column found in {file_path}")
                    continue
                
                # Filter for loaded batches
                df = df[df["Груж\\пор"] == "ГРУЖ"]
                
                # Select relevant columns and handle empty wagon types
                required_columns = ["Месяц", "Ст. отправления", "Ст. назначения", "Тип вагона"]
                if not all(col in df.columns for col in required_columns):
                    logger.warning(f"Missing required columns in {file_path}")
                    continue
                
                route_data = df[required_columns].copy()
                # Fill empty wagon types with empty string
                route_data["Тип вагона"] = route_data["Тип вагона"].fillna("")
                all_data.append(route_data)
                
            except Exception as e:
                logger.error(f"Error processing {file_path}: {str(e)}")
                continue
        
        if not all_data:
            raise ValueError("No valid data found in STG files")
        
        # Combine all data
        combined_data = pd.concat(all_data, ignore_index=True)
        
        # Group by route and count batches
        grouped = combined_data.groupby(
            ["Месяц", "Ст. отправления", "Ст. назначения", "Тип вагона"]
        ).size().reset_index(name="Count")
        
        # Sort by station names
        grouped = grouped.sort_values(["Ст. отправления", "Ст. назначения"])
        
        # Add ZNP values from lookup
        result = []
        for _, row in grouped.iterrows():
            key = (
                int(row["Месяц"]),
                str(row["Ст. отправления"]),
                str(row["Ст. назначения"]),
                str(row["Тип вагона"]) if pd.notna(row["Тип вагона"]) else ""
            )
            route_dict = row.to_dict()
            route_dict["ЗНП"] = znp_lookup.get(key, "")  # Add existing ZNP value if found
            result.append(route_dict)
        
        return result

    def process_stg_file(self, file_path: str) -> None:
        """Process a single STG file."""
        try:
            logger.info(f"Processing STG file: {file_path}")
            
            # Read the file
            df = pd.read_excel(file_path)
            
            # Ensure column headers are standardized
            df.columns = [col.strip() for col in df.columns]
            
            # Apply column type standardization
            column_types = {
                "Вагон №": 'int64',
                "Накладная №": 'str',
                "Ст. отправления": 'str',
                "Ст. назначения": 'str',
                "Прибытие на ст. отправл.": 'datetime64',
                "Отчетная дата": 'datetime64',
                "Прибытие на ст. назн.": 'datetime64',
                "Груж\\пор": 'str',
                "Тип вагона": 'str',
                "Расстояние": 'int64',
                "Собственник": 'str',
                "Грузоотправитель": 'str',
                "Грузополучатель": 'str',
                "Простой в ожидании ремонта": 'float'
            }
            
            df = standardize_column_types(df, column_types)
            
            # Extract month if not present
            if "Месяц" not in df.columns:
                df["Месяц"] = pd.to_datetime(df["Отчетная дата"]).dt.month
            
            # Save processed file
            output_filename = f"Processed_{os.path.basename(file_path)}"
            output_path = os.path.join(self.output_dir, output_filename)
            df.to_excel(output_path, index=False)
            
            logger.info(f"Successfully processed {file_path} -> {output_path}")
            
        except Exception as e:
            logger.error(f"Error processing STG file {file_path}: {str(e)}")
            raise
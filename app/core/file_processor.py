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
        sorted_data = data.sort_values(by=["Вагон №", "Отчетная дата"])
        
        # Initialize variables for batch assignment
        last_batch_id = 0
        last_gruz_batch_id = 0
        last_wagon = None
        result_data = []
        
        # Process each row
        for _, row in sorted_data.iterrows():
            current_wagon = row["Вагон №"]
            gruzh_por_value = row["Груж\\пор"]
            
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
            row_with_batch["Batch ID"] = new_batch_id
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
            if "Месяц" not in batched_data.columns:
                # Ensure Отчетная дата is datetime
                if "Отчетная дата" not in batched_data.columns:
                    raise ValueError("Required column 'Отчетная дата' is missing")
                
                batched_data["Отчетная дата"] = pd.to_datetime(batched_data["Отчетная дата"], errors='coerce')
                # Extract month, handling NaT values
                batched_data["Месяц"] = batched_data["Отчетная дата"].dt.month.fillna(0).astype(int)
                
                # Log the unique months found
                unique_months = batched_data["Месяц"].unique()
                logger.info(f"Extracted months from dates: {unique_months}")
                
                # If all months are 0, there might be an issue with the dates
                if all(month == 0 for month in unique_months):
                    logger.error("All months are 0, indicating possible date conversion issues")
                    raise ValueError("Failed to extract valid months from dates")
            else:
                # If Месяц column exists, ensure it's an integer
                batched_data["Месяц"] = pd.to_numeric(batched_data["Месяц"], errors='coerce').fillna(0).astype(int)
        except Exception as e:
            logger.error(f"Error processing month data: {str(e)}")
            logger.error(f"Available columns: {batched_data.columns.tolist()}")
            if "Отчетная дата" in batched_data.columns:
                logger.error(f"Sample of Отчетная дата values: {batched_data['Отчетная дата'].head()}")
            raise ValueError(f"Failed to process date/month data: {str(e)}")
        
        # Filter for loaded batches (ГРУЖ)
        loaded_batches = batched_data[batched_data["Груж\\пор"] == "ГРУЖ"].copy()
        
        # Get ЗНП reference data
        znp_data = get_znp_data()
        
        # Check if we have any ZNP data
        if znp_data.empty:
            logger.error("No ZNP data found in the database. Please import ZNP data first.")
            raise ValueError("No ZNP data found in the database. Please import ZNP data first.")
        
        # Ensure month column in znp_data is also integer
        znp_data["Месяц"] = pd.to_numeric(znp_data["Месяц"], errors='coerce').fillna(0).astype(int)
        
        # Log the unique months in both dataframes for debugging
        logger.info(f"Months in loaded_batches: {loaded_batches['Месяц'].unique()}")
        logger.info(f"Months in znp_data: {znp_data['Месяц'].unique()}")
        
        # Merge based on month, stations, and wagon type
        merge_columns = ["Месяц", "Ст. отправления", "Ст. назначения", "Тип вагона"]
        merged_data = pd.merge(
            loaded_batches,
            znp_data,
            on=merge_columns,
            how="left"
        )
        
        # Get exceptions data
        exceptions_data = get_exceptions()
        
        # Ensure invoice numbers are strings for merging
        if "Накладная №" in merged_data.columns:
            merged_data["Накладная №"] = merged_data["Накладная №"].astype(str)
        if "Накладная №" in exceptions_data.columns:
            exceptions_data["Накладная №"] = exceptions_data["Накладная №"].astype(str)
        
        # Merge exceptions
        exceptions_merged = pd.merge(
            merged_data,
            exceptions_data,
            on="Накладная №",
            how="left"
        )
        
        # Create Final RouteID (Exceptions > ЗНП)
        batch_to_znp = exceptions_merged.groupby("Batch ID").agg({
            "ExceptionRouteID": lambda x: next((i for i in x if pd.notna(i)), None),
            "ЗНП": lambda x: next((i for i in x if pd.notna(i)), None)
        }).reset_index()
        
        # Create Final RouteID column
        batch_to_znp["Final RouteID"] = batch_to_znp.apply(
            lambda row: row["ExceptionRouteID"] if pd.notna(row["ExceptionRouteID"]) else row["ЗНП"],
            axis=1
        )
        
        # Merge final RouteID back to original data
        final_data = pd.merge(
            batched_data,
            batch_to_znp[["Batch ID", "Final RouteID"]],
            on="Batch ID",
            how="left"
        )
        
        # Apply overrides
        overrides_data = get_overrides()
        
        # Normalize column names for merging
        merge_columns = {
            "Вагон №": lambda df: pd.to_numeric(df["Вагон №"], errors='coerce').fillna(0).astype('int64'),
            "Накладная №": lambda df: df["Накладная №"].astype(str)
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
            on=["Вагон №", "Накладная №"],
            how="left",
            suffixes=("", " Override")
        )
        
        # Replace RouteID with Overrides
        merged_with_overrides["Updated Final RouteID"] = merged_with_overrides.apply(
            lambda row: row["ЗНП Override"] if pd.notna(row["ЗНП Override"]) else row["Final RouteID"],
            axis=1
        )
        
        # Propagate RouteID within batches
        result_data = []
        for batch_id, batch_df in merged_with_overrides.groupby("Batch ID"):
            if batch_id == 0:  # Skip unassigned batches
                result_data.append(batch_df)
                continue
                
            # Find the first non-null RouteID in the batch
            valid_route_ids = batch_df["Updated Final RouteID"].dropna()
            if not valid_route_ids.empty:
                final_route_id = valid_route_ids.iloc[0]
                batch_df["Propagated Final RouteID"] = final_route_id
            else:
                batch_df["Propagated Final RouteID"] = None
                
            result_data.append(batch_df)
        
        # Combine all processed batches
        result_df = pd.concat(result_data, ignore_index=True)
        
        # Clean up and rename
        selected_columns = [
            "Месяц", "Propagated Final RouteID", "Batch ID", "Вагон №", "Накладная №", 
            "W&N", "Груж\\пор", "Ст. отправления", "Ст. назначения", 
            "Прибытие на ст. отправл.", "Отчетная дата", "Прибытие на ст. назн."
        ]
        final_table = result_df[selected_columns].copy()
        final_table = final_table.rename(columns={"Propagated Final RouteID": "ЗНП"})
        
        # Filter for records with valid ЗНП
        final_table = final_table.dropna(subset=["ЗНП"])
        
        # Add custom route description
        final_table["Custom"] = final_table["Ст. отправления"] + " - " + final_table["Ст. назначения"]
        
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
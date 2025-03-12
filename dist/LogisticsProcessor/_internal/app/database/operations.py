import pandas as pd
import logging
from sqlalchemy.orm import Session
from typing import List, Dict, Optional, Any
from datetime import datetime
from sqlalchemy import func, create_engine
from sqlalchemy.orm import sessionmaker
import os

from app.database.models import ZNP, Exception, Override, ActiveRoute, MatrixMapping, WagonInvoice, ProcessingLog, STGData

logger = logging.getLogger(__name__)

def get_database_path():
    """Get the path to the database file in AppData."""
    db_dir = os.path.join(os.getenv('APPDATA'), 'Logistics Data Processor')
    os.makedirs(db_dir, exist_ok=True)
    return os.path.join(db_dir, "logistics_processor.db")

# Create engine and session factory
DATABASE_URL = f'sqlite:///{get_database_path()}'
engine = create_engine(DATABASE_URL)
SessionFactory = sessionmaker(bind=engine)

# Global session variable
_session = None

def init_session(session: Session):
    """Initialize the global session."""
    global _session
    _session = session

def get_session() -> Session:
    """Get the current database session."""
    global _session
    if _session is None:
        raise RuntimeError("Database session not initialized")
    return _session

# ZNP operations
def get_znp_data() -> pd.DataFrame:
    """Get all ZNP data as a DataFrame."""
    try:
        session = get_session()
        znp_records = session.query(ZNP).all()
        
        if not znp_records:
            logger.warning("No ZNP records found in database")
            return pd.DataFrame(columns=['Месяц', 'Ст. отправления', 'Ст. назначения', 'Тип вагона', 'ЗНП'])
        
        data = [{
            'Месяц': record.month,
            'Ст. отправления': record.departure_station,
            'Ст. назначения': record.destination_station,
            'Тип вагона': record.wagon_type,
            'ЗНП': record.znp_code
        } for record in znp_records]
        
        return pd.DataFrame(data)
    except Exception as e:
        logger.error(f"Error retrieving ZNP data: {str(e)}")
        return pd.DataFrame(columns=['Месяц', 'Ст. отправления', 'Ст. назначения', 'Тип вагона', 'ЗНП'])

def add_znp_data(df: pd.DataFrame) -> int:
    """
    Add ZNP data from a DataFrame.
    Returns the number of records added.
    """
    session = get_session()
    count = 0
    
    try:
        # Convert month column to integer
        df["Месяц"] = pd.to_numeric(df["Месяц"], errors='coerce').fillna(0).astype(int)
        
        # Validate required columns
        required_columns = ['Месяц', 'Ст. отправления', 'Ст. назначения', 'Тип вагона', 'ЗНП']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
        
        # Get current year
        current_year = datetime.now().year
        
        # Clear existing ZNP data
        session.query(ZNP).delete()
        session.commit()
        
        # Add each record
        for _, row in df.iterrows():
            try:
                znp = ZNP(
                    month=int(row['Месяц']),  # Ensure integer
                    year=current_year,  # Set current year as default
                    departure_station=str(row['Ст. отправления']),  # Ensure string
                    destination_station=str(row['Ст. назначения']),  # Ensure string
                    wagon_type=str(row['Тип вагона']),  # Ensure string
                    znp_code=str(row['ЗНП'])  # Ensure string
                )
                session.add(znp)
                count += 1
            except BaseException as e:
                logger.error(f"Error adding ZNP record: {str(e)}, Row: {row.to_dict()}")
                continue
        
        session.commit()
        return count
    except BaseException as e:
        session.rollback()
        logger.error(f"Error adding ZNP data: {str(e)}")
        if isinstance(e, ValueError):
            raise
        raise RuntimeError(f"Failed to add ZNP data: {str(e)}")

# Exception operations
def get_exceptions() -> pd.DataFrame:
    """Get all exceptions data as a DataFrame."""
    session = get_session()
    exception_records = session.query(Exception).all()
    
    data = [{
        'Накладная №': record.invoice_number,
        'ExceptionRouteID': record.exception_route_id
    } for record in exception_records]
    
    return pd.DataFrame(data)

def add_exceptions(df: pd.DataFrame) -> int:
    """
    Add exceptions from a DataFrame.
    Returns the number of records added.
    """
    session = get_session()
    count = 0
    
    try:
        # Clear existing exceptions
        session.query(Exception).delete()
        session.commit()
        
        for _, row in df.iterrows():
            try:
                exception = Exception(
                    invoice_number=str(row['Накладная №']).strip(),
                    exception_route_id=str(row['ExceptionRouteID']).strip()
                )
                session.add(exception)
                count += 1
            except BaseException as e:
                logger.error(f"Error adding exception record: {str(e)}, Row: {row.to_dict()}")
                continue
        
        session.commit()
        return count
    except BaseException as e:
        session.rollback()
        logger.error(f"Error adding exceptions: {str(e)}")
        raise

# Override operations
def get_overrides() -> pd.DataFrame:
    """Get all overrides data as a DataFrame."""
    session = get_session()
    override_records = session.query(Override).all()
    
    data = []
    for record in override_records:
        try:
            # Convert wagon number to integer after cleaning
            wagon_num = int(str(record.wagon_number).strip())
            data.append({
                'wagon_number': wagon_num,
                'invoice_number': str(record.invoice_number).strip(),
                'znp_code': str(record.znp_code).strip()
            })
        except (ValueError, TypeError) as e:
            logger.error(f"Error processing override record: {str(e)}")
            continue
    
    df = pd.DataFrame(data)
    
    # Ensure correct data types
    if not df.empty:
        df['wagon_number'] = df['wagon_number'].astype('int64')
        df['invoice_number'] = df['invoice_number'].astype(str)
        df['znp_code'] = df['znp_code'].astype(str)
    
    return df

def add_overrides(df: pd.DataFrame) -> int:
    """
    Add overrides from a DataFrame.
    Returns the number of records added.
    """
    session = get_session()
    count = 0
    
    try:
        # Clear existing overrides
        session.query(Override).delete()
        session.commit()
        
        # Rename columns from Russian to English
        column_mapping = {
            'Вагон №': 'wagon_number',
            'Накладная №': 'invoice_number',
            'ЗНП': 'znp_code'  # Changed from 'ЗНП Override' to 'ЗНП'
        }
        
        # Create a copy of the DataFrame with renamed columns
        df_processed = df.rename(columns=column_mapping)
        
        # Log column names for debugging
        logger.info(f"Input DataFrame columns: {df.columns.tolist()}")
        logger.info(f"Processed DataFrame columns: {df_processed.columns.tolist()}")
        
        for _, row in df_processed.iterrows():
            try:
                # Convert wagon number to integer and then to string
                wagon_num = str(int(float(row['wagon_number'])))
                override = Override(
                    wagon_number=wagon_num.strip(),
                    invoice_number=str(row['invoice_number']).strip(),
                    znp_code=str(row['znp_code']).strip()
                )
                session.add(override)
                count += 1
            except BaseException as e:
                logger.error(f"Error processing override row: {str(e)}, Row data: {row.to_dict()}")
                continue
        
        session.commit()
        logger.info(f"Successfully added {count} overrides")
        return count
    except BaseException as e:
        session.rollback()
        logger.error(f"Error adding overrides: {str(e)}")
        raise

# Active routes operations
def get_active_routes() -> pd.DataFrame:
    """Get all active routes as a DataFrame."""
    session = get_session()
    active_records = session.query(ActiveRoute).all()
    
    if not active_records:
        # If no active routes in database, return empty DataFrame with correct column
        return pd.DataFrame(columns=['route_id'])
    
    data = [{
        'route_id': record.route_id
    } for record in active_records]
    
    return pd.DataFrame(data)

def add_active_routes(routes: List[str]) -> int:
    """
    Add active routes from a list.
    Returns the number of records added.
    """
    session = get_session()
    count = 0
    
    try:
        # Log the number of routes to process
        logger.info(f"Processing {len(routes)} active routes")
        
        # Clear existing routes
        session.query(ActiveRoute).delete()
        session.commit()
        logger.info("Cleared existing active routes")
        
        # Add new routes
        for route in routes:
            route_id = str(route).strip()
            if not route_id:  # Skip empty route IDs
                continue
                
            active = ActiveRoute(route_id=route_id)
            session.add(active)
            count += 1
        
        session.commit()
        logger.info(f"Successfully added {count} active routes")
        return count
    except BaseException as e:
        session.rollback()
        logger.error(f"Error adding active routes: {str(e)}")
        raise

# Matrix mappings operations
def get_matrix_mappings() -> pd.DataFrame:
    """Get all matrix mappings as a DataFrame."""
    session = get_session()
    matrix_records = session.query(MatrixMapping).all()
    
    data = [{
        'source_value': record.source_value,
        'target_value': record.target_value,
        'mapping_group': record.mapping_group
    } for record in matrix_records]
    
    return pd.DataFrame(data)

def add_matrix_mappings(df: pd.DataFrame) -> int:
    """
    Add matrix mappings from a DataFrame.
    Returns the number of records added.
    """
    session = get_session()
    count = 0
    
    try:
        # Clear existing mappings
        session.query(MatrixMapping).delete()
        session.commit()
        
        # Process each row in the matrix file
        for _, row in df.iterrows():
            # Skip empty rows
            if row.isna().all():
                continue
                
            # Get non-null values
            values = row.dropna().astype(str).values
            if len(values) < 2:
                continue
                
            # Create mappings only from each value to the next one
            group_id = f"group_{count}"
            for i in range(len(values) - 1):
                # Create forward mapping only
                mapping = MatrixMapping(
                    source_value=values[i].strip(),
                    target_value=values[i + 1].strip(),
                    mapping_group=group_id
                )
                session.add(mapping)
                count += 1
            
        session.commit()
        logger.info(f"Successfully added {count} matrix mappings")
        return count
    except Exception as e:
        session.rollback()
        logger.error(f"Error adding matrix mappings: {str(e)}")
        raise

# WagonInvoice operations
def add_wagon_invoice_data(df: pd.DataFrame) -> int:
    """
    Add processed wagon-invoice data from a DataFrame.
    Returns the number of records added.
    """
    session = get_session()
    count = 0
    
    try:
        for _, row in df.iterrows():
            record = WagonInvoice(
                wagon_number=row['Вагон №'],
                invoice_number=row['Накладная №'],
                route_id=row['ЗНП'],
                batch_id=row.get('Batch ID'),
                departure_station=row['Ст. отправления'],
                destination_station=row['Ст. назначения'],
                departure_date=row.get('Прибытие на ст. отправл.'),
                report_date=row['Отчетная дата'],
                arrival_date=row.get('Прибытие на ст. назн.'),
                status=row['Груж\\пор'],
                wagon_type=row.get('Тип вагона', 'Unknown')
            )
            session.add(record)
            count += 1
        
        session.commit()
        return count
    except Exception as e:
        session.rollback()
        logger.error(f"Error adding wagon-invoice data: {str(e)}")
        raise

def get_route_id_data() -> pd.DataFrame:
    """
    Get route ID data for the expense processor.
    This provides the same data as the "Route ID.csv" file.
    """
    session = get_session()
    records = session.query(WagonInvoice).all()
    
    data = [{
        'ЗНП': record.route_id,
        'Вагон №': record.wagon_number,
        'Накладная №': record.invoice_number
    } for record in records]
    
    return pd.DataFrame(data)

# Logging operations
def log_operation(operation: str, status: str, file_name: Optional[str] = None, 
                 message: Optional[str] = None) -> None:
    """Log an operation to the database."""
    session = get_session()
    
    try:
        log = ProcessingLog(
            operation=operation,
            status=status,
            file_name=file_name,
            message=message
        )
        session.add(log)
        session.commit()
    except Exception as e:
        session.rollback()
        logger.error(f"Error logging operation: {str(e)}")

def add_stg_data(df):
    """Add STG data to the database from a pandas DataFrame."""
    try:
        # Create a new session
        session = SessionFactory()
        
        # Clear existing STG data
        session.query(STGData).delete()
        
        # Convert DataFrame rows to STGData objects
        stg_objects = []
        for _, row in df.iterrows():
            try:
                stg_data = STGData(
                    wagon_number=None if pd.isna(row['wagon_number']) else int(row['wagon_number']),
                    invoice_number=row['invoice_number'],
                    departure_station=row['departure_station'],
                    destination_station=row['destination_station'],
                    departure_arrival=None if pd.isna(row['departure_arrival']) else row['departure_arrival'],
                    report_date=None if pd.isna(row['report_date']) else row['report_date'],
                    destination_arrival=None if pd.isna(row['destination_arrival']) else row['destination_arrival'],
                    load_status=row['load_status'],
                    wagon_type=row['wagon_type'],
                    distance=None if pd.isna(row['distance']) else float(row['distance']),
                    owner=row['owner'],
                    shipper=row['shipper'],
                    consignee=row['consignee'],
                    repair_wait_time=None if pd.isna(row['repair_wait_time']) else float(row['repair_wait_time']),
                    wn_code=row['wn_code'],
                    batch_id=None if pd.isna(row['batch_id']) else int(row['batch_id']),
                    month=None if pd.isna(row['month']) else int(row['month']),
                    route_id=row['route_id']
                )
                stg_objects.append(stg_data)
            except Exception as e:
                logger.warning(f"Error processing row: {row}. Error: {str(e)}")
                continue
        
        # Bulk save the objects
        if stg_objects:
            session.bulk_save_objects(stg_objects)
            session.commit()
            logger.info(f"Added {len(stg_objects)} records to STG data")
        else:
            logger.warning("No valid records to add to STG data")
            
    except Exception as e:
        logger.error(f"Error adding STG data: {str(e)}")
        if session:
            session.rollback()
        raise
    finally:
        if session:
            session.close()

def get_stg_data(filters: Dict[str, Any] = None) -> pd.DataFrame:
    """
    Retrieve STG data from the database with optional filters.
    Returns a DataFrame with the results.
    """
    session = get_session()
    
    try:
        # Start with base query
        query = session.query(STGData)
        
        # Apply filters if provided
        if filters:
            if 'month' in filters:
                query = query.filter(STGData.month == filters['month'])
            if 'wagon_type' in filters:
                query = query.filter(STGData.wagon_type == filters['wagon_type'])
            if 'departure_station' in filters:
                query = query.filter(STGData.departure_station == filters['departure_station'])
            if 'destination_station' in filters:
                query = query.filter(STGData.destination_station == filters['destination_station'])
        
        # Convert to DataFrame
        results = query.all()
        if not results:
            return pd.DataFrame()
        
        data = []
        for result in results:
            data.append({
                'Вагон №': result.wagon_number,
                'Накладная №': result.invoice_number,
                'Ст. отправления': result.departure_station,
                'Ст. назначения': result.destination_station,
                'Прибытие на ст. отправл.': result.departure_arrival,
                'Отчетная дата': result.report_date,
                'Прибытие на ст. назн.': result.destination_arrival,
                'Груж\\пор': result.load_status,
                'Тип вагона': result.wagon_type,
                'Расстояние': result.distance,
                'Собственник': result.owner,
                'Грузоотправитель': result.shipper,
                'Грузополучатель': result.consignee,
                'Простой в ожидании ремонта': result.repair_wait_time,
                'W&N': result.wn_code,
                'Batch ID': result.batch_id,
                'Месяц': result.month,
                'Final RouteID': result.route_id
            })
        
        return pd.DataFrame(data)
    
    except Exception as e:
        log_operation("get_stg_data", "ERROR", str(e))
        raise

def update_stg_wagon_types(changes: List[Dict[str, Any]]) -> int:
    """
    Update wagon types in STG data based on provided changes.
    Returns the number of records updated.
    """
    session = get_session()
    
    try:
        updated_count = 0
        for change in changes:
            # Update matching records
            result = session.query(STGData).filter(
                STGData.month == change['Месяц'],
                STGData.departure_station == change['Ст. отправления'],
                STGData.destination_station == change['Ст. назначения'],
                STGData.wagon_type == change['old_wagon_type']
            ).update({'wagon_type': change['new_wagon_type']})
            
            updated_count += result
        
        session.commit()
        log_operation("update_stg_wagon_types", "SUCCESS", f"Updated {updated_count} records")
        
        return updated_count
    
    except Exception as e:
        session.rollback()
        log_operation("update_stg_wagon_types", "ERROR", str(e))
        raise
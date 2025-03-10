import pandas as pd
import logging
from sqlalchemy.orm import Session
from typing import List, Dict, Optional

from app.database.models import ZNP, Exception, Override, ActiveRoute, MatrixMapping, WagonInvoice, ProcessingLog

logger = logging.getLogger(__name__)

# Global session variable, will be initialized in the application startup
_db_session = None

def init_session(session):
    """Initialize the global database session."""
    global _db_session
    _db_session = session

def get_session() -> Session:
    """Get the current database session."""
    if _db_session is None:
        raise RuntimeError("Database session not initialized")
    return _db_session

# ZNP operations
def get_znp_data() -> pd.DataFrame:
    """Get all ZNP data as a DataFrame."""
    session = get_session()
    znp_records = session.query(ZNP).all()
    
    data = [{
        'Месяц': record.month,
        'Ст. отправления': record.departure_station,
        'Ст. назначения': record.destination_station,
        'Тип вагона': record.wagon_type,
        'ЗНП': record.znp_code
    } for record in znp_records]
    
    return pd.DataFrame(data)

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
        
        # Add each record
        for _, row in df.iterrows():
            try:
                znp = ZNP(
                    month=int(row['Месяц']),  # Ensure integer
                    departure_station=str(row['Ст. отправления']),  # Ensure string
                    destination_station=str(row['Ст. назначения']),  # Ensure string
                    wagon_type=str(row['Тип вагона']),  # Ensure string
                    znp_code=str(row['ЗНП'])  # Ensure string
                )
                session.add(znp)
                count += 1
            except Exception as e:
                logger.error(f"Error adding ZNP record: {str(e)}, Row: {row.to_dict()}")
                continue
        
        session.commit()
        return count
    except Exception as e:
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
        for _, row in df.iterrows():
            exception = Exception(
                invoice_number=row['Накладная №'],
                exception_route_id=row['ExceptionRouteID']
            )
            session.add(exception)
            count += 1
        
        session.commit()
        return count
    except Exception as e:
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
                'Вагон №': wagon_num,
                'Накладная №': str(record.invoice_number).strip(),
                'ЗНП Override': str(record.znp_code).strip()
            })
        except (ValueError, TypeError) as e:
            logger.error(f"Error processing override record: {str(e)}")
            continue
    
    df = pd.DataFrame(data)
    
    # Ensure correct data types
    if not df.empty:
        df['Вагон №'] = df['Вагон №'].astype('int64')
        df['Накладная №'] = df['Накладная №'].astype(str)
        df['ЗНП Override'] = df['ЗНП Override'].astype(str)
    
    return df

def add_overrides(df: pd.DataFrame) -> int:
    """
    Add overrides from a DataFrame.
    Returns the number of records added.
    """
    session = get_session()
    count = 0
    
    try:
        for _, row in df.iterrows():
            try:
                # Store wagon number as string
                wagon_num = str(int(row['Вагон №'])).strip()
                override = Override(
                    wagon_number=wagon_num,
                    invoice_number=str(row['Накладная №']).strip(),
                    znp_code=str(row['ЗНП']).strip()
                )
                session.add(override)
                count += 1
            except (ValueError, TypeError) as e:
                logger.error(f"Error processing override row: {str(e)}")
                continue
        
        session.commit()
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
                
            # Create mappings for each pair of values
            group_id = f"group_{count}"
            for i in range(len(values) - 1):
                for j in range(i + 1, len(values)):
                    # Create bidirectional mappings
                    mapping1 = MatrixMapping(
                        source_value=values[i].strip(),
                        target_value=values[j].strip(),
                        mapping_group=group_id
                    )
                    mapping2 = MatrixMapping(
                        source_value=values[j].strip(),
                        target_value=values[i].strip(),
                        mapping_group=group_id
                    )
                    session.add(mapping1)
                    session.add(mapping2)
                    count += 2
            
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
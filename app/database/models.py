from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker
import datetime

Base = declarative_base()

class ZNP(Base):
    """
    Model for ЗНП (planning) data.
    Replaces your ЗНП.xlsx file.
    """
    __tablename__ = 'znp'
    
    id = Column(Integer, primary_key=True)
    month = Column(Integer, nullable=False)
    departure_station = Column(String, nullable=False)
    destination_station = Column(String, nullable=False)
    wagon_type = Column(String, nullable=False)
    znp_code = Column(String, nullable=False)
    
    def __repr__(self):
        return f"<ZNP(id={self.id}, znp_code='{self.znp_code}')>"

class Exception(Base):
    """
    Model for exceptions.
    Replaces your Exceptions.xlsx file.
    """
    __tablename__ = 'exceptions'
    
    id = Column(Integer, primary_key=True)
    invoice_number = Column(String, nullable=False, index=True)
    exception_route_id = Column(String, nullable=False)
    
    def __repr__(self):
        return f"<Exception(id={self.id}, invoice='{self.invoice_number}', route='{self.exception_route_id}')>"

class Override(Base):
    """
    Model for manual overrides.
    Replaces your Overrides.xlsx file.
    """
    __tablename__ = 'overrides'
    
    id = Column(Integer, primary_key=True)
    wagon_number = Column(String, nullable=False)
    invoice_number = Column(String, nullable=False)
    znp_code = Column(String, nullable=False)
    
    def __repr__(self):
        return f"<Override(id={self.id}, wagon={self.wagon_number}, invoice='{self.invoice_number}')>"

class ActiveRoute(Base):
    """
    Model for active routes.
    Replaces your Active.csv file.
    """
    __tablename__ = 'active_routes'
    
    id = Column(Integer, primary_key=True)
    route_id = Column(String, nullable=False, unique=True)
    
    def __repr__(self):
        return f"<ActiveRoute(id={self.id}, route='{self.route_id}')>"

class MatrixMapping(Base):
    """
    Model for matrix mappings.
    Replaces your Matrix.csv file.
    """
    __tablename__ = 'matrix_mappings'
    
    id = Column(Integer, primary_key=True)
    source_value = Column(String, nullable=False)  # The original value
    target_value = Column(String, nullable=False)  # The mapped value
    mapping_group = Column(String, nullable=False)  # Group identifier for related mappings
    
    def __repr__(self):
        return f"<MatrixMapping(id={self.id}, source='{self.source_value}', target='{self.target_value}')>"

class WagonInvoice(Base):
    """
    Model for processed wagon-invoice combinations.
    Stores the final output of the processing.
    """
    __tablename__ = 'wagon_invoices'
    
    id = Column(Integer, primary_key=True)
    wagon_number = Column(Integer, nullable=False, index=True)
    invoice_number = Column(String, nullable=False, index=True)
    route_id = Column(String, nullable=False)
    batch_id = Column(Integer, nullable=True)
    departure_station = Column(String, nullable=False)
    destination_station = Column(String, nullable=False)
    departure_date = Column(DateTime, nullable=True)
    report_date = Column(DateTime, nullable=False)
    arrival_date = Column(DateTime, nullable=True)
    status = Column(String, nullable=False)  # "ГРУЖ" or "ПОР"
    wagon_type = Column(String, nullable=False)
    
    def __repr__(self):
        return f"<WagonInvoice(id={self.id}, wagon={self.wagon_number}, invoice='{self.invoice_number}')>"

class ProcessingLog(Base):
    """
    Model for tracking processing activities.
    """
    __tablename__ = 'processing_logs'
    
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, default=datetime.datetime.now)
    operation = Column(String, nullable=False)
    status = Column(String, nullable=False)  # "SUCCESS", "ERROR", etc.
    file_name = Column(String, nullable=True)
    message = Column(String, nullable=True)
    
    def __repr__(self):
        return f"<ProcessingLog(id={self.id}, operation='{self.operation}', status='{self.status}')>"

# Database initialization function
def init_db(db_path):
    """Initialize the database and create tables."""
    engine = create_engine(f'sqlite:///{db_path}')
    Base.metadata.create_all(engine)
    SessionMaker = sessionmaker(bind=engine)
    return engine, SessionMaker
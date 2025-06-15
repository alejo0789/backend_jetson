import os
import uuid 
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base

# Importamos la base declarativa y los modelos de la base de datos local
from app.models.edge_database_models import Base, ConfiguracionJetsonLocal

# --- Configuración de la Base de Datos SQLite para el Edge ---

SQLITE_DB_PATH = "sqlite:///./edge_data.db" 

edge_engine = create_engine(
    SQLITE_DB_PATH, 
    connect_args={"check_same_thread": False}, 
    echo=False 
)

EdgeSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=edge_engine)

# --- Funciones de Utilidad para la Base de Datos ---

def get_edge_db():
    """
    Proporciona una sesión de base de datos local para la Jetson Nano.
    """
    db = EdgeSessionLocal()
    try:
        yield db
    finally:
        db.close()

def create_edge_tables():
    """
    Crea todas las tablas definidas en 'edge_database_models.py' en la base de datos SQLite local.
    """
    Base.metadata.create_all(bind=edge_engine)
    print(f"Tablas de la base de datos Edge creadas en: {SQLITE_DB_PATH.replace('sqlite:///','')}")

def initialize_jetson_config(db, id_hardware_jetson: str, id_bus_asignado: uuid.UUID = None):
    """
    Inicializa o actualiza la configuración local de la Jetson (la única fila en ConfiguracionJetsonLocal).
    Se llama en el arranque de la Jetson.
    :param db: Sesión de la base de datos.
    :param id_hardware_jetson: El ID único del hardware de esta Jetson.
    :param id_bus_asignado: UUID del bus al que está asignada esta Jetson.
    """
    config = db.query(ConfiguracionJetsonLocal).first()
    if not config:
        # Si no existe, creamos la primera entrada
        config = ConfiguracionJetsonLocal(
            id_hardware_jetson=id_hardware_jetson,
            # >>>>>>>>>>>>> CAMBIO CLAVE AQUI: USAMOS EL MISMO UUID DEL BUS DE PRUEBA <<<<<<<<<<<<<
            id_bus_asignado=id_bus_asignado, 
            version_firmware_local="1.0.0", 
            estado_operativo_local="Activo"
        )
        db.add(config)
        db.commit()
        db.refresh(config)
        print(f"Configuración inicial de Jetson creada: ID_Hardware={config.id_hardware_jetson}, ID_Bus_Asignado={config.id_bus_asignado}")
    else:
        # Si ya existe, actualizamos campos si es necesario.
        needs_commit = False
        if config.id_hardware_jetson != id_hardware_jetson:
            config.id_hardware_jetson = id_hardware_jetson
            needs_commit = True
        # Aseguramos que el ID del bus asignado sea consistente si se actualiza
        if config.id_bus_asignado != id_bus_asignado: 
            config.id_bus_asignado = id_bus_asignado
            needs_commit = True
        
        if needs_commit:
            db.commit()
            db.refresh(config)
            print(f"Configuración de Jetson actualizada: ID_Hardware={config.id_hardware_jetson}, ID_Bus_Asignado={config.id_bus_asignado}")
        else:
            print(f"Configuración de Jetson ya actualizada: ID_Hardware={config.id_hardware_jetson}, ID_Bus_Asignado={config.id_bus_asignado}")
    return config
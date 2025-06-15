import uuid
from datetime import datetime
from sqlalchemy.orm import Session # Importa Session para el tipo
import logging

# Configuración del logger para este script
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# Para asegurar que los logs se muestren si se ejecuta directamente
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


# Importamos las funciones de la base de datos y los modelos
from app.config.edge_database import create_edge_tables, get_edge_db, initialize_jetson_config
from app.models.edge_database_models import (
    ConfiguracionJetsonLocal,
    ConductorLocal,
    BusLocal,
    AsignacionConductorBusLocal, # Aunque no se crea aquí, es bueno tener la referencia
    EventoLocal, # Idem
    AlertaLocal, # Idem
    SincronizacionMetadata # Idem
)
# También importamos la UUIDType personalizada si la necesitamos para los datos de prueba
from app.models.edge_database_models import UUIDType


def load_demo_data():
    """
    Función principal para cargar datos demo en la base de datos local de la Jetson.
    """
    logger.info("Iniciando carga de datos demo en la BD local de la Jetson Nano...")

    # 1. Crear las tablas de la BD si no existen
    create_edge_tables()

    # 2. Obtener una sesión de base de datos
    db: Session = next(get_edge_db()) # Usamos next() para obtener la sesión del generador
    
    try:
        # 3. Inicializar/Configurar la Jetson Nano (la única entrada en ConfiguracionJetsonLocal)
        # Es crucial usar un ID de hardware REAL y ÚNICO para cada Jetson en un despliegue real.
        # Para demo, usamos un ID fijo.
        jetson_hardware_id_demo = "JETSON-NANO-DEMO-001" 
        
        # UUIDs de ejemplo para el bus y conductor. En un escenario real, estos vendrían de la nube.
        # Puedes cambiar estos UUIDs por los que desees para tus pruebas.
        demo_bus_id = uuid.UUID("11111111-1111-1111-1111-111111111111")
        demo_conductor_id_1 = uuid.UUID("22222222-2222-2222-2222-222222222222")
        demo_conductor_id_2 = uuid.UUID("33333333-3333-3333-3333-333333333333")

        # Inicializar la configuración de la Jetson, asociándola a un bus
        jetson_config = initialize_jetson_config(db, jetson_hardware_id_demo, demo_bus_id)
        logger.info(f"Configuración de Jetson inicializada/actualizada: ID_Hardware={jetson_config.id_hardware_jetson}, ID_Bus_Asignado={jetson_config.id_bus_asignado}")

        # 4. Insertar/Verificar datos de Buses de prueba
        if not db.query(BusLocal).filter_by(id=demo_bus_id).first():
            new_bus = BusLocal(
                id=demo_bus_id,
                placa="ABC-123",
                numero_interno="BUS-DEMO-001",
                last_updated_at=datetime.utcnow()
            )
            db.add(new_bus)
            logger.info(f"Bus de prueba '{new_bus.placa}' añadido.")
        else:
            logger.info(f"Bus de prueba '{demo_bus_id}' ya existe.")

        # 5. Insertar/Verificar datos de Conductores de prueba
        # Conductor 1: Activo, para pruebas de inicio/fin de sesión
        cedula_conductor_1 = "1001001001"
        if not db.query(ConductorLocal).filter_by(id=demo_conductor_id_1).first():
            new_conductor_1 = ConductorLocal(
                id=demo_conductor_id_1,
                cedula=cedula_conductor_1,
                nombre_completo="Carlos Andrés Demo",
                codigo_qr_hash=cedula_conductor_1, # Simulamos que el QR es la cédula
                activo=True,
                last_updated_at=datetime.utcnow(),
                caracteristicas_faciales_embedding=None # Simulado para ahora
            )
            db.add(new_conductor_1)
            logger.info(f"Conductor de prueba '{new_conductor_1.nombre_completo}' añadido.")
        else:
            logger.info(f"Conductor de prueba '{demo_conductor_id_1}' ya existe.")

        # Conductor 2: Inactivo, para probar el caso de conductor inactivo
        cedula_conductor_2 = "2002002002"
        if not db.query(ConductorLocal).filter_by(id=demo_conductor_id_2).first():
            new_conductor_2 = ConductorLocal(
                id=demo_conductor_id_2,
                cedula=cedula_conductor_2,
                nombre_completo="Ana María Inactiva",
                codigo_qr_hash=cedula_conductor_2,
                activo=False, # Este conductor está inactivo
                last_updated_at=datetime.utcnow(),
                caracteristicas_faciales_embedding=None
            )
            db.add(new_conductor_2)
            logger.info(f"Conductor de prueba '{new_conductor_2.nombre_completo}' (inactivo) añadido.")
        else:
            logger.info(f"Conductor de prueba '{demo_conductor_id_2}' ya existe.")

        db.commit()
        logger.info("Datos demo cargados/verificados exitosamente en la BD local.")

    except Exception as e:
        db.rollback() # Si algo falla, deshace todos los cambios
        logger.error(f"Error al cargar datos demo: {e}", exc_info=True)
    finally:
        db.close() # Asegúrate de cerrar la sesión

if __name__ == "__main__":
    # Asegúrate de que el directorio 'scripts' esté al mismo nivel que 'config' y 'jetson_app'
    # Ejecuta este script desde la raíz de tu proyecto:
    # python scripts/initial_data_setup.py
    load_demo_data()
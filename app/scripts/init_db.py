# scripts/init_db.py

from app.config.edge_database import create_edge_tables, initialize_jetson_config, get_edge_db
import uuid # Asegúrate de importar uuid aquí también
import os

def init_local_database():
    print("Creando tablas de la base de datos local de la Jetson...")
    create_edge_tables()
    print("Tablas creadas. Inicializando configuración de la Jetson...")

    # Esto es solo un ejemplo. En un despliegue real, deberías tener un mecanismo
    # para obtener el ID de hardware real de la Jetson.
    hardware_id_example = "JETSON_SN_PROTOTIPO_001" # <<<<<< CAMBIA ESTO
    # Si sabes el UUID del bus al que va asignada desde la BD central
    bus_uuid_ejemplo = uuid.UUID("a1b2c3d4-e5f6-7890-1234-567890abcdef") # <<<<<< CAMBIA ESTO (o deja en None)

    db_session = next(get_edge_db())
    initialize_jetson_config(db_session, id_hardware_jetson=hardware_id_example, id_bus_asignado=bus_uuid_ejemplo)
    db_session.close()
    print("Base de datos local y configuración de Jetson inicializadas correctamente.")

if __name__ == "__main__":
    # Asegúrate de que el path de importación sea correcto desde donde ejecutas este script
    # Si ejecutas 'python scripts/init_db.py' desde la raíz del proyecto, funcionará.
    init_local_database()
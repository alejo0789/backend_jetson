# jetson_app/alerts/local_alerts.py
import logging
import platform # Para detectar el sistema operativo
import os # Para la reproducción de sonido
import subprocess # Para comandos de sistema
import uuid
from typing import Dict
from typing import Any
from datetime import datetime, timedelta
# Importar el CRUD de alertas locales para guardar el registro de la alerta

# Importar el CRUD de alertas locales
from app.local_db.crud_edge import create_local_alert, mark_alert_as_visualized 

# Importar los modelos necesarios (incluida AlertaLocal y la propia Base)
# <<<<<<<<<<<<<<<<< CAMBIO AQUI >>>>>>>>>>>>>>>>>>>
from app.models.edge_database_models import AlertaLocal, Base, BusLocal, ConductorLocal, AsignacionConductorBusLocal, ConfiguracionJetsonLocal 
# <<<<<<<<<<<<<<<<< FIN CAMBIO >>>>>>>>>>>>>>>>>>>

# Importar la configuración y el get_edge_db REAL
from app.config.edge_database import get_edge_db, create_edge_tables, initialize_jetson_config 

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# --- Configuración de Hardware (EJEMPLO - ADAPTAR A TU JETSON NANO) ---
# En un entorno real en la Jetson, usarías librerías como Jetson.GPIO o RPi.GPIO (si es compatible)
# o simplemente comandos de sistema para controlar el hardware.

# BCM pins (ejemplo si usas un relé para zumbador y un LED)
# import Jetson.GPIO as GPIO # if available
# BUZZER_PIN = 18 # Ejemplo de pin GPIO
# LED_PIN = 23    # Ejemplo de pin GPIO

# def setup_gpio():
#     if platform.system() == "Linux" and "aarch64" in platform.machine(): # Detectar Jetson
#         try:
#             GPIO.setmode(GPIO.BCM)
#             GPIO.setup(BUZZER_PIN, GPIO.OUT)
#             GPIO.setup(LED_PIN, GPIO.OUT)
#             logger.info("GPIOs de alerta configurados.")
#         except Exception as e:
#             logger.error(f"Error al configurar GPIOs de alerta: {e}. Las alertas de hardware no funcionarán.", exc_info=True)
#     else:
#         logger.info("No es entorno Jetson. GPIOs de alerta no configurados.")

# setup_gpio() # Llamar para configurar al inicio del módulo


def trigger_visual_alert(message: str):
    """
    Activa un indicador visual de alerta en la cabina del bus (ej. un LED intermitente).
    En un PC, solo imprime en consola.
    """
    logger.warning(f"[ALERTA VISUAL]: {message}")
    if platform.system() == "Linux" and "aarch64" in platform.machine():
        # Lógica para encender un LED o activar una luz
        # try:
        #     GPIO.output(LED_PIN, GPIO.HIGH)
        #     time.sleep(0.5) # Encendido por 0.5 segundos
        #     GPIO.output(LED_PIN, GPIO.LOW)
        #     # Para parpadeo continuo, esto se haría en un hilo separado
        # except Exception as e:
        #     logger.error(f"Fallo al activar LED de alerta: {e}")
        pass
    else:
        # Esto es solo para que la función haga algo en un PC
        print(f"*** VISUAL ALERT: {message} ***") 


def trigger_audio_alert(message: str):
    """
    Activa una alarma audible en la cabina del bus (ej. un zumbador o un archivo de sonido).
    En un PC, solo imprime en consola o reproduce un sonido simple.
    """
    logger.warning(f"[ALERTA AUDIBLE]: {message}")
    if platform.system() == "Linux" and "aarch64" in platform.machine():
        # Lógica para activar un zumbador o reproducir un archivo de sonido en Jetson
        # try:
        #     # Opción 1: Activar zumbador directamente desde GPIO
        #     # GPIO.output(BUZZER_PIN, GPIO.HIGH)
        #     # time.sleep(1) # Sonar por 1 segundo
        #     # GPIO.output(BUZZER_PIN, GPIO.LOW)
        #     # Opción 2: Reproducir un archivo de sonido (necesitaría un altavoz)
        #     # subprocess.run(["aplay", "/path/to/your/alert_sound.wav"])
        #     pass
        # except Exception as e:
        #     logger.error(f"Fallo al activar alarma audible: {e}")
        pass
    elif platform.system() == "Windows":
        # Para Windows, puedes usar el módulo winsound si hay altavoces
        try:
            import winsound
            winsound.Beep(1000, 500) # Frecuencia 1000Hz, duración 500ms
        except ImportError:
            print(f"*** AUDIO ALERT: {message} *** (winsound no disponible)")
    else: # Otros sistemas como macOS
        print(f"*** AUDIO ALERT: {message} ***")

def store_local_alert(alert_data: Dict[str, Any]):
    """
    Guarda el registro de la alerta en la base de datos local de la Jetson.
    Asume que alert_data ya contiene id_bus, id_conductor, tipo_alerta, etc.
    """
    logger.info(f"Guardando alerta local: {alert_data.get('tipo_alerta')}")
    db = next(get_edge_db()) 
    try:
        if 'id' not in alert_data:
            alert_data['id'] = uuid.uuid4()
        else:
             alert_data['id'] = uuid.UUID(alert_data['id']) if isinstance(alert_data['id'], str) else alert_data['id']

        if 'id_evento' in alert_data and alert_data['id_evento'] and isinstance(alert_data['id_evento'], str):
            alert_data['id_evento'] = uuid.UUID(alert_data['id_evento'])
        if 'id_sesion_conduccion' in alert_data and alert_data['id_sesion_conduccion'] and isinstance(alert_data['id_sesion_conduccion'], str):
            alert_data['id_sesion_conduccion'] = uuid.UUID(alert_data['id_sesion_conduccion'])

        new_local_alert: AlertaLocal = create_local_alert(db, alert_data)
        logger.info(f"Alerta local '{new_local_alert.tipo_alerta}' guardada con ID: {new_local_alert.id}")
    except Exception as e:
        logger.error(f"Error al guardar alerta local: {e}", exc_info=True)
        db.rollback() 
    finally:
        db.close()


def acknowledge_local_alert(alert_id: uuid.UUID):
    """
    Permite al conductor "silenciar" o reconocer una alerta local (si hay botón).
    Marca la alerta como visualizada en la BD local.
    """
    logger.info(f"Alerta local ID '{alert_id}' reconocida por el usuario.")
    db = next(get_edge_db())
    try:
        mark_alert_as_visualized(db, alert_id) 
        logger.info(f"Alerta local '{alert_id}' marcada como visualizada.")
    except Exception as e:
        logger.error(f"Error al reconocer alerta local {alert_id}: {e}", exc_info=True)
    finally:
        db.close()
# Ejemplo de uso para pruebas
if __name__ == '__main__':
    print("--- Probando jetson_app/alerts/local_alerts.py ---")
    print("Esto simulará alertas visuales y audibles en la consola (o con winsound en Windows).")
    print("Las alertas se guardarán en tu archivo 'edge_data.db'.")

    # Importar los métodos de CRUD que se usarán en el setup de prueba
    from app.local_db.crud_edge import create_or_update_bus_local, create_or_update_conductor_local, create_asignacion_conductor_bus_local, get_jetson_config_local, get_active_asignacion_for_bus

    # <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<
    # ¡CAMBIO CLAVE AQUI! Apuntando a tu DB persistente 'edge_data.db'
    DATABASE_URL_TEST = "sqlite:///./edge_data.db" # Usar la DB persistente
    
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    
    engine_test = create_engine(DATABASE_URL_TEST, connect_args={"check_same_thread": False})
    # Base ya está importada desde edge_database_models
    Base.metadata.create_all(engine_test) # Crea las tablas en la BD de prueba (si no existen)
    SessionLocalTest = sessionmaker(autocommit=False, autoflush=False, bind=engine_test)

    def get_edge_db_test_instance(): 
        db_t = SessionLocalTest()
        try:
            yield db_t
        finally:
            db_t.close()
    
    # Sobreescribe la función get_edge_db para que apunte a la instancia de prueba
    get_edge_db = get_edge_db_test_instance 
    
    db_session_for_setup = next(get_edge_db()) 
    try:
        # Asegurarse de que las tablas existan (útil si edge_data.db fue borrado)
        # create_edge_tables() # Esta línea se llamaría si el archivo .db es nuevo y no tiene tablas
        # Sin embargo, si quieres que los datos persistan, y ya usas initial_data_setup.py,
        # lo mejor es que initial_data_setup.py cree las tablas y los datos.
        # Aquí nos aseguramos de que los datos mínimos de prueba existan para esta prueba.

        test_bus_id_val = uuid.UUID("11111111-1111-1111-1111-111111111111")
        test_conductor_id_val = uuid.UUID("22222222-2222-2222-2222-222222222222")
        test_session_id_val = uuid.UUID("55555555-5555-5555-5555-555555555555")
        test_empresa_id_val = uuid.UUID("00000000-0000-0000-0000-000000000001") 
        
        jetson_hw_id_for_alert_test = "JETSON-ALERT-TEST-001"
        config = get_jetson_config_local(db_session_for_setup)
        if not config or config.id_hardware_jetson != jetson_hw_id_for_alert_test:
             initialize_jetson_config(db_session_for_setup, jetson_hw_id_for_alert_test, test_bus_id_val)
        
        bus_data = {"id": test_bus_id_val, "placa": "ALERT001", "numero_interno": "A001", "id_empresa": test_empresa_id_val}
        create_or_update_bus_local(db_session_for_setup, bus_data)

        conductor_data = {"id": test_conductor_id_val, "cedula": "ALRTCDRV", "nombre_completo": "Alert Test Conductor", "id_empresa": test_empresa_id_val, "codigo_qr_hash": "ALRTCDRV"}
        create_or_update_conductor_local(db_session_for_setup, conductor_data)

        active_session = get_active_asignacion_for_bus(db_session_for_setup, test_bus_id_val)
        if not active_session:
            create_asignacion_conductor_bus_local(db_session_for_setup, test_conductor_id_val, test_bus_id_val, test_session_id_val, datetime.utcnow(), estado_turno='Activo')

        db_session_for_setup.commit() 
        print("Datos de prueba (mínimos) verificados/insertados en BD persistente.")

    except Exception as e:
        print(f"Error durante el setup de la BD persistente para la prueba: {e}")
        db_session_for_setup.rollback()
        exit() 
    finally:
        db_session_for_setup.close()

    trigger_visual_alert("¡Alerta de prueba visual!")
    trigger_audio_alert("¡Alerta de prueba audible!")

    alert_test_data = {
        "id_evento": uuid.uuid4(), 
        "id_conductor": test_conductor_id_val, 
        "id_bus": test_bus_id_val,     
        "id_sesion_conduccion": test_session_id_val, 
        "timestamp_alerta": datetime.utcnow(), 
        "tipo_alerta": "Fatiga de Prueba",
        "descripcion": "El conductor de prueba parece fatigado.",
         
        "estado_visualizado": False 
    }

    print("\nIntentando guardar alerta local...")
    store_local_alert(alert_test_data)
    print("Verifica los logs para confirmar que la alerta fue guardada.")

    acknowledged_alert_id = alert_test_data['id'] 
    print(f"\nIntentando reconocer alerta local con ID: {acknowledged_alert_id}")
    acknowledge_local_alert(acknowledged_alert_id)
    
    print("\nPrueba de local_alerts.py finalizada. Revisa los logs y la consola.")
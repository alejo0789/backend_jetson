import logging
import requests
import json
import uuid 
from datetime import datetime
from typing import List, Dict, Any, Optional

from sqlalchemy.orm import Session 

# Importaciones de módulos locales para obtener datos de la BD y configuración
from config.edge_database import get_edge_db 
from app.local_db.crud_edge import (
    get_unsynced_events,
    mark_event_as_synced,
    create_or_update_sync_metadata,
    get_sync_metadata,
    get_jetson_config_local,
    create_or_update_conductor_local, 
    create_or_update_bus_local 
)
from app.models.edge_database_models import (
    EventoLocal, 
    SincronizacionMetadata,
    ConductorLocal, 
    BusLocal, 
    ConfiguracionJetsonLocal,
    AsignacionConductorBusLocal # Necesitamos este modelo para enviar sus datos como 'SesionConduccion'
)

# Configuración del logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# --- Configuración del API Central (¡AJUSTAR ESTO A TU API REAL!) ---
# Estas URL base DEBEN coincidir con los endpoints que definirás en tu backend de la nube.
CLOUD_API_BASE_URL = "http://localhost:8000/api/v1" 
                                                  
# Endpoints específicos
CLOUD_API_EVENTS_ENDPOINT = f"{CLOUD_API_BASE_URL}/events"
CLOUD_API_TELEMETRY_ENDPOINT = f"{CLOUD_API_BASE_URL}/jetson_telemetry"
# Endpoint para recibir los datos de SesionConduccion (que vienen de AsignacionConductorBusLocal)
CLOUD_API_SESION_CONDUCCION_ENDPOINT = f"{CLOUD_API_BASE_URL}/sesiones_conduccion" # <<<<<<< NUEVO ENDPOINT

# Endpoints para el aprovisionamiento (pull)
CLOUD_API_GET_BUS_BY_PLACA = f"{CLOUD_API_BASE_URL}/buses/by_placa" 
CLOUD_API_GET_DRIVERS_BY_BUS_ID = f"{CLOUD_API_BASE_URL}/buses" 

# --- Autenticación (Simple por ahora, mejorar para producción) ---
AUTH_TOKEN = "your_secret_auth_token" 


def _get_auth_headers() -> Dict[str, str]:
    """Retorna los headers de autenticación para las peticiones a la API."""
    return {"Authorization": f"Bearer {AUTH_TOKEN}", "Content-Type": "application/json"}

# --- Funciones de PUSH ---
def send_events_to_cloud(db: Session, batch_size: int = 50) -> bool:
    """
    Envía eventos pendientes de sincronización desde la BD local a la API central.
    Procesa los eventos en lotes.

    Args:
        db (Session): Sesión de la base de datos local.
        batch_size (int): Número de eventos a procesar por lote.

    Returns:
        bool: True si la sincronización del lote fue exitosa, False en caso contrario.
    """
    logger.info("Iniciando sincronización de eventos pendientes con la nube...")
    unsynced_events: List[EventoLocal] = get_unsynced_events(db, limit=batch_size)
    
    if not unsynced_events:
        logger.info("No hay eventos pendientes para sincronizar.")
        return True

    events_to_send = []
    for event in unsynced_events:
        event_dict = {
            "id": str(event.id),
            "id_bus": str(event.id_bus),
            "id_conductor": str(event.id_conductor),
            "id_sesion_conduccion_jetson": str(event.id_sesion_conduccion) if event.id_sesion_conduccion else None, # <<<<< CAMBIO: Renombrado para la nube
            "timestamp_evento": event.timestamp_evento.isoformat(),
            "tipo_evento": event.tipo_evento,
            "subtipo_evento": event.subtipo_evento,
            "duracion_segundos": float(event.duracion_segundos) if event.duracion_segundos is not None else None,
            "severidad": event.severidad,
            "confidence_score_ia": float(event.confidence_score_ia) if event.confidence_score_ia is not None else None,
            "alerta_disparada": event.alerta_disparada,
            "ubicacion_gps_evento": event.ubicacion_gps_evento,
            "metadatos_ia_json": event.metadatos_ia_json,
        }
        events_to_send.append(event_dict)

    try:
        response = requests.post(
            CLOUD_API_EVENTS_ENDPOINT,
            json={"events": events_to_send}, 
            headers=_get_auth_headers(),
            timeout=10 
        )
        response.raise_for_status() 

        for event in unsynced_events:
            mark_event_as_synced(db, event.id)
        
        logger.info(f"Sincronizados {len(unsynced_events)} eventos con la nube.")
        create_or_update_sync_metadata(db, 'eventos_local', last_pushed_at=datetime.utcnow(), ultimo_id_sincronizado_local=unsynced_events[-1].id)
        return True

    except requests.exceptions.Timeout:
        logger.error(f"Tiempo de espera agotado al enviar eventos a {CLOUD_API_EVENTS_ENDPOINT}")
        return False
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Error de conexión al enviar eventos a la nube: {e}")
        return False
    except requests.exceptions.HTTPError as e:
        logger.error(f"Error HTTP al enviar eventos (Código: {e.response.status_code}, Respuesta: {e.response.text}): {e}")
        return False
    except Exception as e:
        logger.error(f"Error inesperado durante la sincronización de eventos: {e}", exc_info=True)
        return False

def send_telemetry_to_cloud(db: Session, metrics: Dict[str, Any]) -> bool:
    # ... (código existente para send_telemetry_to_cloud) ...
    logger.info("Intentando enviar telemetría a la nube...")

    jetson_config = get_jetson_config_local(db)
    if not jetson_config or not jetson_config.id_hardware_jetson:
        logger.warning("No se pudo obtener la configuración local de la Jetson para la telemetría.")
        metrics['jetson_id'] = "UNKNOWN_JETSON_ID"
        metrics['bus_id'] = None
    else:
        metrics['jetson_id'] = jetson_config.id_hardware_jetson
        metrics['bus_id'] = str(jetson_config.id_bus_asignado) if jetson_config.id_bus_asignado else None
    
    try:
        response = requests.post(
            CLOUD_API_TELEMETRY_ENDPOINT,
            json=metrics, 
            headers=_get_auth_headers(),
            timeout=10 
        )
        response.raise_for_status() 

        logger.info(f"Telemetría enviada con éxito para Jetson ID: {metrics.get('jetson_id', 'N/A')}")
        return True

    except requests.exceptions.Timeout:
        logger.error(f"Tiempo de espera agotado al enviar telemetría a {CLOUD_API_TELEMETRY_ENDPOINT}")
        return False
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Error de conexión al enviar telemetría a la nube: {e}")
        return False
    except requests.exceptions.HTTPError as e:
        logger.error(f"Error HTTP al enviar telemetría (Código: {e.response.status_code}, Respuesta: {e.response.text}): {e}")
        return False
    except Exception as e:
        logger.error(f"Error inesperado durante el envío de telemetría: {e}", exc_info=True)
        return False

def send_session_data_to_cloud(db: Session, session_obj: AsignacionConductorBusLocal) -> bool:
    """
    Envía los datos de una sesión de conducción (AsignacionConductorBusLocal) a la API central
    para ser registrados como SesionConduccion en la nube.
    Esto se llamará al iniciar y finalizar una sesión en la Jetson.
    """
    logger.info(f"Intentando enviar datos de sesión {session_obj.id_sesion_conduccion} a la nube...")
    
    session_data = {
        "id_sesion_conduccion_jetson": str(session_obj.id_sesion_conduccion), # Este es el ID global de la sesión
        "id_conductor": str(session_obj.id_conductor),
        "id_bus": str(session_obj.id_bus),
        "fecha_inicio_real": session_obj.fecha_inicio_asignacion.isoformat(),
        "fecha_fin_real": session_obj.fecha_fin_asignacion.isoformat() if session_obj.fecha_fin_asignacion else None,
        "estado_sesion": session_obj.estado_turno, # 'estado_turno' en Edge mapea a 'estado_sesion' en Nube
        "duracion_total_seg": float(session_obj.tiempo_conduccion_acumulado_seg) if session_obj.tiempo_conduccion_acumulado_seg is not None else None,
        # Podrías añadir más campos si los necesitas en la nube desde la sesión local
    }

    try:
        response = requests.post(
            CLOUD_API_SESION_CONDUCCION_ENDPOINT, # Endpoint para SesionesConduccion
            json=session_data,
            headers=_get_auth_headers(),
            timeout=10
        )
        response.raise_for_status()

        logger.info(f"Datos de sesión {session_obj.id_sesion_conduccion} enviados con éxito a la nube.")
        # Opcional: Marcar la sesión local como sincronizada si es necesario.
        # Por ahora, asumimos que los eventos de la sesión son los que controlan la sincronización principal.
        return True

    except requests.exceptions.Timeout:
        logger.error(f"Tiempo de espera agotado al enviar datos de sesión {session_obj.id_sesion_conduccion}.")
        return False
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Error de conexión al enviar datos de sesión {session_obj.id_sesion_conduccion}: {e}")
        return False
    except requests.exceptions.HTTPError as e:
        logger.error(f"Error HTTP al enviar datos de sesión (Código: {e.response.status_code}, Respuesta: {e.response.text}): {e}")
        return False
    except Exception as e:
        logger.error(f"Error inesperado durante el envío de datos de sesión {session_obj.id_sesion_conduccion}: {e}", exc_info=True)
        return False


# --- Funciones de PULL para Aprovisionamiento ---

def pull_bus_data_by_placa(db: Session, placa: str) -> Optional[BusLocal]:
    # ... (código existente para pull_bus_data_by_placa) ...
    logger.info(f"Intentando obtener datos del bus con placa '{placa}' desde la nube...")
    try:
        response = requests.get(
            f"{CLOUD_API_GET_BUS_BY_PLACA}?placa={placa}",
            headers=_get_auth_headers(),
            timeout=10
        )
        response.raise_for_status() 

        bus_data: Dict[str, Any] = response.json()
        
        # Convertir id_empresa a UUID si viene como string
        if 'id_empresa' in bus_data and isinstance(bus_data['id_empresa'], str): # <<<<< CAMBIO: id_empresa
            try:
                bus_data['id_empresa'] = uuid.UUID(bus_data['id_empresa'])
            except ValueError:
                logger.warning(f"ID de empresa '{bus_data['id_empresa']}' no es un UUID válido. Se usará None.")
                bus_data['id_empresa'] = None 

        bus_data['id'] = uuid.UUID(bus_data['id']) if isinstance(bus_data['id'], str) else bus_data['id']
        
        bus_local = create_or_update_bus_local(db, bus_data)
        logger.info(f"Datos del bus '{placa}' obtenidos y actualizados/creados localmente.")
        return bus_local

    except requests.exceptions.Timeout:
        logger.error(f"Tiempo de espera agotado al obtener datos del bus '{placa}'.")
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Error de conexión al obtener datos del bus '{placa}': {e}")
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            logger.warning(f"Bus con placa '{placa}' no encontrado en la nube.")
        else:
            logger.error(f"Error HTTP al obtener datos del bus (Código: {e.response.status_code}, Respuesta: {e.response.text}): {e}")
    except Exception as e:
        logger.error(f"Error inesperado al obtener datos del bus '{placa}': {e}", exc_info=True)
    return None

def pull_assigned_drivers_for_bus(db: Session, bus_id: uuid.UUID) -> List[ConductorLocal]:
    # ... (código existente para pull_assigned_drivers_for_bus) ...
    logger.info(f"Intentando obtener conductores asignados al bus '{bus_id}' desde la nube...")
    conductores_sincronizados: List[ConductorLocal] = []
    try:
        response = requests.get(
            f"{CLOUD_API_GET_DRIVERS_BY_BUS_ID}/{bus_id}/drivers", 
            headers=_get_auth_headers(),
            timeout=10
        )
        response.raise_for_status()

        drivers_data: List[Dict[str, Any]] = response.json()
        
        for driver_data in drivers_data:
            driver_data['id'] = uuid.UUID(driver_data['id']) if isinstance(driver_data['id'], str) else driver_data['id']
            # Convertir id_empresa a UUID si viene como string
            if 'id_empresa' in driver_data and isinstance(driver_data['id_empresa'], str): # <<<<< CAMBIO: id_empresa
                try:
                    driver_data['id_empresa'] = uuid.UUID(driver_data['id_empresa'])
                except ValueError:
                    logger.warning(f"ID de empresa '{driver_data['id_empresa']}' para conductor {driver_data.get('id', 'N/A')} no es un UUID válido. Se usará None.")
                    driver_data['id_empresa'] = None
            
            if 'caracteristicas_faciales_embedding' in driver_data and isinstance(driver_data['caracteristicas_faciales_embedding'], str):
                try:
                    driver_data['caracteristicas_faciales_embedding'] = json.loads(driver_data['caracteristicas_faciales_embedding'])
                except json.JSONDecodeError:
                    logger.warning(f"Embedding facial para conductor {driver_data.get('id', 'N/A')} no es JSON válido. Se usará None.")
                    driver_data['caracteristicas_faciales_embedding'] = None

            conductor_local = create_or_update_conductor_local(db, driver_data)
            conductores_sincronizados.append(conductor_local)
        
        logger.info(f"Sincronizados {len(conductores_sincronizados)} conductores asignados al bus '{bus_id}' localmente.")
        return conductores_sincronizados

    except requests.exceptions.Timeout:
        logger.error(f"Tiempo de espera agotado al obtener conductores para el bus '{bus_id}'.")
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Error de conexión al obtener conductores para el bus '{bus_id}': {e}")
    except requests.exceptions.HTTPError as e:
        logger.error(f"Error HTTP al obtener conductores para el bus (Código: {e.response.status_code}, Respuesta: {e.response.text}): {e}")
    except Exception as e:
        logger.error(f"Error inesperado al obtener conductores para el bus '{bus_id}': {e}", exc_info=True)
    return []

# --- Ejemplo de uso (este bloque se moverá a main_jetson.py o un script de provisionamiento) ---
if __name__ == '__main__':
    print("--- Probando cloud_sync.py Pull Functions (SIMULADO) ---")
    print("Este script intentará conectar con un servidor API en http://localhost:8000")
    print("Asegúrate de tener un servidor de backend corriendo con los endpoints:")
    print(f" - GET {CLOUD_API_GET_BUS_BY_PLACA}?placa=XYZ")
    print(f" - GET {CLOUD_API_GET_DRIVERS_BY_BUS_ID}/<UUID_BUS>/drivers")
    print(f" - POST {CLOUD_API_SESION_CONDUCCION_ENDPOINT} (para enviar sesiones)")

    from config.edge_database import EdgeSessionLocal, create_edge_tables, initialize_jetson_config, get_edge_db
    from app.models.edge_database_models import Base, EventoLocal, ConfiguracionJetsonLocal, BusLocal, ConductorLocal, AsignacionConductorBusLocal
    from app.local_db.crud_edge import create_or_update_conductor_local 

    test_bus_id_pull = uuid.UUID("11111111-1111-1111-1111-111111111111") 
    test_conductor_id_sync_pull = uuid.UUID("44444444-4444-4444-4444-444444444444") 

    db_session_test = None
    try:
        create_edge_tables() 
        db_session_test = next(get_edge_db()) 
        
        jetson_hw_id_sync_pull = "JETSON-PULL-TEST-001"
        initialize_jetson_config(db_session_test, jetson_hw_id_sync_pull, test_bus_id_pull)

        print("\n--- Probando pull_bus_data_by_placa ---")
        test_placa = "BUS-NUBE-001" 
        pulled_bus = pull_bus_data_by_placa(db_session_test, test_placa)
        if pulled_bus:
            print(f"Bus '{pulled_bus.placa}' (ID: {pulled_bus.id}) obtenido y guardado localmente.")
            test_bus_id_pull = pulled_bus.id 
        else:
            print(f"Fallo al obtener bus con placa '{test_placa}'. Asegura que el backend está corriendo y el bus existe.")

        if pulled_bus:
            print(f"\n--- Probando pull_assigned_drivers_for_bus para bus ID: {test_bus_id_pull} ---")
            pulled_drivers = pull_assigned_drivers_for_bus(db_session_test, test_bus_id_pull)
            if pulled_drivers:
                for driver in pulled_drivers:
                    print(f"  Conductor '{driver.nombre_completo}' (Cédula: {driver.cedula}) obtenido y guardado localmente.")
            else:
                print(f"Fallo al obtener conductores para el bus '{test_bus_id_pull}'. Asegura que el backend tiene conductores asignados.")
        
        print("\n--- Sincronizando algunos eventos de prueba (PUSH) ---")
        conductor_para_evento_push = db_session_test.query(ConductorLocal).filter_by(id=test_conductor_id_sync_pull).first()
        if not conductor_para_evento_push:
             conductor_para_evento_push = create_or_update_conductor_local(db_session_test, {
                 "id": test_conductor_id_sync_pull,
                 "cedula": "4444444444",
                 "nombre_completo": "Conductor para Evento Push",
                 "codigo_qr_hash": "4444444444",
                 "activo": True,
                 "id_empresa": uuid.UUID("00000000-0000-0000-0000-000000000001") # ID de empresa de prueba
             })
             print(f"Conductor de prueba para eventos push creado: {conductor_para_evento_push.nombre_completo}")

        new_event = EventoLocal(
            id_bus=test_bus_id_pull,
            id_conductor=conductor_para_evento_push.id, 
            id_sesion_conduccion=uuid.uuid4(),
            timestamp_evento=datetime.utcnow(),
            tipo_evento='TestPull', subtipo_evento='TestPull', severidad='Baja', alerta_disparada=False
        )
        db_session_test.add(new_event)
        db_session_test.commit()
        send_events_to_cloud(db_session_test)

        # --- Prueba de PUSH de datos de Sesión de Conducción ---
        print("\n--- Sincronizando datos de Sesión de Conducción (PUSH) ---")
        # Simular una AsignacionConductorBusLocal (sesión) para enviar
        test_session_id = uuid.uuid4()
        simulated_session = AsignacionConductorBusLocal(
            id=uuid.uuid4(), # ID PK local
            id_conductor=conductor_para_evento_push.id,
            id_bus=test_bus_id_pull,
            id_sesion_conduccion=test_session_id, # Este es el ID que va a SesionConduccion.id_sesion_conduccion_jetson
            fecha_inicio_asignacion=datetime.utcnow() - timedelta(hours=1),
            fecha_fin_asignacion=datetime.utcnow(),
            estado_turno="Finalizado",
            tiempo_conduccion_acumulado_seg=3600,
            tipo_asignacion="Test"
        )
        # No guardamos esta sesión en la DB local aquí, solo la usamos para el envío.
        # En la lógica real, esta sesión ya estaría en la DB local.
        send_session_data_to_cloud(db_session_test, simulated_session)


    except Exception as e:
        logger.error(f"Error durante la prueba de funciones PULL/PUSH: {e}", exc_info=True)
        if db_session_test:
            db_session_test.rollback()
    finally:
        if db_session_test:
            db_session_test.close()
    print("\n--- Prueba de cloud_sync.py Pull/Push Functions finalizada ---")
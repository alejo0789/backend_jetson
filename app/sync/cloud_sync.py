import logging
import requests
import json
import uuid 
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from sqlalchemy.orm import Session 

# Importaciones de módulos locales para obtener datos de la BD y configuración
from app.config.edge_database import get_edge_db 
from app.local_db.crud_edge import (
    get_unsynced_events,
    mark_event_as_synced,
    create_or_update_sync_metadata,
    get_sync_metadata,
    get_jetson_config_local,
    create_or_update_conductor_local_selective,  # ACTUALIZADO
    create_or_update_bus_local,
    # NUEVAS FUNCIONES MULTIMEDIA
    get_events_with_unsynced_files,
    mark_event_files_as_synced
)
from app.models.edge_database_models import (
    EventoLocal, 
    SincronizacionMetadata,
    ConductorLocal, 
    BusLocal, 
    ConfiguracionJetsonLocal,
    AsignacionConductorBusLocal
)

# Configuración del logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# --- Configuración del API Central ---
CLOUD_API_BASE_URL = "http://localhost:8000/api/v1"

# Endpoints específicos
CLOUD_API_EVENTS_ENDPOINT = f"{CLOUD_API_BASE_URL}/events"
CLOUD_API_TELEMETRY_ENDPOINT = f"{CLOUD_API_BASE_URL}/jetson_telemetry"
CLOUD_API_SESION_CONDUCCION_ENDPOINT = f"{CLOUD_API_BASE_URL}/sesiones_conduccion"

# Endpoints para el aprovisionamiento (pull)
CLOUD_API_GET_BUS_BY_PLACA = f"{CLOUD_API_BASE_URL}/buses/by_placa"
CLOUD_API_GET_DRIVERS_BY_BUS_ID = f"{CLOUD_API_BASE_URL}/buses"
CLOUD_API_GET_CONDUCTOR_BY_ID = f"{CLOUD_API_BASE_URL}/conductores"  # NUEVO

# --- Autenticación ---
AUTH_TOKEN = "your_secret_auth_token"

def _get_auth_headers() -> Dict[str, str]:
    """Retorna los headers de autenticación para las peticiones a la API."""
    return {"Authorization": f"Bearer {AUTH_TOKEN}", "Content-Type": "application/json"}

# --- Funciones de PUSH ---
def send_events_to_cloud(db: Session, batch_size: int = 50) -> bool:
    """
    Envía eventos pendientes de sincronización desde la BD local a la API central.
    ACTUALIZADO: Incluye rutas de archivos multimedia.
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
            "id_sesion_conduccion_jetson": str(event.id_sesion_conduccion) if event.id_sesion_conduccion else None,
            "timestamp_evento": event.timestamp_evento.isoformat(),
            "tipo_evento": event.tipo_evento,
            "subtipo_evento": event.subtipo_evento,
            "duracion_segundos": float(event.duracion_segundos) if event.duracion_segundos is not None else None,
            "severidad": event.severidad,
            "confidence_score_ia": float(event.confidence_score_ia) if event.confidence_score_ia is not None else None,
            "alerta_disparada": event.alerta_disparada,
            "ubicacion_gps_evento": event.ubicacion_gps_evento,
            "metadatos_ia_json": event.metadatos_ia_json,
            # NUEVOS CAMPOS MULTIMEDIA (solo rutas locales para referencia)
            "has_snapshot": bool(event.snapshot_local_path),
            "has_video": bool(event.video_clip_local_path),
            "archivos_synced": event.archivos_synced
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
    """
    Envía datos de telemetría a la nube.
    """
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
    Envía los datos de una sesión de conducción a la API central.
    """
    logger.info(f"Intentando enviar datos de sesión {session_obj.id_sesion_conduccion} a la nube...")
    
    session_data = {
        "id_sesion_conduccion_jetson": str(session_obj.id_sesion_conduccion),
        "id_conductor": str(session_obj.id_conductor),
        "id_bus": str(session_obj.id_bus),
        "fecha_inicio_real": session_obj.fecha_inicio_asignacion.isoformat(),
        "fecha_fin_real": session_obj.fecha_fin_asignacion.isoformat() if session_obj.fecha_fin_asignacion else None,
        "estado_sesion": session_obj.estado_turno,
        "duracion_total_seg": float(session_obj.tiempo_conduccion_acumulado_seg) if session_obj.tiempo_conduccion_acumulado_seg is not None else None,
    }

    try:
        response = requests.post(
            CLOUD_API_SESION_CONDUCCION_ENDPOINT,
            json=session_data,
            headers=_get_auth_headers(),
            timeout=10
        )
        response.raise_for_status()

        logger.info(f"Datos de sesión {session_obj.id_sesion_conduccion} enviados con éxito a la nube.")
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
    """
    Obtiene datos del bus desde la nube por placa.
    """
    logger.info(f"Intentando obtener datos del bus con placa '{placa}' desde la nube...")
    try:
        response = requests.get(
            f"{CLOUD_API_GET_BUS_BY_PLACA}?placa={placa}",
            headers=_get_auth_headers(),
            timeout=10
        )
        response.raise_for_status() 

        bus_data: Dict[str, Any] = response.json()
        
        # Procesar UUID
        bus_data['id'] = uuid.UUID(bus_data['id']) if isinstance(bus_data['id'], str) else bus_data['id']
        
        # Usar la nueva función selectiva
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
    """
    Obtiene conductores asignados al bus desde la nube.
    """
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
            
            if 'caracteristicas_faciales_embedding' in driver_data and isinstance(driver_data['caracteristicas_faciales_embedding'], str):
                try:
                    driver_data['caracteristicas_faciales_embedding'] = json.loads(driver_data['caracteristicas_faciales_embedding'])
                except json.JSONDecodeError:
                    logger.warning(f"Embedding facial para conductor {driver_data.get('id', 'N/A')} no es JSON válido. Se usará None.")
                    driver_data['caracteristicas_faciales_embedding'] = None

            # USAR LA NUEVA FUNCIÓN SELECTIVA (sin force_update en aprovisionamiento)
            conductor_local = create_or_update_conductor_local_selective(db, driver_data, force_update=False)
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

# --- NUEVA FUNCIÓN PARA QR SCANNING ---
def pull_conductor_by_id(conductor_uuid: uuid.UUID) -> Optional[Dict[str, Any]]:
    """
    Obtiene un conductor específico desde la nube por su UUID.
    Esta función se usa en el flujo de QR scanning.
    
    Args:
        conductor_uuid: UUID del conductor
    
    Returns:
        Dict con datos del conductor o None si no se encuentra
    """
    logger.info(f"Intentando obtener conductor {conductor_uuid} desde la nube...")
    try:
        response = requests.get(
            f"{CLOUD_API_GET_CONDUCTOR_BY_ID}/{conductor_uuid}",
            headers=_get_auth_headers(),
            timeout=10
        )
        response.raise_for_status()

        conductor_data: Dict[str, Any] = response.json()
        
        # Procesar datos
        conductor_data['id'] = uuid.UUID(conductor_data['id']) if isinstance(conductor_data['id'], str) else conductor_data['id']
        
        if 'caracteristicas_faciales_embedding' in conductor_data and isinstance(conductor_data['caracteristicas_faciales_embedding'], str):
            try:
                conductor_data['caracteristicas_faciales_embedding'] = json.loads(conductor_data['caracteristicas_faciales_embedding'])
            except json.JSONDecodeError:
                conductor_data['caracteristicas_faciales_embedding'] = None

        logger.info(f"Conductor {conductor_data.get('nombre_completo', 'N/A')} obtenido desde la nube.")
        return conductor_data

    except requests.exceptions.Timeout:
        logger.error(f"Tiempo de espera agotado al obtener conductor {conductor_uuid}.")
    except requests.exceptions.ConnectionError as e:
        logger.error(f"Error de conexión al obtener conductor {conductor_uuid}: {e}")
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            logger.warning(f"Conductor {conductor_uuid} no encontrado en la nube.")
        else:
            logger.error(f"Error HTTP al obtener conductor (Código: {e.response.status_code}, Respuesta: {e.response.text}): {e}")
    except Exception as e:
        logger.error(f"Error inesperado al obtener conductor {conductor_uuid}: {e}", exc_info=True)
    
    return None

# --- NUEVAS FUNCIONES PARA MULTIMEDIA ---
def sync_multimedia_files(db: Session, file_upload_function, batch_size: int = 5) -> Dict[str, int]:
    """
    Sincroniza archivos multimedia pendientes con el cloud storage.
    
    Args:
        db: Sesión de base de datos
        file_upload_function: Función que maneja el upload (ej. AWS S3)
        batch_size: Número de eventos a procesar por lote
    
    Returns:
        Dict con estadísticas de sincronización
    """
    logger.info("Iniciando sincronización de archivos multimedia...")
    
    stats = {'processed': 0, 'uploaded': 0, 'failed': 0, 'skipped': 0}
    
    # Obtener eventos con archivos pendientes
    events_with_files = get_events_with_unsynced_files(db, limit=batch_size)
    
    if not events_with_files:
        logger.info("No hay archivos multimedia pendientes para sincronizar.")
        return stats
    
    for evento in events_with_files:
        stats['processed'] += 1
        
        try:
            upload_success = file_upload_function(evento)
            
            if upload_success:
                mark_event_files_as_synced(db, evento.id)
                stats['uploaded'] += 1
                logger.info(f"Archivos del evento {evento.id} sincronizados exitosamente")
            else:
                stats['failed'] += 1
                logger.warning(f"Fallo al sincronizar archivos del evento {evento.id}")
                
        except Exception as e:
            stats['failed'] += 1
            logger.error(f"Error sincronizando archivos del evento {evento.id}: {e}")
    
    logger.info(f"Sincronización multimedia completada: {stats}")
    return stats

# --- Ejemplo de uso actualizado ---
if __name__ == '__main__':
    print("--- Probando cloud_sync.py actualizado ---")
    print("Este script probará las nuevas funciones de sincronización multimedia")
    print("y el flujo actualizado de obtención de conductores por UUID.")
    
    # Test básico de obtención de conductor por UUID
    test_conductor_uuid = uuid.UUID("22222222-2222-2222-2222-222222222222")
    conductor_data = pull_conductor_by_id(test_conductor_uuid)
    
    if conductor_data:
        print(f"✅ Conductor obtenido: {conductor_data.get('nombre_completo', 'N/A')}")
    else:
        print("❌ No se pudo obtener conductor desde cloud")
    
    print("\n--- Para probar sincronización multimedia, necesitas:")
    print("1. Implementar file_upload_function (AWS S3, etc.)")
    print("2. Tener eventos con archivos multimedia en BD local")
    print("3. Ejecutar sync_multimedia_files(db, upload_function)")
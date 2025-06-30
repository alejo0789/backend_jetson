import time
import uuid
import logging
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

# Importaciones de módulos locales
from app.config.edge_database import EdgeSessionLocal as SessionLocal, edge_engine as engine, Base
from app.models.edge_database_models import ConfiguracionJetsonLocal, EventoLocal, TelemetryLocal
from app.local_db.crud_edge import (
    get_jetson_config_local, update_jetson_config_local,
    create_driver_session_from_qr_robust, get_active_asignacion_for_bus,
    get_synced_events_for_cleanup, cleanup_event_files,
    create_local_telemetry, # Import the local creation function
    get_synced_telemetry_for_cleanup, cleanup_telemetry_records
)
from app.data_ingestion.qr_scanner import scan_qr_code # Corrected import
from app.data_ingestion.video_capture import VideoCapture # Corrected import
from app.sync.cloud_sync import (
    pull_bus_data_by_placa,
    pull_assigned_drivers_for_bus,
    pull_conductor_by_id,
    send_events_to_cloud,
    send_session_data_to_cloud,
    sync_multimedia_files,
    send_unsynced_telemetry_to_cloud # Renamed function
)
from app.monitoring.device_telemetry import gather_system_metrics # Still needed to gather metrics

# Configuración del logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# --- Intervalos de ejecución (en segundos) ---
QR_SCAN_INTERVAL_SECONDS = 5
TELEMETRY_GATHER_SAVE_INTERVAL_SECONDS = 3  # 5 minutos: interval for gathering and saving locally
TELEMETRY_SYNC_INTERVAL_SECONDS = 60 # 1 minute: interval for sending unsynced telemetry to cloud
EVENT_SYNC_INTERVAL_SECONDS = 30
SESSION_SYNC_INTERVAL_SECONDS = 60 # Cada minuto
CLEANUP_INTERVAL_SECONDS = 3600 # Cada hora
MULTIMEDIA_SYNC_INTERVAL_SECONDS = 60 # Cada minuto

# --- Variables de control de tiempo para el bucle principal ---
last_qr_scan_time = 0
last_telemetry_gather_save_time = 0 # New variable for local saving interval
last_telemetry_sync_time = 0 # Variable for cloud sync interval
last_event_sync_time = 0
last_session_sync_time = 0
last_cleanup_time = 0
last_multimedia_sync_time = 0

# --- Función de carga de archivos multimedia (PLACEHOLDER) ---
def YOUR_CLOUD_FILE_UPLOAD_FUNCTION(event_obj: EventoLocal) -> bool:
    """
    Esta es una función placeholder para la carga de archivos multimedia (snapshots, video clips)
    a tu sistema de almacenamiento en la nube (ej. AWS S3, Google Cloud Storage, Azure Blob Storage).

    DEBES IMPLEMENTAR ESTO con la lógica real de tu proveedor de almacenamiento.

    Debe tomar un objeto EventoLocal, cargar los archivos especificados en
    event_obj.snapshot_local_path y event_obj.video_clip_local_path,
    y retornar True si la carga fue exitosa para AMBOS (si existen), False en caso contrario.
    También deberías actualizar event_obj.snapshot_url y event_obj.video_clip_url
    con las URLs públicas una vez cargados.

    Args:
        event_obj: El objeto EventoLocal que contiene las rutas de los archivos locales.

    Returns:
        bool: True si la carga fue exitosa, False en caso de fallo.
    """
    logger.warning(f"PLACEHOLDER: Implementar la carga real de archivos para evento {event_obj.id}")
    # Ejemplo de lógica placeholder: simular éxito si hay rutas
    if event_obj.snapshot_local_path or event_obj.video_clip_local_path:
        # Aquí iría la lógica para interactuar con la API de tu proveedor de almacenamiento
        # Por ejemplo:
        # s3_client.upload_file(event_obj.snapshot_local_path, 'my-bucket', f'snapshots/{event_obj.id}.jpg')
        # event_obj.snapshot_url = f'https://my-bucket.s3.amazonaws.com/snapshots/{event_obj.id}.jpg'

        # Simula un pequeño retraso para la carga
        time.sleep(0.1)
        return True # Asumimos éxito para la demostración
    return False


def run_jetson_provisioning() -> Optional[ConfiguracionJetsonLocal]:
    """
    Ejecuta el proceso de aprovisionamiento inicial para la Jetson.
    Asigna un ID de hardware si no existe y lo vincula a un bus.
    """
    with SessionLocal() as db:
        config = get_jetson_config_local(db)

        if config and config.id_bus_asignado:
            logger.info(f"Jetson ya aprovisionada y asignada al bus ID: {config.id_bus_asignado}. Hardware ID: {config.id_hardware_jetson}")
            return config

        logger.info("Iniciando proceso de aprovisionamiento de la Jetson...")

        # Obtener o generar ID de hardware de la Jetson
        hardware_id = None
        if config:
            hardware_id = config.id_hardware_jetson
        if not hardware_id:
            hardware_id = f"jetson-{uuid.uuid4().hex[:12]}" # Generar un ID único
            if config:
                config.id_hardware_jetson = hardware_id
                config = update_jetson_config_local(db, config)
            else:
                config = ConfiguracionJetsonLocal(id_hardware_jetson=hardware_id)
                db.add(config)
                db.commit()
                db.refresh(config)
            logger.info(f"ID de hardware de Jetson generado: {hardware_id}")
        else:
            logger.info(f"Usando ID de hardware de Jetson existente: {hardware_id}")

        # Solicitar la placa del bus para la asignación
        bus_placa = input("Por favor, ingrese la PLACA del bus a asignar a esta Jetson: ").strip().upper()
        if not bus_placa:
            logger.error("Placa del bus no proporcionada. No se puede completar el aprovisionamiento.")
            return None

        # Pull de datos del bus
        logger.info(f"Intentando obtener datos del bus con placa '{bus_placa}' desde la nube...")
        bus_local = pull_bus_data_by_placa(db, bus_placa)

        if bus_local:
            # Asignar bus a la configuración de la Jetson
            config.id_bus_asignado = bus_local.id
            config = update_jetson_config_local(db, config)
            logger.info(f"Jetson asignada exitosamente al bus '{bus_local.placa}' (ID: {bus_local.id})")

            # Pull de conductores asignados al bus
            logger.info(f"Obteniendo conductores asignados al bus '{bus_local.id}'...")
            pull_assigned_drivers_for_bus(db, bus_local.id)
            logger.info("Aprovisionamiento de conductores completado.")
            return config
        else:
            logger.error(f"No se pudo encontrar el bus con placa '{bus_placa}' en la nube. Aprovisionamiento fallido.")
            return None

def run_main_loop():
    """
    Ejecuta el bucle principal de la aplicación de la Jetson.
    """
    global last_qr_scan_time, last_telemetry_gather_save_time, last_telemetry_sync_time, last_event_sync_time, last_session_sync_time, last_cleanup_time, last_multimedia_sync_time

    logger.info("Iniciando bucle principal de la Jetson...")

    # Initialize camera manager
    # Adjust camera_index, width, height, fps as per your camera setup
    camera_manager = VideoCapture(camera_index=0, width=640, height=480, fps=30)
    if not camera_manager.initialize_camera(): # Use the method on the instance
        logger.error("No se pudo inicializar la cámara. Asegúrese de que esté conectada y configurada.")
        return

    # Obtener configuración de Jetson para ID de hardware y bus asignado
    with SessionLocal() as db:
        jetson_config = get_jetson_config_local(db)
        if not jetson_config or not jetson_config.id_bus_asignado:
            logger.error("Jetson no aprovisionada correctamente. Por favor, ejecute el aprovisionamiento.")
            camera_manager.release_camera() # Release camera resources
            return
        current_bus_id = jetson_config.id_bus_asignado
        jetson_hardware_id = jetson_config.id_hardware_jetson


    while True:
        current_time_loop = time.time()
        db_session = None # Asegurar que db_session se inicializa a None

        try:
            db_session = SessionLocal()

            # --- QR Scanning y Gestión de Sesiones ---
            if current_time_loop - last_qr_scan_time >= QR_SCAN_INTERVAL_SECONDS:
                logger.debug("Intentando escanear QR...")
                frame = camera_manager.read_frame() # Use the method on the instance
                if frame is not None:
                    qr_data = scan_qr_code(frame)
                    if qr_data:
                        logger.info(f"QR detectado: {qr_data}")
                        _, conductor, resultado = create_driver_session_from_qr_robust(
                            db_session, qr_data, current_bus_id, pull_conductor_by_id
                        )
                        logger.info(f"Resultado sesión QR: {resultado['message']}")
                        if conductor and not resultado.get('conductor_sincronizado'):
                            logger.warning(f"Conductor {conductor.nombre_completo} (ID: {conductor.id}) operando con datos locales/temporales.")
                    else:
                        logger.debug("No se detectó QR.")
                last_qr_scan_time = current_time_loop

            # --- Recopilar y Guardar Telemetría Localmente ---
            if current_time_loop - last_telemetry_gather_save_time >= TELEMETRY_GATHER_SAVE_INTERVAL_SECONDS:
                logger.info("Iniciando recopilación y guardado local de telemetría...")
                metrics = gather_system_metrics()
                # Ensure hardware ID is included for local saving
                metrics['id_hardware_jetson'] = jetson_hardware_id
                
                # Map keys for the local database model
                telemetry_data_for_local_db = {
                    'id_hardware_jetson': metrics.get('id_hardware_jetson'),
                    'timestamp_telemetry': datetime.fromisoformat(metrics['timestamp']) if 'timestamp' in metrics else datetime.utcnow(),
                    'ram_usage_gb': metrics.get('ram_used_gb'),
                    'cpu_usage_percent': metrics.get('cpu_usage_percent'),
                    'disk_usage_gb': metrics.get('disk_used_gb'),
                    'disk_usage_percent': metrics.get('disk_percent'),
                    'temperatura_celsius': metrics.get('temperature_celsius'),
                }
                
                try:
                    create_local_telemetry(db_session, telemetry_data_for_local_db)
                    logger.info("Métricas de telemetría guardadas localmente.")
                except Exception as e:
                    logger.error(f"Error al guardar métricas de telemetría localmente: {e}", exc_info=True)
                
                last_telemetry_gather_save_time = current_time_loop

            # --- Sincronización de Telemetría (Enviar registros no sincronizados a la nube) ---
            if current_time_loop - last_telemetry_sync_time >= TELEMETRY_SYNC_INTERVAL_SECONDS:
                logger.info("Iniciando ciclo de envío de telemetría no sincronizada a la nube...")
                send_unsynced_telemetry_to_cloud(db_session)
                last_telemetry_sync_time = current_time_loop

            # --- Sincronización de Eventos ---
            if current_time_loop - last_event_sync_time >= EVENT_SYNC_INTERVAL_SECONDS:
                logger.info("Iniciando ciclo de sincronización de eventos...")
                send_events_to_cloud(db_session)
                last_event_sync_time = current_time_loop

            # --- Sincronización de Archivos Multimedia ---
            if current_time_loop - last_multimedia_sync_time >= MULTIMEDIA_SYNC_INTERVAL_SECONDS:
                logger.info("Iniciando ciclo de sincronización de archivos multimedia...")
                sync_stats = sync_multimedia_files(db_session, YOUR_CLOUD_FILE_UPLOAD_FUNCTION)
                logger.info(f"Estadísticas de sincronización multimedia: {sync_stats}")
                last_multimedia_sync_time = current_time_loop

            # --- Sincronización de Sesiones de Conducción (si hay cambios activos) ---
            if current_time_loop - last_session_sync_time >= SESSION_SYNC_INTERVAL_SECONDS:
                logger.debug("Verificando sesiones activas para posible sincronización...")
                active_assignment = get_active_asignacion_for_bus(db_session, current_bus_id)
                if active_assignment:
                    # Si la sesión es "Activa", enviarla para actualizar duración, etc.
                    # Esto asegura que la nube siempre tenga el estado más reciente de la sesión activa
                    send_session_data_to_cloud(db_session, active_assignment)
                last_session_sync_time = current_time_loop

            # --- Limpieza de la Base de Datos Local ---
            if current_time_loop - last_cleanup_time >= CLEANUP_INTERVAL_SECONDS:
                logger.info("Iniciando ciclo de limpieza de la base de datos local...")

                # Limpieza de eventos
                events_to_cleanup = get_synced_events_for_cleanup(db_session, days_old=7, limit=100)
                for event_obj in events_to_cleanup:
                    cleanup_stats_event = cleanup_event_files(db_session, event_obj)
                    if cleanup_stats_event['archivos_borrados'] > 0 or cleanup_stats_event['errores']:
                        logger.info(f"Limpieza de archivos de evento {event_obj.id}: {cleanup_stats_event}")

                # Limpieza de telemetría
                telemetry_to_cleanup = get_synced_telemetry_for_cleanup(db_session, days_old=30, limit=500)
                if telemetry_to_cleanup:
                    cleanup_stats_telemetry = cleanup_telemetry_records(db_session, telemetry_to_cleanup)
                    logger.info(f"Limpieza de registros de telemetría: {cleanup_stats_telemetry}")
                else:
                    logger.info("No hay registros de telemetría para limpiar.")

                last_cleanup_time = current_time_loop

            # Pequeña pausa para evitar un uso excesivo de la CPU
            time.sleep(1)

        except Exception as e:
            logger.error(f"Error crítico en el bucle principal: {e}", exc_info=True)
        finally:
            if db_session:
                db_session.close() # Asegurarse de cerrar la sesión de la BD

        # This release should happen once, when the application is truly shutting down,
        # not inside the loop. Moving it outside or handling via a signal.
        # For now, it remains here as per previous code structure, but note this.
        # camera_manager.release_camera()


if __name__ == '__main__':
    logger.info("Iniciando aplicación de monitoreo de conductores en Jetson...")

    # 1. Crear todas las tablas en la base de datos SQLite si no existen
    logger.info("Verificando/Creando tablas de la base de datos local...")
    Base.metadata.create_all(engine)
    logger.info("Tablas de base de datos listas.")

    # 2. Ejecutar el aprovisionamiento inicial de la Jetson
    provisioned_config = run_jetson_provisioning()
    if provisioned_config:
        logger.info("Aprovisionamiento de Jetson completado. Iniciando operación normal.")
        # 3. Iniciar el bucle principal solo si el aprovisionamiento fue exitoso
        run_main_loop()
    else:
        logger.critical("El aprovisionamiento de la Jetson falló. No se puede iniciar el bucle principal. Revise los logs.")

    # Ensure camera is released if main loop exits
    # If run_main_loop handles release internally, this might be redundant.
    # The current design releases it inside run_main_loop's error handling.
    # If a clean shutdown is implemented, this would be part of that.
    # For now, relying on the 'return' paths inside run_main_loop to release.
# main_jetson.py
import time
import logging
import uuid
from datetime import datetime, timedelta
from typing import Optional

# Importaciones de configuración
from app.config.edge_database import create_edge_tables, initialize_jetson_config, get_edge_db

# Importaciones de módulos de la Jetson App
from app.data_ingestion.video_capture import VideoCapture
from app.data_ingestion.qr_scanner import scan_qr_code, process_qr_data
# from app.data_ingestion.ai_inference import AIInference # Se integrará después
from app.identification.driver_identity import identify_and_manage_session, check_active_driver_session_status
from app.monitoring.device_telemetry import gather_system_metrics
from app.sync.cloud_sync import send_events_to_cloud, send_telemetry_to_cloud, pull_bus_data_by_placa, pull_assigned_drivers_for_bus
from app.alerts.local_alerts import trigger_audio_alert # Para alertas de configuración
from app.models.edge_database_models import ConfiguracionJetsonLocal, BusLocal, ConductorLocal # Para tipado y datos de prueba

# Configuración del logger principal de la aplicación
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- Constantes de Frecuencia y Umbrales ---
QR_SCAN_INTERVAL_SECONDS = 5            # Frecuencia de escaneo QR (en segundos)
SESSION_CHECK_INTERVAL_SECONDS = 60     # Frecuencia de chequeo de sesión (ej. tiempo excedido)
TELEMETRY_SEND_INTERVAL_SECONDS = 300   # Frecuencia de envío de telemetría (5 minutos)
CLOUD_SYNC_EVENTS_INTERVAL_SECONDS = 30 # Frecuencia de sincronización de eventos con la nube

# --- Estado Global de la Jetson ---
current_driver_id: Optional[uuid.UUID] = None
current_bus_id: Optional[uuid.UUID] = None
jetson_hardware_id: Optional[str] = None

# --- Variables de Tiempo para Control de Frecuencia ---
last_qr_scan_time = time.time()
last_session_check_time = time.time()
last_telemetry_send_time = time.time()
last_cloud_sync_events_time = time.time()

# --- Instancia de la cámara ---
camera_manager: Optional[VideoCapture] = None
# ai_processor: Optional[AIInference] = None # Para cuando se integre la IA

def run_jetson_provisioning():
    """
    Función para el aprovisionamiento inicial de la Jetson Nano.
    Pide la placa del bus, descarga datos de la nube y los guarda localmente.
    """
    global jetson_hardware_id, current_bus_id  # MOVER AL INICIO
    
    db = next(get_edge_db())
    try:
        # Verificar si la Jetson ya está aprovisionada
        config = initialize_jetson_config(db, id_hardware_jetson="TEMPORAL_JETSON_ID", id_bus_asignado=None)
        if config.id_bus_asignado and config.id_hardware_jetson != "TEMPORAL_JETSON_ID":
            jetson_hardware_id = config.id_hardware_jetson
            current_bus_id = config.id_bus_asignado
            logger.info(f"Jetson ya aprovisionada con ID: {jetson_hardware_id} y Bus: {current_bus_id}.")
            return True # Ya aprovisionada

        logger.info("--- INICIANDO PROCESO DE APROVISIONAMIENTO DE JETSON NANO ---")
        trigger_audio_alert("Iniciando aprovisionamiento. Por favor, introduzca la placa del bus.")

        placa_bus = input("Por favor, ingrese la PLACA del bus al que se asignará esta Jetson: ").strip().upper()
        if not placa_bus:
            logger.error("Placa no proporcionada. Aprovisionamiento cancelado.")
            trigger_audio_alert("Placa no válida. Aprovisionamiento fallido.")
            return False

        logger.info(f"Intentando obtener bus '{placa_bus}' y sus conductores desde la nube...")
        
        # 1. Obtener datos del bus desde la nube
        bus_data_from_cloud: Optional[BusLocal] = pull_bus_data_by_placa(db, placa_bus)
        if not bus_data_from_cloud:
            logger.error(f"No se pudo obtener el bus con placa '{placa_bus}' de la nube. Verifique la placa y la conectividad.")
            trigger_audio_alert(f"Bus {placa_bus} no encontrado. Aprovisionamiento fallido.")
            return False

        # 2. Obtener conductores asignados a ese bus desde la nube
        drivers_from_cloud = pull_assigned_drivers_for_bus(db, bus_data_from_cloud.id)
        if not drivers_from_cloud:
            logger.warning(f"No se encontraron conductores asignados al bus '{placa_bus}' en la nube. Continúe, pero sin conductores precargados.")
            trigger_audio_alert(f"No hay conductores asignados al bus {placa_bus}.")
        else:
            logger.info(f"Descargados {len(drivers_from_cloud)} conductores para el bus '{placa_bus}'.")
        
        # 3. Actualizar la configuración de esta Jetson
        # Generar un ID de hardware único para esta Jetson (ej. UUID)
        new_jetson_hw_id = str(uuid.uuid4()) # ID único de esta Jetson
        
        updated_config = initialize_jetson_config(db, new_jetson_hw_id, bus_data_from_cloud.id)
        
        jetson_hardware_id = updated_config.id_hardware_jetson
        current_bus_id = updated_config.id_bus_asignado

        logger.info(f"--- APROVISIONAMIENTO COMPLETADO PARA BUS: {placa_bus} (ID: {current_bus_id}) ---")
        trigger_audio_alert(f"Aprovisionamiento completado para bus {placa_bus}. Listo para operar.")
        return True

    except Exception as e:
        logger.error(f"Error crítico durante el aprovisionamiento de la Jetson: {e}", exc_info=True)
        trigger_audio_alert("Error crítico de aprovisionamiento. Reinicie el sistema.")
        return False
    finally:
        db.close()

def start_jetson_services():
    """
    Función principal para inicializar todos los módulos y servicios de la Jetson.
    """
    global camera_manager, jetson_hardware_id, current_bus_id  # MOVER AL INICIO

    logger.info("Iniciando servicios de la Jetson Nano...")

    # 1. Inicializar cámara
    camera_manager = VideoCapture(camera_index=0, width=640, height=480, fps=30)
    if not camera_manager.initialize_camera():
        logger.critical("Fallo al inicializar la cámara. No se puede continuar sin video.")
        trigger_audio_alert("Error: Cámara no disponible. Reinicie el sistema.")
        return False

    # 2. Inicializar modelos de IA (DEJAR PENDIENTE POR AHORA)
    # logger.info("Inicializando modelos de IA...")
    # ai_processor = AIInference(
    #     fatigue_model_path=get_jetson_settings().fatigue_model_path,
    #     distraction_model_path=get_jetson_settings().distraction_model_path
    # )
    # if not ai_processor.fatigue_session or not ai_processor.distraction_session:
    #     logger.warning("Fallo al cargar uno o ambos modelos de IA. La detección de fatiga/distracción no funcionará.")
    #     # Opcional: Terminar si los modelos son críticos
    
    # 3. Cargar configuración de Jetson ya guardada (si no se hizo en aprovisionamiento)
    db = next(get_edge_db())
    try:
        config = initialize_jetson_config(db, id_hardware_jetson="TEMPORAL_JETSON_ID_PLACEHOLDER") # Solo para cargar config
        if config and config.id_hardware_jetson:
            jetson_hardware_id = config.id_hardware_jetson
            current_bus_id = config.id_bus_asignado # Puede ser None si nunca se aprovisionó
            logger.info(f"Configuración cargada: HW ID={jetson_hardware_id}, Bus ID={current_bus_id}")
        else:
            logger.error("No se pudo cargar la configuración de Jetson. Aprovisionamiento pendiente.")
            # Esto se manejaría si run_jetson_provisioning() es llamado antes
    finally:
        db.close()

    logger.info("Servicios de la Jetson Nano inicializados. Listo para el bucle principal.")
    return True

def run_main_loop():
    """
    Contiene el bucle principal de ejecución para mantener los procesos activos.
    """
    global last_qr_scan_time, last_session_check_time, last_telemetry_send_time, last_cloud_sync_events_time
    global current_bus_id, jetson_hardware_id  # AÑADIR ESTA LÍNEA

    logger.info("Iniciando bucle principal de la Jetson Nano...")
    
    if not camera_manager or not camera_manager.cap.isOpened():
        logger.critical("Bucle principal no iniciado: Cámara no disponible.")
        return

    # Asegurarse de que la Jetson esté asignada a un bus para operar normalmente
    if not jetson_hardware_id or not current_bus_id:
        logger.error("Jetson no aprovisionada. Ejecute el aprovisionamiento primero.")
        trigger_audio_alert("Sistema no asignado a bus. Ejecute aprovisionamiento.")
        return

    while True:
        current_time = time.time()
        db = next(get_edge_db()) # Obtener una sesión de BD para cada iteración del bucle
        try:
            frame = camera_manager.read_frame()
            if frame is None:
                logger.warning("No se pudo leer el fotograma. Posiblemente la cámara se desconectó. Reintentando...")
                time.sleep(1) # Espera antes de reintentar
                continue

            # --- 1. Escaneo y Gestión de Sesión QR ---
            if current_time - last_qr_scan_time >= QR_SCAN_INTERVAL_SECONDS:
                last_qr_scan_time = current_time
                logger.debug("Realizando escaneo QR...")
                qr_data = scan_qr_code(frame)
                if qr_data:
                    conductor_cedula = process_qr_data(qr_data)
                    logger.info(f"QR detectado: {conductor_cedula}. Gestionando sesión...")
                    identify_and_manage_session(conductor_cedula)
                else:
                    logger.debug("No se detectó QR.")

            # --- 2. Inferencia de IA (PENDIENTE) ---
            # if ai_processor:
            #     ai_results = ai_processor.run_inference(frame)
            #     if ai_results['inference_status'] == 'Success':
            #         # Aquí se usarían los resultados de la IA para crear eventos locales (distracción, fatiga)
            #         # y luego se dispararían alertas locales si cumplen umbrales.
            #         pass
            # else:
            #     logger.debug("Modelos de IA no cargados o no disponibles. Saltando inferencia.")

            # --- 3. Monitoreo de Sesión (Tiempo Excedido) ---
            if current_time - last_session_check_time >= SESSION_CHECK_INTERVAL_SECONDS:
                last_session_check_time = current_time
                logger.debug("Chequeando estado de sesión (tiempo límite)...")
                check_active_driver_session_status() # Esta función ya está implementada en driver_identity.py

            # --- 4. Envío de Telemetría a la Nube ---
            if current_time - last_telemetry_send_time >= TELEMETRY_SEND_INTERVAL_SECONDS:
                last_telemetry_send_time = current_time
                logger.debug("Recopilando y enviando telemetría...")
                metrics = gather_system_metrics()
                if metrics:
                    send_telemetry_to_cloud(db, metrics)
                else:
                    logger.warning("No se pudieron recopilar métricas de telemetría.")

            # --- 5. Sincronización de Eventos/Sesiones con la Nube ---
            if current_time - last_cloud_sync_events_time >= CLOUD_SYNC_EVENTS_INTERVAL_SECONDS:
                last_cloud_sync_events_time = current_time
                logger.debug("Iniciando sincronización de eventos pendientes con la nube...")
                # send_events_to_cloud marcará los eventos como sincronizados
                send_events_to_cloud(db)

            # Pequeña pausa para no saturar la CPU y permitir otros procesos
            time.sleep(0.01) # 10ms de pausa

        except Exception as e:
            logger.error(f"Error en el bucle principal de la Jetson: {e}", exc_info=True)
            trigger_audio_alert("Error en el sistema. Reinicie el dispositivo.")
            time.sleep(5) # Espera antes de reintentar el bucle
        finally:
            db.close() # Asegurarse de cerrar la sesión de BD en cada iteración

def main():
    """
    Función de entrada principal para la aplicación de la Jetson Nano.
    """
    logger.info("Iniciando aplicación Jetson Nano.")

    # 1. Ejecutar el aprovisionamiento si es necesario
    if not run_jetson_provisioning():
        logger.critical("Aprovisionamiento fallido. Saliendo de la aplicación.")
        return

    # 2. Iniciar servicios principales (cámara, IA, etc.)
    if not start_jetson_services():
        logger.critical("Fallo al iniciar servicios de la Jetson. Saliendo.")
        return

    # 3. Ejecutar el bucle principal de operación
    try:
        run_main_loop()
    except KeyboardInterrupt:
        logger.info("Aplicación Jetson detenida por el usuario.")
    finally:
        if camera_manager:
            camera_manager.release_camera()
            logger.info("Cámara liberada.")
        logger.info("Aplicación Jetson Nano finalizada.")

if __name__ == "__main__":
    main()
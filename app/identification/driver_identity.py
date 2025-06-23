import uuid
import logging
from datetime import datetime, timedelta
from typing import Optional

from sqlalchemy.orm import Session 

# Importaciones de módulos locales esenciales
from app.config.edge_database import get_edge_db
from app.local_db.crud_edge import (
    get_conductor_by_uuid,
    create_driver_session_from_qr_robust,
    get_active_asignacion_for_bus,
    get_jetson_config_local
)
from app.models.edge_database_models import (
    ConductorLocal, 
    AsignacionConductorBusLocal, 
    EventoLocal,
    ConfiguracionJetsonLocal
)

# Importación de función de sincronización con cloud
from app.sync.cloud_sync import pull_conductor_by_id

# --- SIMULACIÓN DE MÓDULOS NO IMPLEMENTADOS AÚN ---
# En un entorno real, estos módulos serían importados y usados directamente.
class MockLocalAlerts:
    def trigger_visual_alert(self, message: str):
        print(f"[ALERTA VISUAL - SIMULADA]: {message}")
    def trigger_audio_alert(self, message: str):
        print(f"[ALERTA SONORA - SIMULADA]: {message}")

class MockCloudSync:
    def send_events_to_cloud(self, db_session):
        print("[SINCRONIZACIÓN - SIMULADA]: Intentando enviar eventos a la nube...")
    def send_telemetry_to_cloud(self, db_session):
        print("[SINCRONIZACIÓN - SIMULADA]: Intentando enviar telemetría a la nube...")

# Instancias de los mocks para usar en lugar de las importaciones reales
simulated_local_alerts = MockLocalAlerts()
simulated_cloud_sync = MockCloudSync()

# Configuración del logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# --- Constantes para la lógica de negocio ---
MAX_DRIVING_HOURS = 8
MAX_DRIVING_DURATION = timedelta(hours=MAX_DRIVING_HOURS)


def identify_and_manage_session(qr_data_uuid: str) -> Optional[ConductorLocal]:
    """
    Intenta identificar un conductor usando UUID desde QR y gestiona su sesión de conducción.
    ACTUALIZADO: Usa el nuevo flujo robusto con UUID y sincronización condicional.

    Args:
        qr_data_uuid (str): El UUID del conductor decodificado del código QR.

    Returns:
        Optional[ConductorLocal]: El objeto del conductor identificado, o None si no se encontró o la sesión finalizó.
    """
    logger.info(f"Intentando identificar y gestionar sesión para conductor UUID: {qr_data_uuid}")
    
    db = next(get_edge_db())
    current_time = datetime.utcnow()
    
    try:
        # 1. Obtener la configuración de esta Jetson para saber a qué bus está asignada
        jetson_config: Optional[ConfiguracionJetsonLocal] = get_jetson_config_local(db)
        if not jetson_config or not jetson_config.id_bus_asignado:
            logger.error("Jetson Nano no configurada o sin bus asignado. No se puede gestionar sesión.")
            simulated_local_alerts.trigger_audio_alert("Sistema no configurado. Contacte a soporte.")
            return None
        
        current_bus_id = jetson_config.id_bus_asignado
        logger.debug(f"Jetson asignada al bus ID: {current_bus_id}")

        # 2. Usar el nuevo flujo robusto para crear/gestionar sesión
        session, conductor, resultado = create_driver_session_from_qr_robust(
            db=db,
            qr_data=qr_data_uuid,
            bus_id=current_bus_id,
            cloud_sync_function=pull_conductor_by_id,
            current_time=current_time
        )
        
        # 3. Manejar el resultado según el estado
        if resultado['status'] == 'session_started':
            # Sesión iniciada exitosamente
            if resultado.get('datos_temporales', False):
                simulated_local_alerts.trigger_audio_alert(f"Bienvenido. Datos temporales - verificar conectividad")
                logger.warning(f"Conductor {conductor.nombre_completo} operando con datos temporales")
            elif not resultado.get('conductor_actualizado', True):
                simulated_local_alerts.trigger_audio_alert(f"Bienvenido {conductor.nombre_completo}. Sin actualización cloud")
                logger.info(f"Conductor {conductor.nombre_completo} sin actualización desde cloud")
            else:
                simulated_local_alerts.trigger_audio_alert(f"Bienvenido {conductor.nombre_completo}")
                logger.info(f"Sesión iniciada para {conductor.nombre_completo}")
            
            # Enviar datos de sesión a la nube
            if session:
                try:
                    from app.sync.cloud_sync import send_session_data_to_cloud
                    send_session_data_to_cloud(db, session)
                except Exception as e:
                    logger.warning(f"Error enviando datos de sesión a cloud: {e}")
            
            return conductor
            
        elif resultado['status'] == 'session_ended':
            # Sesión finalizada
            simulated_local_alerts.trigger_audio_alert("Sesión finalizada")
            logger.info(f"Sesión finalizada para conductor UUID: {qr_data_uuid}")
            
            # Enviar datos de sesión finalizada a la nube
            active_session = get_active_asignacion_for_bus(db, current_bus_id)
            if active_session and active_session.fecha_fin_asignacion:
                try:
                    from app.sync.cloud_sync import send_session_data_to_cloud
                    send_session_data_to_cloud(db, active_session)
                except Exception as e:
                    logger.warning(f"Error enviando datos de sesión finalizada a cloud: {e}")
            
            return conductor
            
        else:
            # Error en la gestión de sesión
            error_message = resultado.get('message', 'Error desconocido')
            simulated_local_alerts.trigger_audio_alert(f"Error: {error_message}")
            logger.error(f"Error gestionando sesión para UUID {qr_data_uuid}: {error_message}")
            
            # Registrar evento de error si es necesario
            if conductor:
                _record_session_error_event(db, current_bus_id, conductor.id, error_message, current_time)
            else:
                _record_unidentified_driver_event(db, current_bus_id, qr_data_uuid, current_time)
            
            return conductor
            
    except Exception as e:
        logger.error(f"Error en identify_and_manage_session: {e}", exc_info=True)
        simulated_local_alerts.trigger_audio_alert("Error en el sistema de identificación. Contacte a soporte.")
        return None
    finally:
        db.close()


def _record_unidentified_driver_event(db: Session, bus_id: uuid.UUID, qr_data_uuid: str, event_time: datetime):
    """
    Registra un evento cuando un conductor no es identificado o el QR es inválido.
    """
    try:
        jetson_config = get_jetson_config_local(db)
        jetson_hardware_id = jetson_config.id_hardware_jetson if jetson_config else "UNKNOWN_JETSON_ID_PLACEHOLDER"

        new_event = EventoLocal(
            id_bus=bus_id,
            id_conductor=uuid.UUID('00000000-0000-0000-0000-000000000000'),  # Placeholder para conductor no identificado
            id_sesion_conduccion=None,  # No hay sesión válida
            timestamp_evento=event_time,
            tipo_evento='Identificacion',
            subtipo_evento='Conductor No Identificado',
            severidad='Alta',
            alerta_disparada=True,
            metadatos_ia_json={
                "qr_data_scanned": qr_data_uuid, 
                "jetson_id": jetson_hardware_id,
                "error_type": "invalid_uuid_or_not_found"
            }
        )
        db.add(new_event)
        db.commit()
        db.refresh(new_event)
        logger.warning(f"Evento de 'Conductor No Identificado' registrado para bus {bus_id}.")
        simulated_cloud_sync.send_events_to_cloud(db)
    except Exception as e:
        logger.error(f"Error al registrar evento de conductor no identificado: {e}", exc_info=True)


def _record_session_error_event(db: Session, bus_id: uuid.UUID, conductor_id: uuid.UUID, error_message: str, event_time: datetime):
    """
    Registra un evento cuando hay un error en la gestión de sesión.
    """
    try:
        jetson_config = get_jetson_config_local(db)
        jetson_hardware_id = jetson_config.id_hardware_jetson if jetson_config else "UNKNOWN_JETSON_ID_PLACEHOLDER"

        new_event = EventoLocal(
            id_bus=bus_id,
            id_conductor=conductor_id,
            id_sesion_conduccion=None,
            timestamp_evento=event_time,
            tipo_evento='SistemaError',
            subtipo_evento='Error Gestion Sesion',
            severidad='Media',
            alerta_disparada=True,
            metadatos_ia_json={
                "error_message": error_message,
                "jetson_id": jetson_hardware_id,
                "error_type": "session_management_error"
            }
        )
        db.add(new_event)
        db.commit()
        db.refresh(new_event)
        logger.info(f"Evento de error de gestión de sesión registrado para conductor {conductor_id}.")
        simulated_cloud_sync.send_events_to_cloud(db)
    except Exception as e:
        logger.error(f"Error al registrar evento de error de sesión: {e}", exc_info=True)


def check_active_driver_session_status():
    """
    Verifica el estado de la sesión de conducción activa para el bus de esta Jetson.
    Si excede el tiempo límite, dispara una alerta y registra un evento.
    """
    db = next(get_edge_db())
    current_time = datetime.utcnow()
    
    try:
        jetson_config = get_jetson_config_local(db)
        if not jetson_config or not jetson_config.id_bus_asignado:
            logger.debug("Jetson Nano no configurada o sin bus asignado. No se verifica estado de sesión.")
            return

        active_assignment: Optional[AsignacionConductorBusLocal] = get_active_asignacion_for_bus(db, jetson_config.id_bus_asignado)

        if active_assignment and active_assignment.estado_turno == 'Activo':
            time_elapsed = current_time - active_assignment.fecha_inicio_asignacion
            logger.debug(f"Sesión activa: {active_assignment.id_sesion_conduccion}, Conductor: {active_assignment.id_conductor}, Tiempo transcurrido: {time_elapsed.total_seconds()/3600:.2f} horas.")

            if time_elapsed > MAX_DRIVING_DURATION:
                logger.warning(f"Conductor {active_assignment.id_conductor} ha excedido las {MAX_DRIVING_HOURS} horas de conducción continua.")
                
                # Disparar alerta local
                simulated_local_alerts.trigger_visual_alert("EXCESO TIEMPO CONDUCCIÓN")
                simulated_local_alerts.trigger_audio_alert("ALERTA: TIEMPO DE CONDUCCIÓN EXCEDIDO")
                
                # Registrar evento
                _record_time_exceeded_event(db, active_assignment, current_time)
                
                # Opcional: Forzar el fin de la sesión si se desea que no exceda más (política de la flota)
                # active_assignment.estado_turno = 'Forzado_Fin'
                # update_asignacion_conductor_bus_local(db, active_assignment)
                # logger.info(f"Sesión {active_assignment.id_sesion_conduccion} forzada a finalizar por tiempo excedido.")
        else:
            logger.debug("No hay sesión de conductor activa para verificar.")
    except Exception as e:
        logger.error(f"Error en check_active_driver_session_status: {e}", exc_info=True)
    finally:
        db.close()


def _record_time_exceeded_event(db: Session, assignment: AsignacionConductorBusLocal, event_time: datetime):
    """
    Registra un evento cuando el tiempo de conducción excede el límite.
    """
    try:
        jetson_config = get_jetson_config_local(db)
        jetson_hardware_id = jetson_config.id_hardware_jetson if jetson_config else "UNKNOWN_JETSON_ID_PLACEHOLDER"

        new_event = EventoLocal(
            id_bus=assignment.id_bus,
            id_conductor=assignment.id_conductor,
            id_sesion_conduccion=assignment.id_sesion_conduccion,
            timestamp_evento=event_time,
            tipo_evento='RegulacionConduccion',
            subtipo_evento='Exceso Horas Conduccion',
            severidad='Crítica',
            alerta_disparada=True,
            metadatos_ia_json={
                "limite_horas": MAX_DRIVING_HOURS, 
                "jetson_id": jetson_hardware_id,
                "tiempo_total_horas": (event_time - assignment.fecha_inicio_asignacion).total_seconds() / 3600
            }
        )
        db.add(new_event)
        db.commit()
        db.refresh(new_event)
        logger.info(f"Evento de 'Exceso Horas Conduccion' registrado para sesión {assignment.id_sesion_conduccion}.")
        simulated_cloud_sync.send_events_to_cloud(db)
    except Exception as e:
        logger.error(f"Error al registrar evento de exceso de tiempo: {e}", exc_info=True)


def get_current_driver_info() -> Optional[dict]:
    """
    Obtiene información del conductor actualmente activo en este bus.
    
    Returns:
        Optional[dict]: Información del conductor activo o None si no hay sesión activa.
    """
    db = next(get_edge_db())
    try:
        jetson_config = get_jetson_config_local(db)
        if not jetson_config or not jetson_config.id_bus_asignado:
            return None

        active_assignment = get_active_asignacion_for_bus(db, jetson_config.id_bus_asignado)
        if not active_assignment:
            return None

        conductor = get_conductor_by_uuid(db, active_assignment.id_conductor)
        if not conductor:
            return None

        time_elapsed = datetime.utcnow() - active_assignment.fecha_inicio_asignacion
        
        return {
            'conductor_id': str(conductor.id),
            'conductor_nombre': conductor.nombre_completo,
            'sesion_id': str(active_assignment.id_sesion_conduccion),
            'tiempo_conduccion_horas': time_elapsed.total_seconds() / 3600,
            'estado_sesion': active_assignment.estado_turno,
            'datos_temporales': conductor.nombre_completo.startswith("Conductor Pendiente")
        }
        
    except Exception as e:
        logger.error(f"Error obteniendo información del conductor actual: {e}", exc_info=True)
        return None
    finally:
        db.close()


# Ejemplo de uso simplificado (solo para referencia, esto se orquestaría desde main_jetson.py)
if __name__ == '__main__':
    print("=== Probando driver_identity.py actualizado ===")
    print("IMPORTANTE: Asegúrate de haber ejecutado scripts/initial_data_setup.py primero")
    print("para que la base de datos contenga los datos demo necesarios.")
    print()
    
    # UUIDs de conductores demo del sistema
    uuid_conductor_activo = "22222222-2222-2222-2222-222222222222"
    uuid_conductor_inactivo = "33333333-3333-3333-3333-333333333333"
    uuid_conductor_inexistente = "99999999-9999-9999-9999-999999999999"
    
    print("--- Escenario 1: Conductor Activo escanea QR ---")
    resultado1 = identify_and_manage_session(uuid_conductor_activo)
    print(f"Resultado: {resultado1.nombre_completo if resultado1 else 'None'}")
    
    print("\n--- Escenario 2: Conductor Inactivo escanea QR ---")
    resultado2 = identify_and_manage_session(uuid_conductor_inactivo)
    print(f"Resultado: {resultado2.nombre_completo if resultado2 else 'None'}")
    
    print("\n--- Escenario 3: UUID No Registrado ---")
    resultado3 = identify_and_manage_session(uuid_conductor_inexistente)
    print(f"Resultado: {resultado3.nombre_completo if resultado3 else 'None'}")
    
    print("\n--- Info del conductor actual ---")
    current_info = get_current_driver_info()
    if current_info:
        print(f"Conductor activo: {current_info['conductor_nombre']}")
        print(f"Tiempo conduciendo: {current_info['tiempo_conduccion_horas']:.2f} horas")
    else:
        print("No hay conductor activo")
    
    print("\n=== Prueba completada ===")
    print("Revisa los logs para ver el flujo detallado de cada escenario.")
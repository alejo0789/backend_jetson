import uuid
import os
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple
from pathlib import Path

from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError, SQLAlchemyError # Import SQLAlchemyError for broader catch
from sqlalchemy import and_,or_
import logging
# Configuración del logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# Importamos los modelos de la base de datos local de la Jetson
from app.models.edge_database_models import (
    ConductorLocal,
    BusLocal,
    AsignacionConductorBusLocal,
    EventoLocal,
    AlertaLocal,
    ConfiguracionJetsonLocal,
    SincronizacionMetadata,
    TelemetryLocal
)

# --- Funciones CRUD para ConfiguracionJetsonLocal ---

def get_jetson_config_local(db: Session) -> Optional[ConfiguracionJetsonLocal]:
    """
    Obtiene la única fila de configuración de la Jetson local.
    """
    return db.query(ConfiguracionJetsonLocal).first()

def update_jetson_config_local(db: Session, config_obj: ConfiguracionJetsonLocal) -> ConfiguracionJetsonLocal:
    """
    Actualiza una configuración de Jetson existente.
    """
    db.add(config_obj)
    db.commit()
    db.refresh(config_obj)
    return config_obj

# --- Funciones CRUD para ConductoresLocales ---

def get_conductor_by_id_local(db: Session, conductor_id: uuid.UUID) -> Optional[ConductorLocal]:
    """
    Obtiene un conductor de la BD local por su UUID.
    """
    return db.query(ConductorLocal).filter(ConductorLocal.id == conductor_id).first()

def get_conductor_by_cedula_hash(db: Session, cedula_hash: str) -> Optional[ConductorLocal]:
    """
    Obtiene un conductor de la BD local por el hash de su cédula (código QR).
    """
    return db.query(ConductorLocal).filter(ConductorLocal.codigo_qr_hash == cedula_hash).first()

def _create_conductor_local_internal(db: Session, conductor_data: dict) -> ConductorLocal:
    """
    Función interna para crear un nuevo conductor localmente.
    """
    conductor_data_processed = {k: v for k, v in conductor_data.items() if k in ConductorLocal.__table__.columns.keys()}

    if 'id' in conductor_data_processed and isinstance(conductor_data_processed['id'], str):
        conductor_data_processed['id'] = uuid.UUID(conductor_data_processed['id'])

    new_conductor = ConductorLocal(**conductor_data_processed)
    db.add(new_conductor)
    db.commit()
    db.refresh(new_conductor)
    return new_conductor

def _update_conductor_local_internal(db: Session, conductor_obj: ConductorLocal, updates: dict) -> ConductorLocal:
    """
    Función interna para actualizar un conductor local.
    """
    for key, value in updates.items():
        if hasattr(conductor_obj, key):
            setattr(conductor_obj, key, value)
    db.commit()
    db.refresh(conductor_obj)
    return conductor_obj

def create_or_update_conductor_local(db: Session, conductor_data: Dict[str, Any]) -> ConductorLocal:
    """
    Crea o actualiza un conductor en la BD local.
    Busca por 'id' (UUID) del conductor.
    """
    conductor_id = conductor_data.get('id')
    if not conductor_id:
        raise ValueError("ID del conductor es requerido para crear o actualizar.")

    existing_conductor = db.query(ConductorLocal).filter(ConductorLocal.id == uuid.UUID(str(conductor_id))).first()

    if existing_conductor:
        updated_conductor = _update_conductor_local_internal(db, existing_conductor, conductor_data)
        return updated_conductor
    else:
        if isinstance(conductor_data['id'], str):
            conductor_data['id'] = uuid.UUID(conductor_data['id'])
        try:
            new_conductor = _create_conductor_local_internal(db, conductor_data)
            return new_conductor
        except IntegrityError as e:
            db.rollback()
            raise ValueError(f"Error de integridad al crear conductor: {e.orig}")

# --- Funciones CRUD para BusesLocales ---

def get_bus_local_by_id(db: Session, bus_id: uuid.UUID) -> Optional[BusLocal]:
    """
    Obtiene un bus de la BD local por su UUID.
    """
    return db.query(BusLocal).filter(BusLocal.id == bus_id).first()

def _create_bus_local_internal(db: Session, bus_data: dict) -> BusLocal:
    """
    Función interna para crear un nuevo bus localmente.
    """
    bus_data_processed = {k: v for k, v in bus_data.items() if k in BusLocal.__table__.columns.keys()}
    if 'id' in bus_data_processed and isinstance(bus_data_processed['id'], str):
        bus_data_processed['id'] = uuid.UUID(bus_data_processed['id'])

    new_bus = BusLocal(**bus_data_processed)
    db.add(new_bus)
    db.commit()
    db.refresh(new_bus)
    return new_bus

def _update_bus_local_internal(db: Session, bus_obj: BusLocal, updates: dict) -> BusLocal:
    """
    Función interna para actualizar un bus local.
    """
    for key, value in updates.items():
        if hasattr(bus_obj, key):
            setattr(bus_obj, key, value)
    db.commit()
    db.refresh(bus_obj)
    return bus_obj

def create_or_update_bus_local(db: Session, bus_data: Dict[str, Any]) -> BusLocal:
    """
    Crea o actualiza un bus en la BD local.
    Busca por 'id' (UUID) del bus.
    """
    bus_id = bus_data.get('id')
    if not bus_id:
        raise ValueError("ID del bus es requerido para crear o actualizar.")

    existing_bus = db.query(BusLocal).filter(BusLocal.id == uuid.UUID(str(bus_id))).first()

    if existing_bus:
        updated_bus = _update_bus_local_internal(db, existing_bus, bus_data)
        return updated_bus
    else:
        if isinstance(bus_data['id'], str):
            bus_data['id'] = uuid.UUID(bus_data['id'])
        try:
            new_bus = _create_bus_local_internal(db, bus_data)
            return new_bus
        except IntegrityError as e:
            db.rollback()
            raise ValueError(f"Error de integridad al crear bus: {e.orig}")

# --- Funciones CRUD para AsignacionConductorBusLocal ---

def create_asignacion_conductor_bus_local(
    db: Session,
    id_conductor: uuid.UUID,
    id_bus: uuid.UUID,
    id_sesion_conduccion: uuid.UUID,
    fecha_inicio_asignacion: datetime,
    estado_turno: str = 'Activo',
    tipo_asignacion: Optional[str] = None
) -> AsignacionConductorBusLocal:
    """
    Crea un nuevo registro de asignación de conductor-bus en la BD local.
    """
    new_assignment = AsignacionConductorBusLocal(
        id_conductor=id_conductor,
        id_bus=id_bus,
        id_sesion_conduccion=id_sesion_conduccion,
        fecha_inicio_asignacion=fecha_inicio_asignacion,
        estado_turno=estado_turno,
        tipo_asignacion=tipo_asignacion
    )
    db.add(new_assignment)
    db.commit()
    db.refresh(new_assignment)
    return new_assignment

def get_active_asignacion_for_bus(db: Session, bus_id: uuid.UUID) -> Optional[AsignacionConductorBusLocal]:
    """
    Obtiene la asignación de conductor activa para un bus específico.
    Se considera activa si `estado_turno` es 'Activo' y `fecha_fin_asignacion` es NULL.
    """
    return db.query(AsignacionConductorBusLocal).filter(
        AsignacionConductorBusLocal.id_bus == bus_id,
        AsignacionConductorBusLocal.estado_turno == 'Activo',
        AsignacionConductorBusLocal.fecha_fin_asignacion.is_(None)
    ).first()

def update_asignacion_conductor_bus_local(db: Session, asignacion_obj: AsignacionConductorBusLocal) -> AsignacionConductorBusLocal:
    """
    Actualiza un objeto de asignación de conductor-bus existente (ej. para finalizar un turno).
    """
    db.add(asignacion_obj)
    db.commit()
    db.refresh(asignacion_obj)
    return asignacion_obj

# --- FUNCIONES CRUD PARA EventoLocal CON MULTIMEDIA ---

def create_local_event(db: Session, event_data: dict) -> EventoLocal:
    """
    Crea un nuevo evento en la base de datos local de la Jetson.
    """
    new_event = EventoLocal(**event_data)
    db.add(new_event)
    db.commit()
    db.refresh(new_event)
    return new_event

def create_event_with_multimedia(
    db: Session,
    event_data: Dict[str, Any],
    snapshot_path: Optional[str] = None,
    video_clip_path: Optional[str] = None
) -> EventoLocal:
    """
    Crea un evento con archivos multimedia asociados.

    Args:
        db: Sesión de base de datos
        event_data: Datos del evento (tipo_evento, timestamp_evento, etc.)
        snapshot_path: Ruta local del snapshot (opcional)
        video_clip_path: Ruta local del video clip (opcional)

    Returns:
        EventoLocal: El evento creado con archivos asociados
    """
    # Añadir rutas de archivos al evento
    if snapshot_path:
        event_data['snapshot_local_path'] = snapshot_path
    if video_clip_path:
        event_data['video_clip_local_path'] = video_clip_path

    # Los archivos no están sincronizados inicialmente
    event_data['archivos_synced'] = False

    return create_local_event(db, event_data)

def get_events_with_unsynced_files(db: Session, limit: int = 10) -> List[EventoLocal]:
    """
    Obtiene eventos que tienen archivos pendientes de sincronizar.

    Args:
        db: Sesión de base de datos
        limit: Número máximo de eventos a retornar

    Returns:
        List[EventoLocal]: Lista de eventos con archivos pendientes
    """
    return db.query(EventoLocal).filter(
        and_(
            EventoLocal.archivos_synced == False,
            or_(
                EventoLocal.snapshot_local_path.isnot(None),
                EventoLocal.video_clip_local_path.isnot(None)
            )
        )
    ).limit(limit).all()

def mark_event_files_as_synced(db: Session, event_id: uuid.UUID) -> Optional[EventoLocal]:
    """
    Marca los archivos de un evento como sincronizados.

    Args:
        db: Sesión de base de datos
        event_id: UUID del evento

    Returns:
        EventoLocal: El evento actualizado o None si no se encontró
    """
    event = db.query(EventoLocal).filter(EventoLocal.id == event_id).first()
    if event:
        event.archivos_synced = True
        db.commit()
        db.refresh(event)
    return event

def get_synced_events_for_cleanup(db: Session, days_old: int = 7, limit: int = 50) -> List[EventoLocal]:
    """
    Obtiene eventos sincronizados que son candidatos para limpieza.

    Args:
        db: Sesión de base de datos
        days_old: Días de antigüedad mínima
        limit: Número máximo de eventos a retornar

    Returns:
        List[EventoLocal]: Lista de eventos candidatos para limpieza
    """
    cutoff_date = datetime.utcnow() - timedelta(days=days_old)

    return db.query(EventoLocal).filter(
        and_(
            EventoLocal.archivos_synced == True,
            EventoLocal.timestamp_evento < cutoff_date,
            or_(
                EventoLocal.snapshot_local_path.isnot(None),
                EventoLocal.video_clip_local_path.isnot(None)
            )
        )
    ).limit(limit).all()

def cleanup_event_files(db: Session, event: EventoLocal) -> Dict[str, Any]:
    """
    Limpia los archivos físicos de un evento y actualiza las rutas en BD.

    Args:
        db: Sesión de base de datos
        event: Evento a limpiar

    Returns:
        Dict: Estadísticas de limpieza (archivos_borrados, espacio_liberado_bytes)
    """
    cleanup_stats = {
        'archivos_borrados': 0,
        'espacio_liberado_bytes': 0,
        'errores': []
    }

    # Limpiar snapshot
    if event.snapshot_local_path and os.path.exists(event.snapshot_local_path):
        try:
            file_size = Path(event.snapshot_local_path).stat().st_size
            os.remove(event.snapshot_local_path)
            cleanup_stats['archivos_borrados'] += 1
            cleanup_stats['espacio_liberado_bytes'] += file_size
            event.snapshot_local_path = None
        except Exception as e:
            cleanup_stats['errores'].append(f"Error borrando snapshot: {e}")

    # Limpiar video clip
    if event.video_clip_local_path and os.path.exists(event.video_clip_local_path):
        try:
            file_size = Path(event.video_clip_local_path).stat().st_size
            os.remove(event.video_clip_local_path)
            cleanup_stats['archivos_borrados'] += 1
            cleanup_stats['espacio_liberado_bytes'] += file_size
            event.video_clip_local_path = None
        except Exception as e:
            cleanup_stats['errores'].append(f"Error borrando video: {e}")

    # Actualizar evento en BD
    if cleanup_stats['archivos_borrados'] > 0:
        db.commit()
        db.refresh(event)

    return cleanup_stats

def get_storage_statistics_from_events(db: Session) -> Dict[str, Any]:
    """
    Obtiene estadísticas de almacenamiento basadas en los eventos.

    Returns:
        Dict: Estadísticas de eventos y archivos
    """
    # Contar eventos totales
    total_events = db.query(EventoLocal).count()

    # Contar eventos con archivos
    events_with_snapshots = db.query(EventoLocal).filter(
        EventoLocal.snapshot_local_path.isnot(None)
    ).count()

    events_with_videos = db.query(EventoLocal).filter(
        EventoLocal.video_clip_local_path.isnot(None)
    ).count()

    # Contar eventos sincronizados
    synced_events = db.query(EventoLocal).filter(
        EventoLocal.archivos_synced == True
    ).count()

    pending_sync_events = db.query(EventoLocal).filter(
        and_(
            EventoLocal.archivos_synced == False,
            or_(
                EventoLocal.snapshot_local_path.isnot(None),
                EventoLocal.video_clip_local_path.isnot(None)
            )
        )
    ).count()

    return {
        'total_eventos': total_events,
        'eventos_con_snapshots': events_with_snapshots,
        'eventos_con_videos': events_with_videos,
        'eventos_sincronizados': synced_events,
        'eventos_pendientes_sync': pending_sync_events,
        'timestamp': datetime.utcnow().isoformat()
    }

# --- FUNCIONES CRUD ESPECÍFICAS PARA EL FLUJO QR → CLOUD SYNC ---

def get_conductor_by_uuid(db: Session, conductor_uuid: uuid.UUID) -> Optional[ConductorLocal]:
    """
    Obtiene un conductor de la BD local por su UUID (usado en QR).
    """
    return db.query(ConductorLocal).filter(ConductorLocal.id == conductor_uuid).first()

def ensure_conductor_exists_minimal(
    db: Session,
    conductor_uuid: uuid.UUID
) -> ConductorLocal:
    """
    Asegura que el conductor existe en la BD local con datos mínimos.
    Si no existe, lo crea solo con UUID (sin datos de cloud).

    Args:
        db: Sesión de base de datos
        conductor_uuid: UUID del conductor (desde QR)

    Returns:
        ConductorLocal: Conductor (existente o creado con datos mínimos)
    """
    # Buscar conductor en BD local
    conductor = get_conductor_by_uuid(db, conductor_uuid)

    if conductor:
        return conductor

    # No existe → crear con datos mínimos para operación
    conductor_minimal_data = {
        'id': conductor_uuid,
        'cedula': f"PENDING_SYNC_{str(conductor_uuid)[:8]}",  # Marca clara de datos pendientes
        'nombre_completo': f"Conductor Pendiente {str(conductor_uuid)[:8]}",  # Marca clara
        'codigo_qr_hash': str(conductor_uuid),
        'activo': True  # Asumir activo hasta verificar con cloud
    }

    conductor = _create_conductor_local_internal(db, conductor_minimal_data)
    logger.info(f"Conductor {conductor_uuid} creado con datos mínimos (pendiente sincronización)")

    return conductor

def is_conductor_data_minimal(conductor: ConductorLocal) -> bool:
    """
    Determina si el conductor tiene datos mínimos/temporales (no sincronizado).

    Args:
        conductor: Conductor local a verificar

    Returns:
        bool: True si tiene datos mínimos, False si tiene datos completos
    """
    return (
        conductor.cedula.startswith("PENDING_SYNC_") or
        conductor.nombre_completo.startswith("Conductor Pendiente")
    )

def should_update_conductor_data(conductor: ConductorLocal, max_age_hours: int = 24) -> bool:
    """
    Determina si los datos del conductor necesitan actualizarse desde cloud.

    Args:
        conductor: Conductor local existente
        max_age_hours: Máxima antigüedad de datos antes de requerir actualización

    Returns:
        bool: True si necesita actualización, False si no
    """
    # Si tiene datos mínimos/temporales (nunca sincronizado)
    if is_conductor_data_minimal(conductor):
        return True

    # Si los datos son muy antiguos
    if conductor.last_updated_at:
        age = datetime.utcnow() - conductor.last_updated_at
        if age > timedelta(hours=max_age_hours):
            return True

    # Si no tiene embeddings faciales (datos incompletos)
    if not conductor.caracteristicas_faciales_embedding:
        return True

    return False

def try_sync_conductor_from_cloud_conditional(
    db: Session,
    conductor: ConductorLocal,
    cloud_sync_function,
    force_update: bool = False
) -> bool:
    """
    Intenta sincronizar datos del conductor desde cloud SOLO si es necesario.

    Args:
        db: Sesión de base de datos
        conductor: Conductor local existente
        cloud_sync_function: Función para obtener datos del conductor desde cloud
        force_update: Forzar actualización independientemente de la edad de datos

    Returns:
        bool: True si se sincronizó, False si no fue necesario o falló
    """
    # Verificar si necesita actualización
    if not force_update and not should_update_conductor_data(conductor):
        logger.debug(f"Conductor {conductor.nombre_completo} no necesita actualización")
        return True  # No necesita actualización = éxito

    try:
        conductor_cloud_data = cloud_sync_function(conductor.id)

        if not conductor_cloud_data:
            logger.warning(f"Conductor {conductor.id} no encontrado en cloud")
            return False

        # Verificar si los datos de cloud son diferentes a los locales
        updated = False

        # Actualizar cédula si es temporal o diferente
        cloud_cedula = conductor_cloud_data.get('cedula')
        if cloud_cedula and (conductor.cedula.startswith("PENDING_SYNC_") or conductor.cedula != cloud_cedula):
            conductor.cedula = cloud_cedula
            updated = True

        # Actualizar nombre si es temporal o diferente
        cloud_nombre = conductor_cloud_data.get('nombre_completo')
        if cloud_nombre and (conductor.nombre_completo.startswith("Conductor Pendiente") or conductor.nombre_completo != cloud_nombre):
            conductor.nombre_completo = cloud_nombre
            updated = True

        # Actualizar embeddings si no existen localmente o son diferentes
        cloud_embedding = conductor_cloud_data.get('caracteristicas_faciales_embedding')
        if cloud_embedding and conductor.caracteristicas_faciales_embedding != cloud_embedding:
            conductor.caracteristicas_faciales_embedding = cloud_embedding
            updated = True

        # Actualizar estado activo (siempre importante si es diferente)
        cloud_activo = conductor_cloud_data.get('activo', True)
        if conductor.activo != cloud_activo:
            conductor.activo = cloud_activo
            updated = True

        if updated:
            conductor.last_updated_at = datetime.utcnow()
            db.commit()
            db.refresh(conductor)
            logger.info(f"Conductor {conductor.nombre_completo} actualizado en BD local")
        else:
            logger.debug(f"Conductor {conductor.nombre_completo} ya tenía datos actualizados")

        return True

    except Exception as e:
        logger.warning(f"Error sincronizando conductor {conductor.id} desde cloud: {e}")
        return False

def create_driver_session_from_qr_robust(
    db: Session,
    qr_data: str,  # UUID del conductor desde QR
    bus_id: uuid.UUID,
    cloud_sync_function,
    current_time: datetime = None
) -> Tuple[Optional[AsignacionConductorBusLocal], Optional[ConductorLocal], Dict[str, str]]:
    """
    Crea una sesión de conductor basada en escaneo QR.
    ROBUSTO: Siempre crea sesión, actualización desde cloud es condicional.

    Args:
        db: Sesión de base de datos
        qr_data: Datos del QR escaneado (UUID del conductor)
        bus_id: UUID del bus actual
        cloud_sync_function: Función para obtener datos del conductor desde cloud
        current_time: Timestamp del escaneo (opcional)

    Returns:
        Tuple[AsignacionConductorBusLocal, ConductorLocal, Dict]:
        (asignacion_creada, conductor, resultado_info)
    """
    if current_time is None:
        current_time = datetime.utcnow()

    resultado = {
        'status': 'error',
        'message': 'Error desconocido',
        'conductor_actualizado': False,
        'operacion_offline': False,
        'datos_temporales': False
    }

    try:
        # Validar formato UUID del QR
        try:
            conductor_uuid = uuid.UUID(qr_data)
        except ValueError:
            resultado['message'] = 'QR inválido: no es un UUID válido'
            return None, None, resultado

        # PASO 1: Buscar conductor existente o crear con datos mínimos
        conductor = get_conductor_by_uuid(db, conductor_uuid)

        if not conductor:
            # No existe → crear con datos mínimos
            conductor = ensure_conductor_exists_minimal(db, conductor_uuid)
            resultado['datos_temporales'] = True
            logger.info(f"Conductor {conductor_uuid} creado con datos mínimos")

        # PASO 2: Verificar si necesita actualización y sincronizar condicionalmente
        sync_success = try_sync_conductor_from_cloud_conditional(
            db, conductor, cloud_sync_function, force_update=False
        )

        resultado['conductor_actualizado'] = sync_success
        resultado['operacion_offline'] = not sync_success

        # Verificar si sigue teniendo datos temporales después del sync
        resultado['datos_temporales'] = is_conductor_data_minimal(conductor)

        # PASO 3: Verificar estado del conductor (usar datos locales actuales)
        if not conductor.activo:
            resultado['message'] = f'Conductor {conductor.nombre_completo} está inactivo'
            return None, conductor, resultado

        # PASO 4: Verificar si ya hay una sesión activa para este bus
        active_session = get_active_asignacion_for_bus(db, bus_id)

        if active_session:
            if active_session.id_conductor == conductor.id:
                # Mismo conductor → finalizar sesión actual
                active_session.fecha_fin_asignacion = current_time
                active_session.estado_turno = 'Finalizado'
                update_asignacion_conductor_bus_local(db, active_session)

                resultado.update({
                    'status': 'session_ended',
                    'message': f'Sesión finalizada para {conductor.nombre_completo}'
                })
                return None, conductor, resultado
            else:
                # Diferente conductor → finalizar sesión anterior e iniciar nueva
                active_session.fecha_fin_asignacion = current_time
                active_session.estado_turno = 'Finalizado'
                update_asignacion_conductor_bus_local(db, active_session)

        # PASO 5: Crear nueva sesión (SIEMPRE funciona, con o sin cloud)
        session_id = uuid.uuid4()
        new_session = create_asignacion_conductor_bus_local(
            db=db,
            id_conductor=conductor.id,
            id_bus=bus_id,
            id_sesion_conduccion=session_id,
            fecha_inicio_asignacion=current_time,
            estado_turno='Activo',
            tipo_asignacion='QR_Scan'
        )

        # Mensaje según el estado de los datos
        base_message = f'Sesión iniciada para {conductor.nombre_completo}'

        if resultado['datos_temporales']:
            resultado['message'] = f'{base_message} (datos temporales - verificar conectividad)'
        elif not sync_success:
            resultado['message'] = f'{base_message} (sin actualización cloud)'
        else:
            resultado['message'] = base_message

        resultado['status'] = 'session_started'

        return new_session, conductor, resultado

    except Exception as e:
        logger.error(f"Error creando sesión desde QR {qr_data}: {e}", exc_info=True)
        resultado['message'] = f'Error del sistema: {str(e)}'
        return None, None, resultado

# ACTUALIZAR TAMBIÉN la función de creación/actualización para ser más selectiva
def create_or_update_conductor_local_selective(db: Session, conductor_data: Dict[str, Any], force_update: bool = False) -> ConductorLocal:
    """
    Crea o actualiza un conductor en la BD local de forma selectiva.
    Solo actualiza si force_update=True o si los datos locales son muy antiguos/incompletos.

    Args:
        db: Sesión de base de datos
        conductor_data: Datos del conductor
        force_update: Forzar actualización incluso si los datos son recientes

    Returns:
        ConductorLocal: Conductor creado o actualizado
    """
    conductor_id = conductor_data.get('id')
    if not conductor_id:
        raise ValueError("ID del conductor es requerido para crear o actualizar.")

    existing_conductor = db.query(ConductorLocal).filter(ConductorLocal.id == uuid.UUID(str(conductor_id))).first()

    if existing_conductor:
        # Conductor existe → verificar si necesita actualización
        if force_update or should_update_conductor_data(existing_conductor):
            updated_conductor = _update_conductor_local_internal(db, existing_conductor, conductor_data)
            logger.info(f"Conductor {updated_conductor.nombre_completo} actualizado en BD local")
            return updated_conductor
        else:
            logger.debug(f"Conductor {existing_conductor.nombre_completo} no necesita actualización")
            return existing_conductor
    else:
        # Conductor no existe → crear nuevo
        if isinstance(conductor_data['id'], str):
            conductor_data['id'] = uuid.UUID(conductor_data['id'])

        try:
            new_conductor = _create_conductor_local_internal(db, conductor_data)
            logger.info(f"Conductor {new_conductor.nombre_completo} creado en BD local")
            return new_conductor
        except IntegrityError as e:
            db.rollback()
            raise ValueError(f"Error de integridad al crear conductor: {e.orig}")

def create_driver_session_from_qr_robust(
    db: Session,
    qr_data: str,  # UUID del conductor desde QR
    bus_id: uuid.UUID,
    cloud_sync_function,
    current_time: datetime = None
) -> Tuple[Optional[AsignacionConductorBusLocal], Optional[ConductorLocal], Dict[str, str]]:
    """
    Crea una sesión de conductor basada en escaneo QR.
    ROBUSTO: Siempre crea sesión, actualización desde cloud es condicional.

    Args:
        db: Sesión de base de datos
        qr_data: Datos del QR escaneado (UUID del conductor)
        bus_id: UUID del bus actual
        cloud_sync_function: Función para obtener datos del conductor desde cloud
        current_time: Timestamp del escaneo (opcional)

    Returns:
        Tuple[AsignacionConductorBusLocal, ConductorLocal, Dict]:
        (asignacion_creada, conductor, resultado_info)
    """
    if current_time is None:
        current_time = datetime.utcnow()

    resultado = {
        'status': 'error',
        'message': 'Error desconocido',
        'conductor_actualizado': False,
        'operacion_offline': False,
        'datos_temporales': False
    }

    try:
        # Validar formato UUID del QR
        try:
            conductor_uuid = uuid.UUID(qr_data)
        except ValueError:
            resultado['message'] = 'QR inválido: no es un UUID válido'
            return None, None, resultado

        # PASO 1: Buscar conductor existente o crear con datos mínimos
        conductor = get_conductor_by_uuid(db, conductor_uuid)

        if not conductor:
            # No existe → crear con datos mínimos
            conductor = ensure_conductor_exists_minimal(db, conductor_uuid)
            resultado['datos_temporales'] = True
            logger.info(f"Conductor {conductor_uuid} creado con datos mínimos")

        # PASO 2: Verificar si necesita actualización y sincronizar condicionalmente
        sync_success = try_sync_conductor_from_cloud_conditional(
            db, conductor, cloud_sync_function, force_update=False
        )

        resultado['conductor_actualizado'] = sync_success
        resultado['operacion_offline'] = not sync_success

        # Verificar si sigue teniendo datos temporales después del sync
        resultado['datos_temporales'] = is_conductor_data_minimal(conductor)

        # PASO 3: Verificar estado del conductor (usar datos locales actuales)
        if not conductor.activo:
            resultado['message'] = f'Conductor {conductor.nombre_completo} está inactivo'
            return None, conductor, resultado

        # PASO 4: Verificar si ya hay una sesión activa para este bus
        active_session = get_active_asignacion_for_bus(db, bus_id)

        if active_session:
            if active_session.id_conductor == conductor.id:
                # Mismo conductor → finalizar sesión actual
                active_session.fecha_fin_asignacion = current_time
                active_session.estado_turno = 'Finalizado'
                update_asignacion_conductor_bus_local(db, active_session)

                resultado.update({
                    'status': 'session_ended',
                    'message': f'Sesión finalizada para {conductor.nombre_completo}'
                })
                return None, conductor, resultado
            else:
                # Diferente conductor → finalizar sesión anterior e iniciar nueva
                active_session.fecha_fin_asignacion = current_time
                active_session.estado_turno = 'Finalizado'
                update_asignacion_conductor_bus_local(db, active_session)

        # PASO 5: Crear nueva sesión (SIEMPRE funciona, con o sin cloud)
        session_id = uuid.uuid4()
        new_session = create_asignacion_conductor_bus_local(
            db=db,
            id_conductor=conductor.id,
            id_bus=bus_id,
            id_sesion_conduccion=session_id,
            fecha_inicio_asignacion=current_time,
            estado_turno='Activo',
            tipo_asignacion='QR_Scan'
        )

        # Mensaje según el estado de los datos
        base_message = f'Sesión iniciada para {conductor.nombre_completo}'

        if resultado['datos_temporales']:
            resultado['message'] = f'{base_message} (datos temporales - verificar conectividad)'
        elif not sync_success:
            resultado['message'] = f'{base_message} (sin actualización cloud)'
        else:
            resultado['message'] = base_message

        resultado['status'] = 'session_started'

        return new_session, conductor, resultado

    except Exception as e:
        logger.error(f"Error creando sesión desde QR {qr_data}: {e}", exc_info=True)
        resultado['message'] = f'Error del sistema: {str(e)}'
        return None, None, resultado

def create_event_with_session_validation(
    db: Session,
    bus_id: uuid.UUID,
    event_data: Dict[str, Any],
    snapshot_path: Optional[str] = None,
    video_clip_path: Optional[str] = None
) -> Tuple[Optional[EventoLocal], Dict[str, str]]:
    """
    Crea un evento validando que hay una sesión activa.

    Args:
        db: Sesión de base de datos
        bus_id: UUID del bus
        event_data: Datos del evento (sin id_conductor ni id_sesion_conduccion)
        snapshot_path: Ruta local del snapshot
        video_clip_path: Ruta local del video

    Returns:
        Tuple[EventoLocal, Dict]: (evento_creado, resultado_info)
    """
    resultado = {'status': 'error', 'message': 'Error desconocido'}

    # Verificar que hay una sesión activa
    active_session = get_active_asignacion_for_bus(db, bus_id)

    if not active_session:
        resultado['message'] = 'No hay conductor activo en este bus'
        return None, resultado

    # Completar datos del evento con sesión activa
    event_data.update({
        'id_conductor': active_session.id_conductor,
        'id_sesion_conduccion': active_session.id_sesion_conduccion,
        'id_bus': bus_id
    })

    # Crear evento con archivos multimedia
    evento = create_event_with_multimedia(
        db, event_data, snapshot_path, video_clip_path
    )

    resultado.update({
        'status': 'success',
        'message': f'Evento {evento.tipo_evento} creado para sesión {active_session.id_sesion_conduccion}'
    })

    return evento, resultado

def get_unsynced_events(db: Session, limit: int = 100) -> List[EventoLocal]:
    """
    Obtiene una lista de eventos locales que aún no han sido sincronizados con la nube.
    """
    return db.query(EventoLocal).filter(EventoLocal.synced_to_cloud == False).limit(limit).all()

def mark_event_as_synced(db: Session, event_id: uuid.UUID) -> Optional[EventoLocal]:
    """
    Marca un evento local como sincronizado con la nube.
    """
    event = db.query(EventoLocal).filter(EventoLocal.id == event_id).first()
    if event:
        event.synced_to_cloud = True
        event.sent_to_cloud_at = datetime.utcnow()
        db.commit()
        db.refresh(event)
    return event

# --- Funciones CRUD para AlertaLocal ---

def create_local_alert(db: Session, alert_data: dict) -> AlertaLocal:
    """
    Crea una nueva alerta en la base de datos local de la Jetson.
    """
    new_alert = AlertaLocal(**alert_data)
    db.add(new_alert)
    db.commit()
    db.refresh(new_alert)
    return new_alert

def get_pending_local_alerts(db: Session) -> List[AlertaLocal]:
    """
    Obtiene las alertas locales que aún no han sido visualizadas o resueltas localmente.
    """
    return db.query(AlertaLocal).filter(AlertaLocal.estado_visualizado == False).all()

def mark_alert_as_visualized(db: Session, alert_id: uuid.UUID) -> Optional[AlertaLocal]:
    """
    Marca una alerta local como visualizada (ej. por el conductor).
    """
    alert = db.query(AlertaLocal).filter(AlertaLocal.id == alert_id).first()
    if alert:
        alert.estado_visualizado = True
        db.commit()
        db.refresh(alert)
    return alert

# --- Funciones CRUD para SincronizacionMetadata ---

def get_sync_metadata(db: Session, table_name: str) -> Optional[SincronizacionMetadata]:
    """
    Obtiene el registro de metadatos de sincronización para una tabla específica.
    """
    return db.query(SincronizacionMetadata).filter(SincronizacionMetadata.tabla_nombre == table_name).first()

def create_or_update_sync_metadata(db: Session, table_name: str, **kwargs) -> SincronizacionMetadata:
    """
    Crea o actualiza un registro de metadatos de sincronización.
    """
    metadata = get_sync_metadata(db, table_name)
    if not metadata:
        metadata = SincronizacionMetadata(tabla_nombre=table_name)
        db.add(metadata)

    for key, value in kwargs.items():
        setattr(metadata, key, value)

    db.commit()
    db.refresh(metadata)
    return metadata

# --- Funciones CRUD para TelemetryLocal ---

def create_local_telemetry(db: Session, telemetry_data: Dict[str, Any]) -> Optional[TelemetryLocal]: # Added Optional return type
    """
    Crea un nuevo registro de telemetría en la base de datos local de la Jetson.

    Args:
        db: Sesión de base de datos
        telemetry_data: Diccionario con los datos de telemetría (ram_usage_gb, cpu_usage_percent, etc.)
                        Debe incluir 'id_hardware_jetson'.

    Returns:
        TelemetryLocal: El registro de telemetría creado, o None si hubo un error.
    """
    logger.debug(f"Attempting to create local telemetry with data: {telemetry_data}")
    try:
        new_telemetry = TelemetryLocal(**telemetry_data)
        db.add(new_telemetry)
        db.commit()
        db.refresh(new_telemetry)
        logger.info(f"Telemetry record {new_telemetry.id} successfully created locally.")
        return new_telemetry
    except SQLAlchemyError as e: # Catch SQLAlchemy specific errors
        db.rollback() # Rollback the session in case of an error
        logger.error(f"SQLAlchemy Error creating local telemetry: {e}", exc_info=True)
        return None
    except Exception as e: # Catch any other unexpected errors
        db.rollback()
        logger.error(f"Unexpected Error creating local telemetry: {e}", exc_info=True)
        return None

def get_unsynced_telemetry(db: Session, limit: int = 100) -> List[TelemetryLocal]:
    """
    Obtiene una lista de registros de telemetría locales que aún no han sido sincronizados con la nube.

    Args:
        db: Sesión de base de datos
        limit: Número máximo de registros de telemetría a retornar.

    Returns:
        List[TelemetryLocal]: Lista de registros de telemetría pendientes de sincronizar.
    """
    return db.query(TelemetryLocal).filter(TelemetryLocal.synced_to_cloud == False).limit(limit).all()

def mark_telemetry_as_synced(db: Session, telemetry_id: uuid.UUID) -> Optional[TelemetryLocal]:
    """
    Marca un registro de telemetría local como sincronizado con la nube.

    Args:
        db: Sesión de base de datos
        telemetry_id: UUID del registro de telemetría a marcar.

    Returns:
        Optional[TelemetryLocal]: El registro de telemetría actualizado o None si no se encontró.
    """
    telemetry = db.query(TelemetryLocal).filter(TelemetryLocal.id == telemetry_id).first()
    if telemetry:
        telemetry.synced_to_cloud = True
        telemetry.sent_to_cloud_at = datetime.utcnow()
        db.commit()
        db.refresh(telemetry)
    return telemetry

def get_synced_telemetry_for_cleanup(db: Session, days_old: int = 30, limit: int = 500) -> List[TelemetryLocal]:
    """
    Obtiene registros de telemetría sincronizados que son candidatos para limpieza.

    Args:
        db: Sesión de base de datos
        days_old: Días de antigüedad mínima para considerar un registro para limpieza.
        limit: Número máximo de registros de telemetría a retornar para limpieza.

    Returns:
        List[TelemetryLocal]: Lista de registros de telemetría candidatos para limpieza.
    """
    cutoff_date = datetime.utcnow() - timedelta(days=days_old)

    return db.query(TelemetryLocal).filter(
        and_(
            TelemetryLocal.synced_to_cloud == True,
            TelemetryLocal.timestamp_telemetry < cutoff_date
        )
    ).limit(limit).all()

def cleanup_telemetry_records(db: Session, telemetry_records: List[TelemetryLocal]) -> Dict[str, Any]:
    """
    Elimina registros de telemetría de la base de datos local.

    Args:
        db: Sesión de base de datos
        telemetry_records: Lista de objetos TelemetryLocal a eliminar.

    Returns:
        Dict: Estadísticas de limpieza (registros_borrados, errores).
    """
    cleanup_stats = {
        'registros_borrados': 0,
        'errores': []
    }

    if not telemetry_records:
        return cleanup_stats

    for record in telemetry_records:
        try:
            db.delete(record)
            cleanup_stats['registros_borrados'] += 1
        except Exception as e:
            cleanup_stats['errores'].append(f"Error borrando registro de telemetría {record.id}: {e}")
            logger.error(f"Error borrando registro de telemetría {record.id}: {e}", exc_info=True)

    try:
        db.commit()
        logger.info(f"Limpieza de telemetría: {cleanup_stats['registros_borrados']} registros borrados.")
    except Exception as e:
        db.rollback()
        cleanup_stats['errores'].append(f"Error en commit de limpieza de telemetría: {e}")
        logger.error(f"Error en commit de limpieza de telemetría: {e}", exc_info=True)

    return cleanup_stats
import uuid
from datetime import datetime
from typing import Optional, List, Dict, Any

from sqlalchemy.orm import Session 
from sqlalchemy.exc import IntegrityError 

# Importamos los modelos de la base de datos local de la Jetson
from app.models.edge_database_models import (
    ConductorLocal,
    BusLocal,
    AsignacionConductorBusLocal,
    EventoLocal,
    AlertaLocal,
    ConfiguracionJetsonLocal,
    SincronizacionMetadata
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
# Mantenemos estas, pero la principal de uso externo será create_or_update_conductor_local

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
    Asume que conductor_data contiene todos los campos necesarios, incluido el 'id' (UUID).
    """
    conductor_data_processed = {k: v for k, v in conductor_data.items() if k in ConductorLocal.__table__.columns.keys()}
    
    if 'id' in conductor_data_processed and isinstance(conductor_data_processed['id'], str):
        conductor_data_processed['id'] = uuid.UUID(conductor_data_processed['id'])
    
    # >>>>>>>>>>>>> CAMBIO AQUI: Procesar id_empresa <<<<<<<<<<<<<
    if 'id_empresa' in conductor_data_processed and isinstance(conductor_data_processed['id_empresa'], str):
        try:
            conductor_data_processed['id_empresa'] = uuid.UUID(conductor_data_processed['id_empresa'])
        except ValueError:
            conductor_data_processed['id_empresa'] = None # Manejo de error para id_empresa
    
    if 'fecha_nacimiento' in conductor_data_processed and isinstance(conductor_data_processed['fecha_nacimiento'], str):
        try:
            conductor_data_processed['fecha_nacimiento'] = datetime.strptime(conductor_data_processed['fecha_nacimiento'], '%Y-%m-%d').date()
        except ValueError:
            pass 

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
        # >>>>>>>>>>>>> CAMBIO AQUI: Procesar id_empresa en actualizaciones <<<<<<<<<<<<<
        if key == 'id_empresa' and isinstance(value, str):
            try:
                setattr(conductor_obj, key, uuid.UUID(value))
            except ValueError:
                setattr(conductor_obj, key, None)
        elif key == 'fecha_nacimiento' and isinstance(value, str):
             try:
                 setattr(conductor_obj, key, datetime.strptime(value, '%Y-%m-%d').date())
             except ValueError:
                 pass
        else:
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
# Mantenemos estas, pero la principal de uso externo será create_or_update_bus_local

def get_bus_local_by_id(db: Session, bus_id: uuid.UUID) -> Optional[BusLocal]:
    """
    Obtiene un bus de la BD local por su UUID.
    """
    return db.query(BusLocal).filter(BusLocal.id == bus_id).first()

def _create_bus_local_internal(db: Session, bus_data: dict) -> BusLocal:
    """
    Función interna para crear un nuevo bus localmente.
    Asume que bus_data contiene todos los campos necesarios, incluido el 'id' (UUID).
    """
    bus_data_processed = {k: v for k, v in bus_data.items() if k in BusLocal.__table__.columns.keys()}
    if 'id' in bus_data_processed and isinstance(bus_data_processed['id'], str):
        bus_data_processed['id'] = uuid.UUID(bus_data_processed['id'])
    # >>>>>>>>>>>>> CAMBIO AQUI: Procesar id_empresa <<<<<<<<<<<<<
    if 'id_empresa' in bus_data_processed and isinstance(bus_data_processed['id_empresa'], str):
        try:
            bus_data_processed['id_empresa'] = uuid.UUID(bus_data_processed['id_empresa'])
        except ValueError:
            bus_data_processed['id_empresa'] = None

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
        # >>>>>>>>>>>>> CAMBIO AQUI: Procesar id_empresa en actualizaciones <<<<<<<<<<<<<
        if key == 'id_empresa' and isinstance(value, str):
            try:
                setattr(bus_obj, key, uuid.UUID(value))
            except ValueError:
                setattr(bus_obj, key, None)
        else:
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

# --- Funciones CRUD para EventoLocal ---

def create_local_event(db: Session, event_data: dict) -> EventoLocal:
    """
    Crea un nuevo evento en la base de datos local de la Jetson.
    """
    new_event = EventoLocal(**event_data)
    db.add(new_event)
    db.commit()
    db.refresh(new_event)
    return new_event

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
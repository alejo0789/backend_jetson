import uuid 
from datetime import datetime, date
from sqlalchemy import Column, String, Integer, DateTime, Date, Boolean, Numeric, ForeignKey, Text, JSON, TypeDecorator, CHAR
from sqlalchemy.dialects.postgresql import UUID as PG_UUID
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects import postgresql 


# --- Tipo de dato UUID personalizado para compatibilidad con SQLite ---
class UUIDType(TypeDecorator):
    """
    Tipo de dato UUID personalizado que se adapta al dialecto de la base de datos.
    Almacena UUIDs como TEXT en SQLite y como UUID nativo en PostgreSQL.
    """
    impl = CHAR(36)  
    cache_ok = True

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        if dialect.name == 'postgresql':
            return str(value) 
        else: 
            return str(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        if dialect.name == 'postgresql':
            return uuid.UUID(value) if isinstance(value, str) else value
        else: 
            return uuid.UUID(value)

    def load_dialect_impl(self, dialect):
        if dialect.name == 'postgresql':
            return dialect.type_descriptor(postgresql.UUID()) 
        else: 
            return dialect.type_descriptor(CHAR(36)) 

    def copy(self, **kw):
        return UUIDType(self.impl.length)


# Base declarativa para los modelos ORM del Edge
Base = declarative_base()

# --- Modelos para la Base de Datos Local en la Jetson Nano (SQLite) ---

class ConfiguracionJetsonLocal(Base):
    """
    Tabla para almacenar la configuración local y el estado de la propia Jetson Nano.
    Contendrá una única fila para identificar el dispositivo y su asignación.
    """
    __tablename__ = 'configuracion_jetson_local'
    id = Column(Integer, primary_key=True, autoincrement=True) 
    id_hardware_jetson = Column(String, unique=True, nullable=False) 
    id_bus_asignado = Column(UUIDType, ForeignKey('buses_local.id'), nullable=True) 
    version_firmware_local = Column(String) 
    estado_operativo_local = Column(String, default='Activo', nullable=False) 
    ultima_actualizacion_config_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    
    bus_asignado = relationship("BusLocal", back_populates="configuracion_jetson_asociada", uselist=False)

    def __repr__(self):
        return (f"<ConfiguracionJetsonLocal(id_hardware='{self.id_hardware_jetson}', "
                f"bus_id='{self.id_bus_asignado}', estado='{self.estado_operativo_local}')>")

class ConductorLocal(Base):
    """
    Modelo de conductor simplificado para el Edge.
    Contiene solo la información necesaria para la identificación local y el vínculo con eventos.
    """
    __tablename__ = 'conductores_local'
    id = Column(UUIDType, primary_key=True, default=uuid.uuid4, unique=True, nullable=False) 
    cedula = Column(String, unique=True, nullable=False)
    nombre_completo = Column(String, nullable=False)
    codigo_qr_hash = Column(String, unique=True) 
    caracteristicas_faciales_embedding = Column(JSON) 
    activo = Column(Boolean, default=True, nullable=False) 
    last_updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False) 

    def __repr__(self):
        return f"<ConductorLocal(id='{self.id}', cedula='{self.cedula}', nombre='{self.nombre_completo}')>"

class BusLocal(Base):
    """
    Modelo de bus simplificado para el Edge.
    Contiene solo la información necesaria para la operación local y la asignación.
    """
    __tablename__ = 'buses_local'
    id = Column(UUIDType, primary_key=True, default=uuid.uuid4, unique=True, nullable=False) 
    placa = Column(String, nullable=False)
    numero_interno = Column(String, nullable=False)
    last_updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False) 

    configuracion_jetson_asociada = relationship("ConfiguracionJetsonLocal", back_populates="bus_asignado", uselist=False)

    def __repr__(self):
        return f"<BusLocal(id='{self.id}', placa='{self.placa}')>"

class AsignacionConductorBusLocal(Base):
    """
    Modelo de asignación de conductor a bus para el Edge.
    Esencial para el control de horas de conducción local y sesiones de turno.
    """
    __tablename__ = 'asignaciones_conductores_buses_local'
    id = Column(UUIDType, primary_key=True, default=uuid.uuid4, unique=True, nullable=False)
    id_conductor = Column(UUIDType, ForeignKey('conductores_local.id'), nullable=False)
    id_bus = Column(UUIDType, ForeignKey('buses_local.id'), nullable=False)
    id_sesion_conduccion = Column(UUIDType, default=uuid.uuid4, unique=True, nullable=False) 
    fecha_inicio_asignacion = Column(DateTime, nullable=False) 
    fecha_fin_asignacion = Column(DateTime, nullable=True) 
    estado_turno = Column(String, default='Activo', nullable=False) 
    tipo_asignacion = Column(String, nullable=True) # Este campo ya está nullable=True
    tiempo_conduccion_acumulado_seg = Column(Integer, default=0) 
    last_updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)

    # Relaciones locales
    conductor = relationship("ConductorLocal", backref="asignaciones_local")
    bus = relationship("BusLocal", backref="asignaciones_local")
    eventos = relationship("EventoLocal", back_populates="asignacion_local") 
    alertas = relationship("AlertaLocal", back_populates="asignacion_local") 

    def __repr__(self):
        return (f"<AsignacionLocal(id='{self.id}', sesion='{self.id_sesion_conduccion}', "
                f"conductor='{self.id_conductor}', estado='{self.estado_turno}')>")


class EventoLocal(Base):
    """
    Modelo de Evento para el Edge.
    Estos son los eventos de IA generados localmente por la Jetson antes de la sincronización.
    """
    __tablename__ = 'eventos_local'
    id = Column(UUIDType, primary_key=True, default=uuid.uuid4, unique=True, nullable=False) 
    # >>>>>>>>>>>>> CAMBIO AQUI: nullable=True para id_local_jetson <<<<<<<<<<<<<
    id_local_jetson = Column(Integer, autoincrement=True, unique=True, nullable=True) 
    id_bus = Column(UUIDType, ForeignKey('buses_local.id'), nullable=False)
    id_conductor = Column(UUIDType, ForeignKey('conductores_local.id'), nullable=False)
    id_sesion_conduccion = Column(UUIDType, ForeignKey('asignaciones_conductores_buses_local.id_sesion_conduccion'), nullable=True)
    timestamp_evento = Column(DateTime, nullable=False)
    tipo_evento = Column(String, nullable=False)
    subtipo_evento = Column(String)
    duracion_segundos = Column(Numeric)
    severidad = Column(String)
    confidence_score_ia = Column(Numeric)
    alerta_disparada = Column(Boolean, default=False, nullable=False)
    ubicacion_gps_evento = Column(String)
    metadatos_ia_json = Column(JSON) 
    synced_to_cloud = Column(Boolean, default=False, nullable=False) 
    sent_to_cloud_at = Column(DateTime) 

    #evento multimedia
    snapshot_local_path = Column(String)  # Ruta local del snapshot
    video_clip_local_path = Column(String)  # Ruta local del video
    archivos_synced = Column(Boolean, default=False)

    # Relaciones locales
    bus = relationship("BusLocal", backref="eventos_local")
    conductor = relationship("ConductorLocal", backref="eventos_local")
    asignacion_local = relationship("AsignacionConductorBusLocal", back_populates="eventos")

    def __repr__(self):
        return (f"<EventoLocal(id='{self.id}', tipo='{self.tipo_evento}', "
                f"time='{self.timestamp_evento}', synced='{self.synced_to_cloud}')>")


class AlertaLocal(Base):
    """
    Modelo de Alerta para el Edge.
    Representa alertas que se disparan y manejan localmente en la cabina del bus.
    """
    __tablename__ = 'alertas_local'
    id = Column(UUIDType, primary_key=True, default=uuid.uuid4, unique=True, nullable=False)
    id_evento = Column(UUIDType, ForeignKey('eventos_local.id'), nullable=True) 
    id_conductor = Column(UUIDType, ForeignKey('conductores_local.id'), nullable=False)
    id_bus = Column(UUIDType, ForeignKey('buses_local.id'), nullable=False)
    id_sesion_conduccion = Column(UUIDType, ForeignKey('asignaciones_conductores_buses_local.id_sesion_conduccion'), nullable=True)
    timestamp_alerta = Column(DateTime, default=datetime.utcnow, nullable=False)
    tipo_alerta = Column(String, nullable=False) 
    descripcion = Column(Text, nullable=False)
    estado_visualizado = Column(Boolean, default=False, nullable=False) 

    # Relaciones locales
    evento_local = relationship("EventoLocal", backref="alertas_local", uselist=False) 
    asignacion_local = relationship("AsignacionConductorBusLocal", back_populates="alertas")

    def __repr__(self):
        return (f"<AlertaLocal(id='{self.id}', tipo='{self.tipo_alerta}', "
                f"time='{self.timestamp_alerta}', visualizada='{self.estado_visualizado}')>")

class SincronizacionMetadata(Base):
    """
    Tabla para gestionar el estado de sincronización de datos de las tablas locales con la nube.
    """
    __tablename__ = 'sincronizacion_metadata'
    id = Column(Integer, primary_key=True, autoincrement=True)
    tabla_nombre = Column(String, unique=True, nullable=False) 
    ultimo_id_sincronizado_local = Column(UUIDType, nullable=True) 
    ultimo_timestamp_sincronizado_nube = Column(DateTime, nullable=True) 
    last_pushed_at = Column(DateTime) 
    last_pulled_at = Column(DateTime) 

    def __repr__(self):
        return (f"<SincronizacionMetadata(tabla='{self.tabla_nombre}', "
                f"last_pushed='{self.last_pushed_at}', last_pulled='{self.last_pulled_at}')>")
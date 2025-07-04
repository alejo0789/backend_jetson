import cv2
import uuid
from pyzbar.pyzbar import decode
import numpy as np
import logging
from typing import Optional, Tuple  # ← Añadir Tuple aquí

# Configuración básica del logger para este módulo
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def scan_qr_code(frame: np.ndarray) -> Optional[str]:
    """
    Escanea un fotograma (imagen) en busca de códigos QR y devuelve el dato decodificado
    del primer QR encontrado.

    Args:
        frame (np.ndarray): El fotograma de imagen de la cámara (array NumPy).

    Returns:
        Optional[str]: El dato decodificado del código QR como string, o None si no se encuentra.
    """
    if frame is None:
        logger.warning("scan_qr_code: El fotograma de entrada es None. No se puede escanear.")
        return None

    # Convierte el fotograma a escala de grises para mejorar la detección de QR
    gray_frame = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)

    # Decodifica los códigos de barras (incluidos QR) en el fotograma
    decoded_objects = decode(gray_frame)

    if decoded_objects:
        # Se encontró al menos un código QR
        for obj in decoded_objects:
            qr_data = obj.data.decode('utf-8')  # Decodifica los bytes a string UTF-8
            qr_type = obj.type  # Tipo de código (ej. 'QRCODE')
            
            logger.info(f"QR detectado: Tipo={qr_type}, Datos={qr_data}")
            
            return qr_data
    
    # Si no se encontró ningún código QR
    return None

def process_qr_data(qr_data: str) -> str:
    """
    Procesa el dato decodificado del QR para extraer y validar el UUID del conductor.
    El QR debe contener un UUID válido que identifica al conductor.

    Args:
        qr_data (str): El string decodificado del código QR.

    Returns:
        str: El UUID del conductor como string válido.
        
    Raises:
        ValueError: Si el QR no contiene un UUID válido.
    """
    try:
        # Limpiar espacios en blanco y caracteres no deseados
        qr_data_clean = qr_data.strip()
        
        # Validar que es un UUID válido
        conductor_uuid = uuid.UUID(qr_data_clean)
        
        # Convertir de vuelta a string para consistencia
        conductor_uuid_str = str(conductor_uuid)
        
        logger.info(f"process_qr_data: UUID válido del conductor: {conductor_uuid_str}")
        return conductor_uuid_str
        
    except ValueError as e:
        error_msg = f"QR inválido: '{qr_data}' no es un UUID válido. Error: {e}"
        logger.error(error_msg)
        raise ValueError(error_msg)
    except Exception as e:
        error_msg = f"Error inesperado procesando QR '{qr_data}': {e}"
        logger.error(error_msg)
        raise ValueError(error_msg)

def validate_conductor_qr(qr_data: str) -> Tuple[bool, str, Optional[str]]:  # ← Cambiar tuple por Tuple
    """
    Valida si un código QR contiene un UUID de conductor válido.
    
    Args:
        qr_data (str): Datos del código QR escaneado.
    
    Returns:
        Tuple[bool, str, Optional[str]]: (es_válido, mensaje, uuid_conductor)
    """
    try:
        conductor_uuid_str = process_qr_data(qr_data)
        return True, f"QR válido: Conductor {conductor_uuid_str[:8]}...", conductor_uuid_str
        
    except ValueError as e:
        return False, str(e), None
    except Exception as e:
        return False, f"Error validando QR: {e}", None
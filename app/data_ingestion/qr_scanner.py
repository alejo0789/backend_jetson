import cv2
from pyzbar.pyzbar import decode
import numpy as np
import logging
from typing import Optional # <<<<<<<<<<<<<<<< ¡AÑADIDA ESTA IMPORTACIÓN!

# Configuración básica del logger para este módulo
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO) # Puedes ajustar el nivel (INFO, DEBUG, etc.)
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


def scan_qr_code(frame: np.ndarray) -> Optional[str]: # <<<<<<<<<<<<<<<< ¡CAMBIADA ESTA LÍNEA!
    """
    Escanea un fotograma (imagen) en busca de códigos QR y devuelve el dato decodificado
    del primer QR encontrado.

    Args:
        frame (np.ndarray): El fotograma de imagen de la cámara (array NumPy).

    Returns:
        str | None: El dato decodificado del código QR como string, o None si no se encuentra.
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
            qr_data = obj.data.decode('utf-8') # Decodifica los bytes a string UTF-8
            qr_type = obj.type # Tipo de código (ej. 'QRCODE')
            
            logger.info(f"QR detectado: Tipo={qr_type}, Datos={qr_data}")
            
            return qr_data
    
    # Si no se encontró ningún código QR
    return None

def process_qr_data(qr_data: str) -> str:
    """
    Procesa el dato decodificado del QR para extraer el ID del conductor.
    Asume que el QR contendrá directamente el ID de cédula del conductor,
    o algún formato que pueda ser parseado para obtenerlo.

    Args:
        qr_data (str): El string decodificado del código QR.

    Returns:
        str: El ID de cédula del conductor.
    """
    conductor_id_cedula = qr_data
    logger.info(f"process_qr_data: Cédula extraída del QR: {conductor_id_cedula}")
    return conductor_id_cedula

# Ejemplo de uso (solo para pruebas, esto no se ejecutará en la Jetson app principal)
if __name__ == '__main__':
    # Este bloque solo se ejecuta si corres qr_scanner.py directamente
    # Para probarlo, necesitarías una imagen con un QR o configurar una cámara virtual.
    # Ejemplo con una imagen de prueba:
    # try:
    #     test_image_path = "path/to/your/test_qr_image.png" # Asegúrate de tener una imagen con un QR
    #     test_frame = cv2.imread(test_image_path)
    #     if test_frame is None:
    #         logger.error(f"Error: No se pudo cargar la imagen de prueba en '{test_image_path}'.")
    #     else:
    #         print("Escaneando QR en la imagen de prueba...")
    #         decoded_qr = scan_qr_code(test_frame)
    #         if decoded_qr:
    #             print(f"QR decodificado: {decoded_qr}")
    #             conductor_cedula = process_qr_data(decoded_qr)
    #             print(f"Cédula del conductor: {conductor_cedula}")
    #         else:
    #             print("No se detectó ningún QR en la imagen de prueba.")
    # except ImportError:
    #     logger.error("Error: Asegúrate de tener 'opencv-python' y 'pyzbar' instalados.")
    #     logger.error("Usa: pip install opencv-python pyzbar")
    pass
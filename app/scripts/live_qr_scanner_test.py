import cv2
import logging
from typing import Optional
import numpy as np

# Configuración del logger para este script
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# Importamos las clases y funciones necesarias
from app.data_ingestion.video_capture import VideoCapture
from app.data_ingestion.qr_scanner import scan_qr_code, process_qr_data

def run_live_qr_scan():
    """
    Inicializa la cámara, escanea continuamente fotogramas en busca de códigos QR
    y procesa los datos encontrados.
    """
    logger.info("--- Iniciando prueba de escaneo de QR en vivo con cámara ---")

    # Configuración de la cámara. AJUSTA ESTOS VALORES si es necesario para tu cámara.
    # camera_index=0 es común para USB. Para CSI, revisa VideoCapture para la cadena GStreamer.
    cam_width, cam_height, cam_fps = 640, 480, 30
    camera_manager = VideoCapture(camera_index=0, width=cam_width, height=cam_height, fps=cam_fps)

    if not camera_manager.initialize_camera():
        logger.error("No se pudo inicializar la cámara. Asegúrate de que esté conectada y configurada.")
        return

    print("\nCámara inicializada. Muestra un código QR frente a la cámara.")
    print("Presiona 'q' para salir (si usas la ventana de visualización de OpenCV).")
    print("El escaneo buscará la cédula '1001001001' de ejemplo.")

    found_qr = False
    try:
        while True:
            frame: Optional[np.ndarray] = camera_manager.read_frame()
            if frame is None:
                logger.warning("No se pudo leer el fotograma de la cámara. Reintentando...")
                continue # Continúa al siguiente ciclo

            # Aquí podrías opcionalmente mostrar el fotograma si tienes una pantalla
            # cv2.imshow('Camera Feed (QR Scan)', frame) 
            # if cv2.waitKey(1) & 0xFF == ord('q'):
            #     break

            # Escanear el fotograma en busca de un QR
            qr_data: Optional[str] = scan_qr_code(frame)

            if qr_data:
                logger.info(f"QR decodificado: {qr_data}")
                # Procesar el dato del QR (ej. obtener la cédula)
                conductor_cedula = process_qr_data(qr_data)
                print(f"\n¡QR DETECTADO Y PROCESADO! Cédula: {conductor_cedula}")
                found_qr = True
                # Puedes agregar aquí la lógica para detener el escaneo después de encontrar el QR,
                # o permitir múltiples escaneos.
                break # Para detener el bucle una vez que se encuentra el QR

            # Pequeña pausa para no saturar la CPU y permitir que el sistema responda
            if found_qr: # Si ya se encontró, no necesita más pausa
                break
            cv2.waitKey(10) # Espera 10ms entre fotogramas para no saturar

    except KeyboardInterrupt:
        logger.info("Prueba de escaneo de QR en vivo interrumpida por el usuario.")
    except Exception as e:
        logger.error(f"Ocurrió un error inesperado durante la prueba de QR en vivo: {e}", exc_info=True)
    finally:
        camera_manager.release_camera()
        # if 'cv2.imshow' in locals() or 'cv2.imshow' in globals():
        #     cv2.destroyAllWindows()
        logger.info("Recursos de cámara liberados. Prueba finalizada.")

if __name__ == "__main__":
    # IMPORTANTE: Asegúrate de que 'opencv-python' y 'pyzbar' estén instalados.
    # Si estás en Jetson con cámara CSI, asegúrate de haber configurado el pipeline GStreamer
    # en jetson_app/data_ingestion/video_capture.py
    run_live_qr_scan()
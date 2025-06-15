import cv2
import logging
import numpy as np
import time
from typing import Optional

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
from app.identification.driver_identity import identify_and_manage_session

def run_qr_to_driver_identity_test():
    """
    Inicializa la cámara, escanea continuamente fotogramas en busca de códigos QR
    y los usa para probar la lógica de identificación y gestión de sesión del conductor.
    """
    logger.info("--- Iniciando prueba de QR Scanner integrado con Driver Identity ---")

    # Configuración de la cámara. AJUSTA ESTOS VALORES si es necesario para tu cámara.
    cam_width, cam_height, cam_fps = 640, 480, 30
    camera_manager = VideoCapture(camera_index=0, width=cam_width, height=cam_height, fps=cam_fps)

    if not camera_manager.initialize_camera():
        logger.error("No se pudo inicializar la cámara. Asegúrate de que esté conectada y configurada.")
        return

    print("\nCámara inicializada. Muestra un código QR (cédula) frente a la cámara.")
    print("Verás los logs de identificación del conductor. Presiona 'q' para salir (si hay ventana).")
    print("Prueba con las cédulas: '1001001001' (activo), '2002002002' (inactivo), '9999999999' (no registrado).")

    try:
        qr_scan_interval_sec = 2 # Intentar escanear QR cada 2 segundos para evitar saturación
        last_qr_scan_time = time.time()
        
        while True:
            frame: Optional[np.ndarray] = camera_manager.read_frame()
            if frame is None:
                logger.warning("No se pudo leer el fotograma. Reintentando...")
                time.sleep(0.1) # Pequeña pausa para evitar bucle apretado
                continue

            # Opcional: Mostrar el fotograma si tienes una pantalla.
            # cv2.imshow('QR to Driver Identity Test', frame) 
            # if cv2.waitKey(1) & 0xFF == ord('q'):
            #     break

            current_time = time.time()
            if current_time - last_qr_scan_time >= qr_scan_interval_sec:
                last_qr_scan_time = current_time
                
                qr_data: Optional[str] = scan_qr_code(frame)

                if qr_data:
                    logger.info(f"QR decodificado: {qr_data}. Pasando a Driver Identity.")
                    # Procesar el dato del QR (obtener la cédula)
                    conductor_cedula = process_qr_data(qr_data)
                    
                    # >>>>> LLAMADA A LA LÓGICA DE IDENTIFICACIÓN DEL CONDUCTOR <<<<<
                    identified_conductor = identify_and_manage_session(conductor_cedula)
                    
                    if identified_conductor:
                        print(f"**Identificado y Sesión Gestionada:** {identified_conductor.nombre_completo}")
                    else:
                        print("**Sesión gestionada (puede haber finalizado o no identificado).**")
                    
                    # Después de un escaneo exitoso y gestión, puedes esperar o romper el bucle
                    # Para pruebas repetidas, podrías quitar el 'break' y manejar la lógica de "escaneado recientemente"
                    time.sleep(3) # Pausa para ver los logs y evitar re-escaneo inmediato
                    
            # Una pequeña pausa para no saturar la CPU
            time.sleep(0.01) # Espera 10ms entre fotogramas

    except KeyboardInterrupt:
        logger.info("Prueba de integración interrumpida por el usuario.")
    except Exception as e:
        logger.error(f"Ocurrió un error inesperado durante la prueba de integración: {e}", exc_info=True)
    finally:
        camera_manager.release_camera()
        # if 'cv2.imshow' in locals() or 'cv2.imshow' in globals():
        #     cv2.destroyAllWindows()
        logger.info("Recursos de cámara liberados. Prueba de integración finalizada.")

if __name__ == "__main__":
    # --- PASOS PREVIOS REQUERIDOS ---
    # 1. Asegúrate de que 'opencv-python' y 'pyzbar' estén instalados.
    # 2. Si estás en Jetson con cámara CSI, **descomenta y ajusta la cadena GStreamer**
    #    en 'jetson_app/data_ingestion/video_capture.py'.
    # 3. **¡IMPORTANTE!** Asegúrate de haber ejecutado 'scripts/initial_data_setup.py'
    #    para que tu base de datos 'edge_data.db' esté creada y poblada con los datos demo
    #    (conductores 1001001001, 2002002002, etc. y la configuración de la Jetson).
    # --- FIN PASOS PREVIOS ---

    run_qr_to_driver_identity_test()
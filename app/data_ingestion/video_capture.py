import cv2
import logging
import numpy as np
from typing import Optional

# Configuración del logger para este módulo
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
# Asegura que los logs se muestren en consola si se ejecuta directamente
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

class VideoCapture:
    """
    Clase para manejar la captura de video desde una cámara.
    Permite inicializar, leer fotogramas y liberar los recursos de la cámara.
    """
    def __init__(self, camera_index: int = 0, width: int = 640, height: int = 480, fps: int = 30):
        """
        Inicializa el objeto VideoCapture.

        Args:
            camera_index (int): Índice de la cámara (0 para la predeterminada, 1 para la segunda, etc.).
                                 Para cámaras CSI en Jetson, puede requerir una cadena de GStreamer.
            width (int): Ancho deseado de los fotogramas.
            height (int): Alto deseado de los fotogramas.
            fps (int): Cuadros por segundo (frames per second) deseados.
        """
        self.camera_index = camera_index
        self.width = width
        self.height = height
        self.fps = fps
        self.cap: Optional[cv2.VideoCapture] = None # Objeto VideoCapture de OpenCV

    def initialize_camera(self) -> bool:
        """
        Inicializa la conexión con la cámara.

        Para cámaras CSI (ej. Raspberry Pi Camera Module v2) en Jetson Nano,
        es recomendable usar una cadena de GStreamer para un mejor rendimiento.
        Para cámaras USB, basta con el índice numérico.
        """
        # --- Ejemplo de cadena GStreamer para cámara CSI (AJUSTAR SEGÚN TU CÁMARA) ---
        # Si usas una cámara CSI (como la oficial de Raspberry Pi para Jetson),
        # DESCOMENTA y ajusta esta sección. El sensor_id es generalmente 0.
        # El flip-method es importante si la imagen aparece invertida (0-7).
        # gstreamer_pipeline = (
        #     f"nvarguscamerasrc sensor_id={self.camera_index} ! "
        #     "video/x-raw(memory:NVMM), width=(int)1920, height=(int)1080, format=(string)NV12, framerate=(fraction)30/1 ! "
        #     "nvvidconv flip-method=0 ! " 
        #     f"video/x-raw, width=(int){self.width}, height=(int){self.height}, format=(string)BGRx ! "
        #     "videoconvert ! video/x-raw, format=(string)BGR ! appsink"
        # )
        # logger.info(f"Intentando abrir cámara con pipeline GStreamer: {gstreamer_pipeline}")
        # self.cap = cv2.VideoCapture(gstreamer_pipeline, cv2.CAP_GSTREAMER)


        # --- Para cámaras USB o la cámara predeterminada (webcam, etc.) ---
        # Si usas una cámara USB, descomenta y usa estas líneas.
        logger.info(f"Intentando abrir cámara con índice: {self.camera_index}")
        self.cap = cv2.VideoCapture(self.camera_index)
        
        # Intentar establecer propiedades (puede que no todas las cámaras o drivers lo soporten)
        self.cap.set(cv2.CAP_PROP_FRAME_WIDTH, self.width)
        self.cap.set(cv2.CAP_PROP_FRAME_HEIGHT, self.height)
        self.cap.set(cv2.CAP_PROP_FPS, self.fps)


        if not self.cap.isOpened():
            logger.error(f"Error: No se pudo abrir la cámara {self.camera_index}.")
            self.cap = None
            return False
        
        # Confirma la resolución y FPS reales que la cámara pudo configurar
        logger.info(f"Cámara {self.camera_index} inicializada con éxito. Resolución: {self.cap.get(cv2.CAP_PROP_FRAME_WIDTH)}x{self.cap.get(cv2.CAP_PROP_FRAME_HEIGHT)} @ {self.cap.get(cv2.CAP_PROP_FPS)} FPS.")
        return True

    def read_frame(self) -> Optional[np.ndarray]:
        """
        Lee un fotograma de la cámara.

        Returns:
            Optional[np.ndarray]: El fotograma de imagen (array NumPy BGR), o None si la lectura falla.
        """
        if self.cap is None or not self.cap.isOpened():
            logger.warning("read_frame: La cámara no está inicializada o abierta.")
            return None
        
        ret, frame = self.cap.read()
        if not ret:
            logger.warning("read_frame: No se pudo leer el fotograma de la cámara. ¿Posiblemente desconectada o error?")
            return None
        
        return frame

    def release_camera(self):
        """
        Libera los recursos de la cámara.
        """
        if self.cap is not None and self.cap.isOpened():
            self.cap.release()
            logger.info("Cámara liberada.")
        self.cap = None

# Ejemplo de uso (esto no se ejecutará en el bucle principal de la Jetson, solo para pruebas)
if __name__ == '__main__':
    print("--- Probando VideoCapture ---")
    
    # IMPORTANTE: Ajusta estos parámetros a tu cámara.
    # camera_index=0 es común para USB. Para CSI, lee las notas arriba.
    # La resolución y FPS deben ser soportados por tu cámara.
    camera_manager = VideoCapture(camera_index=0, width=640, height=480, fps=30) 
    
    if camera_manager.initialize_camera():
        print("Cámara inicializada. Presiona 'q' para salir del bucle de visualización (si hay una ventana).")
        frame_count = 0
        try:
            while True:
                frame = camera_manager.read_frame()
                if frame is not None:
                    frame_count += 1
                    # Opcional: Mostrar el fotograma (solo si tienes un monitor conectado a la Jetson)
                    # Si no tienes monitor, la línea cv2.imshow causará un error si no hay un servidor X.
                    # cv2.imshow('Camera Feed', frame) 
                    # if cv2.waitKey(1) & 0xFF == ord('q'): # Espera 1ms por una tecla y revisa 'q'
                    #     break
                    if frame_count % 30 == 0: # Imprime las dimensiones cada 30 fotogramas
                        print(f"Fotograma leído: Dimensiones={frame.shape}, Tipo={frame.dtype}")
                else:
                    logger.warning("No se pudo leer el fotograma. Saliendo del bucle de prueba...")
                    break
                
                # Una pequeña pausa para no saturar la CPU en esta simulación de lectura
                cv2.waitKey(1) # Espera 1ms entre fotogramas

        except KeyboardInterrupt:
            print("Prueba de captura de video interrumpida por el usuario.")
        except Exception as e:
            logger.error(f"Ocurrió un error inesperado durante la prueba de cámara: {e}", exc_info=True)
        finally:
            camera_manager.release_camera()
            # if 'cv2.imshow' in locals() or 'cv2.imshow' in globals():
            #     cv2.destroyAllWindows() # Si usaste imshow, cierra las ventanas de OpenCV
    else:
        print("No se pudo inicializar la cámara. Verifica la conexión, los permisos y el índice/pipeline GStreamer.")
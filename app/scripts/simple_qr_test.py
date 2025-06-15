import logging

# Configuración básica del logger para este script
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

# Importamos la función principal de identificación del conductor
from app.identification.driver_identity import identify_and_manage_session

# Importamos el procesador de datos QR para simular su salida
from app.data_ingestion.qr_scanner import process_qr_data


def run_simple_qr_identification_test():
    """
    Ejecuta una prueba sencilla de identificación de conductor usando datos QR simulados.
    """
    logger.info("--- Iniciando prueba sencilla de identificación por QR ---")

    # Cédulas de los conductores demo insertados:
    CEDULA_CONDUCTOR_ACTIVO = "1001001001"
    CEDULA_CONDUCTOR_INACTIVO = "2002002002"
    CEDULA_CONDUCTOR_NO_REGISTRADO = "9999999999"

    print("\n--- Escenario 1: Conductor Activo escanea QR ---")
    # Simulamos el dato que el escáner QR devolvería
    qr_data_scanned_active = process_qr_data(CEDULA_CONDUCTOR_ACTIVO)
    # Llamamos a la función de identificación del conductor
    identify_and_manage_session(qr_data_scanned_active)

    print("\n--- Escenario 2: Conductor Inactivo escanea QR ---")
    qr_data_scanned_inactive = process_qr_data(CEDULA_CONDUCTOR_INACTIVO)
    identify_and_manage_session(qr_data_scanned_inactive)

    print("\n--- Escenario 3: Conductor No Registrado escanea QR ---")
    qr_data_scanned_unknown = process_qr_data(CEDULA_CONDUCTOR_NO_REGISTRADO)
    identify_and_manage_session(qr_data_scanned_unknown)

    print("\n--- Prueba de identificación por QR finalizada ---")

if __name__ == "__main__":
    # IMPORTANTE: Asegúrate de haber ejecutado scripts/initial_data_setup.py al menos una vez
    # para que la base de datos 'edge_data.db' esté creada y contenga los datos demo.
    # Puedes ejecutarlo así desde la raíz de tu proyecto:
    # python scripts/initial_data_setup.py
    # Una vez que los datos demo estén cargados, puedes ejecutar este script:
    # python scripts/simple_qr_test.py
    run_simple_qr_identification_test()
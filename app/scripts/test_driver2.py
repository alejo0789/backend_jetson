#!/usr/bin/env python3
"""
Script para probar el flujo completo de QR con cámara real - MODO OFFLINE
Escanea QR real con la cámara y prueba driver_identity sin conexión cloud
"""

import cv2
import logging
import time
import uuid
from datetime import datetime

# Configuración del logger
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Importaciones necesarias
from app.config.edge_database import create_edge_tables, get_edge_db, initialize_jetson_config
from app.local_db.crud_edge import create_or_update_conductor_local_selective, create_or_update_bus_local
from app.data_ingestion.video_capture import VideoCapture
from app.data_ingestion.qr_scanner import scan_qr_code, process_qr_data, validate_conductor_qr
from app.identification.driver_identity import identify_and_manage_session, get_current_driver_info


def setup_offline_test_environment():
    """
    Configura el entorno de prueba offline con datos mínimos
    """
    print("🔧 Configurando entorno de prueba offline...")
    
    # Crear tablas
    create_edge_tables()
    
    db = next(get_edge_db())
    try:
        # IDs de prueba
        test_bus_id = uuid.UUID("11111111-1111-1111-1111-111111111111")
        test_conductor_demo_id = uuid.UUID("22222222-2222-2222-2222-222222222222")
        
        # Configurar Jetson
        jetson_hw_id = "JETSON-QR-CAMERA-TEST"
        initialize_jetson_config(db, jetson_hw_id, test_bus_id)
        print(f"✅ Jetson configurada: {jetson_hw_id}")
        
        # Crear bus de prueba
        bus_data = {
            "id": test_bus_id,
            "placa": "QR-TEST-123",
            "numero_interno": "BUS-QR-001"
        }
        create_or_update_bus_local(db, bus_data)
        print(f"✅ Bus creado: {bus_data['placa']}")
        
        # Crear un conductor demo (por si el QR coincide)
        conductor_demo_data = {
            "id": test_conductor_demo_id,
            "cedula": "1234567890",
            "nombre_completo": "Juan Demo QR",
            "codigo_qr_hash": str(test_conductor_demo_id),
            "activo": True
        }
        create_or_update_conductor_local_selective(db, conductor_demo_data, force_update=True)
        print(f"✅ Conductor demo creado: {conductor_demo_data['nombre_completo']}")
        
        print("🎯 Entorno configurado. El sistema funcionará OFFLINE.")
        print("📱 Cualquier QR con UUID válido creará conductor con datos temporales.\n")
        
        return test_bus_id
        
    finally:
        db.close()


def display_instructions():
    """
    Muestra instrucciones para la prueba
    """
    print("=" * 60)
    print("🎯 PRUEBA DE QR CON CÁMARA REAL - MODO OFFLINE")
    print("=" * 60)
    print()
    print("📋 INSTRUCCIONES:")
    print("1. Asegúrate de que tu QR contenga un UUID válido")
    print("2. Mantén el QR visible y estable frente a la cámara")
    print("3. El sistema escaneará cada 2 segundos")
    print("4. Verás logs detallados del proceso")
    print()
    print("📱 EJEMPLOS DE UUID VÁLIDOS PARA QR:")
    print("   • 22222222-2222-2222-2222-222222222222 (conductor demo)")
    print("   • 99999999-9999-9999-9999-999999999999 (datos temporales)")
    print("   • Cualquier UUID válido")
    print()
    print("🚫 EJEMPLOS DE QR INVÁLIDOS:")
    print("   • 12345 (no es UUID)")
    print("   • texto-aleatorio")
    print("   • números de cédula")
    print()
    print("⌨️  Presiona 'q' para salir")
    print("=" * 60)
    print()


def run_qr_camera_test():
    """
    Ejecuta la prueba principal con cámara
    """
    print("📹 Inicializando cámara...")
    
    # Configurar cámara
    camera_manager = VideoCapture(camera_index=0, width=640, height=480, fps=30)
    
    if not camera_manager.initialize_camera():
        print("❌ Error: No se pudo inicializar la cámara")
        print("   • Verifica que la cámara esté conectada")
        print("   • Cierra otras aplicaciones que usen la cámara")
        print("   • En Linux: verifica permisos con 'ls -la /dev/video*'")
        return False
    
    print("✅ Cámara inicializada correctamente")
    print()
    print("🔍 Iniciando escaneo de QR...")
    print("   Muestra tu QR frente a la cámara...")
    print()
    
    # Variables de control
    last_scan_time = time.time()
    scan_interval = 2  # Escanear cada 2 segundos
    last_qr_data = None
    scan_count = 0
    
    try:
        while True:
            # Leer frame de la cámara
            frame = camera_manager.read_frame()
            if frame is None:
                print("⚠️  No se pudo leer frame de la cámara")
                time.sleep(0.1)
                continue
            
            # Mostrar frame (opcional - descomenta si quieres ver la imagen)
            # cv2.imshow('QR Scanner Test', frame)
            # if cv2.waitKey(1) & 0xFF == ord('q'):
            #     break
            
            current_time = time.time()
            
            # Escanear QR cada X segundos
            if current_time - last_scan_time >= scan_interval:
                last_scan_time = current_time
                scan_count += 1
                
                print(f"🔄 Escaneando... (intento #{scan_count})")
                
                # Escanear QR
                qr_raw = scan_qr_code(frame)
                
                if qr_raw:
                    print(f"📱 QR detectado: {qr_raw}")
                    
                    # Evitar procesar el mismo QR repetidamente
                    if qr_raw == last_qr_data:
                        print("   ↳ Mismo QR que antes, saltando...")
                        continue
                    
                    last_qr_data = qr_raw
                    
                    # Validar QR
                    is_valid, message, uuid_str = validate_conductor_qr(qr_raw)
                    
                    if is_valid:
                        print(f"✅ QR válido: {message}")
                        print(f"📋 UUID del conductor: {uuid_str}")
                        print()
                        print("🚀 Procesando con driver_identity...")
                        print("-" * 40)
                        
                        # Procesar con driver_identity
                        try:
                            conductor = identify_and_manage_session(uuid_str)
                            
                            if conductor:
                                print(f"✅ Resultado exitoso:")
                                print(f"   • Conductor: {conductor.nombre_completo}")
                                print(f"   • UUID: {conductor.id}")
                                print(f"   • Activo: {conductor.activo}")
                                
                                # Mostrar info de sesión actual
                                current_info = get_current_driver_info()
                                if current_info:
                                    print(f"   • Estado sesión: {current_info['estado_sesion']}")
                                    print(f"   • Tiempo conduciendo: {current_info['tiempo_conduccion_horas']:.2f} horas")
                                    if current_info['datos_temporales']:
                                        print(f"   • ⚠️  Operando con datos temporales (sin cloud)")
                                else:
                                    print(f"   • ℹ️  Sesión finalizada")
                            else:
                                print("ℹ️  Resultado: Sesión finalizada o error")
                                
                        except Exception as e:
                            print(f"❌ Error procesando QR: {e}")
                        
                    else:
                        print(f"❌ QR inválido: {message}")
                    
                    print()
                    print("=" * 50)
                    print("🔄 Continuando escaneo... (muestra otro QR o presiona Ctrl+C)")
                    print("=" * 50)
                    print()
                    
                else:
                    print("   ↳ No se detectó QR")
            
            # Pequeña pausa
            time.sleep(0.1)
    
    except KeyboardInterrupt:
        print("\n🛑 Prueba interrumpida por el usuario")
    
    except Exception as e:
        print(f"\n❌ Error inesperado: {e}")
        logger.error(f"Error en prueba QR: {e}", exc_info=True)
    
    finally:
        camera_manager.release_camera()
        # cv2.destroyAllWindows()  # Si usaste cv2.imshow
        print("📹 Cámara liberada")
        print("✅ Prueba finalizada")


def show_final_session_status():
    """
    Muestra el estado final de la sesión
    """
    print("\n📊 ESTADO FINAL DE LA SESIÓN:")
    print("-" * 30)
    
    current_info = get_current_driver_info()
    if current_info:
        print(f"👤 Conductor activo: {current_info['conductor_nombre']}")
        print(f"🕐 Tiempo total: {current_info['tiempo_conduccion_horas']:.2f} horas")
        print(f"📱 Sesión ID: {current_info['sesion_id']}")
        print(f"📊 Estado: {current_info['estado_sesion']}")
        if current_info['datos_temporales']:
            print(f"⚠️  Datos temporales (requiere sincronización cloud)")
    else:
        print("🚫 No hay sesión activa")


def main():
    """
    Función principal de la prueba
    """
    print("🚀 Iniciando prueba de QR con cámara real")
    print()
    
    try:
        # Configurar entorno
        setup_offline_test_environment()
        
        # Mostrar instrucciones
        display_instructions()
        
        # Ejecutar prueba
        run_qr_camera_test()
        
        # Mostrar estado final
        show_final_session_status()
        
    except Exception as e:
        print(f"❌ Error crítico: {e}")
        logger.error(f"Error crítico en prueba: {e}", exc_info=True)
    
    print("\n🎯 Prueba completada")


if __name__ == "__main__":
    main()
#!/usr/bin/env python3
"""
Script para probar la conexión entre Jetson y el backend cloud
"""

import requests
import os
import json
from dotenv import load_dotenv

def test_cloud_connection():
    """Prueba la conexión al backend cloud"""
    
    # Cargar variables de entorno
    load_dotenv()
    
    cloud_url = os.getenv('CLOUD_API_BASE_URL', 'http://172.16.16.132:5000/api/v1')
    timeout = int(os.getenv('HTTP_TIMEOUT', 30))
    
    print(f"🔗 Probando conexión a: {cloud_url}")
    print("=" * 50)
    
    # Test 1: Conectividad básica
    try:
        base_url = cloud_url.replace('/api/v1', '')
        print(f"📡 Test 1: Conectividad básica a {base_url}")
        
        response = requests.get(base_url, timeout=timeout)
        print(f"✅ Conectividad OK - Status: {response.status_code}")
        
    except requests.exceptions.ConnectRefused:
        print(f"❌ Conexión rechazada - ¿Está el servidor corriendo en {base_url}?")
        return False
    except requests.exceptions.Timeout:
        print(f"❌ Timeout - El servidor no responde en {timeout}s")
        return False
    except requests.exceptions.ConnectionError as e:
        print(f"❌ Error de conexión: {e}")
        return False
    except Exception as e:
        print(f"❌ Error inesperado: {e}")
        return False
    
    # Test 2: API endpoints
    endpoints_to_test = [
        '/empresas',
        '/buses',
        '/conductores',
        '/sesiones_conduccion',
        '/eventos',
        '/jetson_nanos'
    ]
    
    print(f"\n🧪 Test 2: Endpoints de la API")
    api_working = True
    
    for endpoint in endpoints_to_test:
        try:
            url = cloud_url + endpoint
            print(f"  📋 Probando {endpoint}...", end=" ")
            
            response = requests.get(url, timeout=10)
            
            if response.status_code in [200, 404, 405]:  # 405 = Method not allowed es OK
                print(f"✅ OK ({response.status_code})")
            else:
                print(f"⚠️  Status {response.status_code}")
                
        except Exception as e:
            print(f"❌ Error: {e}")
            api_working = False
    
    # Test 3: Probar endpoint específico para buses (usado en aprovisionamiento)
    print(f"\n🚌 Test 3: Endpoint de buses (crítico para aprovisionamiento)")
    try:
        # Probar endpoint que usa la Jetson para encontrar buses por placa
        url = f"{cloud_url}/buses"
        response = requests.get(url, timeout=10)
        
        if response.status_code == 200:
            buses = response.json()
            print(f"✅ Endpoint de buses OK - {len(buses)} buses encontrados")
            
            if buses:
                print("📋 Buses disponibles:")
                for bus in buses[:3]:  # Mostrar solo los primeros 3
                    placa = bus.get('placa', 'N/A')
                    print(f"  - {placa}")
            else:
                print("⚠️  No hay buses en la base de datos")
                
        else:
            print(f"❌ Error en endpoint de buses: {response.status_code}")
            api_working = False
            
    except Exception as e:
        print(f"❌ Error probando buses: {e}")
        api_working = False
    
    # Test 4: Configuración de red
    print(f"\n🌐 Test 4: Información de red")
    try:
        import socket
        
        # IP local de la Jetson
        hostname = socket.gethostname()
        local_ip = socket.gethostbyname(hostname)
        print(f"📍 IP local de Jetson: {local_ip}")
        print(f"🏠 Hostname: {hostname}")
        
        # Verificar que pueden alcanzarse mutuamente
        server_ip = cloud_url.split('//')[1].split(':')[0]
        print(f"🎯 IP del servidor cloud: {server_ip}")
        
    except Exception as e:
        print(f"⚠️  No se pudo obtener info de red: {e}")
    
    # Resumen
    print(f"\n📊 RESUMEN")
    print("=" * 30)
    
    if api_working:
        print("✅ Conexión al backend cloud: OK")
        print("✅ La Jetson puede comunicarse con el servidor")
        print(f"✅ URL configurada: {cloud_url}")
        print("\n🚀 ¡Ya puedes ejecutar main_jetson.py!")
        return True
    else:
        print("❌ Hay problemas de conectividad")
        print("\n🔧 Pasos para solucionar:")
        print("1. Verificar que el backend cloud esté corriendo")
        print("2. Verificar que Flask esté en host='0.0.0.0'")
        print("3. Verificar firewall en el PC")
        print("4. Verificar que ambos dispositivos estén en la misma red")
        return False

def test_jetson_to_cloud_data_flow():
    """Prueba el flujo de datos específico que usa la Jetson"""
    
    load_dotenv()
    cloud_url = os.getenv('CLOUD_API_BASE_URL', 'http://172.16.16.132:5000/api/v1')
    
    print(f"\n🔄 PRUEBA DE FLUJO DE DATOS JETSON → CLOUD")
    print("=" * 50)
    
    # Simular solicitud de aprovisionamiento
    test_placa = "TEST123"
    
    try:
        # 1. Buscar bus por placa (como hace run_jetson_provisioning)
        url = f"{cloud_url}/buses/by_placa/{test_placa}"
        print(f"🔍 Buscando bus con placa '{test_placa}'...")
        
        response = requests.get(url, timeout=10)
        
        if response.status_code == 404:
            print(f"✅ Endpoint funciona (bus '{test_placa}' no existe, como se esperaba)")
        elif response.status_code == 200:
            bus_data = response.json()
            print(f"✅ Bus encontrado: {bus_data}")
        else:
            print(f"⚠️  Status inesperado: {response.status_code}")
            
    except Exception as e:
        print(f"❌ Error en prueba de flujo: {e}")
        return False
    
    print("✅ Flujo de datos básico: OK")
    return True

if __name__ == "__main__":
    print("🧪 PRUEBA DE CONEXIÓN JETSON → BACKEND CLOUD")
    print("=" * 60)
    
    # Cargar configuración
    if os.path.exists('.env'):
        print("✅ Archivo .env encontrado")
    else:
        print("⚠️  Archivo .env no encontrado, usando valores por defecto")
    
    print()
    
    # Ejecutar pruebas
    connection_ok = test_cloud_connection()
    
    if connection_ok:
        test_jetson_to_cloud_data_flow()
    
    print(f"\n{'=' * 60}")
    if connection_ok:
        print("🎉 ¡CONEXIÓN EXITOSA! La Jetson puede comunicarse con el backend cloud")
    else:
        print("❌ HAY PROBLEMAS DE CONEXIÓN - Revisa la configuración de red")
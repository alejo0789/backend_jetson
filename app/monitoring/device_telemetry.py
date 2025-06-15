import psutil
import logging
from typing import Dict, Any, Optional
from datetime import datetime
import platform # Importar para detectar el sistema operativo

# Configuración del logger para este módulo
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

def gather_system_metrics() -> Dict[str, Any]:
    """
    Recopila métricas clave del sistema de la Jetson Nano (o PC de desarrollo).

    Returns:
        Dict[str, Any]: Un diccionario con métricas como uso de CPU, RAM, disco y temperatura.
    """
    metrics = {}
    try:
        # Uso de CPU (porcentaje)
        metrics['cpu_usage_percent'] = psutil.cpu_percent(interval=1) 
        
        # Uso de RAM (GB y porcentaje)
        mem = psutil.virtual_memory()
        metrics['ram_total_gb'] = round(mem.total / (1024**3), 2)
        metrics['ram_used_gb'] = round(mem.used / (1024**3), 2)
        metrics['ram_percent'] = mem.percent

        # Uso de Disco (GB y porcentaje de la partición raíz o unidad principal)
        # En Windows, psutil.disk_usage('/') podría no funcionar, usar una unidad específica
        if platform.system() == "Windows":
            disk_path = "C:\\" # Unidad principal en Windows
        else: # Linux, macOS
            disk_path = "/" # Partición raíz
        
        disk = psutil.disk_usage(disk_path)
        metrics['disk_total_gb'] = round(disk.total / (1024**3), 2)
        metrics['disk_used_gb'] = round(disk.used / (1024**3), 2)
        metrics['disk_percent'] = disk.percent
        
        # Temperatura (Solo disponible en sistemas Linux/FreeBSD con psutil)
        temp_celsius = None
        if hasattr(psutil, 'sensors_temperatures') and platform.system() in ["Linux", "FreeBSD"]:
            try:
                temperatures = psutil.sensors_temperatures()
                # Buscar una temperatura relevante, esto puede variar según el sistema
                if 'coretemp' in temperatures and temperatures['coretemp']:
                    temp_celsius = temperatures['coretemp'][0].current 
                elif 'cpu_thermal' in temperatures and temperatures['cpu_thermal']: # Común en ARM/SBCs
                    temp_celsius = temperatures['cpu_thermal'][0].current 
                # Si no se encuentra una específica, toma la primera disponible si existe
                elif temperatures:
                    for sensor_name, sensor_list in temperatures.items():
                        if sensor_list:
                            temp_celsius = sensor_list[0].current
                            break
            except Exception as e:
                logger.warning(f"Error al leer temperatura con psutil.sensors_temperatures(): {e}")
        
        # --- NOTA IMPORTANTE PARA JETSON NANO ---
        # En Jetson Nano, la forma más fiable de obtener la temperatura es con 'tegrastats'.
        # Descomenta y ajusta este bloque CUANDO PRUEBES EN LA JETSON NANO.
        # import subprocess
        # import re
        # if platform.system() == "Linux": # Solo ejecutar tegrastats en Linux
        #     try:
        #         # Ejecutar tegrastats y buscar la temperatura de la CPU/GPU (ej. "CPU@xx.xC")
        #         # Podrías necesitar permisos para tegrastats o correrlo con sudo si no está en el PATH
        #         output = subprocess.check_output("tegrastats --interval 1 --display-cpu | grep CPU", shell=True, text=True, stderr=subprocess.PIPE)
        #         match = re.search(r'CPU@(\d+\.?\d*)C', output) # o r'GPU@(\d+\.?\d*)C' si quieres la GPU
        #         if match:
        #             temp_celsius = float(match.group(1))
        #     except Exception as e:
        #         logger.warning(f"No se pudo leer la temperatura con tegrastats (común fuera de Jetson o si no está en PATH): {e}")


        metrics['temperature_celsius'] = temp_celsius # Será None en Windows, o el valor en Linux
        
        metrics['timestamp'] = datetime.utcnow().isoformat() 

        logger.info(f"Métricas del sistema recopiladas: CPU={metrics['cpu_usage_percent']}%, RAM={metrics['ram_percent']}%, Disco={metrics['disk_percent']}%, Temp={metrics['temperature_celsius']}°C")

    except Exception as e:
        logger.error(f"Error al recopilar métricas del sistema: {e}", exc_info=True)
        metrics = {"error": str(e), "timestamp": datetime.utcnow().isoformat()}

    return metrics

# Ejemplo de uso (esto no se ejecutará directamente en el bucle principal de la Jetson, solo para pruebas)
if __name__ == '__main__':
    print("--- Probando device_telemetry.py ---")
    
    system_metrics = gather_system_metrics()
    
    print("\nMétricas del Sistema Recopiladas:")
    for key, value in system_metrics.items():
        print(f"  {key}: {value}")

    print("\nNOTA: La temperatura podría ser None en Windows ya que psutil no la soporta directamente en este OS.")
    print("      Para Jetson Nano, se recomienda usar 'tegrastats' (ver comentarios en el código).")
    print("Este módulo sería llamado periódicamente (ej. cada 5-10 minutos) desde main_jetson.py para enviar esta información a la nube.")
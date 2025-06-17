import pandas as pd
import os
from pathlib import Path
import logging

# Configuración del logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def verificar_dependencias():
    """
    Verifica que todas las dependencias necesarias estén instaladas.
    """
    try:
        import xlrd
        import openpyxl
        logger.info("Todas las dependencias están instaladas correctamente.")
        return True
    except ImportError as e:
        logger.error(f"Falta una dependencia necesaria: {str(e)}")
        logger.info("Por favor, instala las dependencias con: pip install pandas openpyxl xlrd>=2.0.1")
        return False

def convertir_excel_a_csv(ruta_archivo):
    """
    Convierte un archivo Excel (xlsx o xls) a formato CSV.
    
    Args:
        ruta_archivo (str): Ruta del archivo Excel a convertir
        
    Returns:
        bool: True si la conversión fue exitosa, False en caso contrario
    """
    try:
        # Obtener el nombre del archivo sin la extensión
        nombre_base = Path(ruta_archivo).stem
        ruta_csv = f"{nombre_base}.csv"
        
        # Determinar el tipo de archivo
        extension = Path(ruta_archivo).suffix.lower()
        
        # Leer el archivo Excel
        logger.info(f"Leyendo archivo: {ruta_archivo}")
        if extension == '.xls':
            df = pd.read_excel(ruta_archivo, engine='xlrd')
        else:  # .xlsx
            df = pd.read_excel(ruta_archivo, engine='openpyxl')
        
        # Guardar como CSV
        df.to_csv(ruta_csv, index=False, encoding='utf-8')
        logger.info(f"Archivo convertido exitosamente: {ruta_csv}")
        return True
        
    except Exception as e:
        logger.error(f"Error al convertir {ruta_archivo}: {str(e)}")
        return False

def procesar_directorio(directorio):
    """
    Procesa todos los archivos Excel en un directorio.
    
    Args:
        directorio (str): Ruta del directorio a procesar
    """
    if not verificar_dependencias():
        return
        
    archivos_procesados = 0
    archivos_fallidos = 0
    
    # Obtener todos los archivos Excel en el directorio
    archivos_excel = list(Path(directorio).glob("*.xls*"))
    
    if not archivos_excel:
        logger.warning(f"No se encontraron archivos Excel en {directorio}")
        return
    
    logger.info(f"Se encontraron {len(archivos_excel)} archivos Excel para procesar")
    
    for archivo in archivos_excel:
        if convertir_excel_a_csv(str(archivo)):
            archivos_procesados += 1
        else:
            archivos_fallidos += 1
    
    logger.info(f"Proceso completado. Archivos procesados: {archivos_procesados}, Fallidos: {archivos_fallidos}")

if __name__ == "__main__":
    # Directorio actual como directorio por defecto
    directorio_actual = os.getcwd()
    
    try:
        procesar_directorio(directorio_actual)
    except Exception as e:
        logger.error(f"Error general en el proceso: {str(e)}")

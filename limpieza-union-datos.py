import pandas as pd
import numpy as np
import os
from pathlib import Path
import logging
from datetime import datetime
import re

# Configuración del logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('proceso_limpieza.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def limpiar_nombre_columna(columna):
    """
    Limpia el nombre de una columna eliminando caracteres especiales y espacios.
    """
    # Convertir a minúsculas
    columna = columna.lower()
    # Reemplazar espacios y caracteres especiales con guiones bajos
    columna = re.sub(r'[^a-z0-9]+', '_', columna)
    # Eliminar guiones bajos múltiples
    columna = re.sub(r'_+', '_', columna)
    # Eliminar guiones bajos al inicio y final
    columna = columna.strip('_')
    return columna

def limpiar_datos(df):
    """
    Realiza la limpieza de datos en el DataFrame.
    """
    try:
        # Limpiar nombres de columnas
        df.columns = [limpiar_nombre_columna(col) for col in df.columns]
        
        # Eliminar filas duplicadas
        filas_antes = len(df)
        df = df.drop_duplicates()
        filas_despues = len(df)
        if filas_antes != filas_despues:
            logger.info(f"Se eliminaron {filas_antes - filas_despues} filas duplicadas")
        
        # Convertir columnas de fecha si existen
        columnas_fecha = [col for col in df.columns if 'fecha' in col]
        for col in columnas_fecha:
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            except Exception as e:
                logger.warning(f"No se pudo convertir la columna {col} a fecha: {str(e)}")
        
        # Reemplazar valores nulos en columnas numéricas con 0
        columnas_numericas = df.select_dtypes(include=[np.number]).columns
        df[columnas_numericas] = df[columnas_numericas].fillna(0)
        
        # Reemplazar valores nulos en columnas de texto con 'N/A'
        columnas_texto = df.select_dtypes(include=['object']).columns
        df[columnas_texto] = df[columnas_texto].fillna('N/A')
        
        return df
        
    except Exception as e:
        logger.error(f"Error en la limpieza de datos: {str(e)}")
        return df

def segmentar_valle_cauca(df):
    """
    Segmenta los datos para el departamento del Valle del Cauca (código 76).
    
    Args:
        df (DataFrame): DataFrame con los datos a segmentar
        
    Returns:
        DataFrame: DataFrame filtrado para el Valle del Cauca
    """
    try:
        # Filtrar datos del Valle del Cauca usando cod_dpto_o
        df_valle = df[df['cod_dpto_o'] == 76].copy()
        
        # Agregar información de metadatos
        df_valle['region'] = 'Valle del Cauca'
        
        logger.info(f"Se encontraron {len(df_valle)} registros para el Valle del Cauca")
        return df_valle
        
    except Exception as e:
        logger.error(f"Error al segmentar datos del Valle del Cauca: {str(e)}")
        return df

def combinar_y_limpiar_archivos_csv(directorio, nombre_archivo_salida=None):
    """
    Combina y limpia todos los archivos CSV en un directorio.
    """
    try:
        # Obtener todos los archivos CSV en el directorio
        archivos_csv = list(Path(directorio).glob("Datos_*.csv"))
        
        if not archivos_csv:
            logger.warning(f"No se encontraron archivos CSV en {directorio}")
            return
        
        logger.info(f"Se encontraron {len(archivos_csv)} archivos CSV para procesar")
        
        # Lista para almacenar todos los DataFrames
        dfs = []
        
        # Leer cada archivo CSV
        for archivo in archivos_csv:
            try:
                logger.info(f"Leyendo archivo: {archivo}")
                df = pd.read_csv(archivo, encoding='utf-8', low_memory=False)
                
                # Extraer el año del nombre del archivo
                match = re.search(r'Datos_(\d{4})', archivo.name)
                año = match.group(1) if match else 'Desconocido'
                
                # Agregar columnas de metadatos
                df['año_datos'] = año
                df['archivo_origen'] = archivo.name
                
                # Limpiar datos
                df = limpiar_datos(df)
                
                dfs.append(df)
                logger.info(f"Archivo {archivo.name} procesado correctamente")
                
            except Exception as e:
                logger.error(f"Error al procesar {archivo}: {str(e)}")
                continue
        
        if not dfs:
            logger.error("No se pudo procesar ningún archivo CSV")
            return
        
        # Combinar todos los DataFrames
        df_combinado = pd.concat(dfs, ignore_index=True)
        
        # Generar nombre del archivo de salida si no se proporciona
        if nombre_archivo_salida is None:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            nombre_archivo_salida = f"datos_combinados_limpios_{timestamp}.csv"
        
        # Guardar el archivo combinado completo
        ruta_salida = os.path.join(directorio, nombre_archivo_salida)
        df_combinado.to_csv(ruta_salida, index=False, encoding='utf-8')
        
        # Segmentar y guardar datos del Valle del Cauca
        df_valle = segmentar_valle_cauca(df_combinado)
        ruta_valle = os.path.join(directorio, f"datos_valle_cauca_{timestamp}.csv")
        df_valle.to_csv(ruta_valle, index=False, encoding='utf-8')
        
        # Generar reporte de estadísticas
        generar_reporte_estadisticas(df_combinado, directorio)
        generar_reporte_estadisticas(df_valle, directorio, prefijo="valle_cauca_")
        
        logger.info(f"Archivos combinados y limpios exitosamente en: {ruta_salida}")
        logger.info(f"Datos del Valle del Cauca guardados en: {ruta_valle}")
        logger.info(f"Total de registros combinados: {len(df_combinado)}")
        logger.info(f"Total de registros Valle del Cauca: {len(df_valle)}")
        
    except Exception as e:
        logger.error(f"Error general en el proceso: {str(e)}")

def generar_reporte_estadisticas(df, directorio, prefijo=""):
    """
    Genera un reporte de estadísticas básicas del DataFrame.
    
    Args:
        df (DataFrame): DataFrame a analizar
        directorio (str): Directorio donde guardar el reporte
        prefijo (str): Prefijo para el nombre del archivo de reporte
    """
    try:
        # Crear reporte
        reporte = []
        reporte.append("REPORTE DE ESTADÍSTICAS")
        reporte.append("=" * 50)
        reporte.append(f"\nTotal de registros: {len(df)}")
        reporte.append(f"Total de columnas: {len(df.columns)}")
        
        # Estadísticas por año
        reporte.append("\nRegistros por año:")
        if 'año_datos' in df.columns:
            for año, count in df['año_datos'].value_counts().items():
                reporte.append(f"- {año}: {count} registros")
        
        # Información de columnas
        reporte.append("\nColumnas en el dataset:")
        for columna in df.columns:
            reporte.append(f"- {columna}")
        
        # Guardar reporte
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        ruta_reporte = os.path.join(directorio, f"{prefijo}reporte_estadisticas_{timestamp}.txt")
        
        with open(ruta_reporte, 'w', encoding='utf-8') as f:
            f.write('\n'.join(reporte))
            
        logger.info(f"Reporte de estadísticas generado en: {ruta_reporte}")
        
    except Exception as e:
        logger.error(f"Error al generar reporte de estadísticas: {str(e)}")

if __name__ == "__main__":
    # Directorio actual como directorio por defecto
    directorio_actual = os.getcwd()
    
    try:
        combinar_y_limpiar_archivos_csv(directorio_actual)
    except Exception as e:
        logger.error(f"Error en la ejecución principal: {str(e)}")

import numpy as np
import os
import pandas as pd
import re
import requests
import time
import urllib.parse
from datetime import datetime
from psycopg2.extensions import register_adapter, AsIs
from urllib.parse import urlparse

# Directorio donde están los archivos con errores
current_dir = os.path.dirname(os.path.abspath(__file__))
cat_image_path = 'cat_image_path.json'
cat_supplies = 'catalog_supplies.json'
path_catalogs = 'catalogs'

# Habilitar/Dehabilitar LOGs
ENABLE_LOGS = True

# ==== DIRECTORIO DE ARCHIVOS ====
PROCESSED_DIR = "data_processed"
ERRORS_DIR = "data_errors"

# Registrar adaptadores para tipos NumPy
def adapt_numpy_float64(numpyFloat):
    return AsIs(float(numpyFloat))

def adapt_numpy_int64(numpyInt):
    return AsIs(int(numpyInt))

register_adapter(np.float64, adapt_numpy_float64)
register_adapter(np.int32, adapt_numpy_int64)
register_adapter(np.int64, adapt_numpy_int64)
register_adapter(np.float32, adapt_numpy_float64)

def print_log(message):
    if ENABLE_LOGS:
        print(message)

def ultra_convert(value):
    """Versión mejorada con manejo de NumPy"""
    if value is None or pd.isna(value):
        return None
    if hasattr(value, 'item'):  # numpy.generic
        return value.item()
    if isinstance(value, (np.floating, np.integer)):
        return float(value) if isinstance(value, np.floating) else int(value)
    if isinstance(value, (float, int)):
        return float(value) if isinstance(value, float) else int(value)
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    return str(value) if not isinstance(value, (str, bytes)) else value

def safe_convert_to_float(value):
    """
    Conversión segura a float que maneja 'None', NaN, y otros casos especiales
    """
    if pd.isna(value) or value is None or str(value).strip().lower() in ['none', 'nan', '']:
        return None
    try:
        return float(str(value).replace(',', '.'))  # Maneja formatos con coma decimal
    except (ValueError, TypeError):
        return None

def ensure_default(value):
    ntv_value = ensure_native(value)
    if ntv_value:
        return ntv_value
    else:
        return 0

def ensure_native(value):
    """Convierte cualquier valor a tipos nativos de Python"""
    if value is None or (hasattr(value, '__array__') and np.isnan(value).any()):
        return None
    if hasattr(value, 'item'):  # Para numpy.generic
        return value.item()
    if isinstance(value, (np.floating, np.float32, np.float64)):
        return float(value)
    if isinstance(value, (np.integer, np.int32, np.int64)):
        return int(value)
    if isinstance(value, (float, int, str)):
        return value
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    try:
        return float(value)
    except (ValueError, TypeError):
        return str(value)

def verify_url(url):
    # Define un User-Agent que simula un navegador web.
    # Puedes usar cualquier cadena de User-Agent de un navegador popular.
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'
    }
    try:
        response = requests.head(url, allow_redirects=True, timeout=10, headers=headers)
        status_code = response.status_code
        print_log(f"status_code: {status_code}, response: {response}")
        if status_code == 200:
            return True
        else:
            return False
    except requests.RequestException as e:
        print_log(f"RequestException: {e}")
        return False

def debug_types(values):
    """Función auxiliar para verificar tipos"""
    return [str(type(v)) for v in values]

def get_store_name(url):
    # Caso especial
    if url == "ML":
        return "mercadolibre"
    # Extraer solo el dominio de la URL
    hostname = urlparse(url).hostname
    if hostname is None:
        return None
    # Quitar prefijos no deseados
    parts = hostname.split(".")
    skip_prefixes = {"www", "es", "articulo", "super"}
    # Filtrar
    filtered = [p for p in parts if p not in skip_prefixes]
    if len(filtered) >= 2:
        # El primer elemento ahora es el nombre de la tienda
        return filtered[0].lower()
    return None

def get_provider_store(url):
    """
    Versión mejorada con manejo de más dominios y casos edge.
    """
    if not url or pd.isna(url) or not isinstance(url, str):
        return None
    # Limpiar espacios y caracteres extraños
    url = url.strip()
    try:
        partes = urllib.parse.urlparse(url)
        scheme = partes.scheme
        host = partes.netloc.lower()  # Normalizar a minúsculas
        # Lista de dominios que solo necesitan el dominio base
        base_only_domains = [
            "temu.com", "shein.com", "walmart.com.mx", "soriana.com", 
            "costco.com.mx", "liverpool.com.mx", "sears.com.mx",
            "coppel.com", "elektra.com.mx", "samscLub.com.mx"
        ]
        # Lista de dominios que conservan el path pero sin parámetros
        keep_path_domains = [
            "ebay.", "mercado", "aliexpress", "amazon", "bestbuy",
            "target", "homeDepot", "lowes", "officedepot"
        ]
        # Verificar dominios de solo base
        for domain in base_only_domains:
            if domain in host:
                return f"{scheme}://{host}"
        # Verificar dominios que conservan path
        for domain in keep_path_domains:
            if domain in host:
                clean_path = partes.path.split('?')[0]
                return f"{scheme}://{host}{clean_path}"
        # Caso por caso específico
        if "mercadolibre.com.mx" in host:
            new_host = host.replace("articulo.", "www.")
            return f"{scheme}://{new_host}"
        if "amazon." in host:
            path = partes.path
            if "/dp/" in path or "/gp/product/" in path:
                clean_path = path.split('?')[0].split('/ref')[0]
                return f"{scheme}://{host}{clean_path}"
            return f"{scheme}://{host}{path.split('?')[0]}"
        # Caso por defecto: eliminar parámetros pero conservar path
        clean_path = partes.path.split('?')[0]
        return f"{scheme}://{host}{clean_path}"
    except Exception as e:
        print_log(f"❌ Error procesando URL {url}: {e}")
        # Fallback: intentar eliminar parámetros de forma simple
        if '?' in url:
            return url.split('?')[0]
        return url

def get_domain_store(url):
    if url == "mercadolibre":
        return "www.mercadolibre.com.mx"
    pattern = re.compile(r'https?://([^/]+)')
    match = pattern.search(url)
    if match:
        indx = 1
        print_log(f"indx: {indx}, match: {match}")
        domain = str(match.group(indx)).lower()
        if "articulo.mercadolibre" in domain:
            domain.replace('articulo.mercadolibre', 'www.mercadolibre')
        return domain.lower()
    else:
        return None

def move_file(filePath, success=True):
    """Mueve el archivo con múltiples intentos y manejo de errores"""
    max_attempts = 3
    wait_time = 1  # segundos
    file_name = os.path.basename(filePath)
    dest_dir = PROCESSED_DIR if success else ERRORS_DIR
    dest_path = os.path.join(dest_dir, file_name)
    # Verificar si el archivo fuente existe
    if not os.path.exists(filePath):
        print_log(f"⚠️ Archivo fuente no existe: {filePath}")
        return False
    # Si el archivo ya existe en destino, añadir timestamp
    if os.path.exists(dest_path):
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        base, ext = os.path.splitext(file_name)
        file_name = f"{base}_{timestamp}{ext}"
        dest_path = os.path.join(dest_dir, file_name)
    # Intentar mover con reintentos
    for attempt in range(max_attempts):
        try:
            os.rename(filePath, dest_path)
            print_log(f"Archivo movido a: {dest_path}")
            return True
        except PermissionError as e:
            if attempt == max_attempts - 1:
                print_log(f"❌ Error moviendo archivo después de {max_attempts} intentos: {e}")
                return False
            print_log(f"Intento {attempt + 1}: Archivo en uso, reintentando...")
            time.sleep(wait_time)
        except Exception as e:
            print_log(f"❌ Error inesperado moviendo archivo: {e}")
            return False
    return False

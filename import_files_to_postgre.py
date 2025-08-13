import numpy as np
import os
import pandas as pd
import psycopg2
import re
import requests
import urllib.parse
from openpyxl import load_workbook
from psycopg2.extensions import register_adapter, AsIs

# ==== CONFIGURACIÓN DE CONEXIÓN A POSTGRES ====
MARGEN_GANANCIA = 0.30
DESCUENTO_OFERTA = 0.10

DB_CONFIG = {
    "host": "localhost",
    "dbname": "stockflow",
    "user": "postgres",
    "password": "MaTm1512#",
    "port": 5432,
    "options": "-c search_path=public"
}

MARGEN_GANANCIA = 0.30  # 30% de margen de ganancia
DESCUENTO_OFERTA = 0.15  # 15% de descuento en ofertas

# Habilitar/Dehabilitar LOGs
ENABLE_LOGS = True

# ==== DIRECTORIO DE ARCHIVOS ====
DATA_DIR = "data_files_ingestion"

# Mapeo de catálogos
CAT_PAYMENT_TYPE = {}
CAT_STORE = {}
PICTURE_URL = []

def print_log(message):
    if ENABLE_LOGS:
        print(message)

# ======= DB GET CATALOGS =======
def get_catalogs(cursor):
    """Recupera diccionarios de catálogos."""
    # Obtener tiendas
    cursor.execute("SELECT id_payment_type, payment_type FROM payment_type ;")
    CAT_PAYMENT_TYPE.update({name: id for id, name in cursor.fetchall()})

    # Obtener tiendas
    cursor.execute("SELECT id_store, store_name FROM store;")
    CAT_STORE.update({name: id for id, name in cursor.fetchall()})

# ==== FUNCIONES AUXILIARES ====
from openpyxl import load_workbook

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

def extract_hyperlinks(file_path, sheet_name="Precios", columna="Picture"):
    """Extrae los hipervínculos de una columna específica en una hoja de Excel."""
    wb = load_workbook(file_path, data_only=True)
    ws = wb[sheet_name]
    # Encuentra el índice de la columna "Picture"
    headers = [cell.value for cell in next(ws.iter_rows(min_row=1, max_row=1))]
    col_idx = headers.index(columna) + 1  # openpyxl usa 1-based indexing
    urls = []
    for row in ws.iter_rows(min_row=2, min_col=col_idx, max_col=col_idx):
        cell = row[0]
        if cell.hyperlink:
            urls.append(cell.hyperlink.target)
        else:
            urls.append(None)  # o cell.value si quieres el texto visible
    print_log(f"LEN: {len(urls)}")
    return urls

def procesar_compras(df_prchss, urls=[]):
    """Procesa el dataframe de compras."""
    df = df_prchss.copy()
    df['C. Unit'] = df['C. Unit'].astype(float)
    df['Total Cmpr'] = df['Total Cmpr'].astype(float)
    df['Costo Final'] = df['Costo Final'].astype(float)
    df["Picture_URL"] = urls
    return df

def procesar_precios(df_prices, df_prchss):
    """Procesa el dataframe de precios."""
    df = df_prices.copy()
    df['P. Tienda'] = df['P. Tienda'].astype(float)
    df['C. Unit'] = df['C. Unit'].astype(float)
    df = df.merge(
        df_prchss[["Descripción", "Cant", "% Desc", "Costo Final", "Pzs"]], 
        on="Descripción", 
        how="left"
    )
    return df

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

# Registrar adaptadores para tipos NumPy
def adapt_numpy_float64(numpy_float):
    return AsIs(float(numpy_float))

def adapt_numpy_int64(numpy_int):
    return AsIs(int(numpy_int))

register_adapter(np.float64, adapt_numpy_float64)
register_adapter(np.int32, adapt_numpy_int64)
register_adapter(np.int64, adapt_numpy_int64)
register_adapter(np.float32, adapt_numpy_float64)

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

def deep_clean_data(df):
    """Limpieza profunda de datos mejorada"""
    for col in df.columns:
        if pd.api.types.is_numeric_dtype(df[col]):
            # Conversión segura para columnas numéricas
            df[col] = df[col].apply(safe_convert_to_float)
        elif pd.api.types.is_datetime64_any_dtype(df[col]):
            # Manejo seguro de fechas
            df[col] = pd.to_datetime(df[col], errors='coerce').apply(
                lambda x: x.to_pydatetime() if pd.notna(x) else None
            )
    return df.replace([np.nan, pd.NA, 'None', 'none', 'NONE'], None)

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
    if url == "ML":
        return "mercadolibre"
    pattern = re.compile(r'https?://(?:www\.)?([a-zA-Z0-9\-]+)\.')
    match = pattern.search(url)
    if match:
        indx = 1
        print_log(f"indx: {indx}, match: {match}")
        name = str(match.group(indx))
        return name.lower()
    else:
        return None

def get_provider_store(url):
    # Analiza la URL para obtener el nombre del host (dominio)
    partes = urllib.parse.urlparse(url)
    host = partes.netloc
    
    # Reglas específicas para cada dominio
    if "aliexpress.com" in host:
        # Extrae la ruta hasta el ".html"
        path = partes.path
        if ".html" in path:
            final_path = path.split('.html')[0] + '.html'
            return f"{partes.scheme}://{host}{final_path}"
    elif "amazon.com.mx" in host:
        # Extrae la ruta hasta el ASIN (identificador del producto)
        path = partes.path
        if "/gp/product/" in path:
            return f"{partes.scheme}://{host}{path.split('?')[0].split('/ref')[0]}"
    elif "walmart.com.mx" in host or "soriana.com" in host:
        # Devuelve solo la URL base
        return f"{partes.scheme}://{host}"
    elif "mercadolibre.com.mx" in host:
        # Reemplaza 'articulo' por 'www' en el host y devuelve la URL base
        new_host = host.replace("articulo.", "www.")
        return f"{partes.scheme}://{new_host}"
    # Para los casos que no cumplen las reglas anteriores (como eBay),
    # simplemente devuelve la URL original, o la versión sin parámetros de consulta
    else:
        # Devuelve la URL sin parámetros de consulta para casos genéricos
        return f"{partes.scheme}://{host}{partes.path}"

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

def get_id_payment_type(str_payment):
    """Obtiene id_payment_type."""
    print_log(f"str_payment: {str_payment}")
    if str_payment is None:
        return None
    
    if str_payment in CAT_PAYMENT_TYPE:
        return CAT_PAYMENT_TYPE[str_payment]
    else:
        return None

def get_or_create_store(cursor, store_url=None):
    """Obtiene o crea una tienda y devuelve su ID."""
    print_log(f"store_url: {store_url}")
    if store_url is None:
        return None
    
    store_name = get_store_name(store_url)
    print_log(f"store_name: {store_name}")
    if store_name is None or store_name == "none":
        return None
    if store_name in CAT_STORE:
        return CAT_STORE[store_name]
    
    domain_store = get_domain_store(store_url)
    print_log(f"* INSERT INTO store ({store_name},{domain_store})...")
    cursor.execute(
        """
            INSERT INTO store (store_name, store_url, status)
            VALUES (%s, %s, TRUE)
            ON CONFLICT (store_name) DO UPDATE
            SET store_url = EXCLUDED.store_url
            RETURNING id_store;
        """,
        (store_name, domain_store)
    )
    id_store = cursor.fetchone()[0]
    print_log(f"id_store: {id_store}")
    CAT_STORE[store_name] = id_store
    return id_store

def get_or_create_provider(cursor, id_store, str_url=None):
    """Obtiene o crea un proveedor y devuelve su ID."""
    print_log(f"id_store: {id_store}, str_url: {str_url}")
    provider_url = get_provider_store(str_url)
    print_log(f"provider_url: {provider_url}")
    cursor.execute(
        """
            SELECT id_provider FROM provider
            WHERE id_store = %s AND provider_url = %s;
        """,
        (id_store, provider_url)
    )
    id_provider = cursor.fetchone()
    if id_provider:
        return id_provider[0]
    is_active = verify_url(provider_url)
    print_log(f"is_active: {is_active}")
    cursor.execute(
        """
            INSERT INTO provider (id_store, provider_url, is_active)
            VALUES (%s, %s, %s)
            RETURNING id_provider;
        """,
        (id_store, provider_url, is_active)
    )
    id_provider = cursor.fetchone()[0]
    print_log(f"id_provider: {id_provider}")
    return id_provider

def create_product(cursor, product_name, description=None, image_url=None):
    """Obtiene o crea un producto y devuelve su ID."""
    print_log(f"image_url: {image_url}")
    cursor.execute(
        """
            INSERT INTO product (product_name, description, image_url)
            VALUES (%s, %s, %s)
            RETURNING id_product;
        """,
        (product_name, description, image_url)
    )
    id_product = cursor.fetchone()[0]
    print_log(f"id_product: {id_product}")
    return id_product

def insert_purchase(cursor, purchase_data):
    """Inserta una compra y sus operaciones relacionadas."""
    # Insertar compra
    values = (
        purchase_data["id_provider"],
        purchase_data["id_payment_type"],
        purchase_data["total"],
        purchase_data["tax"],
        purchase_data["ieps"],
        purchase_data["purchase_date"],
        purchase_data.get("delivery_date"),
        purchase_data.get("exchange_rate"),
        purchase_data.get("shipping_cost", 0),
        purchase_data.get("discount", 0)
    )
    print_log(f"values: {values}")
    print_log("INSERT INTO purchase ()...")
    cursor.execute(
        """
            INSERT INTO purchase (
                id_provider, id_payment_type, total, tax, ieps, purchase_date,
                delivery_date, exchange_rate, shipping_cost, discount
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id_purchase;
        """,
        values
    )
    id_purchase = cursor.fetchone()[0]
    print_log(f"id_purchase: {id_purchase}")
    return id_purchase

def insert_operations(cursor, id_purchase, id_product, operation_items):
    """Versión final garantizada sin errores"""
    for item in operation_items:
        try:
            # Conversión ultra profunda con manejo explícito de NumPy
            safe_item = {}
            for k, v in item.items():
                converted = ultra_convert(v)
                # Conversión adicional para asegurar tipos nativos de Python
                if hasattr(converted, 'item'):  # Para numpy types
                    safe_item[k] = converted.item()
                else:
                    safe_item[k] = converted
            # Construcción de valores con conversión explícita a tipos nativos
            itm_vls = (
                int(id_purchase),
                int(id_product),
                int(safe_item.get("quantity", 0)),
                float(str(safe_item.get("unit_price", 0))),
                float(str(safe_item.get("unit_price_usd"))) if safe_item.get("unit_price_usd") is not None else None,
                float(str(safe_item.get("discount_percentage", 0))),
                int(safe_item.get("pieces_per_unit", 1)),
                float(str(safe_item.get("final_cost"))) if safe_item.get("final_cost") is not None else None,
                str(safe_item.get("product_url", ""))
            )
            # Verificación EXTRA de tipos
            for v in itm_vls:
                if v is not None and 'numpy' in str(type(v)):
                    raise TypeError(f"Tipo NumPy detectado después de conversión: {type(v)}")
            # Construir consulta
            query = """
                INSERT INTO operation (
                    id_purchase, id_product, quantity, unit_price, unit_price_usd,
                    discount_percentage, pieces_per_unit, final_cost, product_url
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            # Preparar valores para psycopg2
            vls = (
                int(itm_vls[0]),
                int(itm_vls[1]),
                int(itm_vls[2]),
                float(itm_vls[3]),
                float(itm_vls[4]) if itm_vls[4] is not None else None,
                float(itm_vls[5]),
                int(itm_vls[6]),
                float(itm_vls[7]) if itm_vls[7] is not None else None,
                str(itm_vls[8])
            )
            print_log(f"Valores finales para INSERT: {vls}")
            print_log(f"Tipos finales: {[type(v) for v in vls]}")
            # Ejecutar consulta
            cursor.execute(query, vls)
        except Exception as e:
            print_log(f"❌ ERROR: {str(e)}")
            print_log(f"Item original: {item}")
            print_log(f"Item convertido: {safe_item}")
            print_log(f"Valores intermedios: {itm_vls}")
            print_log(f"Tipos intermedios: {[type(v) for v in itm_vls]}")
            raise RuntimeError("Error de inserción - abortando") from e
    return True

def fun_insert_operation(cursor, id_purchase, id_product, operation_items):
    """Versión definitiva con triple validación de tipos"""
    for item in operation_items:
        try:
            # Primera conversión
            safe_item = {k: ensure_native(v) for k, v in item.items()}
            # Segunda conversión explícita
            params = (
                int(ensure_native(id_purchase)),
                int(ensure_native(id_product)),
                int(ensure_native(safe_item.get("quantity", 0))),
                float(ensure_native(safe_item.get("unit_price", 0))),
                float(ensure_native(safe_item.get("unit_price_usd"))) if safe_item.get("unit_price_usd") is not None else None,
                float(ensure_native(safe_item.get("discount_percentage", 0))),
                int(ensure_native(safe_item.get("pieces_per_unit", 1))),
                float(ensure_native(safe_item.get("final_cost"))) if safe_item.get("final_cost") is not None else None,
                str(ensure_native(safe_item.get("product_url", "")))[:500]
            )
            # Tercera validación
            for i, param in enumerate(params):
                if param is not None and type(param).__module__.startswith('numpy'):
                    raise TypeError(f"Parámetro {i} sigue siendo NumPy: {type(param)}")
            print_log(f"Params verificados: {params}")
            print_log(f"Tipos verificados: {[type(p) for p in params]}")
            # Consulta parametrizada
            query = """
                INSERT INTO operation (
                    id_purchase, id_product, quantity, unit_price, unit_price_usd,
                    discount_percentage, pieces_per_unit, final_cost, product_url
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(query, params)
        except Exception as e:
            print_log(f"❌ ERROR FATAL: {str(e)}")
            print_log(f"Datos originales: {item}")
            print_log(f"Tipos originales: {[type(v) for v in item.values()]}")
            print_log(f"Params fallidos: {params}")
            raise RuntimeError("Error crítico en inserción") from e
    return True

def check_price_constraint(cursor):
    """Verifica si existe la restricción única en id_product"""
    cursor.execute("""
        SELECT 1 FROM pg_constraint 
        WHERE conrelid = 'price'::regclass 
        AND contype IN ('u', 'p')  -- 'u' para UNIQUE, 'p' para PRIMARY KEY
        AND conkey::int[] @> ARRAY[
            (SELECT attnum FROM pg_attribute 
             WHERE attrelid = 'price'::regclass AND attname = 'id_product')
        ]
    """)
    return cursor.fetchone() is not None

def insert_price(cursor, id_product, price_data):
    """Versión sin ON CONFLICT"""
    # Conversión segura de tipos NumPy
    price_val = float(price_data["price"]) if price_data["price"] is not None else None
    offer_val = float(price_data.get("offer_price")) if price_data.get("offer_price") is not None else None
    
    # UPSERT manual en dos pasos
    cursor.execute("""
        UPDATE price SET
            price = %s,
            offer_price = %s,
            end_date = CASE WHEN price != %s THEN CURRENT_DATE ELSE end_date END,
            start_date = CASE WHEN price != %s THEN CURRENT_DATE ELSE start_date END
        WHERE id_product = %s
    """, (price_val, offer_val, price_val, price_val, id_product))
    
    if cursor.rowcount == 0:  # Si no actualizó nada, insertar nuevo
        cursor.execute("""
            INSERT INTO price (
                id_product, price, offer_price, start_date
            ) VALUES (%s, %s, %s, CURRENT_DATE)
        """, (id_product, price_val, offer_val))

def data_ingestion(df_compras, df_precios):
    """Realiza la ingesta de datos a la base de datos."""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        cur = conn.cursor()
        # Inicializar catálogos
        print_log("get_catalogs()...")
        get_catalogs(cur)
        
        previous_link = ""
        # Procesar cada compra
        for _, row in df_compras.iterrows():
            # Obtener o crear tienda y proveedor
            str_link = row.get("Liga")
            print_log(f"str_link: {str_link}")
            if not str_link:
                print_log(f"previous_link: {previous_link}")
                str_link = previous_link
            previous_link = row.get("Liga")
            print_log("get_or_create_store()...")
            id_store = get_or_create_store(cur, str_link)
            if id_store is None:
                continue
            print_log(f"get_or_create_provider({id_store})...")
            id_provider = get_or_create_provider(cur, id_store, str_link)
            if id_provider is None:
                continue
            # Obtener o crear producto
            product_name = row["Descripción"]
            print_log(f"create_product({product_name})...")
            id_product = create_product(cur, product_name, "", row["Picture_URL"])

            id_payment_type = get_id_payment_type("Tarjeta de Crédito")
            print_log(f"id_payment_type: {id_payment_type}")
            
            # Preparar datos de compra
            purchase_data = {
                "id_provider": id_provider,
                "id_payment_type": id_payment_type,
                "total": row["Total Cmpr"],
                "tax": 0,
                "ieps": 0,
                "purchase_date": row["Fch Cmpr"],
                "delivery_date": row.get("Fch Entrga"),
                "exchange_rate": row.get("Dólar"),
                "shipping_cost": row.get("Envio", 0),
                "discount": row.get("Desct", 0)
            }
                
            # Insertar compra
            print_log("insert_purchase()...")
            id_purchase = insert_purchase(cur, purchase_data)

            # Preparar items de operación
            operation_items = [{
                "quantity": row["Cant"],
                "unit_price": row["C. Unit"],
                "unit_price_usd":  row.get("C. Unit US"),
                "discount_percentage": row.get("% Desc", 0),
                "pieces_per_unit": row.get("Pzs", 1),
                "final_cost": row.get("Costo Final"),
                "product_url": row.get("Liga", "")
            }]
            #print_log(f"insert_operations({id_purchase})...")
            #insert_operations(cur, id_purchase, id_product, operation_items)
            print_log(f"fun_insert_operation({id_purchase})...")
            fun_insert_operation(cur, id_purchase, id_product, operation_items)
            
            # Insertar precios si existe en el df de precios
            if row["Descripción"] in df_precios["Descripción"].values:
                price_row = df_precios[df_precios["Descripción"] == row["Descripción"]].iloc[0]
                price_data = {
                    "price": price_row["P. Venta"],
                    "offer_price": price_row.get("P. Oferta")
                }
                print_log(f"insert_price({id_product})...")
                insert_price(cur, id_product, price_data)
        
        print("conn.commit()...")
        conn.commit()
        print("✅ Datos ingresados correctamente.")
    
    except Exception as e:
        print("conn.rollback()...")
        conn.rollback()
        print(f"❌ Error en la ingesta de datos: {e}")
    
    finally:
        conn.close()

def procesar_archivo(file_path):
    """Procesa un archivo Excel y realiza la ingesta."""
    print(f"Procesando archivo: {file_path}")
    try:
        print("extract_hyperlinks()...")
        links_urls = extract_hyperlinks(file_path)
        xls = pd.ExcelFile(file_path)
        df_prchss = pd.read_excel(xls, "Compras")
        df_prices = pd.read_excel(xls, "Precios")

        # Limpieza profunda
        df_prchss_cln = deep_clean_data(df_prchss)
        df_prices_cln = deep_clean_data(df_prices)
        
        print("procesar_compras()...")
        #df_prchss_upd = procesar_compras(df_prchss_cln, links_urls)
        df_prchss_cln["Picture_URL"] = links_urls[:len(df_prchss_cln)]  # Asegurar misma longitud

        print("procesar_precios()...")
        df_prices_upd = procesar_precios(df_prices_cln, df_prchss_cln)
        
        data_ingestion(df_prchss_cln, df_prices_upd)
    except Exception as e:
        print(f"❌ Error procesando archivo {file_path}: {e}")

# ==== MAIN ====
if __name__ == "__main__":
    # Procesar todos los archivos XLSX en el directorio
    for file_name in os.listdir(DATA_DIR):
        if file_name.endswith(".xlsx"):
            file_path = os.path.join(DATA_DIR, file_name)
            procesar_archivo(file_path)
    
    print("✅ Proceso de ingesta completado.")

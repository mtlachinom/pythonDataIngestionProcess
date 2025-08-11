import numpy as np
import os
import pandas as pd
import psycopg2
import re
import requests
import urllib.parse
from openpyxl import load_workbook

# ==== CONFIGURACIÓN DE CONEXIÓN A POSTGRES ====
MARGEN_GANANCIA = 0.30
DESCUENTO_OFERTA = 0.10
DB_CONFIG = {
    "host": "localhost",
    "dbname": "stockflow",
    "user": "postgres",
    "password": "MaTm1512#",
    "port": 5432
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

def strictly_convert_to_native(value):
    """
    Convierte cualquier valor a tipos nativos de Python de manera estricta.
    Elimina completamente cualquier rastro de tipos NumPy/pandas.
    """
    # Manejar valores nulos/NA primero
    if pd.isna(value) or value is None:
        return None
    # Para arrays NumPy
    if isinstance(value, np.ndarray):
        return [strictly_convert_to_native(x) for x in value]
    # Para tipos NumPy escalares
    if isinstance(value, np.generic):
        if isinstance(value, np.floating):
            return float(value)
        elif isinstance(value, np.integer):
            return int(value)
        else:
            return value.item()  # Método general para otros tipos NumPy
    # Para Timestamp de pandas
    if isinstance(value, pd.Timestamp):
        return value.to_pydatetime()
    # Para otros tipos de pandas
    if isinstance(value, (pd.Series, pd.DataFrame)):
        return value.to_dict()
    # Para decimal.Decimal
    if str(type(value)).endswith("Decimal'>"):
        return float(value)
    # Para listas/tuplas (convertir elementos)
    if isinstance(value, (list, tuple)):
        return [strictly_convert_to_native(x) for x in value]
    # Para diccionarios (convertir valores)
    if isinstance(value, dict):
        return {k: strictly_convert_to_native(v) for k, v in value.items()}
    # Si ya es tipo nativo, devolver tal cual
    return value

def convert_to_native(value):
    if isinstance(value, (np.generic,)):  # cualquier tipo numpy
        return value.item()
    return value

def round_decimals(value, decimales=2):
    # Valor nulo o NaN
    if value is None or pd.isna(value):
        return None
    # Si es numpy/pandas scalar, convertir a tipo Python
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            # Fallback seguro: intentar convertir a float
            try:
                value = float(value)
            except Exception:
                return None
    try:
        # Si es entero (o ya un int de numpy convertido), devolver int
        if isinstance(value, (int, np.integer)) and decimales >= 0:
            return int(value)
        # Convertir y redondear como float nativo
        rounded = round(float(value), decimales)
        # Manejar -0.0
        if rounded == -0.0:
            rounded = 0.0
        return float(rounded)
    except (TypeError, ValueError):
        return None

def round_decimals_int(value):
    if value is None or pd.isna(value):
        return None
    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            try:
                value = float(value)
            except Exception:
                return None
    try:
        return int(round(float(value)))
    except (ValueError, TypeError):
        return None

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
    # Insertar operaciones
    print_log(f"id_purchase: {id_purchase}, id_product: {id_product}")
    for item in operation_items:
        # Convertir TODO el item a tipos nativos primero
        ntv_itm = {k: strictly_convert_to_native(v) for k, v in item.items()}
        qntty = ntv_itm["quantity"]
        unt_prc = ntv_itm["unit_price"]
        unt_prc_usd = ntv_itm.get("unit_price_usd")
        print_log(f"qntty: {qntty}, unt_prc: {unt_prc}, unt_prc_usd: {unt_prc_usd}")
        dscnt_prcntg = ntv_itm["discount_percentage"]
        pcs_pr_unt = ntv_itm["pieces_per_unit"]
        fnl_cst = ntv_itm.get("final_cost")
        print_log(f"dscnt_prcntg: {dscnt_prcntg}, pcs_pr_unt: {pcs_pr_unt}, fnl_cst: {fnl_cst}")
        prdct_url = str(ntv_itm.get("product_url",""))
        for v in (id_purchase, id_product, qntty, unt_prc, unt_prc_usd, dscnt_prcntg, pcs_pr_unt, fnl_cst, prdct_url):
            print(type(v), v)
        itm_val = (
            id_purchase,
            id_product,
            round_decimals_int(qntty),
            round_decimals(unt_prc),
            round_decimals(unt_prc_usd) if unt_prc_usd is not None else None,
            round_decimals(dscnt_prcntg),
            round_decimals_int(pcs_pr_unt),
            round_decimals(fnl_cst) if fnl_cst is not None else None,
            prdct_url
        )
        print_log(f"itm_val: {itm_val}")
        for i, v in enumerate(itm_val, start=1):
            print(f"param {i}: {repr(v)} ({type(v)})")
        print_log(f"INSERT INTO operation ({id_purchase})...")
        cursor.execute(
            """
                INSERT INTO operation (
                    id_purchase, id_product, quantity, unit_price, unit_price_usd,
                    discount_percentage, pieces_per_unit, final_cost, product_url
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id_purchase;
            """,
           itm_val
        )
        id_purchase = cursor.fetchone()[0]
        print_log(f"id_purchase: {id_purchase}")
    return True

def insert_price(cursor, id_product, price_data):
    """Inserta o actualiza un precio."""
    cursor.execute(
        """
            INSERT INTO price (
                id_product, price, offer_price, start_date
            ) VALUES (%s, %s, %s, CURRENT_DATE)
            ON CONFLICT (id_product) DO UPDATE
            SET price = EXCLUDED.price,
                offer_price = EXCLUDED.offer_price,
                end_date = CASE WHEN price.id_product = EXCLUDED.id_product 
                                AND price.price != EXCLUDED.price 
                                THEN CURRENT_DATE 
                           ELSE price.end_date END,
                start_date = CASE WHEN price.id_product = EXCLUDED.id_product 
                                  AND price.price != EXCLUDED.price 
                                  THEN CURRENT_DATE 
                             ELSE price.start_date END;
        """,
        (
            id_product,
            price_data["price"],
            price_data.get("offer_price")
        )
    )

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
            print_log(f"row: {row}")
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
            print_log(f"insert_operations({id_purchase})...")
            insert_operations(cur, id_purchase, id_product, operation_items)
            
            # Insertar precios si existe en el df de precios
            if row["Descripción"] in df_precios["Descripción"].values:
                price_row = df_precios[df_precios["Descripción"] == row["Descripción"]].iloc[0]
                price_data = {
                    "price": price_row["P. Venta"],
                    "offer_price": price_row.get("P. Oferta")
                }
                insert_price(cur, id_product, price_data)
        
        conn.commit()
        print("✅ Datos ingresados correctamente.")
    
    except Exception as e:
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
        
        print("procesar_compras()...")
        df_prchss_upd = procesar_compras(df_prchss, links_urls)

        print("procesar_precios()...")
        df_prices_upd = procesar_precios(df_prices, df_prchss_upd)
        
        data_ingestion(df_prchss_upd, df_prices_upd)
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

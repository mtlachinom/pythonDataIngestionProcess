import configparser
import os
import psycopg2
# Importar funciones generales
from utils_tools import (
    print_log,
    get_store_name,
    get_domain_store,
    get_provider_store,
    verify_url,
    ensure_native
)

# Configuración de la base de datos Postgres SQL
current_dir = os.path.dirname(os.path.abspath(__file__))
config = configparser.ConfigParser()
config.read(os.path.join(current_dir, 'config.ini'))

DB_CONFIG = {
    "host": "localhost",
    "dbname": "stockflow",
    "user": "postgres",
    "password": "MaTm1512#",
    "port": 5432,
    "options": "-c search_path=public"
}

# Mapeo de catálogos
CAT_PAYMENT_TYPE = {}
CAT_STORE = {}

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
def get_id_payment_type(strPayment):
    """Obtiene id_payment_type."""
    print_log(f"strPayment: {strPayment}")
    if strPayment is None:
        return None
    
    if strPayment in CAT_PAYMENT_TYPE:
        return CAT_PAYMENT_TYPE[strPayment]
    else:
        return None

# ======= FUNCTIONS FOR DATA INGESTION =======

def get_or_create_store(cursor, storeUrl=None):
    """Obtiene o crea una tienda y devuelve su ID."""
    print_log(f"storeUrl: {storeUrl}")
    if storeUrl is None:
        return None
    store_name = get_store_name(storeUrl)
    print_log(f"store_name: {store_name}")
    if store_name is None or store_name == "none":
        return None
    if store_name in CAT_STORE:
        return CAT_STORE[store_name]
    domain_store = get_domain_store(storeUrl)
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

def get_or_create_provider(cursor, idStore, strUrl=None):
    """Obtiene o crea un proveedor y devuelve su ID."""
    print_log(f"idStore: {idStore}, strUrl: {strUrl}")
    provider_url = get_provider_store(strUrl)
    print_log(f"provider_url: {provider_url}")
    cursor.execute(
        """
            SELECT id_provider FROM provider
            WHERE id_store = %s AND provider_url = %s;
        """,
        (idStore, provider_url)
    )
    id_provider = cursor.fetchone()
    print_log(f"id_provider: {id_provider}")
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
        (idStore, provider_url, is_active)
    )
    id_provider = cursor.fetchone()[0]
    print_log(f"id_provider: {id_provider}")
    return id_provider

def create_product(cursor, data, productName, descr=None, quantity=None, unitPrice=None, prchsDate=None):
    """ Obtiene o crea un producto validando coincidencias en producto, operación y compra. """
    imageUrl = data["Picture_URL"]
    print_log(f"imageUrl: {imageUrl}")
    # 1️⃣ Buscar producto por nombre
    cursor.execute(
        "SELECT id_product FROM product WHERE product_name = %s", (productName,)
    )
    row = cursor.fetchone()
    print_log(f"imageUrl: {imageUrl}")
    if row:
        id_product = row[0]
        # 2️⃣ Validar en operation + purchase (si se pasaron todos los datos)
        if quantity is not None and unitPrice is not None and prchsDate is not None:
            cursor.execute(
                """
                SELECT 1
                FROM operation o
                JOIN purchase p ON o.id_purchase = p.id_purchase
                WHERE o.id_product = %s AND o.quantity = %s AND o.unit_price = %s AND p.purchase_date = %s
                LIMIT 1
                """,
                (id_product, quantity, unitPrice, prchsDate)
            )
            op_match = cursor.fetchone()
            if op_match:
                print_log(f"Producto existente con datos coincidentes: {id_product}")
                return {"id_product": id_product, "continue": False}
            else:
                print_log(f"Producto '{productName}' existe pero sin coincidencia exacta en operación/compra.")
            return {"id_product": id_product, "continue": True}
        else:
            print_log(f"Producto '{productName}' encontrado (sin validar operación por datos incompletos).")
            return {"id_product": id_product, "continue": True}
    # 3️⃣ Si no existe o no pasó validación → Insertar nuevo
    brand = data["Marca"]
    category = data["Categoria"]
    print_log(f"brand: {brand}, category: {category}")
    query_inser = ""
    values = None
    if brand and category:
        query_inser = """
            INSERT INTO product (product_name, description, image_url, brand, category)
            VALUES (%s, %s, %s, %s, %s)
            RETURNING id_product;
        """
        values = (productName, descr, imageUrl, brand, category)
    else:
        query_inser = """
            INSERT INTO product (product_name, description, image_url)
            VALUES (%s, %s, %s)
            RETURNING id_product;
        """
        values = (productName, descr, imageUrl)
    print_log(f"INSERT INTO product ({productName})...")
    cursor.execute(query_inser, values)
    id_product = cursor.fetchone()[0]
    print_log(f"id_product creado: {id_product}")
    return {"id_product": id_product, "continue": True}

def insert_purchase(cursor, prchsData):
    """Inserta una compra y sus operaciones relacionadas."""
    # Insertar compra
    values = (
        prchsData["id_provider"],
        prchsData["id_payment_type"],
        prchsData["total"],
        prchsData["tax"],
        prchsData["ieps"],
        prchsData["purchase_date"],
        prchsData.get("delivery_date"),
        prchsData.get("exchange_rate"),
        prchsData.get("shipping_cost", 0),
        prchsData.get("discount", 0)
    )
    print_log(f"values: {values}")
    print_log("INSERT INTO purchase ()...")
    cursor.execute(
        """
            INSERT INTO purchase (
                id_provider, id_payment_type, total, tax, ieps,
                purchase_date, delivery_date, exchange_rate, shipping_cost, discount
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id_purchase;
        """,
        values
    )
    id_purchase = cursor.fetchone()[0]
    print_log(f"id_purchase: {id_purchase}")
    return id_purchase

def insert_operations(cursor, idPurchase, idProduct, operationItems):
    """Versión definitiva con triple validación de tipos"""
    for item in operationItems:
        try:
            # Primera conversión
            safe_item = {k: ensure_native(v) for k, v in item.items()}
            print_log(f"* safe_item: {safe_item}")
            # Segunda conversión explícita
            params = (
                int(ensure_native(idPurchase)),
                int(ensure_native(idProduct)),
                int(ensure_native(safe_item.get("quantity", 0))),
                float(ensure_native(safe_item.get("unit_price", 0))),
                float(ensure_native(safe_item.get("unit_price_usd"))) if safe_item.get("unit_price_usd") is not None else None,
                float(ensure_native(safe_item.get("discount_percentage"))) if safe_item.get("discount_percentage") is not None else 0,
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

def insert_price(cursor, idProduct, priceData):
    """Versión sin ON CONFLICT"""
    # Conversión segura de tipos NumPy
    price_val = float(priceData["price"]) if priceData["price"] is not None else None
    offer_val = float(priceData.get("offer_price")) if priceData.get("offer_price") is not None else None
    # UPSERT manual en dos pasos
    cursor.execute("""
        UPDATE price SET
            price = %s,
            offer_price = %s,
            end_date = CASE WHEN price != %s THEN CURRENT_DATE ELSE end_date END,
            start_date = CASE WHEN price != %s THEN CURRENT_DATE ELSE start_date END
        WHERE id_product = %s
    """, (price_val, offer_val, price_val, price_val, idProduct))
    
    if cursor.rowcount == 0:  # Si no actualizó nada, insertar nuevo
        cursor.execute("""
            INSERT INTO price (
                id_product, price, offer_price, start_date
            ) VALUES (%s, %s, %s, CURRENT_DATE)
        """, (idProduct, price_val, offer_val))

# Ejemplo de uso
if __name__ == "__main__":
    conn = psycopg2.connect(**DB_CONFIG)
    success = True
    try:
        cur = conn.cursor()
        # Cargar todos los catálogos para import process
        get_catalogs(cur)
        print("* Import catalogs loaded:")
        print(f"Payment_Type: {len(CAT_PAYMENT_TYPE)}")
        print(f"Store: {CAT_STORE}")
    except Exception as e:
        print(f"❌ Error en la ingesta de datos: {e}")
    finally:
        conn.close()
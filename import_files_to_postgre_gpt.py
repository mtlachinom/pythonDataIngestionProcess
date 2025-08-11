import pandas as pd
import psycopg2
from urllib.parse import urlparse
from pathlib import Path

# ========= CONFIGURACIÓN DE CONEXIÓN =========
DB_CONFIG = {
    'dbname': 'tu_base',
    'user': 'tu_usuario',
    'password': 'tu_password',
    'host': 'localhost',
    'port': 5432
}

# ========= DIRECTORIO DE ARCHIVOS =========
DATA_DIR = Path("data_files_ingestion")

# ========= FUNCIONES AUXILIARES =========
def extraer_tienda(liga):
    if pd.isna(liga):
        return None
    dominio = urlparse(str(liga)).netloc.lower()
    if not dominio:
        return None
    if "walmart" in dominio:
        return "Walmart"
    elif "amazon" in dominio:
        return "Amazon"
    elif "mercadolibre" in dominio:
        return "MercadoLibre"
    else:
        return dominio

def extraer_marca(descripcion, marcas_detectadas):
    for marca in marcas_detectadas:
        if marca.lower() in str(descripcion).lower():
            return marca
    return "Otra"

def extraer_categoria(descripcion, categorias_detectadas):
    for cat in categorias_detectadas:
        if cat.lower() in str(descripcion).lower():
            return cat
    return "Otra"

def insertar_catalogo(cur, tabla, columna, valores):
    ids = {}
    for valor in sorted(set(valores)):
        if valor is None:
            continue
        cur.execute(
            f"INSERT INTO {tabla} ({columna}) VALUES (%s) "
            f"ON CONFLICT ({columna}) DO NOTHING RETURNING id_{columna};",
            (valor,)
        )
        result = cur.fetchone()
        if result:
            ids[valor] = result[0]
        else:
            cur.execute(f"SELECT id_{columna} FROM {tabla} WHERE {columna} = %s;", (valor,))
            ids[valor] = cur.fetchone()[0]
    return ids

# ========= CONEXIÓN A POSTGRES =========
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

# ========= PROCESAR TODOS LOS ARCHIVOS =========
for file_path in DATA_DIR.glob("*.xlsx"):
    print(f"Procesando archivo: {file_path.name}")

    compras_df = pd.read_excel(file_path, sheet_name="Compras")
    precios_df = pd.read_excel(file_path, sheet_name="Precios")

    # --- Extraer valores dinámicos para catálogos ---
    compras_df["Tienda"] = compras_df["Liga"].apply(extraer_tienda)
    marcas_detectadas = set()
    categorias_detectadas = set()

    # Detectar marcas y categorías de forma dinámica a partir de "Descripción"
    for desc in precios_df["Descripción"].dropna():
        palabras = str(desc).split()
        if len(palabras) > 1:
            marcas_detectadas.add(palabras[0])  # primera palabra como marca
        if len(palabras) > 2:
            categorias_detectadas.add(palabras[1])  # segunda palabra como categoría

    precios_df["Marca"] = precios_df["Descripción"].apply(lambda x: extraer_marca(x, marcas_detectadas))
    precios_df["Categoria"] = precios_df["Descripción"].apply(lambda x: extraer_categoria(x, categorias_detectadas))

    # --- Insertar catálogos ---
    tienda_ids = insertar_catalogo(cur, "cat_tienda", "nombre", compras_df["Tienda"])
    marca_ids = insertar_catalogo(cur, "cat_marca", "nombre", precios_df["Marca"])
    categoria_ids = insertar_catalogo(cur, "cat_categoria", "nombre", precios_df["Categoria"])

    # --- Insertar compras ---
    compra_ids_map = {}
    for idx, row in compras_df.iterrows():
        cur.execute("""
            INSERT INTO compra (
                descripcion, cantidad, porcentaje_desc, costo_unit_usd, costo_unit_mxn,
                total_compra, envio_usd, envio_mxn, fecha_compra, fecha_entrega,
                tipo_cambio, descuento_usd, descuento_mxn, piezas_por_unidad, costo_final,
                liga, id_tienda
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            RETURNING id_compra;
        """, (
            row["Descripción"], row["Cant"], row["% Desc"], row["C. Unit US"], row["C. Unit"],
            row["Total Cmpr"], row["Env US"], row["Envio"], row["Fch Cmpr"], row["Fch Entrga"],
            row["Dólar"], row["Dsc US"], row["Desct"], row["Pzs"], row["Costo Final"],
            row["Liga"], tienda_ids.get(row["Tienda"])
        ))
        compra_ids_map[idx + 1] = cur.fetchone()[0]  # "No" en Precios es 1-based

    # --- Insertar precios ---
    for _, row in precios_df.iterrows():
        cur.execute("""
            INSERT INTO precio (
                id_compra, id_marca, id_categoria, precio_tienda,
                porcentaje_desc_compra, cantidad, costo_unit, piezas,
                precio_venta, precio_oferta
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, (
            compra_ids_map.get(row["No"]), 
            marca_ids.get(row["Marca"]), 
            categoria_ids.get(row["Categoria"]),
            row["P. Tienda"], row["% Desc Cmpr"], row["Cant"], row["C. Unit"], row["Pzs"],
            row["P. Venta"], row["P. Oferta"]
        ))

# ========= GUARDAR CAMBIOS =========
conn.commit()
cur.close()
conn.close()

print("Carga de múltiples archivos completada correctamente.")

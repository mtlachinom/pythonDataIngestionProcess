import os
import re
import pandas as pd
import fitz  # PyMuPDF
from datetime import datetime

# Rutas base del proyecto
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_IMPORT_DIR = os.path.join(BASE_DIR, "pdf_files")
OUTPUT_DIR = os.path.join(BASE_DIR, "pdf_to_xlsx_files")
os.makedirs(OUTPUT_DIR, exist_ok=True)

# === Configuración ===
base_output = "cargos_bbva"
#pdf_file = "Estado_Cuenta.pdf"
pdf_file = "EdoCuentaSep25.pdf"

def extraer_datos_bbva():
    # Construir rutas completas
    pdf_path = os.path.join(DATA_IMPORT_DIR, pdf_file)
    
    # Verificar que el archivo PDF existe
    if not os.path.exists(pdf_path):
        raise FileNotFoundError(f"No se encontró el archivo PDF: {pdf_path}")
    
    # Abrir el PDF
    doc = fitz.open(pdf_path)
    text = ""
    for page in doc:
        text += page.get_text() + "\n"
    
    # Inicializar listas para almacenar datos
    msi_data = []  # Meses sin intereses
    compras_data = []  # Compras regulares
    compras_fechas = []  # Para almacenar fechas de operación
    
    # Patrones de regex para extraer datos
    # Patrón para Meses Sin Intereses
    msi_pattern = re.compile(
        r"(\d{2}-[a-z]{3}-\d{4})\s+(.+?)\s+\$([\d,]+\.\d{2})\s+\$([\d,]+\.\d{2})\s+\$([\d,]+\.\d{2})\s+(\d+ de \d+)\s+([\d.]+%)",
        re.IGNORECASE
    )
    
    # Patrón para Compras Regulares
    compras_pattern = re.compile(
        r"(\d{2}-[a-z]{3}-\d{4})\s+(\d{2}-[a-z]{3}-\d{4})\s+(.+?)\s+([+-]\s*\$?[\d,]+\.\d{2})",
        re.IGNORECASE
    )
    
    # Buscar secciones en el texto
    msi_section = re.search(r"COMPRAS Y CARGOS DIFERIDOS A MESES SIN INTERESES(.+?)COMPRAS Y CARGOS DIFERIDOS A MESES CON INTERESES", text, re.DOTALL | re.IGNORECASE)
    compras_section = re.search(r"CARGOS,COMPRAS Y ABONOS REGULARES\(NO A MESES\)(.+?)TOTAL CARGOS", text, re.DOTALL | re.IGNORECASE)
    
    # Procesar Meses Sin Intereses
    if msi_section:
        msi_text = msi_section.group(1)
        for match in msi_pattern.finditer(msi_text):
            fecha_str, descripcion, monto_orig, saldo_pend, pago_req, num_pago, tasa = match.groups()
            
            # Convertir fecha
            try:
                fecha = datetime.strptime(fecha_str, "%d-%b-%Y")
            except:
                fecha = fecha_str
            
            # Limpiar valores numéricos
            monto_orig = float(monto_orig.replace(",", "").replace("$", ""))
            saldo_pend = float(saldo_pend.replace(",", "").replace("$", ""))
            pago_req = float(pago_req.replace(",", "").replace("$", ""))
            
            msi_data.append([fecha, descripcion, monto_orig, saldo_pend, pago_req, num_pago, tasa])
    
    # Procesar Compras Regulares
    if compras_section:
        compras_text = compras_section.group(1)
        for match in compras_pattern.finditer(compras_text):
            fecha_oper, fecha_cargo, descripcion, monto = match.groups()
            
            # Convertir fechas
            try:
                fecha_oper_dt = datetime.strptime(fecha_oper, "%d-%b-%Y")
                compras_fechas.append(fecha_oper_dt)  # Guardar para encontrar la fecha máxima
                fecha_oper = fecha_oper_dt
            except:
                fecha_oper_dt = None
                pass
                
            try:
                fecha_cargo = datetime.strptime(fecha_cargo, "%d-%b-%Y")
            except:
                pass
            
            # Limpiar monto
            monto_clean = monto.replace("+", "").replace(" ", "").replace("$", "").replace(",", "")
            try:
                monto_valor = float(monto_clean)
                # Restaurar signo negativo si existe
                if "-" in monto:
                    monto_valor = -abs(monto_valor)
            except:
                monto_valor = monto
            
            compras_data.append([fecha_oper, fecha_cargo, monto_valor, descripcion])
    
    # Determinar la fecha máxima para el nombre del archivo
    if compras_fechas:
        operation_date = max(compras_fechas)
        operation_date_str = operation_date.strftime("%d%b%Y")
    else:
        # Si no hay fechas de compras, usar la fecha actual
        operation_date_str = datetime.now().strftime("%d%b%Y")
    
    # Generar nombre de archivo dinámico
    output_file = f"{base_output}_{operation_date_str}.xlsx"
    excel_output_path = os.path.join(OUTPUT_DIR, output_file)
    
    # Crear DataFrames
    df_msi = pd.DataFrame(msi_data, columns=["Fecha operación", "Descripción", "Monto original", 
                                            "Saldo pendiente", "Pago requerido", "Núm. de pago", 
                                            "Tasa de interés aplicable"])
    
    df_compras = pd.DataFrame(compras_data, columns=["Fecha de la operación", "Fecha de cargo", 
                                                    "Pago requerido", "Descripción"])
    
    # Guardar en Excel
    with pd.ExcelWriter(excel_output_path, engine='openpyxl') as writer:
        df_msi.to_excel(writer, sheet_name='msi', index=False)
        df_compras.to_excel(writer, sheet_name='compras', index=False)
    
    print(f"Datos extraídos y guardados en {excel_output_path}")
    print(f"- {len(msi_data)} registros en hoja 'msi'")
    print(f"- {len(compras_data)} registros en hoja 'compras'")
    print(f"- Fecha de operación máxima: {operation_date_str}")
    
    return len(msi_data), len(compras_data), operation_date_str

# Uso del script
if __name__ == "__main__":
    try:
        print("Iniciando extracción de datos BBVA...")
        msi_count, compras_count, operation_date = extraer_datos_bbva()
        print(f"Proceso completado. Extraídos: {msi_count} MSI, {compras_count} compras regulares")
        print(f"Archivo generado: cargos_bbva_{operation_date}.xlsx")
    except Exception as e:
        print(f"❌ Error durante la extracción: {e}")
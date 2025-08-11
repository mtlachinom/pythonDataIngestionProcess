import os
import pandas as pd
from sqlalchemy import create_engine, exc
from datetime import datetime
import logging
from typing import Dict, List, Optional

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('xlsx_to_postgres.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class XLSXProcessor:
    def __init__(self, db_connection_str: str):
        """
        Inicializa el procesador con la cadena de conexión a PostgreSQL.
        
        :param db_connection_str: Cadena de conexión a la BD (ej: 'postgresql://user:password@localhost:5432/dbname')
        """
        self.db_engine = create_engine(db_connection_str)
        self.required_sheets = ['Compras', 'Precios']
        self.marcas_mapping = {
            'Mario Bros': 'Mario Bros',
            'Mario Kart': 'Mario Kart',
            'Sonic': 'Sonic',
            'Hot Wheels': 'Hot Wheels',
            'Paw Patrol': 'Paw Patrol',
            'Disney': 'Disney',
            'Race Verse': 'Race Verse',
            'Star Wars': 'Star Wars'
        }
        self.categorias_mapping = {
            'Figura': 'Figura',
            'Kart': 'Kart',
            'Playset': 'Playset',
            'Pista': 'Pista',
            'Pack': 'Pack',
            'Set Figuras': 'Set Figuras'
        }

    def find_xlsx_files(self, directory: str) -> List[str]:
        """
        Busca archivos XLSX en el directorio especificado.
        
        :param directory: Ruta del directorio a escanear
        :return: Lista de rutas de archivos XLSX encontrados
        """
        try:
            logger.info(f"Buscando archivos XLSX en: {directory}")
            xlsx_files = [
                os.path.join(directory, f) for f in os.listdir(directory) 
                if f.endswith('.xlsx') and not f.startswith('~$')
            ]
            logger.info(f"Encontrados {len(xlsx_files)} archivos XLSX")
            return xlsx_files
        except Exception as e:
            logger.error(f"Error buscando archivos XLSX: {str(e)}")
            return []

    def validate_sheets(self, file_path: str) -> bool:
        """
        Valida que el archivo XLSX contenga las hojas requeridas.
        
        :param file_path: Ruta del archivo XLSX
        :return: True si contiene las hojas requeridas, False en caso contrario
        """
        try:
            with pd.ExcelFile(file_path) as xls:
                sheets = xls.sheet_names
                missing = [sheet for sheet in self.required_sheets if sheet not in sheets]
                
                if missing:
                    logger.warning(f"Archivo {file_path} no tiene las hojas requeridas. Faltan: {missing}")
                    return False
                return True
        except Exception as e:
            logger.error(f"Error validando hojas en {file_path}: {str(e)}")
            return False

    def extract_marca(self, descripcion: str) -> Optional[str]:
        """
        Extrae la marca del producto basado en la descripción.
        
        :param descripcion: Descripción del producto
        :return: Marca identificada o None si no se encuentra
        """
        desc_lower = descripcion.lower()
        for marca, mapped in self.marcas_mapping.items():
            if marca.lower() in desc_lower:
                return mapped
        logger.warning(f"No se pudo identificar marca para: {descripcion}")
        return None

    def extract_categoria(self, descripcion: str) -> Optional[str]:
        """
        Extrae la categoría del producto basado en la descripción.
        
        :param descripcion: Descripción del producto
        :return: Categoría identificada o None si no se encuentra
        """
        desc_lower = descripcion.lower()
        for categoria, mapped in self.categorias_mapping.items():
            if categoria.lower() in desc_lower:
                return mapped
        logger.warning(f"No se pudo identificar categoría para: {descripcion}")
        return None

    def extract_proveedor(self, url: str) -> Optional[str]:
        """
        Extrae el proveedor basado en la URL del producto.
        
        :param url: URL del producto
        :return: Proveedor identificado o None si no se encuentra
        """
        if not url or pd.isna(url):
            return None
            
        url_lower = str(url).lower()
        if 'amazon' in url_lower:
            return 'Amazon'
        elif 'walmart' in url_lower:
            return 'Walmart'
        elif 'mercadolibre' in url_lower or 'articulo.mercadolibre' in url_lower:
            return 'MercadoLibre'
        elif 'ebay' in url_lower:
            return 'eBay'
        elif 'soriana' in url_lower:
            return 'Soriana'
        else:
            logger.warning(f"No se pudo identificar proveedor para URL: {url}")
            return None

    def process_compras_sheet(self, df: pd.DataFrame, file_name: str) -> Dict[str, pd.DataFrame]:
        """
        Procesa la hoja 'Compras' del XLSX y prepara los datos para la BD.
        
        :param df: DataFrame con los datos de la hoja 'Compras'
        :param file_name: Nombre del archivo origen para tracking
        :return: Diccionario con DataFrames procesados para cada tabla
        """
        try:
            # Limpieza inicial
            df = df.dropna(how='all')
            df = df.rename(columns={
                'Descripción': 'descripcion',
                'Cant': 'cantidad',
                '% Desc': 'descuento_porcentaje',
                'C. Unit US': 'precio_unitario_usd',
                'C. Unit': 'precio_unitario_mxn',
                'Total Cmpr': 'total_compra_mxn',
                'Env US': 'envio_usd',
                'Envio': 'envio_mxn',
                'Fch Cmpr': 'fecha_compra',
                'Fch Entrga': 'fecha_entrega',
                'Dólar': 'tipo_cambio',
                'Dsc US': 'descuento_adicional_usd',
                'Desct': 'descuento_adicional_mxn',
                'Pzs': 'piezas_por_unidad',
                'Costo Final': 'costo_final',
                'Liga': 'url_producto'
            })

            # Convertir fechas
            df['fecha_compra'] = pd.to_datetime(df['fecha_compra'], errors='coerce')
            df['fecha_entrega'] = pd.to_datetime(df['fecha_entrega'], errors='coerce')
            
            # Extraer marcas y categorías
            df['marca'] = df['descripcion'].apply(self.extract_marca)
            df['categoria'] = df['descripcion'].apply(self.extract_categoria)
            df['proveedor'] = df['url_producto'].apply(self.extract_proveedor)
            
            # Validar datos esenciales
            if df['descripcion'].isnull().any():
                logger.warning("Hay descripciones vacías en el archivo")
            
            # Preparar datos para tablas normalizadas
            productos_df = df[['descripcion', 'marca', 'categoria', 'piezas_por_unidad']].drop_duplicates()
            productos_df['origen'] = file_name
            
            proveedores_df = pd.DataFrame([{'nombre': p} for p in df['proveedor'].unique() if p])
            proveedores_df['origen'] = file_name
            
            # Preparar datos para compras y detalle_compras
            compras_df = df[['fecha_compra', 'fecha_entrega', 'tipo_cambio', 'proveedor']].copy()
            compras_df['numero_factura'] = f"IMPORT-{datetime.now().strftime('%Y%m%d')}"
            compras_df['origen'] = file_name
            
            # Asignar IDs temporales para la relación
            productos_df['temp_id'] = range(1, len(productos_df) + 1)
            proveedores_df['temp_id'] = range(1, len(proveedores_df) + 1)
            
            # Mapear IDs temporales
            df = df.merge(
                productos_df[['descripcion', 'temp_id']], 
                on='descripcion', 
                how='left'
            ).rename(columns={'temp_id': 'producto_id'})
            
            df = df.merge(
                proveedores_df[['nombre', 'temp_id']], 
                left_on='proveedor', 
                right_on='nombre', 
                how='left'
            ).rename(columns={'temp_id': 'proveedor_id'})
            
            detalle_compras_df = df[[
                'producto_id', 'cantidad', 'precio_unitario_usd', 'descuento_porcentaje',
                'precio_unitario_mxn', 'envio_usd', 'envio_mxn', 'descuento_adicional_usd',
                'descuento_adicional_mxn', 'costo_final', 'url_producto'
            ]]
            
            return {
                'productos': productos_df,
                'proveedores': proveedores_df,
                'compras': compras_df,
                'detalle_compras': detalle_compras_df
            }
            
        except Exception as e:
            logger.error(f"Error procesando hoja 'Compras': {str(e)}")
            raise

    def process_precios_sheet(self, df: pd.DataFrame, productos_df: pd.DataFrame, file_name: str) -> pd.DataFrame:
        """
        Procesa la hoja 'Precios' del XLSX y prepara los datos para la BD.
        
        :param df: DataFrame con los datos de la hoja 'Precios'
        :param productos_df: DataFrame de productos procesados
        :param file_name: Nombre del archivo origen para tracking
        :return: DataFrame procesado para la tabla precios_venta
        """
        try:
            # Limpieza inicial
            df = df.dropna(how='all')
            df = df.rename(columns={
                'Descripción': 'descripcion',
                'C. Unit': 'costo_unitario',
                'P. Venta': 'precio_venta',
                'P. Oferta': 'precio_oferta'
            })
            
            # Unir con productos para obtener el ID
            precios_df = df.merge(
                productos_df[['descripcion', 'temp_id']],
                on='descripcion',
                how='left'
            ).rename(columns={'temp_id': 'producto_id'})
            
            # Filtrar solo productos válidos
            precios_df = precios_df[precios_df['producto_id'].notna()]
            
            # Preparar estructura final
            precios_df = precios_df[[
                'producto_id', 'costo_unitario', 'precio_venta', 'precio_oferta'
            ]]
            precios_df['fecha_inicio'] = datetime.now()
            precios_df['origen'] = file_name
            
            return precios_df
            
        except Exception as e:
            logger.error(f"Error procesando hoja 'Precios': {str(e)}")
            raise

    def load_to_database(self, data_dict: Dict[str, pd.DataFrame]):
        """
        Carga los datos procesados a la base de datos PostgreSQL.
        
        :param data_dict: Diccionario con DataFrames para cada tabla
        """
        try:
            with self.db_engine.begin() as conn:
                # 1. Cargar proveedores y obtener IDs reales
                if 'proveedores' in data_dict and not data_dict['proveedores'].empty:
                    data_dict['proveedores'].to_sql(
                        'proveedores', 
                        conn, 
                        if_exists='append', 
                        index=False
                    )
                    
                    # Obtener IDs reales de proveedores recién insertados
                    proveedores_inserted = pd.read_sql(
                        "SELECT nombre, id_proveedor FROM proveedores WHERE origen = %s",
                        conn,
                        params=(data_dict['proveedores']['origen'].iloc[0]
                    )
                    
                    # Mapear IDs reales a los temporales
                    proveedores_map = dict(zip(
                        proveedores_inserted['nombre'],
                        proveedores_inserted['id_proveedor']
                    ))
                    
                    # Actualizar compras_df con IDs reales
                    data_dict['compras']['id_proveedor'] = data_dict['compras']['proveedor'].map(proveedores_map)
                
                # 2. Cargar productos y obtener IDs reales
                if 'productos' in data_dict and not data_dict['productos'].empty:
                    # Primero cargar marcas y categorías si no existen
                    marcas_df = data_dict['productos'][['marca']].drop_duplicates()
                    marcas_existentes = pd.read_sql("SELECT nombre FROM marcas", conn)
                    nuevas_marcas = marcas_df[~marcas_df['marca'].isin(marcas_existentes['nombre'])]
                    
                    if not nuevas_marcas.empty:
                        nuevas_marcas.rename(columns={'marca': 'nombre'}).to_sql(
                            'marcas', 
                            conn, 
                            if_exists='append', 
                            index=False
                        )
                    
                    # Obtener todas las marcas con IDs
                    marcas_map = pd.read_sql("SELECT id_marca, nombre FROM marcas", conn)
                    marcas_map = dict(zip(marcas_map['nombre'], marcas_map['id_marca']))
                    
                    # Hacer lo mismo para categorías
                    categorias_df = data_dict['productos'][['categoria']].drop_duplicates()
                    categorias_existentes = pd.read_sql("SELECT nombre FROM categorias", conn)
                    nuevas_categorias = categorias_df[~categorias_df['categoria'].isin(categorias_existentes['nombre'])]
                    
                    if not nuevas_categorias.empty:
                        nuevas_categorias.rename(columns={'categoria': 'nombre'}).to_sql(
                            'categorias', 
                            conn, 
                            if_exists='append', 
                            index=False
                        )
                    
                    categorias_map = pd.read_sql("SELECT id_categoria, nombre FROM categorias", conn)
                    categorias_map = dict(zip(categorias_map['nombre'], categorias_map['id_categoria']))
                    
                    # Actualizar productos con IDs reales
                    data_dict['productos']['id_marca'] = data_dict['productos']['marca'].map(marcas_map)
                    data_dict['productos']['id_categoria'] = data_dict['productos']['categoria'].map(categorias_map)
                    
                    # Insertar productos
                    productos_cols = ['descripcion', 'id_marca', 'id_categoria', 'piezas_por_unidad', 'origen']
                    data_dict['productos'][productos_cols].to_sql(
                        'productos', 
                        conn, 
                        if_exists='append', 
                        index=False
                    )
                    
                    # Obtener IDs reales de productos insertados
                    productos_inserted = pd.read_sql(
                        "SELECT descripcion, id_producto FROM productos WHERE origen = %s",
                        conn,
                        params=(data_dict['productos']['origen'].iloc[0],)
                    )
                    
                    productos_map = dict(zip(
                        productos_inserted['descripcion'],
                        productos_inserted['id_producto']
                    ))
                    
                    # Actualizar detalle_compras con IDs reales
                    data_dict['detalle_compras']['id_producto'] = data_dict['detalle_compras']['producto_id'].map(
                        {k: v for k, v in productos_map.items() if k in data_dict['productos']['descripcion'].values}
                    )
                
                # 3. Cargar compras
                if 'compras' in data_dict and not data_dict['compras'].empty:
                    compras_cols = ['fecha_compra', 'fecha_entrega', 'tipo_cambio', 'id_proveedor', 'numero_factura', 'origen']
                    data_dict['compras'][compras_cols].to_sql(
                        'compras', 
                        conn, 
                        if_exists='append', 
                        index=False
                    )
                    
                    # Obtener IDs de compras insertadas
                    compras_inserted = pd.read_sql(
                        "SELECT id_compra, numero_factura FROM compras WHERE origen = %s",
                        conn,
                        params=(data_dict['compras']['origen'].iloc[0],)
                    
                    # Asumimos que numero_factura es único por lote
                    compras_map = {data_dict['compras']['numero_factura'].iloc[0]: compras_inserted['id_compra'].iloc[0]}
                    
                    # Actualizar detalle_compras con ID de compra
                    data_dict['detalle_compras']['id_compra'] = compras_map.get(data_dict['compras']['numero_factura'].iloc[0])
                
                # 4. Cargar detalle_compras
                if 'detalle_compras' in data_dict and not data_dict['detalle_compras'].empty:
                    detalle_cols = [
                        'id_compra', 'id_producto', 'cantidad', 'precio_unitario_usd',
                        'descuento_porcentaje', 'precio_unitario_mxn', 'envio_usd',
                        'envio_mxn', 'descuento_adicional_usd', 'descuento_adicional_mxn',
                        'costo_final', 'url_producto'
                    ]
                    data_dict['detalle_compras'][detalle_cols].to_sql(
                        'detalle_compras', 
                        conn, 
                        if_exists='append', 
                        index=False
                    )
                
                # 5. Cargar precios_venta
                if 'precios_venta' in data_dict and not data_dict['precios_venta'].empty:
                    precios_cols = [
                        'producto_id', 'costo_unitario', 'precio_venta', 
                        'precio_oferta', 'fecha_inicio', 'origen'
                    ]
                    data_dict['precios_venta'][precios_cols].to_sql(
                        'precios_venta', 
                        conn, 
                        if_exists='append', 
                        index=False
                    )
                
                logger.info("Datos cargados exitosamente a la base de datos")
        
        except exc.SQLAlchemyError as e:
            logger.error(f"Error de base de datos: {str(e)}")
            raise
        except Exception as e:
            logger.error(f"Error inesperado al cargar datos: {str(e)}")
            raise

    def process_file(self, file_path: str):
        """
        Procesa un archivo XLSX individual y carga sus datos a la BD.
        
        :param file_path: Ruta completa del archivo XLSX
        """
        try:
            if not self.validate_sheets(file_path):
                return
            
            file_name = os.path.basename(file_path)
            logger.info(f"Procesando archivo: {file_name}")
            
            # Leer hojas necesarias
            compras_df = pd.read_excel(file_path, sheet_name='Compras')
            precios_df = pd.read_excel(file_path, sheet_name='Precios')
            
            # Procesar hoja Compras
            compras_data = self.process_compras_sheet(compras_df, file_name)
            
            # Procesar hoja Precios (requiere productos_df de compras_data)
            precios_data = self.process_precios_sheet(
                precios_df, 
                compras_data['productos'], 
                file_name
            )
            compras_data['precios_venta'] = precios_data
            
            # Cargar todo a la base de datos
            self.load_to_database(compras_data)
            
            logger.info(f"Archivo {file_name} procesado exitosamente")
            
        except Exception as e:
            logger.error(f"Error procesando archivo {file_path}: {str(e)}")
            raise

    def process_directory(self, directory: str):
        """
        Procesa todos los archivos XLSX válidos en el directorio especificado.
        
        :param directory: Ruta del directorio a procesar
        """
        try:
            xlsx_files = self.find_xlsx_files(directory)
            
            if not xlsx_files:
                logger.warning("No se encontraron archivos XLSX válidos para procesar")
                return
            
            for file_path in xlsx_files:
                try:
                    self.process_file(file_path)
                except Exception as e:
                    logger.error(f"Error procesando {file_path}, continuando con siguiente archivo...")
                    continue
                    
            logger.info("Procesamiento de directorio completado")
            
        except Exception as e:
            logger.error(f"Error procesando directorio {directory}: {str(e)}")
            raise


if __name__ == "__main__":
    # Configuración
    DB_CONNECTION_STR = "postgresql://usuario:contraseña@localhost:5432/nombre_bd"
    DIRECTORY_TO_SCAN = r"C:\ruta\a\tu\directorio\con\archivos_xlsx"
    
    # Ejecutar procesamiento
    processor = XLSXProcessor(DB_CONNECTION_STR)
    processor.process_directory(DIRECTORY_TO_SCAN)

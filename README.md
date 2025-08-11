# database-py-add-medicalEncyclopedia
Repository that stores console application code for adding items to medical encyclopedia relationships.


# process-automation-testing
Automatizacion de procesos para ingesta y exportacion de datos...


1. Instalar el driver ODBC de SQL Server (msodbcsql17) en macOS

# 1.1. Instalar unixODBC

brew install unixodbc

# 1.2.  Agregar tap de Microsoft y actualizar fórmulas

brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release
brew update

# 1.3. Instalar el driver msodbcsql17
ACCEPT_EULA=Y brew install msodbcsql17

# 1.4 Verificar instalación
odbcinst -q -d -n "ODBC Driver 17 for SQL Server"


2. Configurar credenciales de AWS

# 2.1 Validar version aws cli
aws --version

# 2.2 Si no encuentra aws cli
brew install awscli

# 2.3 Configurar las credenciales
aws configure

# Te pedirá la siguiente información:
# AWS Access Key ID: Tu clave de acceso de AWS
# AWS Secret Access Key: Tu clave secreta
# Default region name: La región AWS que prefieras (us-east-1, eu-west-1, etc...)
# Default output format: El formato de salida (json, text, table)

# 2.4 Verificar la configuración
aws sts get-caller-identity

# 2.5 Configurar variables de entorno (Opcional)
export AWS_ACCESS_KEY_ID=TU_ACCESS_KEY
export AWS_SECRET_ACCESS_KEY=TU_SECRET_KEY
export AWS_DEFAULT_REGION=us-east-1



####  Proof Concept Import S3 to RDS  ###
1. Instale las bibliotecas externas en un nuevo directorio package.

# 1.1 Crear un entorno virtual

rm -rf venv

python3 -m venv venv

# 1.2 Activar el entorno virtual

source venv/bin/activate

which pip

pip list

pip install pyodbc boto3 beautifulsoup4

pip install --upgrade pip

2. Ejecutar prueba.

python import_files_to_postgre.py

python import_files_to_rds.py

# Error:
Traceback (most recent call last):
  File "/Users/marcotlachino/WORKSPACE/GitPLM/dbPyAddMedicalEncyclopedia/prcss_import_s3_to_rds/import_files_to_rds.py", line 3, in <module>
    import pyodbc
ModuleNotFoundError: No module named 'pyodbc'

# Reactivar el entorno virtual:

source venv/bin/activate

###  End Proof Concept Import S3 to RDS  ###


####  Proof Concept Import CSV to RDS  ###
1. Instale las bibliotecas externas en un nuevo directorio package.

which pip

pip list

pip install pyodbc boto3 pandas beautifulsoup4

pip install --upgrade pip

2. Ejecutar prueba.

python import_files_to_postgre.py

###  End Proof Concept Import CSV to RDS  ###

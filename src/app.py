import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
#Asignacion de vars. globales:
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
# 1) Connect to the database with SQLAlchemy
def connect():
    global engine
    #try and exepction = Estructura de desicion como If and else
    try:
        connection_string_direccionhost = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"
        print("Starting the connection...")
        engine = create_engine(connection_string_direccionhost, isolation_level="AUTOCOMMIT") 
        engine.connect()
        print("Connected successfully!")
        return engine
    #Si no es posible para el programa conectarse con el db, "Exception" enviará el mensaje de error
    except Exception as e:
        print(f"Error connecting to the database: {e}")
        return None
#Llamada a la conecxtion of the engine "connection_string_direccionhost"
engine = connect()

#Después de llamar a la conexión, si engine = None(en exception), el siguiente condicional cierra el programa
if engine is None:
    exit() 


# 2) Create the tables
#Al conectar SQL con engine podemos usar lenguaje SQL comentado para enviar el código al entorno de sql
with engine.connect() as connection:
   
    #insert info a sql a través de la var sql_file_path
    sql_file_path = "/workspaces/connecting-to-a-sql-database-project-tutorial/src/sql/create.sql"
    
    with open(sql_file_path, "r", encoding="utf-8") as file:
     sql_script = file.read()

    with engine.connect() as connection:
        connection.execute(text(sql_script))
        connection.commit()

# 3) Insert data

with engine.connect() as connection:
   
    #insert info a sql a través de la var sql_file_path
    sql_file_path = "/workspaces/connecting-to-a-sql-database-project-tutorial/src/sql/insert.sql"
    
    with open(sql_file_path, "r", encoding="utf-8") as file:
     sql_script = file.read()

    with engine.connect() as connection:
        connection.execute(text(sql_script))
        connection.commit()

# 4) Use Pandas to read and display a table
#hacer consulta/leer sql_script utilizando Pandas df
df = pd.read_sql("SELECT * FROM publishers;", engine)
print(df)

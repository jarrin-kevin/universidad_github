import mysql.connector
from mysql.connector import errorcode
import redis
# Configuración de conexión al servidor MySQL
HOST = 'localhost'
ROOT_USER = 'root'         #nombre que se puso igual
ROOT_PASSWORD = '12345678' #contraseña al principio en mysql
DATABASE_NAME = 'universidad'
APP_USER = 'app_user'
APP_PASSWORD = 'app_password'

# Queries para creación de base de datos y tablas
CREATE_DATABASE_QUERY = f"CREATE DATABASE IF NOT EXISTS {DATABASE_NAME} DEFAULT CHARACTER SET 'utf8mb4';"
USE_DATABASE_QUERY = f"USE {DATABASE_NAME};"

CREATE_CONEXIONES_TABLE = """
CREATE TABLE IF NOT EXISTS conexiones (
    id_conexiones INT AUTO_INCREMENT PRIMARY KEY,
    correo VARCHAR(255) NOT NULL,
    ap VARCHAR(255) NOT NULL,
    campus VARCHAR(255) NOT NULL,
    fecha DATE NOT NULL,
    hora TIME  NOT NULL,
    INDEX(correo)
);
"""

CREATE_MOVIMIENTOS_TABLE = """
CREATE TABLE IF NOT EXISTS movimientos (
    id_movimientos INT AUTO_INCREMENT PRIMARY KEY,
    correo VARCHAR(255) NOT NULL,
    campus_anterior VARCHAR(255) NOT NULL,
    campus_actual VARCHAR(255) NOT NULL,
    fecha DATE NOT NULL,
    hora  TIME NOT NULL,
    INDEX(correo)
);
"""

CREATE_USER_QUERY = f"""
CREATE USER IF NOT EXISTS '{APP_USER}'@'localhost' IDENTIFIED BY '{APP_PASSWORD}';
GRANT ALL PRIVILEGES ON {DATABASE_NAME}.* TO '{APP_USER}'@'localhost';
FLUSH PRIVILEGES;
"""

def execute_query(cursor, query, description=None):
    """Ejecutar una consulta y mostrar mensaje de éxito si se proporciona descripción."""
    try:
        cursor.execute(query)
        if description:
            print(description)
    except mysql.connector.Error as err:
        print(f"Error ejecutando '{description}': {err}")
        raise

def setup_database():
    connection = None
    try:
        # Conexión al servidor MySQL como root
        connection = mysql.connector.connect(
            host=HOST,
            user=ROOT_USER,
            password=ROOT_PASSWORD
        )
        # Crear cursor
        cursor = connection.cursor()

        # Crear base de datos y usuario
        execute_query(cursor, CREATE_DATABASE_QUERY, 
                     f"Base de datos '{DATABASE_NAME}' creada o ya existente.")
        
        # Usar la base de datos
        execute_query(cursor, USE_DATABASE_QUERY)
        
        # Crear tablas
        execute_query(cursor, CREATE_CONEXIONES_TABLE,
                     "Tabla 'conexiones' creada o ya existente.")
        execute_query(cursor, CREATE_MOVIMIENTOS_TABLE,
                     "Tabla 'movimientos' creada o ya existente.")
        
        # Crear usuario y asignar permisos
        for query in CREATE_USER_QUERY.split(';')[:-1]:  # Ignorar el último elemento vacío
            if query.strip():
                execute_query(cursor, query + ';')
        print(f"Usuario '{APP_USER}' configurado con acceso a la base de datos.")

        # Confirmar transacciones
        connection.commit()

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Error con las credenciales proporcionadas para MySQL.")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("La base de datos no existe y no pudo ser creada.")
        else:
            print(f"Error: {err}")

        if connection and connection.is_connected():
            connection.rollback()
            
    finally:
        # Liberar recursos
        if connection and connection.is_connected():
            if 'cursor' in locals():
                cursor.close()
            connection.close()
            print("Conexión cerrada.")


def is_redis_running():
    try:
        client = redis.StrictRedis(host='127.0.0.1', port=6379)
        return client.ping()
        print("si inicio")
    except redis.ConnectionError:
        print("no inicio")
        return False




if __name__ == "__main__":
    setup_database()
    is_redis_running()



import socket
from datetime import datetime
from sqlalchemy import create_engine, Column, String, Integer, DateTime
from sqlalchemy.orm import sessionmaker, declarative_base
import json
import redis
import threading
import logging


# Configuración DB
DATABASE_URL = "mysql+mysqlconnector://root:12345678@localhost/universidad"
engine = create_engine(DATABASE_URL)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()
Base.metadata.create_all(bind=engine)

# Configuración de Redis
redis_client = redis.StrictRedis(host='localhost', port=6379, db=0)

# Modelo DB
class Conexiones(Base):
    __tablename__ = "conexiones"
    id_conexiones = Column(Integer, primary_key=True, index=True)
    correo = Column(String(255), index=True, nullable=False)
    ap = Column(String(255), nullable=False)
    campus = Column(String(255), nullable=False)
    fecha = Column(DateTime, nullable=False)
    hora = Column(String(255), nullable=False)

class Movimientos(Base):
    __tablename__ = "movimientos"
    id_movimientos = Column(Integer, primary_key=True, index=True)
    correo = Column(String(255), index=True, nullable=False)
    campus_anterior = Column(String(255), nullable=False)
    campus_actual = Column(String(255), nullable=False)
    fecha = Column(DateTime, nullable=False)
    hora = Column(String(255), nullable=False)

def save_to_conexiones(db, correo, ap, campus, fecha_obj):
    registro = Conexiones(
        correo=correo,
        ap=ap,
        campus=campus,
        fecha=fecha_obj,
        hora=fecha_obj.strftime("%H:%M:%S")
    )
    db.add(registro)
    db.commit()
    print(f"Registro guardado en Conexiones para el correo {correo} en el campus {campus}.")


logging.basicConfig( # no se que hace 
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Función para procesar mensaje del socket
def process_socket_message(message,db):
    try:

        json_message = json.loads(message) #cargo el mensaje
            
        logging.info(f"Received message: {json_message}")
        # Extraer campos de interés
        ap_name = json_message.get("_ap_name", "N/A")
        user = json_message.get("_user", "N/A")
        timestamp = json_message.get("_timestamp", "N/A")
        print(f"_ap_name: {ap_name}, _user: {user}, _timestamp: {timestamp}")

        if "N/A" in (ap_name, user, timestamp):
            print("Mensaje inválido. Se omite.")
            return
        

        # Dividir y procesar los datos
        campus,ap = ap_name.split('-', maxsplit=1)
        fecha_obj = datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")

        # Verificar si el usuario ya existe en la tabla conexiones
        registro_existente = db.query(Conexiones).filter(
            Conexiones.correo == user
        ).order_by(Conexiones.id_conexiones.desc()).first()

        if registro_existente:
            # Verificar si está en el mismo campus
            print(f"Registro existente encontrado: ID={registro_existente.id_conexiones}, Correo={registro_existente.correo}, Campus={registro_existente.campus}")
            if registro_existente.campus == campus:
               save_to_conexiones(db, user, ap, campus, fecha_obj)  # Guardar registro adicional si es necesario 
            else:
                # Registrar movimiento de usuario 
                save_to_conexiones(db, user, ap, campus, fecha_obj)
                movilidad = Movimientos(
                    correo=user,
                    campus_anterior=registro_existente.campus,  # Campus del registro previo
                    campus_actual=campus,                      # Nuevo campus
                    fecha=fecha_obj,
                    hora=fecha_obj.strftime("%H:%M:%S")
                )
                db.add(movilidad)
                db.commit() 
                
                print(f"Registro de usuario {user} se movilizo al campus {campus}.")
                return
            # Actualizar la tabla conexiones con el usuario ya existente pero con la movimiento del mismo
        else:
        # Nuevo registro de usuario
            save_to_conexiones(db, user, ap, campus, fecha_obj)
            print(f"Nuevo registro de usuario creado")

    except json.JSONDecodeError as e:
        print(f"Error al decodificar JSON: {e}")
    except Exception as e:
        print(f"Error procesando mensaje: {e}")

def worker():
    logging.info("Worker started")
    db = SessionLocal()
    try:
        while True:
            message = redis_client.blpop('socket_messages', timeout=5)
            if message:
                logging.info("New message received in Redis queue")
                process_socket_message(message[1].decode('utf-8'), db)
    except Exception as e:
        print(f"Error en el worker: {e}")
        logging.error(f"Worker error: {e}")
    finally:
        db.close()




def socket_server():
    logging.info("Socket server starting...")
    host = '0.0.0.0'
    port = 1234
    
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)f"Received message: {json_message}" #no se que hace
    server_socket.bind((host, port)) #no se que hace
    server_socket.listen(100) #no se que
    print(f"Escuchando en {host}:{port}...")
    
    while True:
        client_socket, client_address = server_socket.accept()
        print(f"Conexión establecida desde: {client_address}")
        
        buffer = b""
        ######db = SessionLocal()
        try:
            while True:
                data = client_socket.recv(4096)
                if not data:
                    break
                buffer += data
                messages = buffer.split(b'\x00') # Separar mensajes completos
                # Procesar mensajes completos
                for message in messages[:-1]:
                    # Enviar mensaje a la cola de Redis
                    redis_client.rpush('socket_messages', message.decode('utf-8'))
                    # # # # process_socket_message(message, db)
                buffer = messages[-1] # Mantener el último fragmento incompleto
                
        except Exception as e:
            print(f"Error en la conexión con {client_address}: {e}")
        finally:
            client_socket.close()
            #db.close()


if __name__ == "__main__":
    logging.info("Starting application...")
    # Iniciar socket server en thread separado
    socket_thread = threading.Thread(target=socket_server, daemon=True)
    # Iniciar el worker
    worker_thread = threading.Thread(target=worker, daemon=True)
    socket_thread.start()
    worker_thread.start()

    try:
        while True:
            threading.Event().wait()
    except KeyboardInterrupt:
        logging.info("Shutting down...")
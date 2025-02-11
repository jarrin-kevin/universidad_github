from abc import ABC, abstractmethod
import redis
import asyncio
import logging
import subprocess
"""
class DataProcessor(ABC): #Clase procesar los datos
    @abstractmethod
    def process_data(self, data):
        #Método para procesar los datos recibidos.
        pass

class DatabaseHandler(ABC): #Clase guardar los datos
    @abstractmethod
    def save_to_database(self, data):
        #Método para guardar datos en la base de datos.
        pass

class DataSender(ABC):     #Clase guardar los datos
    @abstractmethod
    def send_data(self, data):
        #Método para enviar datos a un puerto específico.
        pass

"""
class DataReceiver(ABC): #Clase recibir los datos 
    @abstractmethod
    def receiver_data(self):
        """Método para recibir datos desde un puerto."""
        pass

class receiver_socket(DataReceiver):
    def __init__(self,host,port,redis_url):
        self.host = host
        self.port = port
        self.redis_url = redis_url
    def login_info(self): 
       #configura el modulo de loggin para registrar mensajes
       logging.basicConfig( 
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
        )  
       logging.info("Socket server starting...")      

    async def verificar_redis(self):
        """Verifica si Redis está corriendo antes de iniciar el servidor."""
        try:
            self.redis_client = redis.from_url(self.redis_url)
            self.redis_client.ping()
            logging.info("Conexión con Redis exitosa.")
        except Exception as e:
            logging.error(f"Redis no está disponible: {e}")
            try: #en caso de no estar activo redis lo activa 
                subprocess.Popen(["redis-server"]) # Iniciar redis
                await asyncio.sleep(10)  # Esperar a que Redis inicie
                self.redis_client = redis.from_url(self.redis_url)
                self.redis_client.ping()
                logging.info("Redis iniciado correctamente.")
            except Exception as e:
                logging.error(f"Mismo no inicio redis wacho: {e}")
            exit(1)  # Detener el programa si Redis no está activo

    
    
    async def inicializar_servidor (self):
        #Esta función inicializa y ejecuta el servidor TCP utilizando asyncio.
        await self.verificar_redis() #Inicializa redis
        self.server = await asyncio.start_server(self.receiver_data, self.host, self.port) # elf.handle_client: Es una referencia al método siguiente
        logging.info(f"Escuchando en {self.host}:{self.port}...")
        async with self.server: #Contexto que asegura que el servidor se cierre adecuadamente si ocurre algún error.
            await self.server.serve_forever() #Mantiene el servidor activo y en espera de nuevas conexiones de forma indefinida.
               
    async def receiver_data(self, reader, writer):
        client_address = writer.get_extra_info("peername") #Obtiene información sobre el cliente conectado, tupla (host, port)
        logging.info(f"Conexión establecida desde: {client_address}")
       # El uso de await permite que otras tareas sigan ejecutándose mientras se realiza la operación.

        #Maneja una conexión individual con un cliente, incluyendo la recepción de datos y su procesamiento.
        while True: #(mantiene la conexión mientras el cliente siga enviando dato)
            #Acumulación de datos recibidos, no creo que llegan los mensajes completos?
            buffer = b"" #Es un string de bytes, acumular datos recibidos en partes.
            try:
                while True: # (Acumula y procesa mensajes)
                    data = await reader.read(4096)#lee los datos que llegan en fragmentos.
                    if not data:# si no hay data rompe la conexion
                        break 
                    buffer += data #va guardando en buffer
                    # Para visualizar los datos en consola
                    logging.info(f"Datos recibidos: {data.decode('utf-8', errors='ignore')}")  # Imprime los datos en formato legible
                    messages = buffer.split(b'\x00') # hasta encontrar el separador \x00 separa los mensajes
                    # Procesar mensajes completos
                    for message in messages[:-1]: #Selecciona todos los elementos menos el último. Razón: Si el último mensaje no está completo (falta más información), no se puede procesar, Se guarda en el buffer para completar la información en la próxima iteración.
                        # Enviar mensaje a la cola de Redis
                        decoded_message = message.decode("utf-8")
                        await self.redis_client.rpush("socket_messages", decoded_message) #Inserta el mensaje decodificado al final de una lista en Redis llamada socket_messages
                    buffer = messages[-1] # Si el último mensaje está incompleto, se guarda en el buffer para ser completado más adelante.
                
            except Exception as e:
                print(f"Error en la conexión con {client_address}: {e}")
            finally:
                logging.info(f"Cerrando conexión con {client_address}")
                writer.close() #Cierra la conexión con el cliente de forma ordenada.
                await writer.wait_closed()



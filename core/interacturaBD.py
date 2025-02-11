   async def verificar_dependencias(self):
        """Verifica si Redis y MySQL están activos antes de iniciar el servidor."""
        await self.verificar_redis()
        try:
            connection = mysql.connector.connect(
             host="localhost",
             user="tu_usuario",
             password="tu_contraseña",
             database="tu_bd"
            )
            if connection.is_connected():
                logging.info("Conexión con MySQL exitosa.")
            connection.close()
        except Exception as e:
            logging.error(f"MySQL no está disponible: {e}")
            exit(1)  # Detener el programa si MySQL no está activ
import socket
import json
import time

# Dirección y puerto de Graylog
GRAYLOG_HOST = '127.0.0.1'  # Dirección de tu servidor Graylog
GRAYLOG_PORT = 12201        # Puerto del Input GELF UDP

# Datos del log
log_data = {
  "gl2_message_id": "01JDAGB6QR0Q7F8P7QFQNNC5WM",
  "source": "2024",
  "message": "2024 10.250.14.253 cli[4859]: <541031> <INFO> AP:BALZAY-EADM-PB-105 <10.250.14.253 A8:5B:F7:C4:9B:B0>  Learning client username: mac-7a:8f:af:e8:d1:de usr-evelyn.chicag@ucuenca.edu.ec acct-evelyn.chicag@ucuenca.edu.ec.",
  "gl2_source_input": "662a7fb7c2e1625f63bd5adf",
  "gl2_processing_timestamp": "2024-11-22 17:57:27.176",
  "ap_name": "BALZAY-EADM-PB-105",
  "facility_num": 17,
  "gl2_source_node": "8320539c-3a4d-4076-a6f1-ffa0cde45151",
  "_id": "3c2f3880-a8fb-11ef-925c-0242ac150004",
  "facility": "local1",
  "user": "evelyn.chicag@ucuenca.edu.ec",
  "gl2_processing_duration_ms": 1,
  "timestamp": "2024-11-22T17:57:31.000Z"
}

# Serializa el mensaje a JSON
json_message = json.dumps(log_data)

# Crea un socket UDP
sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

# Enviar el mensaje GELF UDP a Graylog
sock.sendto(json_message.encode('utf-8'), (GRAYLOG_HOST, GRAYLOG_PORT))

print("Mensaje enviado a Graylog.")
sock.close()



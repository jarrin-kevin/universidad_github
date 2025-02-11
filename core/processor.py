import socket

HOST = "192.168.100.16"
PORT = 12345

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
    server.bind((HOST, PORT))
    server.listen()
    print(f"Listening on {HOST}:{PORT}")

    conn, addr = server.accept()
    with conn:
        print(f"Connected by {addr}")
        while True:
            data = conn.recv(1024)
            if not data:
                break
            print("Received:", data.decode())

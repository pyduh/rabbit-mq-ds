
if __name__ == '__main__':
    from app.server import Server

    server = Server()
    
    try:
        server.run()
    except KeyboardInterrupt:
        server.stop()

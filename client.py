
if __name__ == '__main__':
    from app.client import Client

    args = Client.args()

    Client(agent_type=args.type).run()

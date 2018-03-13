import time
import socket


class ClientError(Exception):
    pass


class SocketError(ClientError):
    pass


class ProtocolError(ClientError):
    pass


class Client:

    def __init__(self, host, port, timeout=None):
        self.host = host
        self.port = int(port)
        self.timeout = timeout
        try:
            self.connection = socket.create_connection((host, port), timeout)
        except socket.error as err:
            raise SocketError("failed to establish a connection", err)

    def put(self, key, value, timestamp=str(int(time.time()))):

        # form a request
        message = "put {} {} {}\n".format(key, value, timestamp)

        # send the request
        try:
            self.connection.sendall(message.encode())
        except socket.error as err:
            raise SocketError("failed to send a request", err)

        # obtain a feedback
        try:
            data = self.connection.recv(1024).decode()
        except socket.error as err:
            raise SocketError("failed to receive a feedback", err)
        print("client received: ", data)

        # throw an exception if failed
        if data == "error\nwrong command\n\n":
            raise ProtocolError(data)

    def get(self, key):

        # form a request
        message = "get {}\n".format(key)

        # send the request
        try:
            self.connection.sendall(message.encode())
        except socket.error as err:
            raise SocketError("failed to send a request", err)

        # receive a feedback
        data = b""
        while not data.endswith(b"\n\n"):
            try:
                data += self.connection.recv(1024)
            except socket.error as err:
                raise SocketError("failed to receive a feedback", err)
        decoded_data = data.decode()
        print("client received: ", decoded_data)

        # form an answer
        records = decoded_data.split('\n')
        ans = {}
        if records[0] == 'ok':
            for line in records[1:-2]:
                stat = line.split()
                key = stat[0]
                value = float(stat[1])
                timestamp = int(stat[2])
                info = (timestamp, value)
                if key not in ans:
                    ans[key] = []
                ans[key].append(info)
            for key, info in ans.items():
                info.sort(key=lambda x: x[0])
        else:
            raise ProtocolError("error\nwrong command\n\n")
        return ans

    def close(self):
        try:
            self.connection.close()
        except socket.error as err:
            raise SocketError("failed to close the socket", err)


def _main():
    # check if the client is working fine
    client = Client("127.0.0.1", 10001, timeout=5)
    client.put("test", 0.5, timestamp=1)
    client.put("test", 2.0, timestamp=2)
    client.put("test", 0.5, timestamp=3)
    client.put("load", 3, timestamp=4)
    client.put("load", 4, timestamp=5)
    print(client.get("*"))

    client.close()


if __name__ == "__main__":
    _main()
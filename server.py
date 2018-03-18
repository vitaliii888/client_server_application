import asyncio


class Storage:
    """
    Класс реализует хранилище данных на сервере.
    """

    def __init__(self):
        self._data = {}

    def put(self, key, value, timestamp):

        if key not in self._data:
            self._data[key] = {}

        self._data[key][timestamp] = value

    def get(self, key):

        if key != '*':
            data = {
                key: self._data.get(key, {})
            }
        else:
            data = self._data

        resp = {}
        for key, timestamp_data in data.items():
            resp[key] = sorted(timestamp_data.items())

        return resp

class ParseError(ValueError):
    pass

class Parser:
    """
    Класс отвечает за разбор запроса, полученного от клиента,
    а также за формирование ответа согласно протоколу.
    """

    def encode(self, response):
        """Подготовка текстовой строки для ответа клиенту с помощью сокетов"""

        rows = []
        for item in response:
            if not item:
                continue
            for key, values in response.items():
                for timestamp, value in values:
                    rows.append("{} {} {}".format(key, value, timestamp))

        feedback = "ok\n"

        if rows:
            feedback += "\n".join(rows) + "\n"

        return feedback + "\n"

    def decode(self, data):
        """
        Разбор запроса пользователя и формирование списка команд
        для дальнейшей обработки
        """

        parts = data.split("\n")
        commands = []
        for part in parts:
            if not part:
                continue

            try:
                command, params = part.strip().split(" ", 1)
                if command == "put":
                    key, value, timestamp = params.split()
                    commands.append(
                        (command, key, float(value), int(timestamp))
                    )
                elif command == "get":
                    key = params
                    commands.append(
                        (command, key)
                    )
                else:
                    raise ValueError("wrong command")
            except ValueError:
                raise ParseError("wrong command")

        return commands


class ExecutorError(Exception):
    pass


class Executor:
    """
    Класс выполняет команды, полученные сервером,
    осуществляя взаимодействие между классами Parser и Storage.
    """

    def __init__(self, storage):
        self.storage = storage

    def run(self, command, *params):
        if command == "put":
            return self.storage.put(*params)
        elif command == "get":
            return self.storage.get(*params)
        else:
            raise ExecutorError("wrong command")


class ClientServerProtocol(asyncio.Protocol):
    """
    Класс для асинхронной обработки запросов с помощью библиотеки asyncio
    """

    # Хранилище является атрибутом класса, чтобы быть единым
    # для всех объектов класса ClientServerProtocol, которые
    # будут создаваться при получении каждого нового запроса
    # от клиентов
    storage = Storage()

    def __init__(self):
        self.transport = None
        self.parser = Parser()
        self.executor = Executor(self.storage)
        self._buffer = b''

    def connection_made(self, transport):
        self.transport = transport

    def process_data(self, data):

        # разбор запроса и формирование списка команд
        commands = self.parser.decode(data)

        # выполнение команд
        responses = []
        for command in commands:
            resp = self.executor.run(*command)
            responses.append(resp)

        # преобразование ответов в строку для отсылки клиенту
        return self.parser.encode(responses)

    def data_received(self, data):

        self._buffer += data
        try:
            decoded_data = self._buffer.decode()
        except UnicodeDecodeError:
            return

        if not decoded_data.endswith('\n'):
            return

        self._buffer = b''

        try:
            resp = self.process_data(decoded_data)
        except (ParseError, ExecutorError) as err:
            self.transport.write(
                "error\n{}\n\n".format(err).encode()
            )
            return

        self.transport.write(resp.encode())


def run_server(host, port):
    loop = asyncio.get_event_loop()
    coro = loop.create_server(
        ClientServerProtocol,
        host, port
    )

    server = loop.run_until_complete(coro)

    try:
        loop.run_forever()
    except KeyboardInterrupt:
        pass

    server.close()
    loop.run_until_complete(server.wait_closed())
    loop.close()


def _main():
    host = "127.0.0.1"
    port = 10001
    run_server(host, port)


if __name__ == "__main__":
    _main()

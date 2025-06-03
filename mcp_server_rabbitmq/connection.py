import ssl

import pika


class RabbitMQConnection:
    def __init__(self, host: str, port: int, username: str, password: str, use_tls: bool):
        self.protocol = "amqps" if use_tls else "amqp"
        self.url = f"{self.protocol}://{username}:{password}@{host}:{port}"
        self.parameters = pika.URLParameters(self.url)

        if use_tls:
            # Use modern TLS protocol with proper defaults
            ssl_context = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            # Enable hostname verification and certificate validation
            ssl_context.check_hostname = True
            ssl_context.verify_mode = ssl.CERT_REQUIRED
            # Use secure cipher suites (let OpenSSL choose the best ones)
            ssl_context.set_ciphers(
                "ECDHE+AESGCM:ECDHE+CHACHA20:DHE+AESGCM:DHE+CHACHA20:!aNULL:!MD5:!DSS"
            )
            self.parameters.ssl_options = pika.SSLOptions(context=ssl_context)

    def get_channel(self) -> tuple[pika.BlockingConnection, pika.channel.Channel]:
        try:
            # Set connection timeout to avoid hanging indefinitely
            self.parameters.connection_attempts = 3
            self.parameters.retry_delay = 2.0
            self.parameters.socket_timeout = 10.0

            connection = pika.BlockingConnection(self.parameters)
            channel = connection.channel()
            return connection, channel
        except pika.exceptions.AMQPConnectionError as e:
            raise ConnectionError(f"Failed to connect to RabbitMQ: {e}") from e
        except Exception as e:
            raise RuntimeError(f"Unexpected error creating RabbitMQ connection: {e}") from e


def validate_rabbitmq_name(name: str, field_name: str) -> None:
    """Validate RabbitMQ queue/exchange names"""
    if not name or not name.strip():
        raise ValueError(f"{field_name} cannot be empty")
    if not all(c.isalnum() or c in "-_.:" for c in name):
        raise ValueError(
            f"{field_name} can only contain letters, digits, hyphen, underscore, period, or colon"
        )
    if len(name) > 255:
        raise ValueError(f"{field_name} must be less than 255 characters")

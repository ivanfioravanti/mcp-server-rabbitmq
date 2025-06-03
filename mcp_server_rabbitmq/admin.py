import base64
from typing import Dict, List, Optional

import requests

from mcp_server_rabbitmq.connection import validate_rabbitmq_name


class RabbitMQAdmin:
    def __init__(self, host: str, port: int, username: str, password: str, use_tls: bool):
        self.protocol = "https" if use_tls else "http"
        self.base_url = f"{self.protocol}://{host}:{port}/api"
        self.auth = base64.b64encode(f"{username}:{password}".encode()).decode()
        self.headers = {"Authorization": f"Basic {self.auth}", "Content-Type": "application/json"}
        # Create a session for connection pooling
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def _make_request(
        self, method: str, endpoint: str, data: Optional[Dict] = None
    ) -> requests.Response:
        url = f"{self.base_url}/{endpoint}"
        try:
            response = self.session.request(
                method,
                url,
                json=data,
                verify=True,
                timeout=(5.0, 30.0),  # (connection timeout, read timeout)
            )
            response.raise_for_status()
            return response
        except requests.exceptions.Timeout as e:
            raise TimeoutError(f"Request to RabbitMQ management API timed out: {e}") from e
        except requests.exceptions.ConnectionError as e:
            raise ConnectionError(f"Failed to connect to RabbitMQ management API: {e}") from e
        except requests.exceptions.HTTPError as e:
            raise RuntimeError(f"HTTP error from RabbitMQ management API: {e}") from e

    def list_queues(self) -> List[Dict]:
        """List all queues in the RabbitMQ server"""
        response = self._make_request("GET", "queues")
        return response.json()

    def list_exchanges(self) -> List[Dict]:
        """List all exchanges in the RabbitMQ server"""
        response = self._make_request("GET", "exchanges")
        return response.json()

    def get_queue_info(self, queue: str, vhost: str = "/") -> Dict:
        """Get detailed information about a specific queue"""
        vhost_encoded = requests.utils.quote(vhost, safe="")
        queue_encoded = requests.utils.quote(queue, safe="")
        response = self._make_request("GET", f"queues/{vhost_encoded}/{queue_encoded}")
        return response.json()

    def delete_queue(self, queue: str, vhost: str = "/") -> None:
        """Delete a queue"""
        validate_rabbitmq_name(queue, "Queue name")
        vhost_encoded = requests.utils.quote(vhost, safe="")
        queue_encoded = requests.utils.quote(queue, safe="")
        self._make_request("DELETE", f"queues/{vhost_encoded}/{queue_encoded}")

    def purge_queue(self, queue: str, vhost: str = "/") -> None:
        """Remove all messages from a queue"""
        validate_rabbitmq_name(queue, "Queue name")
        vhost_encoded = requests.utils.quote(vhost, safe="")
        queue_encoded = requests.utils.quote(queue, safe="")
        self._make_request("DELETE", f"queues/{vhost_encoded}/{queue_encoded}/contents")

    def get_exchange_info(self, exchange: str, vhost: str = "/") -> Dict:
        """Get detailed information about a specific exchange"""
        vhost_encoded = requests.utils.quote(vhost, safe="")
        exchange_encoded = requests.utils.quote(exchange, safe="")
        response = self._make_request("GET", f"exchanges/{vhost_encoded}/{exchange_encoded}")
        return response.json()

    def delete_exchange(self, exchange: str, vhost: str = "/") -> None:
        """Delete an exchange"""
        validate_rabbitmq_name(exchange, "Exchange name")
        vhost_encoded = requests.utils.quote(vhost, safe="")
        exchange_encoded = requests.utils.quote(exchange, safe="")
        self._make_request("DELETE", f"exchanges/{vhost_encoded}/{exchange_encoded}")

    def get_bindings(
        self, queue: Optional[str] = None, exchange: Optional[str] = None, vhost: str = "/"
    ) -> List[Dict]:
        """Get bindings, optionally filtered by queue or exchange"""
        vhost_encoded = requests.utils.quote(vhost, safe="")
        if queue:
            validate_rabbitmq_name(queue, "Queue name")
            queue_encoded = requests.utils.quote(queue, safe="")
            response = self._make_request(
                "GET", f"queues/{vhost_encoded}/{queue_encoded}/bindings"
            )
        elif exchange:
            validate_rabbitmq_name(exchange, "Exchange name")
            exchange_encoded = requests.utils.quote(exchange, safe="")
            response = self._make_request(
                "GET", f"exchanges/{vhost_encoded}/{exchange_encoded}/bindings/source"
            )
        else:
            response = self._make_request("GET", f"bindings/{vhost_encoded}")
        return response.json()

    def get_overview(self) -> Dict:
        """Get overview of RabbitMQ server including version, stats, and listeners"""
        response = self._make_request("GET", "overview")
        return response.json()

    def close(self) -> None:
        """Close the HTTP session to free up resources"""
        self.session.close()

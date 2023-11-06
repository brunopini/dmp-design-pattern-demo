from abc import ABC, abstractmethod


class DataSource(ABC):
    def __init__(self, config: dict) -> None:
        self._state: dict = config
        self.source: callable = callable[...]
        self.endpoint = config['endpoint']
        self.params = config['params']
        self.is_new: bool = False
        self._response: dict = {}

    @property
    @abstractmethod
    def state(self) -> dict:
        self._state = {
            'endpoint': self.endpoint,
            'params': self.params,
            'last_response': {
                'date': self.response.get('date', None),
                'status': self.response.get('status', None),
                'message': self.response.get('message', None)
            }
        }
        return self._state

    @property
    @abstractmethod
    def response(self) -> dict:
        return self._response

    @response.setter
    def response(self, response) -> None:
        self._response = response

    @abstractmethod
    def get_audience_data(self) -> bytes:
        self.response = {'response': 'response'}
        self.is_new = True
        ...

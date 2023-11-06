from datasource._datasource import DataSource
from _utils import Time


class ApiGateway(DataSource):
    def __init__(self, config: dict) -> None:
        self._state: dict = config
        self.endpoint = config['endpoint']
        self.params = config['params']
        self.is_new: bool = False
        self._response: dict = {}

    @property
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
    def response(self) -> dict:
        self.get_audience_data()
        return self._response

    @response.setter
    def response(self, response) -> None:
        self._response = response

    def get_audience_data(self) -> bytes:
        self.response = {
            'date': Time.NOW().strftime('%Y%m%d'),
            'status': 200,
            'message': 'Success: Demo response message.'
        }
        self.is_new = True

import requests

from abc import ABC, abstractmethod
from enum import Enum, unique


class Adtech(ABC):
    def __init__(
            self, name: str, description: str,
            state: dict, member_records: list[dict]) -> None:
        self._state = state
        self.audience_id = self._sstate.get(..., None)
        self.audience_name = name
        self.audience_description = description
        ...
        self._member_records: list[dict] = member_records
        self._status = self.Status.check(
            self.audience_id,
            self.member_records
        )
        self._response: dict = state.get('last_response', {})
        if self._status.value == 0:
            self.payload: dict = self._format_payload()
            self.api = Adtech.API({
                'access_token': 'access_token',
                'advertiser_id': 'advertiser_id'
            })

    @unique
    class Status(Enum):
        NOT_FETCHED = -1
        NOT_POSTED = 0
        POSTED = 1

        @classmethod
        @abstractmethod
        def check(
            cls, audience_id: str, member_records: list[dict]
        ) -> 'Adtech.Status':
            status = (
                cls.POSTED if audience_id
                else cls.NOT_POSTED if len(member_records) > 0
                else cls.NOT_FETCHED
            )
            return status

    @property
    def state(self) -> dict:
        self._state = {
            'name': self.audience_name,
            ...: ...,
            'id': self.id,
            'last_response': {
                'date': self.response.get('date', None),
                'status': self.response.get('status', None),
                'message': self.response.get('message', None)
            }
        }
        return self._state

    @property
    @abstractmethod
    def status(self) -> 'Adtech.Status':
        return self._status

    @status.setter
    @abstractmethod
    def status(self, value) -> None:
        self._status = value

    @property
    @abstractmethod
    def member_records(self) -> list[dict]:
        return self._member_records

    @member_records.setter
    @abstractmethod
    def member_records(self, value) -> None:
        self._member_records = value

    @property
    @abstractmethod
    def response(self) -> dict:
        return self._response

    @response.setter
    @abstractmethod
    def response(self, value) -> None:
        self._response = value

    @abstractmethod
    def _format_payload(self) -> dict:
        payload = {
            "name": self.audience_name,
            "description": self.audience_description,
            ...: ...
        }

        def _inject_members(payload: dict) -> dict:

            data = [
                {
                    "emails": [em for em in dct["email"]],
                    "phoneNumbers": [ph for ph in dct["phone_number"]],
                    "zipCodes": [zp for zp in dct["zip_code"]]
                }
                for dct in self.member_records
            ]
            payload.update({"data": [data]})
            return payload

        def _drop_empty_keys(dct: dict) -> dict:
            new_dict = {}
            for key, value in dct.items():
                if isinstance(value, dict):
                    value = _drop_empty_keys(value)
                elif isinstance(value, list):
                    value = [v for v in value if v]
                    if not value:
                        continue
                if value:
                    new_dict[key] = value
            return new_dict

        payload = _inject_members(payload)
        payload = _drop_empty_keys(payload)
        return payload

    @abstractmethod
    def upload(self) -> None:
        response = self.api.post(self.payload)
        self.response = response
        return response

    class API(ABC):
        _API_VERSION = 'v2'
        _ENDPOINT = ('https://{version}/?advertiserId={advertiserId}')
        _HEADERS = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Authorization": "Bearer {access_token}"
        }

        def __init__(self, credentials: dict = None) -> None:
            _advertiser_id = credentials['advertiser_id']
            self.headers: dict = {
                k: v.format(access_token=credentials['access_token'])
                for k, v in self._HEADERS.items()
            }
            self.endpoint: str = self._ENDPOINT.format(
                version=Adtech.API._API_VERSION,
                advertiserId=str(_advertiser_id)
            )

        @abstractmethod
        def post(self, payload: dict) -> requests.Response:
            response = requests.get(
                self.endpoint,
                headers=self.headers,
                json=payload
            )
            return response

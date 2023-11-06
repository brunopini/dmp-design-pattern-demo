import requests

from adtechs._adtech import Adtech
from _utils import Time

from enum import Enum, unique


class AdtechB(Adtech):
    def __init__(
            self, name: str, description: str,
            state: dict, member_records: list[dict]) -> None:
        self._state = state
        self.audience_id = self._state.get('id', None)
        self.audience_name = name
        self.audience_description = description
        self.expiration_time = self.ExpirationTime(
            state.get('expiration_time'))
        self._member_records: list[dict] = member_records
        self._status = self.Status.check(
            self.audience_id,
            self.member_records
        )
        self._response: dict = state.get('last_response', {})
        if self._status.value == 0:
            self.payload: dict = self._format_payload()
            self.api = AdtechB.API({
                'access_token': 'access_token',
                'advertiser_id': 'advertiser_id'
            })

    @unique
    class Status(Enum):
        NOT_FETCHED = -1
        NOT_POSTED = 0
        POSTED = 1

        @classmethod
        def check(
            cls, audience_id: str, member_records: list[dict]
        ) -> 'AdtechB.Status':
            status = (
                cls.POSTED if audience_id
                else cls.NOT_POSTED if len(member_records) > 0
                else cls.NOT_FETCHED
            )
            return status

    class ExpirationTime:
        VALID_DURATION = set(range(541)) | {1_000}

        def __init__(self, value: int | str = 1000) -> None:
            if int(value) not in self.VALID_DURATION:
                raise ValueError(
                    'Invalid expiration time value. Must be between 0 and'
                    ' 540 days, or exactly 1000 if no expiration date.'
                )
            self.value = int(value)

    @property
    def state(self) -> dict:
        self._state = {
            'name': self.audience_name,
            'id': self.response.get('id', None),
            'expiration_time': self.expiration_time.value,
            'last_response': {
                'date': self.response.get('date', None),
                'status': self.response.get('status', None),
                'message': self.response.get('message', None)
            }
        }
        return self._state

    @property
    def status(self) -> 'AdtechB.Status':
        return self._status

    @status.setter
    def status(self, value) -> None:
        self._status = value

    @property
    def member_records(self) -> list[dict]:
        return self._member_records

    @member_records.setter
    def member_records(self, value) -> None:
        self._member_records = value

    @property
    def response(self) -> dict:
        return self._response

    @response.setter
    def response(self, value) -> None:
        self._response = value

    def _format_payload(self) -> dict:
        payload = {
            "name": self.audience_name,
            "description": self.audience_description,
            "expiration": self.expiration_time.value
        }

        def _inject_members(payload) -> dict:

            schema = [
                "EMAIL",
                "PHONE",
                "ZIP"
            ]

            data = [
                [
                    dct["email"],
                    dct["phone_number"],
                    dct["zip_code"]]
                for dct in self.member_records
            ]

            payload.update({
                "schema": schema,
                "data": data
            })
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

    def upload(self) -> None:
        response = self.api.post(self.payload)
        self.response = response
        return response

    class API(Adtech.API):
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
                version=AdtechB.API._API_VERSION,
                advertiserId=str(_advertiser_id)
            )

        def post(self, payload: dict) -> requests.Response:
            response = {
                'id': '0987654321',
                'date': Time.NOW().strftime('%Y%m%d'),
                'status': 200,
                'message': 'Success: Demo response message.'
            }
            return response

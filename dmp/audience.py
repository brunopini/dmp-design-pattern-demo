import phonenumbers
from pydantic import BaseModel, validator

from adtechs.adtechA import AdtechA
from adtechs.adtechB import AdtechB
from datasource.apigateway import ApiGateway
from _utils import Hash, Objects

import re


class Audience:
    def __init__(self, state: dict, data: bytes | None) -> None:
        self._state: dict = state
        self.name = list(state.keys())[0]
        _state = state.get(self.name)
        self.description = _state.get('description')
        self.source: dict = ApiGateway(_state.get('source'))

        if data is None:
            self.data: bytes = self.source.get_audience_data()
        else:
            self.data: bytes = data

        self.members: list[Audience.Member] | None = (
            Audience.Member.from_bytes(self.data))

        _adtech_args = {
            'name': self.name,
            'description': self.description,
            'member_records': Audience.Member.to_records(self.members)
        }

        self.adtech_a = AdtechA(
            **_adtech_args, state=_state.get('adtechA', None))
        self.adtech_b = AdtechB(
            **_adtech_args, state=_state.get('adtechB', None))

    @property
    def state(self) -> dict:
        self._state = {
            self.name: {
                'description': self.description,
                'source': self.source.state,
                'adtechA': self.adtech_a.state,
                'adtechB': self.adtech_b.state
            }
        }
        return self._state

    class Member(BaseModel):

        email: list
        phone_number: list
        zip_code: list

        class Config:
            arbitrary_types_allowed = True

        def __init__(self, **data):
            super().__init__(**data)

        @classmethod
        def from_bytes(cls, bytes_: bytes | None) -> list['Audience.Member']:
            if bytes_:
                data = Objects.gzip_parquet_to_df(bytes_)
                records = data.to_dict(orient='records')
                return [cls(**dct) for dct in records]
            else:
                return None

        @validator('email', pre=True)
        def strip_lower(value: str | list) -> list:
            def _strip_lower(value: str) -> str:
                if not Hash.is_sha256(value):
                    value = value.strip().lower()
                return value
            if isinstance(value, list):
                value = [
                    _strip_lower(email)
                    for email in value
                ]
            elif isinstance(value, str):
                value = _strip_lower(value)
            else:
                value = None
            return value

        @validator('phone_number', pre=True)
        def format_e164(value: str | list) -> list:
            def _format_e164(value: str | int) -> str:
                if not Hash.is_sha256(value):
                    try:
                        parsed_number = phonenumbers.parse(str(value))
                        if phonenumbers.is_valid_number(parsed_number):
                            value = phonenumbers.format_number(
                                parsed_number,
                                phonenumbers.PhoneNumberFormat.E164
                            )
                        else:
                            value = None
                    except phonenumbers.phonenumberutil.NumberFormatException:
                        value = None
                return value
            if isinstance(value, str | int):
                value = _format_e164(value)
            elif isinstance(value, list):
                value = [
                    _format_e164(phone_number)
                    for phone_number in value
                ]
            else:
                value = None
            return value

        @validator('email', 'phone_number', 'zip_code', pre=True)
        def str_to_hashed_list(value: str | list | int) -> list:
            def _list_to_hashed_list(list: list) -> list:
                return [Hash.sha256(value) for value in list]

            if isinstance(value, int):
                value = str(value)

            if not isinstance(value, list):
                separator = '|'
                if re.search(fr'.*?{separator}.*', value):
                    values = value.split(separator)
                    return _list_to_hashed_list(values)
                else:
                    return [Hash.sha256(value)]
            else:
                return _list_to_hashed_list(value)

        @staticmethod
        def to_records(data: list['Audience.Member'] | None) -> list[dict]:
            if data is None:
                records = []
            else:
                records = [
                    {key: value for key, value in vars(member).items()}
                    for member in data
                ]
            return records

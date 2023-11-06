from audience import Audience
from _utils import Objects

from abc import ABC, abstractmethod
from collections.abc import Generator
import glob
import os


class Catalog(ABC):
    _DATA_DIR = 'data'
    _STATE_DIR = 'state'

    def __init__(self, *args, **kwargs) -> None:
        self.bucket = ...
        audience_names: list[str] = [
            prefix.split('/')[-1] for prefix in self._list_objects(
                prefix=self._STATE_DIR, object_extension='yml')
        ]
        self.audiences: Generator[Audience] = self._fetch_audiences(
            audience_names)

    @abstractmethod
    def push_state(self, audience: Audience) -> None:
        object_name = f'{self._STATE_DIR}/{audience.name}.yml'
        content = Objects.dict_to_yaml_bytes(audience.state)
        self._put_object(object_name, content)

        if audience.source.is_new is True:
            object_name = f'{self._DATA_DIR}/{audience.name}.parquet.gz'
            content = audience.data
            self._put_object(object_name, content)

    @abstractmethod
    def _fetch_audiences(
            self, audience_names: list[str]) -> Generator[Audience]:
        for name in audience_names:
            state = Objects.read_yaml_bytes(
                self._get_object(f'{self._STATE_DIR}/{name}.yml')
            )
            data = self._get_object(f'{self._DATA_DIR}/{name}.parquet.gz')
            yield Audience(state=state, data=data)

    @abstractmethod
    def _get_object(self, object_name) -> bytes:
        ...
        pass

    @abstractmethod
    def _put_object(
            self, object_name, content) -> None:
        ...
        pass

    @abstractmethod
    def _list_objects(
            self, prefix: str, object_extension: str = 'any',
            strip_extension: bool = True) -> list:
        prefix = prefix if prefix.endswith('/') else f'{prefix}/'
        ...
        pass


class Local(Catalog):
    _DATA_DIR = 'data'
    _STATE_DIR = 'state'

    def __init__(self, bucket_path) -> None:
        self.bucket = (
            bucket_path if not bucket_path.endswith('/')
            else bucket_path[:-1]
        )
        audience_names: list[str] = [
            prefix.split('/')[-1] for prefix in self._list_objects(
                prefix=self._STATE_DIR, object_extension='yml')
        ]
        self.audiences: Generator[Audience] = self._fetch_audiences(
            audience_names)

    def push_state(self, audience: Audience) -> None:
        object_name = f'{self._STATE_DIR}/{audience.name}.yml'
        content = Objects.dict_to_yaml_bytes(audience.state)
        self._put_object(object_name, content)

        if audience.source.is_new is True:
            object_name = f'{self._DATA_DIR}/{audience.name}.parquet.gz'
            content = audience.data
            self._put_object(object_name, content)

    def _fetch_audiences(
            self, audience_names: list[str]) -> Generator[Audience]:
        for name in audience_names:
            state = Objects.read_yaml_bytes(
                self._get_object(f'{self._STATE_DIR}/{name}.yml')
            )
            data = self._get_object(f'{self._DATA_DIR}/{name}.parquet.gz')
            yield Audience(state=state, data=data)

    def _get_object(self, object_name) -> bytes | None:
        file_path = os.path.join(self.bucket, object_name)
        try:
            with open(file_path, 'rb') as file:
                return file.read()
        except FileNotFoundError:
            return None

    def _put_object(self, object_name, content: bytes | str) -> None:
        file_path = os.path.join(self.bucket, object_name)
        with open(file_path, 'wb') as file:
            if isinstance(content, str):
                content = content.encode('utf-8')
            file.write(content)

    def _list_objects(
            self, prefix: str, object_extension: str = 'any',
            strip_extension: bool = True) -> list:
        objects = []
        prefix = prefix if prefix.endswith('/') else f'{prefix}/'
        prefix = prefix if prefix.startswith('/') else f'/{prefix}'
        search_pattern = f'{self.bucket}{prefix}*'

        if object_extension != 'any':
            search_pattern += f'.{object_extension}'

        for filepath in glob.glob(search_pattern):
            if (
                object_extension == 'any'
                or filepath.endswith(f'.{object_extension}')
            ):
                filename = os.path.basename(filepath)
                objects.append(
                    filename if strip_extension is False
                    else os.path.splitext(filename)[0]
                )
        return objects

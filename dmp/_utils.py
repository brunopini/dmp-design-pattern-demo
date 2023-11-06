import ruamel.yaml as ryaml
import pandas as pd

from datetime import datetime, timezone
import hashlib
from io import BytesIO
import re


class Objects:

    def read_yaml_bytes(bytes) -> dict:
        yaml_content = bytes.decode('utf-8')
        yaml_parser = ryaml.YAML(typ='safe')
        data = dict(yaml_parser.load(yaml_content))
        return data

    def dict_to_yaml_bytes(data_dict: dict) -> bytes:
        yaml = ryaml.YAML()
        stream = BytesIO()
        yaml.dump(data_dict, stream)
        yaml_bytes = stream.getvalue()
        stream.close()
        return yaml_bytes

    def save_yaml_from_bytes(yaml_bytes: bytes, file_path: str) -> None:
        with open(file_path, 'wb') as file:
            file.write(yaml_bytes)

    def gzip_parquet_to_df(bytes: bytes) -> pd.DataFrame:
        data = BytesIO(bytes)
        return pd.read_parquet(data)


class Hash:

    def sha256(str: str) -> str:
        if not Hash.is_sha256(str):
            return Hash._sha256(str)
        else:
            return str

    def is_sha256(str: str) -> bool:
        return bool(re.match(r'^[A-Fa-f0-9]{64}$', str))

    def _sha256(str: str) -> str:
        hash = hashlib.sha256()
        hash.update(str.encode('utf-8'))
        return hash.hexdigest()


class Time:

    def NOW():
        return datetime.utcnow()

    def str_to_dtime(str: str) -> datetime:
        return datetime.strptime(str, '%Y-%m-%d %H:%M:%S')

    def str_to_utc_timestamp(str: str) -> datetime.timestamp:
        return Time.utc_timestamp(Time.str_to_dtime(str))

    def utc_timestamp(
        utc_datetime: datetime | None = None
    ) -> datetime.timestamp:
        if utc_datetime is None:
            utc_datetime = Time.NOW()
        return int(utc_datetime.replace(tzinfo=timezone.utc).timestamp())

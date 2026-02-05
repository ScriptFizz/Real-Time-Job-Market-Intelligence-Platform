from typing import Any
import json

class RawStorage:
    def save(self, data: Any | None = None, filename: str | None = None) -> None:
        raise NotImplementedError


class LocalStorage(RawStorage):
    def save(self, data: Any | None = None, filename: str | None = None) -> None:
        with open(filename, "w") as f:
            json.dump(data, f)

class GCSStorage(RawStorage):
    def save(self, data: Any | None = None, filename: str | None = None) -> None:
        raise NotImplementedError


def get_storage(config) -> RawStorage:
    if config["storage"]["type"] == "local":
        return LocalStorage()
    else:
        return GCSStorage()

from __future__ import annotations

from collections.abc import Iterator, MutableMapping
from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class RequestSession(MutableMapping[str, Any]):
    data: dict[str, Any] = field(default_factory=dict)
    session_id: str | None = None
    created_at: float | None = None
    expires_at: float | None = None
    prospective_expires_at: float | None = None
    accessed: bool = False
    modified: bool = False
    cleared: bool = False
    invalid_cookie: bool = False
    _retired_data: dict[str, Any] = field(default_factory=dict, repr=False)

    def __getitem__(self, key: str) -> Any:
        return self.data[key]

    def __setitem__(self, key: str, value: Any) -> None:
        self.data[key] = value
        self.mark_modified()
        self.cleared = False

    def __delitem__(self, key: str) -> None:
        value = self.data[key]
        del self.data[key]
        self._retired_data.setdefault(key, value)
        self.mark_modified()

    def __iter__(self) -> Iterator[str]:
        return iter(self.data)

    def __len__(self) -> int:
        return len(self.data)

    def mark_accessed(self) -> None:
        self.accessed = True

    def mark_modified(self) -> None:
        self.accessed = True
        self.modified = True

    def clear(self) -> None:
        if self.data or self.session_id is not None:
            self.mark_modified()
        self._retired_data.update(self.data)
        self.data.clear()
        self.cleared = True

    def pop(self, key: str, *args: Any) -> Any:
        if key not in self.data:
            return self.data.pop(key, *args)
        value = self.data.pop(key)
        self._retired_data.setdefault(key, value)
        self.mark_modified()
        return value

    def popitem(self) -> tuple[str, Any]:
        item = self.data.popitem()
        self._retired_data.setdefault(item[0], item[1])
        self.mark_modified()
        return item

    def setdefault(self, key: str, default: Any = None) -> Any:
        if key not in self.data:
            self.mark_modified()
            self.cleared = False
        return self.data.setdefault(key, default)

    def update(self, *args: Any, **kwargs: Any) -> None:
        values = dict(*args, **kwargs)
        if values:
            self.data.update(values)
            self.mark_modified()
            self.cleared = False

    def cleanup_data(self) -> dict[str, Any]:
        return {**self._retired_data, **self.data}


def request_session_from_scope(scope: MutableMapping[str, Any]) -> RequestSession:
    session = scope.get("session")
    if not isinstance(session, RequestSession):
        raise TypeError("Wybra request session is not available.")
    return session


__all__ = ("RequestSession", "request_session_from_scope")

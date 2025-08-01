from typing import Any

from pydantic import BaseModel, Field

from soy_cli import logging

logger = logging.getLogger(__name__)


class InputModel(BaseModel):
    """Represents an input for an asset."""

    name: str
    _data: Any = None
    strategy: str = "spark"
    api: str = None
    args: dict = {}
    columns: list[str] = Field(default_factory=list)


class OutputModel(BaseModel):
    """Represents an output for an asset."""

    name: str
    _data: Any = None
    strategy: str = "spark"
    api: str = None
    args: dict = {}
    columns: list[str] = Field(default_factory=list)

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict

from util import get_logger

LOGGER = get_logger("MODEL_LOADER")

@dataclass
class ModelConfig:
    """
    Auto-persistent config.

    Usage:
        cfg = ModelConfig(target_freq="15T", hold_period="4H")
    """

    # 모든 설정값을 kwargs로 받는다
    data: Dict[str, Any] = field(default_factory=dict)
    path: Path = field(init=False)

    def __init__(self, **kwargs: Any):
        object.__setattr__(self, "path",
            Path(__file__).resolve().parent / "model_config.json"
        )

        # 1) 기존 파일 있으면 로드
        if self.path.exists():
            with self.path.open("r", encoding="utf-8") as f:
                loaded = json.load(f)
            if not isinstance(loaded, dict):
                raise ValueError("model_config.json must be a JSON object")
            self.data = loaded
        else:
            self.data = {}

        self.data.update(kwargs)
        self._save()

        LOGGER.info(f"MODEL_CONFIG: {kwargs} ")

    def _save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.path.open("w", encoding="utf-8") as f:
            json.dump(self.data, f, ensure_ascii=False, indent=2, sort_keys=True)

    def get(self, key: str, default: Any = None) -> Any:
        return self.data.get(key, default)

    def __getitem__(self, key: str) -> Any:
        return self.data[key]

    def __repr__(self) -> str:
        return f"ModelConfig({self.data})"


@dataclass
class BaseModelContainer(ABC):
    config: ModelConfig

    @abstractmethod
    def get_model(self) -> Any:
        raise NotImplementedError


if __name__ == "__main__":
    cfg = ModelConfig(
        target_freq="2",
        hold_period="4H",
    )

    class MyModelContainer(BaseModelContainer):
        def __init__(self):
            super().__init__(config=cfg)

        def get_model(self) -> Any:
            return True

    container = MyModelContainer()
    model = container.get_model()

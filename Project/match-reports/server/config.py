from __future__ import annotations
import os, json
from dataclasses import dataclass
from dotenv import load_dotenv
load_dotenv()

@dataclass(frozen=True)
class AppConfig:
    urls: dict
    secrets: dict

def _load(path, default):
    if path and os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    return default

def load_config() -> AppConfig:
    urls = _load(os.getenv("CONFIG_JSON"), {})
    secrets = _load(os.getenv("SECRETS_JSON"), {})
    return AppConfig(urls=urls, secrets=secrets)

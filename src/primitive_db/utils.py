import json
from typing import Any, Dict


def load_metadata(filepath: str) -> Dict[str, Any]:
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        return {}


def save_metadata(filepath: str, data: Dict[str, Any]) -> None:
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def _data_dir() -> str:
    import os
    path = os.path.join(os.getcwd(), "data")
    os.makedirs(path, exist_ok=True)
    return path


def load_table_data(table_name: str) -> Dict[str, Any]:
    import os
    path = os.path.join(_data_dir(), f"{table_name}.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"rows": []}
    except json.JSONDecodeError:
        return {"rows": []}


def save_table_data(table_name: str, data: Dict[str, Any]) -> None:
    import os
    path = os.path.join(_data_dir(), f"{table_name}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

from typing import Dict, List, Tuple
from .decorators import handle_db_errors, confirm_action, log_time, create_cacher

SUPPORTED_TYPES = {"int", "str", "bool"}


def _parse_columns(raw_columns: List[str]) -> List[Tuple[str, str]]:
    parsed: List[Tuple[str, str]] = []
    for item in raw_columns:
        if ":" not in item:
            raise ValueError(
                f"Некорректное значение: {item}. Попробуйте снова.")
        name, type_name = item.split(":", 1)
        name = name.strip()
        type_name = type_name.strip()
        if not name or not type_name:
            raise ValueError(
                f"Некорректное значение: {item}. Попробуйте снова.")
        if type_name not in SUPPORTED_TYPES:
            raise ValueError(
                f"Некорректное значение: {type_name}. Попробуйте снова.")
        parsed.append((name, type_name))
    return parsed


@handle_db_errors
def create_table(metadata: Dict, table_name: str, columns: List[str]) -> Dict:
    metadata = metadata or {"tables": {}}
    if not table_name:
        raise ValueError(
            "Некорректное значение: имя таблицы. Попробуйте снова.")
    db = metadata.setdefault("tables", {})
    if table_name in db:
        raise RuntimeError(f"Ошибка: Таблица \"{table_name}\" уже существует.")

    user_columns = _parse_columns(columns) if columns else []

    final_columns: List[Tuple[str, str]] = [("ID", "int")] + user_columns
    db[table_name] = {
        "name": table_name,
        "columns": [{"name": n, "type": t} for n, t in final_columns],
    }
    return metadata


@confirm_action("удаление таблицы")
@handle_db_errors
def drop_table(metadata: Dict, table_name: str) -> Dict:
    db = metadata.setdefault("tables", {})
    if table_name not in db:
        raise RuntimeError(f"Ошибка: Таблица \"{table_name}\" не существует.")
    del db[table_name]
    return metadata


def list_tables(metadata: Dict) -> List[str]:
    db = metadata.setdefault("tables", {})
    return sorted(db.keys())


def _schema_for(metadata: Dict, table_name: str) -> List[Tuple[str, str]]:
    db = metadata.setdefault("tables", {})
    if table_name not in db:
        raise RuntimeError(f"Ошибка: Таблица \"{table_name}\" не существует.")
    return [(c["name"], c["type"]) for c in db[table_name]["columns"]]


def _convert_value(type_name: str, value: str):
    if type_name == "int":
        try:
            return int(value)
        except ValueError:
            raise ValueError(
                f"Некорректное значение: {value}. Попробуйте снова.")
    if type_name == "bool":

        v = value.strip().lower()
        if v in ("true", "1"):
            return True
        if v in ("false", "0"):
            return False
        raise ValueError(f"Некорректное значение: {value}. Попробуйте снова.")
    if type_name == "str":
        return value
    raise ValueError(f"Некорректное значение: {type_name}. Попробуйте снова.")


@log_time
@handle_db_errors
def insert(metadata: Dict, table_name: str, values: List[str], table_data: Dict) -> Dict:
    metadata = metadata or {"tables": {}}
    table_data = table_data or {"rows": []}
    schema = _schema_for(metadata, table_name)

    non_id_schema = [s for s in schema if s[0] != "ID"]
    if len(values) != len(non_id_schema):
        raise ValueError(
            "Некорректное значение: количество значений. Попробуйте снова.")
    converted = {}
    for (col, t), raw in zip(non_id_schema, values):
        converted[col] = _convert_value(t, raw)
    rows = table_data.setdefault("rows", [])
    new_id = (max([r.get("ID", 0) for r in rows]) + 1) if rows else 1
    row = {"ID": new_id}
    row.update(converted)

    rows.append(row)
    return table_data


_select_cache = create_cacher()


@log_time
@handle_db_errors
def select(table_data: Dict, where_clause: Dict = None) -> List[Dict]:
    table_data = table_data or {"rows": []}
    rows = table_data.setdefault("rows", [])
    if not where_clause:
        return rows
    key, value = next(iter(where_clause.items()))
    cache_key = (key, value, len(rows))

    def compute():
        return [r for r in rows if r.get(key) == value]
    return _select_cache(cache_key, compute)


@handle_db_errors
def update(table_data: Dict, set_clause: Dict, where_clause: Dict) -> Dict:
    table_data = table_data or {"rows": []}
    rows = table_data.setdefault("rows", [])
    key, value = next(iter(where_clause.items()))
    updated_any = False
    for r in rows:
        if r.get(key) == value:
            for sk, sv in set_clause.items():
                r[sk] = sv
            updated_any = True
    if not updated_any:

        pass
    return table_data


@confirm_action("удаление записей")
@handle_db_errors
def delete(table_data: Dict, where_clause: Dict) -> Dict:
    table_data = table_data or {"rows": []}
    rows = table_data.setdefault("rows", [])
    key, value = next(iter(where_clause.items()))
    table_data["rows"] = [r for r in rows if r.get(key) != value]
    return table_data


@handle_db_errors
def table_info(metadata: Dict, table_name: str, table_data: Dict) -> Dict:
    metadata = metadata or {"tables": {}}
    table_data = table_data or {"rows": []}
    schema = _schema_for(metadata, table_name)
    return {
        "name": table_name,
        "columns": ", ".join(f"{n}:{t}" for n, t in schema),
        "count": len(table_data.get("rows", [])),
    }

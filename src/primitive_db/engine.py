import os
import shlex
from typing import List

from .utils import load_metadata, save_metadata, load_table_data, save_table_data
from .core import (
    create_table,
    drop_table,
    list_tables,
    insert,
    select,
    update as update_rows,
    delete as delete_rows,
    table_info,
    _schema_for,
    _convert_value,
)
from prettytable import PrettyTable


def print_help():
    """Prints the help message for the current mode."""

    print("\n***Операции с данными***")
    print("Функции:")
    print("<command> insert into <имя_таблицы> values (<значение1>, <значение2>, ...) - создать запись.")
    print("<command> select from <имя_таблицы> where <столбец> = <значение> - прочитать записи по условию.")
    print("<command> select from <имя_таблицы> - прочитать все записи.")
    print("<command> update <имя_таблицы> set <столбец1> = <новое_значение1> where <столбец_условия> = <значение_условия> - обновить запись.")
    print("<command> delete from <имя_таблицы> where <столбец> = <значение> - удалить запись.")
    print("<command> info <имя_таблицы> - вывести информацию о таблице.")

    print("\nОбщие команды:")
    print("<command> exit - выход из программы")
    print("<command> help- справочная информация\n")


def _parse_eq(expr: str):

    if "=" not in expr:
        raise ValueError(f"Некорректное значение: {expr}. Попробуйте снова.")
    k, v = expr.split("=", 1)
    k = k.strip()
    v = v.strip()

    if (v.startswith('"') and v.endswith('"')) or (v.startswith("'") and v.endswith("'")):
        v2 = v[1:-1]
        return {k: v2}
    try:
        return {k: int(v)}
    except ValueError:

        vl = v.lower()
        if vl in ("true", "false"):
            return {k: True if vl == "true" else False}

        return {k: v}


def _meta_path() -> str:

    return os.path.join(os.getcwd(), "db_meta.json")


def _handle_create(args: List[str], metadata_path: str):
    if len(args) < 2:
        print("Некорректное значение: недостаточно аргументов. Попробуйте снова.")
        return
    table_name = args[1]
    columns = args[2:]
    metadata = load_metadata(metadata_path)
    try:
        updated = create_table(metadata, table_name, columns)
        if not updated:
            return
        save_metadata(metadata_path, updated)
        cols_fmt = ", ".join(
            f"{c['name']}:{c['type']}" for c in updated["tables"][table_name]["columns"])
        print(
            f"Таблица \"{table_name}\" успешно создана со столбцами: {cols_fmt}")
    except (ValueError, RuntimeError) as e:
        print(str(e))


def _handle_drop(args: List[str], metadata_path: str):
    if len(args) != 2:
        print("Некорректное значение: имя таблицы. Попробуйте снова.")
        return
    table_name = args[1]
    metadata = load_metadata(metadata_path)
    try:
        updated = drop_table(metadata, table_name)
        if not updated:
            return
        save_metadata(metadata_path, updated)
        print(f"Таблица \"{table_name}\" успешно удалена.")
    except RuntimeError as e:
        print(str(e))


def _handle_list(metadata_path: str):
    metadata = load_metadata(metadata_path)
    names = list_tables(metadata)
    if not names:
        print("(нет таблиц)")
    else:
        for n in names:
            print(f"- {n}")


def run():
    meta = _meta_path()
    print(">>> database")
    print_help()
    while True:
        try:
            user_input = input(">>>Введите команду: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nВыход.")
            break

        if not user_input:
            continue

        try:
            args = shlex.split(user_input)
        except ValueError:
            print("Некорректное значение: парсинг ввода. Попробуйте снова.")
            continue

        cmd = args[0]
        if cmd == "exit":
            print("Выход.")
            break
        elif cmd == "help":
            print_help()
        elif cmd == "create_table":
            _handle_create(args, meta)
        elif cmd == "drop_table":
            _handle_drop(args, meta)
        elif cmd == "list_tables":
            _handle_list(meta)
        elif cmd == "insert" and len(args) >= 5 and args[1] == "into":
            table = args[2]
            if args[3] != "values":
                print("Некорректное значение: синтаксис insert. Попробуйте снова.")
                continue

            remainder = user_input[user_input.index(
                "values") + len("values"):].strip()
            if remainder.startswith("(") and remainder.endswith(")"):
                remainder = remainder[1:-1]
            try:
                parts = shlex.split(remainder)
            except ValueError:
                print("Некорректное значение: парсинг ввода. Попробуйте снова.")
                continue

            cleaned = [p.rstrip(",") for p in parts]
            md = load_metadata(meta)
            data = load_table_data(table)
            try:
                updated = insert(md, table, cleaned, data)
                if not updated:
                    return
                save_table_data(table, updated)
                new_id = updated["rows"][-1]["ID"]
                print(
                    f"Запись с ID={new_id} успешно добавлена в таблицу \"{table}\".")
            except (ValueError, RuntimeError) as e:
                print(str(e))
        elif cmd == "select" and len(args) >= 3 and args[1] == "from":
            table = args[2]
            md = load_metadata(meta)
            try:
                _ = _schema_for(md, table)
            except RuntimeError as e:
                print(str(e))
                continue
            data = load_table_data(table)
            where = None
            if len(args) >= 5 and args[3] == "where":
                expr = " ".join(args[4:])
                try:
                    where = _parse_eq(expr)
                except ValueError as e:
                    print(str(e))
                    continue
            rows = select(data, where)

            t = PrettyTable()

            cols = [c[0] for c in _schema_for(md, table)]
            t.field_names = cols
            for r in rows:
                t.add_row([r.get(c) for c in cols])
            print(t)
        elif cmd == "update" and len(args) >= 6 and args[2] == "set":
            table = args[1]

            try:
                if " where " not in user_input:
                    raise ValueError(
                        "Некорректное значение: синтаксис update. Попробуйте снова.")
                set_part = user_input.split(
                    " set ", 1)[1].split(" where ", 1)[0].strip()
                where_part = user_input.split(" where ", 1)[1].strip()
                set_clause = _parse_eq(set_part)
                where_clause = _parse_eq(where_part)
            except ValueError as e:
                print(str(e))
                continue
            md = load_metadata(meta)
            try:
                _ = _schema_for(md, table)
            except RuntimeError as e:
                print(str(e))
                continue
            data = load_table_data(table)

            schema = {n: t for n, t in _schema_for(md, table)}
            for k, v in list(set_clause.items()):
                set_clause[k] = _convert_value(schema.get(k, "str"), str(
                    v)) if not isinstance(v, (bool, int)) else v
            for k, v in list(where_clause.items()):
                where_clause[k] = _convert_value(schema.get(k, "str"), str(
                    v)) if not isinstance(v, (bool, int)) else v
            updated = update_rows(data, set_clause, where_clause)
            if not updated:
                return
            save_table_data(table, updated)

            key, value = next(iter(where_clause.items()))
            ids = [r.get("ID") for r in updated.get(
                "rows", []) if r.get(key) == value]
            if not ids:
                print("Обновление выполнено: подходящих записей не найдено.")
            elif len(ids) == 1:
                print(
                    f"Запись с ID={ids[0]} в таблице \"{table}\" успешно обновлена.")
            else:
                joined = ", ".join(str(i) for i in ids)
                print(
                    f"Записи с ID={joined} в таблице \"{table}\" успешно обновлены.")
        elif cmd == "delete" and len(args) >= 5 and args[1] == "from" and args[3] == "where":
            table = args[2]
            expr = " ".join(args[4:])
            try:
                where_clause = _parse_eq(expr)
            except ValueError as e:
                print(str(e))
                continue
            md = load_metadata(meta)
            try:
                _ = _schema_for(md, table)
            except RuntimeError as e:
                print(str(e))
                continue
            data = load_table_data(table)
            updated = delete_rows(data, where_clause)
            if not updated:
                return
            save_table_data(table, updated)
            if "ID" in where_clause:
                print(
                    f"Запись с ID={where_clause['ID']} успешно удалена из таблицы \"{table}\".")
            else:
                print("Удаление выполнено.")
        elif cmd == "info" and len(args) == 2:
            table = args[1]
            md = load_metadata(meta)
            data = load_table_data(table)
            try:
                info = table_info(md, table, data)
                if not info:
                    continue
            except RuntimeError as e:
                print(str(e))
                continue
            print(f"Таблица: {info['name']}")
            print(f"Столбцы: {info['columns']}")
            print(f"Количество записей: {info['count']}")
        else:
            print(f"Функции {cmd} нет. Попробуйте снова.")

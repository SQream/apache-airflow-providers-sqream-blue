from __future__ import annotations
from contextlib import closing, contextmanager
from functools import wraps
from typing import Any, Callable, Iterable, Mapping

from pysqream_blue import connect, utils

from airflow.providers.common.sql.hooks.sql import DbApiHook, return_single_query_results


def _ensure_prefixes(conn_type):
    """
    Remove when provider min airflow version >= 2.5.0 since this is handled by
    provider manager from that version.
    """

    def dec(func):
        @wraps(func)
        def inner():
            field_behaviors = func()
            conn_attrs = {"host", "login", "password", "port", "extra"}

            def _ensure_prefix(field):
                if field not in conn_attrs and not field.startswith("extra__"):
                    return f"extra__{conn_type}__{field}"
                else:
                    return field

            if "placeholders" in field_behaviors:
                placeholders = field_behaviors["placeholders"]
                field_behaviors["placeholders"] = {_ensure_prefix(k): v for k, v in placeholders.items()}
            return field_behaviors

        return inner

    return dec


class SQreamBlueHook(DbApiHook):
    conn_name_attr = "sqream_blue_conn_id"
    default_conn_name = "sqream_blue_default"
    conn_type = "sqream_blue"
    hook_name = "sqream_blue"
    _test_connection_sql = "select 1"

    @staticmethod
    def get_connection_form_widgets() -> dict[str, Any]:
        """Returns connection widgets to add to connection form"""
        from flask_appbuilder.fieldwidgets import BS3TextFieldWidget
        from flask_babel import lazy_gettext
        from wtforms import StringField

        return {
            "database": StringField(lazy_gettext("Database"), widget=BS3TextFieldWidget())
        }

    @staticmethod
    @_ensure_prefixes(conn_type="sqream_blue")
    def get_ui_field_behaviour() -> dict[str, Any]:
        """Returns custom field behaviour"""

        return {
            "hidden_fields": ["port", "schema", "extra"],
            "relabeling": {},
            "placeholders": {
                "host": "enter host domain to connect to SQream",
                "login": "enter username to connect to SQream",
                "password": "enter password to connect to SQream",
                "database": "enter db name to connect to SQream",
            },
        }

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.database = kwargs.pop("database", None)
        self.query_ids: list[str] = []

    def _get_field(self, extra_dict, field_name):
        backcompat_prefix = "extra__sqream_blue__"
        backcompat_key = f"{backcompat_prefix}{field_name}"
        if field_name.startswith("extra__"):
            raise ValueError(
                f"Got prefixed name {field_name}; please remove the '{backcompat_prefix}' prefix "
                f"when using this method."
            )
        if field_name in extra_dict:
            import warnings

            if backcompat_key in extra_dict:
                warnings.warn(
                    f"Conflicting params `{field_name}` and `{backcompat_key}` found in extras. "
                    f"Using value for `{field_name}`.  Please ensure this is the correct "
                    f"value and remove the backcompat key `{backcompat_key}`."
                )
            return extra_dict[field_name] or None
        return extra_dict.get(backcompat_key) or None

    def _get_conn_params(self) -> dict[str, str | None]:
        """
        One method to fetch connection params as a dict
        used in get_uri() and get_connection()
        """
        conn = self.get_connection(self.sqream_blue_conn_id)  # type: ignore[attr-defined]
        extra_dict = conn.extra_dejson
        database = self._get_field(extra_dict, "database") or "master"
        conn_config = {
            "host": conn.host,
            "username": conn.login,
            "password": conn.password,
            "database": self.database or database
        }
        return conn_config

    def get_conn(self):
        """Returns a sqream_blue.connection object"""
        conn_config = self._get_conn_params()
        conn = connect(**conn_config)
        return conn

    def run(
        self,
        sql: str | Iterable[str],
        autocommit: bool = False,
        parameters: Iterable | Mapping | None = None,
        handler: Callable | None = None,
        split_statements: bool = True,
        return_last: bool = True,
        return_dictionaries: bool = False,
    ) -> Any | list[Any] | None:
        """
        Runs a command or a list of commands. Pass a list of sql
        statements to the sql parameter to get them to execute
        sequentially. The variable execution_info is returned so that
        it can be used in the Operators to modify the behavior
        depending on the result of the query (i.e fail the operator
        if the copy has processed 0 files)
        :param sql: the sql string to be executed with possibly multiple statements,
          or a list of sql statements to execute
        :param autocommit: What to set the connection's autocommit setting to
            before executing the query.
        :param parameters: The parameters to render the SQL query with.
        :param handler: The result handler which is called with the result of each statement.
        :param split_statements: Whether to split a single SQL string into statements and run separately
        :param return_last: Whether to return result for only last statement or for all after split
        :param return_dictionaries: Whether to return dictionaries rather than regular DBApi sequences
            as rows in the result. The dictionaries are of form:
            ``{ 'column1_name': value1, 'column2_name': value2 ... }``.
        :return: return only result of the LAST SQL expression if handler was provided.
        """
        self.query_ids = []

        if isinstance(sql, str):
            if split_statements:
                sql_list = self.split_sql_string(sql)
            else:
                sql_list = [self.strip_sql_string(sql)]
        else:
            sql_list = sql

        if sql_list:
            self.log.info("Executing following statements against Sqream blue DB: %s", sql_list)
        else:
            raise ValueError("List of SQL statements is empty")

        with closing(self.get_conn()) as conn:
            results = []
            for sql_statement in sql_list:
                with self._get_cursor(conn) as cur:
                    self._run_command(cur, sql_statement, parameters)

                    if handler is not None and cur.query_type == 1:
                        result = handler(cur)
                        if return_single_query_results(sql, return_last, split_statements):
                            _last_result = result
                            _last_description = cur.description
                        else:
                            results.append(result)
                            self.descriptions.append(cur.description)

                    query_id = cur.get_statement_id()
                    self.log.info("Rows affected: %s", cur.rowcount)
                    self.log.info("Sqream blue query id: %s", query_id)
                    self.query_ids.append(query_id)

        if handler is None:
            return None
        if return_single_query_results(sql, return_last, split_statements):
            self.descriptions = [_last_description]
            return _last_result
        else:
            return results

    @contextmanager
    def _get_cursor(self, conn: Any):
        cursor = None
        try:
            cursor = conn.cursor()
            yield cursor
        finally:
            if cursor is not None:
                cursor.close()
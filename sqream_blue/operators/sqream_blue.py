from __future__ import annotations
import warnings
from typing import Any, Iterable, Mapping, Sequence, SupportsAbs
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

class SQreamBlueSqlOperator(SQLExecuteQueryOperator):
    template_fields: Sequence[str] = ("sql",)
    template_ext: Sequence[str] = (".sql",)
    template_fields_renderers = {"sql": "sql"}
    ui_color = "#ededed"

    def __init__(
            self,
            *,
            sqream_blue_conn_id: str = "sqream_blue_default",
            warehouse: str | None = None,
            database: str | None = None,
            role: str | None = None,
            # schema: str | None = None,
            authenticator: str | None = None,
            session_parameters: dict | None = None,
            **kwargs,
    ) -> None:
        if any([warehouse, database, role, authenticator, session_parameters]):
            hook_params = kwargs.pop("hook_params", {})
            kwargs["hook_params"] = {
                "warehouse": warehouse,
                "database": database,
                "role": role,
                # "schema": schema,
                "authenticator": authenticator,
                "session_parameters": session_parameters,
                **hook_params,
            }
        super().__init__(conn_id=sqream_blue_conn_id, **kwargs)

    def _process_output(self, results: list[Any], descriptions: list[Sequence[Sequence] | None]) -> list[Any]:
        validated_descriptions: list[Sequence[Sequence]] = []
        for idx, description in enumerate(descriptions):
            if not description:
                raise RuntimeError(
                    f"The query did not return descriptions of the cursor for query number {idx}. "
                    "Cannot return values in a form of dictionary for that query."
                )
            validated_descriptions.append(description)
        returned_results = []
        for result_id, result_list in enumerate(results):
            current_processed_result = []
            for row in result_list:
                dict_result: dict[Any, Any] = {}
                for idx, description in enumerate(validated_descriptions[result_id]):
                    dict_result[description[0]] = row[idx]
                current_processed_result.append(dict_result)
            returned_results.append(current_processed_result)
        return returned_results
from __future__ import annotations
from typing import Any, Sequence
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
            database: str | None = None,
            **kwargs,
    ) -> None:
        if any([database]):
            hook_params = kwargs.pop("hook_params", {})
            kwargs["hook_params"] = {
                "database": database,
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
        self.log.info("All results %s", returned_results)
        return returned_results
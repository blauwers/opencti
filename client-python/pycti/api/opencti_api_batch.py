import base64
import json
import re
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

BATCH_RESULT_TOKEN_PREFIX = "__opencti_batch_result__"


def build_batch_result_token(operation_index: int, path: List[str]) -> str:
    return f"{BATCH_RESULT_TOKEN_PREFIX}:{operation_index}:{'.'.join(path)}"


class BatchMutationPlanUnsupported(Exception):
    pass


class BatchMutationPlanTooLarge(Exception):
    def __init__(self, actual_size: int, max_size: int):
        self.actual_size = actual_size
        self.max_size = max_size
        super().__init__(
            f"Batch mutation request payload exceeds configured limit "
            f"({actual_size} > {max_size} bytes)"
        )


@dataclass
class BatchMutationPlan:
    operations: List[Dict[str, Any]] = field(default_factory=list)
    max_serialized_operations_size: Optional[int] = None
    _next_execution_group: int = field(default=0, init=False, repr=False)
    _active_execution_group: Optional[int] = field(default=None, init=False, repr=False)
    _active_execution_phase: Optional[int] = field(default=None, init=False, repr=False)
    _active_object_id: Optional[str] = field(default=None, init=False, repr=False)
    _serialized_operations_size: int = field(default=2, init=False, repr=False)

    @staticmethod
    def is_mutation(query: str) -> bool:
        return re.search(r"\bmutation\b", query) is not None

    def capture(
        self,
        query: str,
        variables: Dict[str, Any],
        files_vars: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        operation_index = len(self.operations)
        operation: Dict[str, Any] = {
            "query": query,
            "variables": json.dumps(variables),
        }
        if self._active_execution_group is not None:
            operation["execution_group"] = self._active_execution_group
            operation["execution_phase"] = self._active_execution_phase
            if self._active_object_id is not None:
                operation["object_id"] = self._active_object_id
        files = self._serialize_files(files_vars)
        if files:
            operation["files"] = files
        serialized_operation_size = len(json.dumps(operation).encode("utf-8"))
        next_serialized_operations_size = (
            self._serialized_operations_size
            + (1 if len(self.operations) > 0 else 0)
            + serialized_operation_size
        )
        if (
            isinstance(self.max_serialized_operations_size, int)
            and not isinstance(self.max_serialized_operations_size, bool)
            and self.max_serialized_operations_size > 0
            and next_serialized_operations_size > self.max_serialized_operations_size
        ):
            raise BatchMutationPlanTooLarge(
                next_serialized_operations_size, self.max_serialized_operations_size
            )
        self._serialized_operations_size = next_serialized_operations_size
        self.operations.append(operation)
        return {"data": self._build_synthetic_data(query, operation_index)}

    @contextmanager
    def execution_group(self, execution_phase: int, object_id: Optional[str] = None):
        """Tag captured mutations as one serial group within a dependency phase."""
        if not isinstance(execution_phase, int) or execution_phase < 0:
            raise ValueError("Batch execution phase must be a non-negative integer")
        if object_id is not None and (
            not isinstance(object_id, str) or len(object_id) == 0
        ):
            raise ValueError("Batch execution object id must be a non-empty string")
        if self._active_execution_group is not None:
            raise ValueError("A batch execution group is already active")
        self._active_execution_group = self._next_execution_group
        self._active_execution_phase = execution_phase
        self._active_object_id = object_id
        self._next_execution_group += 1
        try:
            yield
        finally:
            self._active_execution_group = None
            self._active_execution_phase = None
            self._active_object_id = None

    @staticmethod
    def _serialize_files(files_vars: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        serialized_files = []
        for file_var in files_vars:
            key = file_var["key"]
            files = file_var["file"] if file_var["multiple"] else [file_var["file"]]
            for index, file in enumerate(files):
                data = (
                    file.data.encode("utf-8")
                    if isinstance(file.data, str)
                    else file.data
                )
                serialized_files.append(
                    {
                        "path": f"{key}.{index}" if file_var["multiple"] else key,
                        "name": file.name,
                        "mime_type": file.mime,
                        "data": base64.b64encode(data).decode("utf-8"),
                    }
                )
        return serialized_files

    @classmethod
    def _build_synthetic_data(cls, query: str, operation_index: int) -> Dict[str, Any]:
        tokens = cls._tokenize(query)
        try:
            selection_index = tokens.index("{")
        except ValueError as exc:
            raise BatchMutationPlanUnsupported(
                "Mutation query has no selection set"
            ) from exc
        selection, _ = cls._parse_selection_set(tokens, selection_index)
        return {
            response_key: cls._build_synthetic_value(
                field_name, children, operation_index, [response_key]
            )
            for response_key, (field_name, children) in selection.items()
        }

    @staticmethod
    def _tokenize(query: str) -> List[str]:
        return re.findall(r"\.\.\.|[_A-Za-z][_0-9A-Za-z]*|[{}()@:!$,\[\]]", query)

    @classmethod
    def _parse_selection_set(
        cls, tokens: List[str], index: int
    ) -> Tuple[Dict[str, Tuple[str, Optional[Dict[str, Any]]]], int]:
        if index >= len(tokens) or tokens[index] != "{":
            raise BatchMutationPlanUnsupported(
                "Mutation query has an invalid selection set"
            )
        selection: Dict[str, Tuple[str, Optional[Dict[str, Any]]]] = {}
        index += 1
        while index < len(tokens) and tokens[index] != "}":
            if tokens[index] == "...":
                index += 1
                if index < len(tokens) and tokens[index] == "on":
                    index += 2
                    while index < len(tokens) and tokens[index] == "@":
                        index += 2
                        if index < len(tokens) and tokens[index] == "(":
                            index = cls._skip_balanced(tokens, index, "(", ")")
                    if index < len(tokens) and tokens[index] == "{":
                        fragment_selection, index = cls._parse_selection_set(
                            tokens, index
                        )
                        selection.update(fragment_selection)
                    continue
                if index < len(tokens):
                    index += 1
                continue
            response_key = tokens[index]
            field_name = response_key
            index += 1
            if index < len(tokens) and tokens[index] == ":":
                index += 1
                if index >= len(tokens):
                    raise BatchMutationPlanUnsupported(
                        "Mutation query alias is incomplete"
                    )
                field_name = tokens[index]
                index += 1
            if index < len(tokens) and tokens[index] == "(":
                index = cls._skip_balanced(tokens, index, "(", ")")
            while index < len(tokens) and tokens[index] == "@":
                index += 2
                if index < len(tokens) and tokens[index] == "(":
                    index = cls._skip_balanced(tokens, index, "(", ")")
            children = None
            if index < len(tokens) and tokens[index] == "{":
                children, index = cls._parse_selection_set(tokens, index)
            selection[response_key] = (field_name, children)
        if index >= len(tokens) or tokens[index] != "}":
            raise BatchMutationPlanUnsupported(
                "Mutation query selection set is incomplete"
            )
        return selection, index + 1

    @staticmethod
    def _skip_balanced(
        tokens: List[str], index: int, opening: str, closing: str
    ) -> int:
        depth = 0
        while index < len(tokens):
            if tokens[index] == opening:
                depth += 1
            elif tokens[index] == closing:
                depth -= 1
                if depth == 0:
                    return index + 1
            index += 1
        raise BatchMutationPlanUnsupported("Mutation query has unbalanced delimiters")

    @classmethod
    def _build_synthetic_value(
        cls,
        field_name: str,
        children: Optional[Dict[str, Tuple[str, Optional[Dict[str, Any]]]]],
        operation_index: int,
        path: List[str],
    ) -> Any:
        if children is not None:
            if field_name in {"edges", "nodes"}:
                return []
            return {
                response_key: cls._build_synthetic_value(
                    child_field_name,
                    child_children,
                    operation_index,
                    [*path, response_key],
                )
                for response_key, (child_field_name, child_children) in children.items()
            }
        if field_name in {"parent_types"}:
            return []
        if field_name in {"delete"}:
            return True
        return build_batch_result_token(operation_index, path)

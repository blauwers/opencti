"""Helpers for the versioned enrichment batch connector contract."""

import json
from enum import Enum
from typing import Any, Dict, List


class EnrichmentBatchResultStatus(str, Enum):
    """Supported per-item enrichment batch outcomes."""

    PROCESSED = "PROCESSED"
    UNCHANGED = "UNCHANGED"
    FAILED = "FAILED"
    RETRYABLE = "RETRYABLE"


def _require_non_empty_string(value: Any, field: str) -> str:
    if not isinstance(value, str) or len(value) == 0:
        raise ValueError(f"{field} must be a non-empty string")
    return value


def _normalize_string_list(value: Any, field: str) -> List[str]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f"{field} must be a list")
    return sorted({_require_non_empty_string(item, field) for item in value})


def parse_enrichment_batch_envelope(serialized_envelope: str) -> Dict[str, Any]:
    """Parse the platform envelope enough for connector-side routing.

    The platform remains the source of truth for full contract validation. The
    client still validates the identity fields it relies on so malformed broker
    payloads fail before callback execution or Work settlement.
    """

    if not isinstance(serialized_envelope, str) or len(serialized_envelope) == 0:
        raise ValueError("enrichment_batch must be a non-empty JSON string")
    try:
        envelope = json.loads(serialized_envelope)
    except json.JSONDecodeError as err:
        raise ValueError("enrichment_batch must be valid JSON") from err
    if not isinstance(envelope, dict):
        raise ValueError("enrichment_batch must be a JSON object")
    if envelope.get("protocol_version") != 1:
        raise ValueError("Unsupported enrichment batch protocol version")
    _require_non_empty_string(envelope.get("batch_id"), "batch_id")
    group_context = envelope.get("group_context")
    if not isinstance(group_context, dict):
        raise ValueError("group_context must be a JSON object")
    _require_non_empty_string(group_context.get("connector_id"), "connector_id")
    items = envelope.get("items")
    if not isinstance(items, list) or len(items) == 0:
        raise ValueError("items must be a non-empty list")
    if envelope.get("item_count") != len(items):
        raise ValueError("item_count does not match items")
    seen_item_ids = set()
    seen_work_ids = set()
    for item in items:
        if not isinstance(item, dict):
            raise ValueError("items must contain JSON objects")
        item_id = _require_non_empty_string(item.get("item_id"), "item_id")
        if item_id in seen_item_ids:
            raise ValueError("items must have unique item_id values")
        seen_item_ids.add(item_id)
        work_id = _require_non_empty_string(item.get("work_id"), "work_id")
        if work_id in seen_work_ids:
            raise ValueError("items must have unique work_id values")
        seen_work_ids.add(work_id)
        _require_non_empty_string(item.get("entity_id"), "entity_id")
        _require_non_empty_string(item.get("entity_type"), "entity_type")
    return envelope


def enrichment_batch_work_ids(envelope: Dict[str, Any]) -> List[str]:
    """Return unique logical Work ids in envelope order."""

    work_ids = []
    seen_work_ids = set()
    for item in envelope["items"]:
        work_id = item["work_id"]
        if work_id not in seen_work_ids:
            seen_work_ids.add(work_id)
            work_ids.append(work_id)
    return work_ids


def _parse_output_bundle_object_ids(output_bundle: Any) -> List[str]:
    if output_bundle is None:
        return []
    if not isinstance(output_bundle, str) or len(output_bundle) == 0:
        raise ValueError("output_bundle must be a non-empty JSON string or None")
    try:
        bundle = json.loads(output_bundle)
    except json.JSONDecodeError as err:
        raise ValueError("output_bundle must be valid JSON") from err
    if not isinstance(bundle, dict) or bundle.get("type") != "bundle":
        raise ValueError("output_bundle must be a STIX bundle")
    objects = bundle.get("objects")
    if not isinstance(objects, list):
        raise ValueError("output_bundle.objects must be a list")
    object_ids = []
    for item in objects:
        if not isinstance(item, dict):
            raise ValueError("output_bundle.objects must contain JSON objects")
        object_ids.append(_require_non_empty_string(item.get("id"), "output_object_id"))
    if len(set(object_ids)) != len(object_ids):
        raise ValueError("output_bundle object ids must be unique")
    return sorted(object_ids)


def build_enrichment_batch_result_envelope(
    envelope: Dict[str, Any], callback_result: Dict[str, Any]
) -> Dict[str, Any]:
    """Normalize one connector callback result into the platform result envelope."""

    if not isinstance(callback_result, dict):
        raise ValueError("Batch callback result must be a dict")
    result_items = callback_result.get("results")
    if not isinstance(result_items, list) or len(result_items) != len(
        envelope["items"]
    ):
        raise ValueError("Batch callback results must cover every item exactly once")
    output_bundle = callback_result.get("output_bundle")
    output_object_ids = _parse_output_bundle_object_ids(output_bundle)
    envelope_items = {item["item_id"]: item for item in envelope["items"]}
    seen_item_ids = set()
    normalized_results = []
    owned_object_ids = set()
    for result in result_items:
        if not isinstance(result, dict):
            raise ValueError("Batch callback results must contain dicts")
        item_id = _require_non_empty_string(result.get("item_id"), "item_id")
        work_id = _require_non_empty_string(result.get("work_id"), "work_id")
        envelope_item = envelope_items.get(item_id)
        if (
            envelope_item is None
            or item_id in seen_item_ids
            or envelope_item["work_id"] != work_id
        ):
            raise ValueError(
                "Batch callback result does not match envelope item identity"
            )
        seen_item_ids.add(item_id)
        status = result.get("status")
        if isinstance(status, EnrichmentBatchResultStatus):
            status = status.value
        if status not in {item.value for item in EnrichmentBatchResultStatus}:
            raise ValueError("Invalid batch callback result status")
        message = result.get("message")
        if message is not None and not isinstance(message, str):
            raise ValueError("Batch callback result message must be a string or None")
        item_output_object_ids = _normalize_string_list(
            result.get("output_object_ids"), "output_object_ids"
        )
        if status == EnrichmentBatchResultStatus.PROCESSED.value:
            if len(item_output_object_ids) == 0:
                raise ValueError(
                    "Processed batch callback results need output_object_ids"
                )
        elif len(item_output_object_ids) > 0:
            raise ValueError(
                "Only processed batch callback results may own output objects"
            )
        if status in {
            EnrichmentBatchResultStatus.FAILED.value,
            EnrichmentBatchResultStatus.RETRYABLE.value,
        } and (message is None or len(message) == 0):
            raise ValueError(
                "Failed or retryable batch callback results need a message"
            )
        owned_object_ids.update(item_output_object_ids)
        normalized_results.append(
            {
                "item_id": item_id,
                "work_id": work_id,
                "status": status,
                "message": message,
                "output_object_ids": item_output_object_ids,
            }
        )
    if len(seen_item_ids) != len(envelope_items):
        raise ValueError("Batch callback results must cover every item exactly once")
    has_processed_result = any(
        result["status"] == EnrichmentBatchResultStatus.PROCESSED.value
        for result in normalized_results
    )
    if has_processed_result and output_bundle is None:
        raise ValueError("Processed batch callback results need an output_bundle")
    if not has_processed_result and output_bundle is not None:
        raise ValueError("output_bundle requires a processed batch callback result")
    if owned_object_ids != set(output_object_ids):
        raise ValueError(
            "output_bundle objects must be owned by batch callback results"
        )
    return {
        "protocol_version": 1,
        "batch_id": envelope["batch_id"],
        "result_count": len(normalized_results),
        "output_object_count": len(output_object_ids),
        "output_bundle": output_bundle,
        "results": sorted(normalized_results, key=lambda item: item["item_id"]),
    }


def serialize_enrichment_batch_result_envelope(result_envelope: Dict[str, Any]) -> str:
    """Serialize result envelopes deterministically for stable retries."""

    return json.dumps(result_envelope, sort_keys=True, separators=(",", ":"))


def has_retryable_enrichment_batch_result(result_envelope: Dict[str, Any]) -> bool:
    """Return whether any result requests whole-envelope broker replay."""

    return any(
        result["status"] == EnrichmentBatchResultStatus.RETRYABLE.value
        for result in result_envelope["results"]
    )

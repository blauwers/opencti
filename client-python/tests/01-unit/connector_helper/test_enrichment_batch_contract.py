import json

import pytest

from pycti.connector.opencti_enrichment_batch import (
    EnrichmentBatchResultStatus,
    build_enrichment_batch_result_envelope,
    enrichment_batch_work_ids,
    has_retryable_enrichment_batch_result,
    parse_enrichment_batch_envelope,
    serialize_enrichment_batch_result_envelope,
)


def _envelope():
    return {
        "protocol_version": 1,
        "batch_id": "enrichment-batch--1",
        "item_count": 2,
        "object_count": 0,
        "group_context": {
            "connector_id": "connector--1",
            "applicant_id": None,
            "draft_id": None,
            "mode": "auto",
            "trigger": "create",
            "resolution": "deferred",
            "playbook_context": None,
            "configuration": None,
            "shared_organization_ids": [],
            "context_fingerprint": "fingerprint",
        },
        "items": [
            {
                "item_id": "item--1",
                "work_id": "work--1",
                "entity_id": "indicator--1",
                "entity_type": "Indicator",
                "payload_fingerprint": "payload--1",
                "stix_entity": None,
                "stix_objects": None,
            },
            {
                "item_id": "item--2",
                "work_id": "work--2",
                "entity_id": "indicator--2",
                "entity_type": "Indicator",
                "payload_fingerprint": "payload--2",
                "stix_entity": None,
                "stix_objects": None,
            },
        ],
    }


def test_enrichment_batch_contract_builds_stable_result_envelope():
    envelope = parse_enrichment_batch_envelope(json.dumps(_envelope()))
    result = build_enrichment_batch_result_envelope(
        envelope,
        {
            "output_bundle": json.dumps(
                {
                    "type": "bundle",
                    "objects": [
                        {"id": "indicator--result-1", "type": "indicator"},
                    ],
                }
            ),
            "results": [
                {
                    "item_id": "item--2",
                    "work_id": "work--2",
                    "status": EnrichmentBatchResultStatus.UNCHANGED,
                    "message": None,
                    "output_object_ids": [],
                },
                {
                    "item_id": "item--1",
                    "work_id": "work--1",
                    "status": EnrichmentBatchResultStatus.PROCESSED,
                    "message": "updated",
                    "output_object_ids": ["indicator--result-1"],
                },
            ],
        },
    )

    assert enrichment_batch_work_ids(envelope) == ["work--1", "work--2"]
    assert result["results"][0]["item_id"] == "item--1"
    assert result["output_object_count"] == 1
    assert has_retryable_enrichment_batch_result(result) is False
    assert serialize_enrichment_batch_result_envelope(result) == json.dumps(
        result, sort_keys=True, separators=(",", ":")
    )


def test_enrichment_batch_contract_rejects_duplicate_work_identity():
    envelope = _envelope()
    envelope["items"][1]["work_id"] = envelope["items"][0]["work_id"]

    with pytest.raises(ValueError, match="unique work_id"):
        parse_enrichment_batch_envelope(json.dumps(envelope))


def test_enrichment_batch_contract_rejects_unowned_output_bundle_objects():
    envelope = parse_enrichment_batch_envelope(json.dumps(_envelope()))

    with pytest.raises(ValueError, match="must be owned"):
        build_enrichment_batch_result_envelope(
            envelope,
            {
                "output_bundle": json.dumps(
                    {
                        "type": "bundle",
                        "objects": [
                            {"id": "indicator--result-1", "type": "indicator"},
                            {"id": "indicator--result-2", "type": "indicator"},
                        ],
                    }
                ),
                "results": [
                    {
                        "item_id": "item--1",
                        "work_id": "work--1",
                        "status": "PROCESSED",
                        "message": "updated",
                        "output_object_ids": ["indicator--result-1"],
                    },
                    {
                        "item_id": "item--2",
                        "work_id": "work--2",
                        "status": "UNCHANGED",
                        "message": None,
                        "output_object_ids": [],
                    },
                ],
            },
        )


def test_enrichment_batch_contract_allows_shared_output_bundle_objects():
    envelope = parse_enrichment_batch_envelope(json.dumps(_envelope()))
    result = build_enrichment_batch_result_envelope(
        envelope,
        {
            "output_bundle": json.dumps(
                {
                    "type": "bundle",
                    "objects": [
                        {"id": "indicator--result-1", "type": "indicator"},
                        {"id": "indicator--result-2", "type": "indicator"},
                        {"id": "label--shared", "type": "label"},
                    ],
                }
            ),
            "results": [
                {
                    "item_id": "item--1",
                    "work_id": "work--1",
                    "status": "PROCESSED",
                    "message": "updated",
                    "output_object_ids": ["indicator--result-1", "label--shared"],
                },
                {
                    "item_id": "item--2",
                    "work_id": "work--2",
                    "status": "PROCESSED",
                    "message": "updated",
                    "output_object_ids": ["indicator--result-2", "label--shared"],
                },
            ],
        },
    )

    assert result["output_object_count"] == 3


def test_enrichment_batch_contract_marks_retryable_results_for_requeue():
    envelope = parse_enrichment_batch_envelope(json.dumps(_envelope()))
    result = build_enrichment_batch_result_envelope(
        envelope,
        {
            "output_bundle": None,
            "results": [
                {
                    "item_id": "item--1",
                    "work_id": "work--1",
                    "status": "RETRYABLE",
                    "message": "upstream timeout",
                    "output_object_ids": [],
                },
                {
                    "item_id": "item--2",
                    "work_id": "work--2",
                    "status": "UNCHANGED",
                    "message": None,
                    "output_object_ids": [],
                },
            ],
        },
    )

    assert has_retryable_enrichment_batch_result(result) is True

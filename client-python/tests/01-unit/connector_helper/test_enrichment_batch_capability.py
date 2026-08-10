import pytest

from pycti.connector.opencti_connector import OpenCTIConnector
from pycti.connector.opencti_connector_helper import (
    OpenCTIConnectorHelper,
    parse_json_object_config,
)


def test_connector_serializes_optional_enrichment_batch_capability():
    capability = {
        "protocol_version": 1,
        "max_items": 100,
        "max_stix_objects": 1000,
        "max_serialized_bytes": 1048576,
        "max_wait_ms": 1000,
    }
    connector = OpenCTIConnector(
        connector_id="connector--1",
        connector_name="Batch Enrichment",
        connector_type="INTERNAL_ENRICHMENT",
        scope="Indicator",
        auto=True,
        only_contextual=False,
        playbook_compatible=False,
        auto_update=False,
        enrichment_resolution="deferred",
        enrichment_batch_capability=capability,
    )

    assert connector.to_input()["input"]["enrichment_batch_capability"] == capability


def test_json_object_config_accepts_yaml_dicts_and_env_json_strings():
    capability = {"protocol_version": 1}

    assert parse_json_object_config(capability, "CAPABILITY") == capability
    assert parse_json_object_config('{"protocol_version": 1}', "CAPABILITY") == (
        capability
    )


def test_batch_capability_requires_a_batch_callback_before_listening():
    helper = object.__new__(OpenCTIConnectorHelper)
    helper.connect_enrichment_batch_capability = {"protocol_version": 1}

    with pytest.raises(ValueError, match="requires an enrichment_batch_callback"):
        helper.listen(lambda _message: "processed")

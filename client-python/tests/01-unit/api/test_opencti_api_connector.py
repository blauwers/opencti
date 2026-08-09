from unittest.mock import MagicMock

from pycti.api.opencti_api_connector import OpenCTIApiConnector


def test_connector_list_forwards_worker_runtime_capability():
    api = MagicMock()
    api.query.return_value = {"data": {"connectorsForWorker": []}}
    connector_api = OpenCTIApiConnector(api)
    worker_runtime = {
        "worker_id": "worker-1",
        "batch_delivery_protocol_max": 2,
    }

    assert connector_api.list(worker_runtime=worker_runtime) == []
    assert api.query.call_args.args[1] == {"workerRuntime": worker_runtime}


def test_acquire_freshness_maps_connector_scoped_decisions():
    api = MagicMock()
    api.query.return_value = {
        "data": {
            "connectorFreshnessAcquire": [
                {
                    "key": "198.51.100.1",
                    "status": "ACQUIRED",
                    "lease_token": "lease-1",
                    "retry_after_ms": 30000,
                }
            ]
        }
    }
    connector_api = OpenCTIApiConnector(api)

    result = connector_api.acquire_freshness(
        connector_id="connector--1",
        namespace="shodan-internetdb",
        keys=["198.51.100.1"],
        lease_ttl_seconds=30,
    )

    assert result == api.query.return_value["data"]["connectorFreshnessAcquire"]
    _, variables = api.query.call_args.args
    assert variables == {
        "input": {
            "connector_id": "connector--1",
            "namespace": "shodan-internetdb",
            "keys": ["198.51.100.1"],
            "lease_ttl_seconds": 30,
            "force_refresh": False,
        }
    }


def test_complete_freshness_maps_successful_transition():
    api = MagicMock()
    api.query.return_value = {"data": {"connectorFreshnessComplete": True}}
    connector_api = OpenCTIApiConnector(api)

    result = connector_api.complete_freshness(
        connector_id="connector--1",
        namespace="shodan-internetdb",
        key="198.51.100.1",
        lease_token="lease-1",
        freshness_ttl_seconds=604800,
    )

    assert result is True
    _, variables = api.query.call_args.args
    assert variables == {
        "input": {
            "connector_id": "connector--1",
            "namespace": "shodan-internetdb",
            "key": "198.51.100.1",
            "lease_token": "lease-1",
            "freshness_ttl_seconds": 604800,
        }
    }


def test_release_freshness_maps_failed_transition_cleanup():
    api = MagicMock()
    api.query.return_value = {"data": {"connectorFreshnessRelease": True}}
    connector_api = OpenCTIApiConnector(api)

    result = connector_api.release_freshness(
        connector_id="connector--1",
        namespace="shodan-internetdb",
        key="198.51.100.1",
        lease_token="lease-1",
    )

    assert result is True
    _, variables = api.query.call_args.args
    assert variables == {
        "input": {
            "connector_id": "connector--1",
            "namespace": "shodan-internetdb",
            "key": "198.51.100.1",
            "lease_token": "lease-1",
        }
    }

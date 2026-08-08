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

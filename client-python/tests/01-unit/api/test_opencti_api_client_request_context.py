import threading

from pycti.api.opencti_api_client import OpenCTIApiClient


def _client():
    client = object.__new__(OpenCTIApiClient)
    client.request_headers = {
        "Authorization": "Bearer test",
        "Content-Type": "application/json",
    }
    client.session_batch_requests_max_payload_size = None
    client.session_batch_requests_max_execution_groups = None
    return client


def _snapshot(client):
    headers = client.get_request_headers(hide_token=False)
    return {
        "draft_id": client.get_draft_id(),
        "work_id": headers.get("opencti-work-id"),
        "applicant_id": headers.get("opencti-applicant-id"),
        "retry_number": headers.get("opencti-retry-number"),
        "batch_wait_until": headers.get("opencti-batch-wait-until"),
    }


def test_contextual_request_headers_are_isolated_across_threads():
    client = _client()
    first_ready = threading.Event()
    second_ready = threading.Event()
    snapshots = {}

    def first_request():
        client.set_draft_id("draft--a")
        client.set_work_id("work--a")
        client.set_applicant_id_header("applicant--a")
        client.set_retry_number(1)
        client.set_batch_wait_until("COMMITTED")
        first_ready.set()
        second_ready.wait()
        snapshots["a"] = _snapshot(client)

    def second_request():
        first_ready.wait()
        client.set_draft_id("draft--b")
        client.set_work_id("work--b")
        client.set_applicant_id_header("applicant--b")
        client.set_retry_number(2)
        client.set_batch_wait_until("MATERIALIZED")
        second_ready.set()
        snapshots["b"] = _snapshot(client)

    first_thread = threading.Thread(target=first_request)
    second_thread = threading.Thread(target=second_request)
    first_thread.start()
    second_thread.start()
    first_thread.join()
    second_thread.join()

    assert snapshots == {
        "a": {
            "draft_id": "draft--a",
            "work_id": "work--a",
            "applicant_id": "applicant--a",
            "retry_number": "1",
            "batch_wait_until": "COMMITTED",
        },
        "b": {
            "draft_id": "draft--b",
            "work_id": "work--b",
            "applicant_id": "applicant--b",
            "retry_number": "2",
            "batch_wait_until": "MATERIALIZED",
        },
    }


def test_request_context_restores_previous_header_overrides():
    client = _client()
    client.set_work_id("work--outer")
    client.set_draft_id("draft--outer")
    client.set_batch_wait_until("COMMITTED")

    with client.request_context():
        assert _snapshot(client) == {
            "draft_id": "",
            "work_id": None,
            "applicant_id": None,
            "retry_number": None,
            "batch_wait_until": None,
        }
        client.set_work_id("work--inner")
        client.set_draft_id("draft--inner")
        client.set_batch_wait_until("MATERIALIZED")
        assert _snapshot(client)["work_id"] == "work--inner"
        assert _snapshot(client)["draft_id"] == "draft--inner"
        assert _snapshot(client)["batch_wait_until"] == "MATERIALIZED"

    assert _snapshot(client)["work_id"] == "work--outer"
    assert _snapshot(client)["draft_id"] == "draft--outer"
    assert _snapshot(client)["batch_wait_until"] == "COMMITTED"


def test_contextual_setters_do_not_mutate_shared_transport_headers():
    client = _client()

    client.set_work_id("work--1")
    client.set_draft_id("draft--1")
    client.set_batch_wait_until("MATERIALIZED")

    assert "opencti-work-id" not in client.request_headers
    assert "opencti-draft-id" not in client.request_headers
    assert "opencti-batch-wait-until" not in client.request_headers


def test_batch_mutation_plans_are_isolated_across_threads():
    client = _client()
    first_ready = threading.Event()
    second_done = threading.Event()
    results = {}
    errors = []
    mutation = "mutation Test($input: String!) { test(input: $input) { id } }"

    def first_request():
        try:
            with client.batch_mutation_plan() as plan:
                first_ready.set()
                second_done.wait()
                client.query(mutation, {"input": "a"})
                results["a"] = len(plan.operations)
        except Exception as exc:  # pragma: no cover - test collects thread failures
            errors.append(exc)

    def second_request():
        try:
            first_ready.wait()
            with client.batch_mutation_plan() as plan:
                client.query(mutation, {"input": "b"})
                results["b"] = len(plan.operations)
        except Exception as exc:  # pragma: no cover - test collects thread failures
            errors.append(exc)
        finally:
            second_done.set()

    first_thread = threading.Thread(target=first_request)
    second_thread = threading.Thread(target=second_request)
    first_thread.start()
    second_thread.start()
    first_thread.join()
    second_thread.join()

    assert errors == []
    assert results == {"a": 1, "b": 1}

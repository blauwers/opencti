"""Tests for deferred enrichment payload compatibility in connector helper."""

import inspect
from unittest import TestCase
from unittest.mock import MagicMock, patch

from pycti.connector.opencti_connector_helper import ListenQueue, OpenCTIConnectorHelper


class DummyLogger:
    def info(self, message, data=None):
        pass

    def debug(self, message, data=None):
        pass

    def warning(self, message, data=None):
        pass

    def error(self, message, data=None):
        pass


class DummyEnrichmentHelper:
    def __init__(self):
        self.connector_logger = DummyLogger()
        self.metric = MagicMock()
        self.work_id = None
        self.validation_mode = None
        self.force_validation = None
        self.draft_id = None
        self.playbook = None
        self.enrichment_shared_organizations = None
        self.connect_type = "INTERNAL_ENRICHMENT"
        self.applicant_id = "test-applicant"
        self.callback = MagicMock(return_value="success")
        self.api = MagicMock()
        self.api_impersonate = MagicMock()
        self.get_attribute_in_extension = MagicMock(return_value=[])


class TestEnrichmentResolutionDefaults(TestCase):
    def test_helper_defaults_new_connectors_to_deferred_resolution(self):
        source = inspect.getsource(OpenCTIConnectorHelper.__init__)

        assert 'default="deferred"' in source

    def test_data_handler_rebuilds_stix_payloads_for_deferred_messages(self):
        helper = DummyEnrichmentHelper()
        helper.api.stix2.get_reader.return_value = MagicMock(
            return_value={"standard_id": "indicator--test"}
        )
        helper.api.stix2.generate_export.return_value = {"generated": True}
        helper.api.stix2.prepare_export.return_value = [
            {"id": "indicator--test", "type": "indicator"}
        ]
        listen_queue = ListenQueue.__new__(ListenQueue)
        listen_queue.helper = helper
        listen_queue.pika_connection = MagicMock()
        listen_queue.callback = helper.callback
        listen_queue.connector_applicant_id = "test-connector-applicant"
        json_data = {
            "event": {
                "entity_id": "indicator--test",
                "entity_type": "Indicator",
                "stix_entity": None,
                "stix_objects": None,
            },
            "internal": {
                "work_id": "work-123",
                "draft_id": "",
                "applicant_id": None,
            },
        }

        with patch.object(listen_queue, "_set_draft_id"):
            listen_queue._data_handler(json_data)

        helper.api.stix2.prepare_export.assert_called_once()
        helper.callback.assert_called_once()
        callback_event = helper.callback.call_args.args[0]
        assert callback_event["stix_entity"] == {
            "id": "indicator--test",
            "type": "indicator",
        }
        assert callback_event["stix_objects"] == [
            {"id": "indicator--test", "type": "indicator"}
        ]

    def test_data_handler_omits_file_projection_when_connector_opts_out(self):
        helper = DummyEnrichmentHelper()
        helper.connect_enrichment_entity_with_files = False
        reader = MagicMock(return_value={"standard_id": "indicator--test"})
        helper.api.stix2.get_reader.return_value = reader
        helper.api.stix2.prepare_export.return_value = [
            {"id": "indicator--test", "type": "indicator"}
        ]
        listen_queue = ListenQueue.__new__(ListenQueue)
        listen_queue.helper = helper
        listen_queue.pika_connection = MagicMock()
        listen_queue.callback = helper.callback
        listen_queue.connector_applicant_id = "test-connector-applicant"
        json_data = {
            "event": {
                "entity_id": "indicator--test",
                "entity_type": "Indicator",
                "stix_entity": None,
                "stix_objects": None,
            },
            "internal": {
                "work_id": "work-123",
                "draft_id": "",
                "applicant_id": None,
            },
        }

        with patch.object(listen_queue, "_set_draft_id"):
            listen_queue._data_handler(json_data)

        reader.assert_called_once_with(id="indicator--test", withFiles=False)

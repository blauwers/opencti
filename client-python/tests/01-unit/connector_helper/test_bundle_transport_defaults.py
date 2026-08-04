import json
from unittest import TestCase
from unittest.mock import MagicMock

import pytest

from pycti.connector.opencti_connector_helper import OpenCTIConnectorHelper


class TestBundleTransportDefaults(TestCase):
    def _helper(self):
        helper = OpenCTIConnectorHelper.__new__(OpenCTIConnectorHelper)
        helper.work_id = None
        helper.validation_mode = "draft"
        helper.draft_id = None
        helper.force_validation = False
        helper.bundle_send_to_queue = False
        helper.bundle_send_to_directory = False
        helper.bundle_send_to_directory_path = None
        helper.bundle_send_to_directory_retention = 0
        helper.bundle_send_to_s3 = False
        helper.bundle_send_to_s3_bucket = None
        helper.enrichment_shared_organizations = None
        helper.playbook = None
        helper.connect_validate_before_import = False
        helper.connector_id = "connector--11111111-1111-4111-8111-111111111111"
        helper.queue_protocol = "amqp"
        helper.api = MagicMock()
        helper.connector_logger = MagicMock()
        helper.metric = MagicMock()
        return helper

    @staticmethod
    def _bundle():
        return json.dumps(
            {
                "type": "bundle",
                "id": "bundle--11111111-1111-4111-8111-111111111111",
                "objects": [
                    {
                        "type": "indicator",
                        "id": "indicator--11111111-1111-4111-8111-111111111111",
                    },
                    {
                        "type": "malware",
                        "id": "malware--22222222-2222-4222-8222-222222222222",
                    },
                ],
            }
        )

    def test_preserves_bundle_by_default(self):
        bundles = self._helper().send_stix2_bundle(self._bundle(), send_to_queue=False)

        self.assertEqual(len(bundles), 1)
        intact_bundle = json.loads(bundles[0])
        self.assertEqual(
            intact_bundle["id"], "bundle--11111111-1111-4111-8111-111111111111"
        )
        self.assertNotIn("x_opencti_seq", intact_bundle)
        self.assertEqual(len(intact_bundle["objects"]), 2)

    def test_no_split_is_ignored_for_current_routing(self):
        bundles = self._helper().send_stix2_bundle(
            self._bundle(), send_to_queue=False, no_split=False
        )

        self.assertEqual(len(bundles), 1)
        self.assertEqual(len(json.loads(bundles[0])["objects"]), 2)

        bundles = self._helper().send_stix2_bundle(
            self._bundle(), send_to_queue=False, no_split=True
        )

        self.assertEqual(len(bundles), 1)
        self.assertEqual(len(json.loads(bundles[0])["objects"]), 2)

    def test_split_bundles_must_be_explicitly_requested(self):
        bundles = self._helper().send_stix2_bundle(
            self._bundle(), send_to_queue=False, split_bundles=True, no_split=True
        )

        self.assertEqual(len(bundles), 2)
        self.assertTrue(
            all(len(json.loads(bundle)["objects"]) == 1 for bundle in bundles)
        )
        self.assertTrue(
            all("x_opencti_seq" in json.loads(bundle) for bundle in bundles)
        )

    def test_truthy_non_boolean_split_bundles_does_not_opt_in(self):
        bundles = self._helper().send_stix2_bundle(
            self._bundle(), send_to_queue=False, split_bundles="true"
        )

        self.assertEqual(len(bundles), 1)
        self.assertEqual(len(json.loads(bundles[0])["objects"]), 2)

    def test_wait_until_rejects_unknown_consistency_mode(self):
        with pytest.raises(ValueError):
            self._helper().send_stix2_bundle(
                self._bundle(), send_to_queue=False, wait_until="LATER"
            )

    def test_unsplit_amqp_uses_backend_admission_before_queueing(self):
        helper = self._helper()

        bundles = helper.send_stix2_bundle(self._bundle(), send_to_queue=True)

        self.assertEqual(len(bundles), 1)
        helper.api.send_bundle_to_api.assert_called_once_with(
            connector_id=helper.connector_id,
            bundle=bundles[0],
            work_id=None,
            split_bundles=False,
            cleanup_inconsistent_bundle=False,
            wait_until=None,
        )
        helper.api.work.add_expectations.assert_not_called()

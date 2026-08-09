import base64

from pycti.api.opencti_api_client import File
from pycti.entities.opencti_stix_cyber_observable import StixCyberObservable
from pycti.utils.opencti_file_utils import BASE64_FILE_MEMORY_THRESHOLD


class _NullLogger:
    def info(self, *args, **kwargs):
        del args, kwargs

    def error(self, *args, **kwargs):
        raise AssertionError("unexpected error log")


class _ArtifactCreateClient:
    def __init__(self, expected_payload):
        self.app_logger = _NullLogger()
        self.expected_payload = expected_payload
        self.retained_uploads = []

    @staticmethod
    def file(name, data, mime):
        return File(name, data, mime)

    @staticmethod
    def get_attribute_in_extension(_attribute, _entity):
        return None

    def query(self, query, variables):
        if "StixCyberObservableAdd" in query:
            return {
                "data": {
                    "stixCyberObservableAdd": {
                        "id": "artifact--1",
                        "entity_type": "Artifact",
                    }
                }
            }
        upload = variables["file"]
        assert not upload.data.closed
        assert upload.data.read() == self.expected_payload
        upload.data.seek(0)
        self.retained_uploads.append(upload)
        return {"data": {"artifactImport": {"id": "artifact--1"}}}

    @staticmethod
    def process_multiple_fields(data):
        return data


def test_artifact_payload_upload_closes_large_decoded_stream():
    payload = b"x" * (BASE64_FILE_MEMORY_THRESHOLD + 1)
    client = _ArtifactCreateClient(payload)
    entity = StixCyberObservable(client)

    entity.create(
        observableData={
            "type": "artifact",
            "mime_type": "application/octet-stream",
            "payload_bin": base64.b64encode(payload).decode("ascii"),
        }
    )

    assert client.retained_uploads[-1].data.closed

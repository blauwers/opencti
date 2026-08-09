import io

import pytest

from pycti.api.opencti_api_client import File, OpenCTIApiClient


def _client(session):
    client = OpenCTIApiClient.__new__(OpenCTIApiClient)
    client.api_url = "http://benchmark.invalid/graphql"
    client.request_headers = {}
    client.ssl_verify = False
    client.cert = None
    client.proxies = None
    client.session_requests_timeout = 300
    client.session = session
    client._batch_mutation_plan = None
    return client


class _UploadResponse:
    status_code = 200

    @staticmethod
    def json():
        return {"data": {"ok": True}}


class _NullLogger:
    def info(self, *args, **kwargs):
        del args, kwargs


class _TrackingFile(io.BytesIO):
    def __init__(self, payload):
        super().__init__(payload)
        self.read_calls = 0

    def read(self, *args, **kwargs):
        self.read_calls += 1
        return super().read(*args, **kwargs)


class _UploadSession:
    def __init__(self, upload=None):
        self.upload = upload
        self.body = None
        self.content_type = None
        self.content_length = None
        self.read_calls_before_post = None

    def post(self, *args, **kwargs):
        del args
        if self.upload is not None:
            self.read_calls_before_post = self.upload.read_calls
        assert "files" not in kwargs
        multipart_stream = kwargs["data"]
        self.content_type = kwargs["headers"]["Content-Type"]
        self.content_length = len(multipart_stream)
        self.body = b"".join(multipart_stream)
        return _UploadResponse()


def test_query_streams_seekable_multipart_file_body_without_prereading():
    upload = _TrackingFile(b"payload")
    session = _UploadSession(upload)

    result = _client(session).query(
        "mutation Upload($file: Upload!) { uploadImport(file: $file) { id } }",
        {"file": File("artifact.txt", upload, "text/plain")},
    )

    assert result == {"data": {"ok": True}}
    assert session.read_calls_before_post == 0
    assert session.content_type.startswith("multipart/form-data; boundary=")
    assert session.content_length == len(session.body)
    assert b'name="operations"' in session.body
    assert b'name="map"' in session.body
    assert b'{"0": ["variables.file"]}' in session.body
    assert b'filename="artifact.txt"' in session.body
    assert b"payload" in session.body


@pytest.mark.parametrize(
    ("data", "expected_payload"),
    [
        pytest.param("payload", b"payload", id="str"),
        pytest.param(b"payload", b"payload", id="bytes"),
        pytest.param(bytearray(b"payload"), b"payload", id="bytearray"),
        pytest.param(memoryview(b"payload"), b"payload", id="memoryview"),
    ],
)
def test_query_stream_preserves_inline_upload_data_types(data, expected_payload):
    session = _UploadSession()

    _client(session).query(
        "mutation Upload($file: Upload!) { uploadImport(file: $file) { id } }",
        {"file": File("artifact.txt", data, "text/plain")},
    )

    assert expected_payload in session.body


def test_query_stream_preserves_seekable_upload_position():
    upload = io.BytesIO(b"prefix-payload")
    upload.seek(len(b"prefix-"))
    session = _UploadSession()

    _client(session).query(
        "mutation Upload($file: Upload!) { uploadImport(file: $file) { id } }",
        {"file": File("artifact.txt", upload, "text/plain")},
    )

    assert b"payload" in session.body
    assert b"prefix-payload" not in session.body


@pytest.mark.parametrize("method_name", ["upload_file", "upload_pending_file"])
def test_path_upload_helpers_stream_file_data_and_close_handle(tmp_path, method_name):
    upload_path = tmp_path / "payload.json"
    upload_path.write_bytes(b"payload")
    client = _client(None)
    client.app_logger = _NullLogger()
    captured = {}

    def query(query, variables):
        del query
        upload = variables["file"]
        captured["handle"] = upload.data
        assert not upload.data.closed
        assert upload.data.read() == b"payload"
        upload.data.seek(0)
        return {"data": {"ok": True}}

    client.query = query

    result = getattr(client, method_name)(file_name=str(upload_path))

    assert result == {"data": {"ok": True}}
    assert captured["handle"].closed


@pytest.mark.parametrize("method_name", ["upload_file", "upload_pending_file"])
def test_path_upload_helpers_leave_caller_data_open(tmp_path, method_name):
    upload_path = tmp_path / "payload.json"
    upload_path.write_bytes(b"payload")
    client = _client(None)
    client.app_logger = _NullLogger()
    client.query = lambda query, variables: {"data": {"ok": True}}

    with upload_path.open("rb") as data:
        getattr(client, method_name)(file_name=str(upload_path), data=data)

        assert not data.closed

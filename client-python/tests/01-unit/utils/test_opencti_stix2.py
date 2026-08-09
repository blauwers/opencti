import base64
import datetime
import json
from collections import Counter
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from pycti.api.opencti_api_client import OpenCTIApiClient
from pycti.api.opencti_api_batch import (
    BatchMutationPlan,
    BatchMutationPlanTooLarge,
    build_batch_result_token,
)
from pycti.utils.opencti_file_utils import BASE64_FILE_MEMORY_THRESHOLD
from pycti.utils.opencti_stix2 import (
    IMPORT_PREFETCH_BATCH_SIZE,
    NESTED_REF_RELATIONSHIP_CREATE_BATCH_SIZE,
    OpenCTIStix2,
)
from pycti.utils.opencti_stix2_splitter import OpenCTIStix2Splitter
from pycti.utils.opencti_stix2_utils import OpenCTIStix2Utils


@pytest.fixture
def opencti_stix2(api_client):
    return OpenCTIStix2(api_client)


def test_unknown_type(opencti_stix2: OpenCTIStix2, caplog):
    opencti_stix2.unknown_type({"type": "foo"})
    for record in caplog.records:
        assert record.levelname == "ERROR"
    assert "Unknown object type, doing nothing..." in caplog.text


def test_convert_markdown(opencti_stix2: OpenCTIStix2):
    # Matched pair is converted to backticks
    result = opencti_stix2.convert_markdown(
        " my <code> is very </special> </code> to me"
    )
    assert " my ` is very </special> ` to me" == result


def test_convert_markdown_multiple_pairs(opencti_stix2: OpenCTIStix2):
    # Multiple matched pairs are all converted
    result = opencti_stix2.convert_markdown("<code>foo</code> and <code>bar</code>")
    assert "`foo` and `bar`" == result


def test_convert_markdown_typo(opencti_stix2: OpenCTIStix2):
    # Malformed opening tag (<code missing closing >) means no valid pair exists; nothing should be replaced
    text = " my <code is very </special> </code> to me"
    result = opencti_stix2.convert_markdown(text)
    assert text == result


def test_convert_markdown_literal_code_tag(opencti_stix2: OpenCTIStix2):
    # A lone <code> without a matching </code> is literal content and must not be altered
    text = 'Run python3 -c "<code>" and pass it to subprocess.run(..., shell=True)'
    result = opencti_stix2.convert_markdown(text)
    assert text == result


def test_convert_markdown_mixed_matched_and_lone(opencti_stix2: OpenCTIStix2):
    # A matched pair is converted, but a trailing lone <code> is left untouched
    result = opencti_stix2.convert_markdown("<code>foo</code> and <code>")
    assert "`foo` and <code>" == result


def test_format_date_with_tz(opencti_stix2: OpenCTIStix2):
    # Test all 4 format_date cases with timestamp + timezone
    my_datetime = datetime.datetime(
        2021, 3, 5, 13, 31, 19, 42621, tzinfo=datetime.timezone.utc
    )
    my_datetime_str = my_datetime.isoformat(timespec="milliseconds").replace(
        "+00:00", "Z"
    )
    assert my_datetime_str == opencti_stix2.format_date(my_datetime)
    my_date = my_datetime.date()
    my_date_str = "2021-03-05T00:00:00.000Z"
    assert my_date_str == opencti_stix2.format_date(my_date)
    assert my_datetime_str == opencti_stix2.format_date(my_datetime_str)
    assert (
        str(
            datetime.datetime.now(tz=datetime.timezone.utc)
            .isoformat(timespec="seconds")
            .replace("+00:00", "")
        )
        in opencti_stix2.format_date()
    )
    with pytest.raises(ValueError):
        opencti_stix2.format_date("No time")

    # Test all 4 format_date cases with timestamp w/o timezone
    my_datetime = datetime.datetime(2021, 3, 5, 13, 31, 19, 42621)
    my_datetime_str = (
        my_datetime.replace(tzinfo=datetime.timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )
    assert my_datetime_str == opencti_stix2.format_date(my_datetime)
    my_date = my_datetime.date()
    my_date_str = "2021-03-05T00:00:00.000Z"
    assert my_date_str == opencti_stix2.format_date(my_date)
    assert my_datetime_str == opencti_stix2.format_date(my_datetime_str)

    # Test the behavior of format_date() when called without arguments.
    # Since it relies on the current time, avoid flaky results by comparing only up to the seconds, using dates generated immediately before and after the function call.
    my_now_date_1 = (
        datetime.datetime.now(tz=datetime.timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "")
    )
    stix_now_date = opencti_stix2.format_date()
    my_now_date_2 = (
        datetime.datetime.now(tz=datetime.timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "")
    )
    assert (str(my_now_date_1) in stix_now_date) or (
        str(my_now_date_2) in stix_now_date
    )

    with pytest.raises(ValueError):
        opencti_stix2.format_date("No time")


def test_filter_objects(opencti_stix2: OpenCTIStix2):
    objects = [{"id": "123"}, {"id": "124"}, {"id": "125"}, {"id": "126"}]
    result = opencti_stix2.filter_objects(["123", "124", "126"], objects)
    assert len(result) == 1
    assert "126" not in result


@pytest.mark.parametrize(
    ("import_method", "stix_object", "result"),
    [
        (
            "import_object",
            {
                "id": "malware--1",
                "type": "malware",
                "name": "benchmark",
                "is_family": False,
                "extensions": {
                    "extension-definition--ea279b3e-5c71-4632-ac08-831c66a786ba": {
                        "files": []
                    }
                },
            },
            {"id": "malware--1", "entity_type": "Malware"},
        ),
        (
            "import_observable",
            {
                "id": "directory--1",
                "type": "directory",
                "path": "/benchmark",
                "extensions": {
                    "extension-definition--ea279b3e-5c71-4632-ac08-831c66a786ba": {
                        "files": []
                    }
                },
            },
            {"id": "directory--1", "entity_type": "Directory"},
        ),
    ],
)
def test_import_reads_extension_files_once(import_method, stix_object, result):
    extension_lookup_counts = Counter()

    def get_attribute_in_extension(key, entity):
        extension_lookup_counts[key] += 1
        return OpenCTIApiClient.get_attribute_in_extension(key, entity)

    fake_opencti = SimpleNamespace(
        app_logger=SimpleNamespace(info=lambda *_args, **_kwargs: None),
        get_attribute_in_extension=get_attribute_in_extension,
        get_draft_id=lambda: "",
        stix_cyber_observable=SimpleNamespace(create=lambda **_kwargs: result),
    )
    opencti_stix2 = OpenCTIStix2(fake_opencti)
    opencti_stix2.extract_embedded_relationships = lambda *_args, **_kwargs: {
        "created_by": None,
        "object_marking": None,
        "object_label": None,
        "open_vocabs": {},
        "granted_refs": [],
        "kill_chain_phases": [],
        "object_refs": [],
        "external_references": [],
        "reports": {},
        "sample_refs": [],
    }
    opencti_stix2.get_stix_helper = lambda: {
        "malware": SimpleNamespace(import_from_stix2=lambda **_kwargs: result)
    }
    opencti_stix2._create_observable_nested_ref_relationships = (
        lambda *_args, **_kwargs: None
    )

    getattr(opencti_stix2, import_method)(stix_object, update=False)

    assert extension_lookup_counts["files"] == 1


class _ExternalReferenceRecorder:
    def __init__(self):
        self.create_calls = 0
        self.generate_id_calls = 0

    @staticmethod
    def _generated_id(url, source_name, external_id):
        if url is not None:
            return f"external-reference--{url}"
        return f"external-reference--{source_name}|{external_id}"

    def generate_id(self, url, source_name, external_id):
        self.generate_id_calls += 1
        return self._generated_id(url, source_name, external_id)

    def create(self, **kwargs):
        self.create_calls += 1
        return {
            "id": self._generated_id(
                kwargs["url"], kwargs["source_name"], kwargs["external_id"]
            )
        }


def _external_reference_opencti():
    return SimpleNamespace(
        external_reference=_ExternalReferenceRecorder(),
        app_logger=SimpleNamespace(warning=lambda *args, **kwargs: None),
        get_draft_id=lambda: "",
        get_attribute_in_extension=lambda _attribute, _entity: None,
        query=lambda _query: {"data": {"vocabularyCategories": []}},
        logger_class=lambda _name: SimpleNamespace(warning=lambda *args: None),
        file=lambda name, data, mime_type: SimpleNamespace(
            name=name, data=data, mime=mime_type
        ),
    )


@pytest.mark.parametrize(
    "field_name", ["external_references", "x_opencti_external_references"]
)
def test_extract_embedded_relationships_reuses_external_reference_without_files(
    field_name,
):
    opencti = _external_reference_opencti()
    opencti_stix2 = OpenCTIStix2(opencti)
    stix_object = {
        "type": "malware",
        field_name: [
            {
                "source_name": "benchmark",
                "url": "https://example.test/reference",
                "external_id": "REF-1",
            }
        ],
    }

    first = opencti_stix2.extract_embedded_relationships(dict(stix_object))
    second = opencti_stix2.extract_embedded_relationships(dict(stix_object))

    assert first["external_references"] == second["external_references"]
    assert opencti.external_reference.generate_id_calls == 1
    assert opencti.external_reference.create_calls == 1


def test_extract_embedded_relationships_keeps_file_upload_external_reference_uncached():
    opencti = _external_reference_opencti()
    opencti_stix2 = OpenCTIStix2(opencti)
    stix_object = {
        "type": "malware",
        "external_references": [
            {
                "source_name": "benchmark",
                "url": "https://example.test/reference",
                "external_id": "REF-1",
                "x_opencti_files": [
                    {
                        "name": "payload.txt",
                        "data": base64.b64encode(b"payload").decode("ascii"),
                    }
                ],
            }
        ],
    }

    opencti_stix2.extract_embedded_relationships(dict(stix_object))
    opencti_stix2.extract_embedded_relationships(dict(stix_object))

    assert opencti.external_reference.create_calls == 2


def test_extract_embedded_relationships_keeps_changed_external_reference_uncached():
    opencti = _external_reference_opencti()
    opencti_stix2 = OpenCTIStix2(opencti)
    first = {
        "type": "malware",
        "external_references": [
            {
                "source_name": "benchmark",
                "url": "https://example.test/reference",
                "external_id": "REF-1",
                "description": "first",
            }
        ],
    }
    second = {
        "type": "malware",
        "external_references": [
            {
                "source_name": "benchmark",
                "url": "https://example.test/reference",
                "external_id": "REF-1",
                "description": "second",
            }
        ],
    }

    opencti_stix2.extract_embedded_relationships(first)
    opencti_stix2.extract_embedded_relationships(second)

    assert opencti.external_reference.create_calls == 2


def test_extract_embedded_relationships_reuses_external_reference_generated_ids():
    opencti = _external_reference_opencti()
    opencti_stix2 = OpenCTIStix2(opencti)
    stix_object = {
        "type": "malware",
        "external_references": [
            {
                "source_name": "benchmark",
                "url": "https://example.test/reference",
                "external_id": "REF-1",
            }
        ],
    }

    opencti_stix2.extract_embedded_relationships(dict(stix_object))
    opencti_stix2.extract_embedded_relationships(dict(stix_object))

    assert opencti.external_reference.generate_id_calls == 1
    assert opencti.external_reference.create_calls == 1


def test_get_external_reference_generated_id_does_not_cache_non_string_inputs():
    opencti = _external_reference_opencti()
    opencti_stix2 = OpenCTIStix2(opencti)

    first = opencti_stix2._get_external_reference_generated_id(42, "benchmark", "REF-1")
    second = opencti_stix2._get_external_reference_generated_id(
        42, "benchmark", "REF-1"
    )

    assert first == second
    assert opencti.external_reference.generate_id_calls == 2


class _ExternalReferencePrefetchRecorder(_ExternalReferenceRecorder):
    def __init__(self):
        super().__init__()
        self.list_filters = []
        self.create_payloads = []

    def list(self, **kwargs):
        ids = kwargs["filters"]["filters"][0]["values"]
        self.list_filters.append(ids)
        return [
            {
                "id": f"internal--{standard_id}",
                "standard_id": standard_id,
                "source_name": "benchmark",
                "url": standard_id.removeprefix("external-reference--"),
                "external_id": None,
                "description": None,
            }
            for standard_id in ids
        ]

    def create(self, **kwargs):
        self.create_calls += 1
        self.create_payloads.append(kwargs)
        return {
            "id": f"internal--{self._generated_id(kwargs['url'], kwargs['source_name'], kwargs['external_id'])}"
        }


def _external_reference_prefetch_opencti():
    opencti = _external_reference_opencti()
    opencti.external_reference = _ExternalReferencePrefetchRecorder()
    return opencti


def _import_bundle_extracting_relationships(opencti_stix2, objects, types=None):
    def import_item_with_retries(item, *_args, **_kwargs):
        opencti_stix2.extract_embedded_relationships(item, types)
        return None

    opencti_stix2.import_item_with_retries = import_item_with_retries
    return opencti_stix2.import_bundle(
        {
            "type": "bundle",
            "id": "bundle--benchmark",
            "objects": objects,
        },
        types=types,
    )


def test_import_bundle_reuses_exact_file_upload_external_reference_across_items():
    opencti = _external_reference_opencti()
    opencti_stix2 = OpenCTIStix2(opencti)
    objects = [
        {
            "id": f"malware--{index}",
            "type": "malware",
            "external_references": [
                {
                    "source_name": "benchmark",
                    "url": "https://example.test/reference",
                    "external_id": "REF-1",
                    "x_opencti_files": [
                        {
                            "name": "payload.txt",
                            "data": base64.b64encode(b"payload").decode("ascii"),
                            "mime_type": "text/plain",
                        }
                    ],
                }
            ],
        }
        for index in range(2)
    ]

    _import_bundle_extracting_relationships(opencti_stix2, objects)

    assert opencti.external_reference.create_calls == 1


@pytest.mark.parametrize(
    ("field_name", "second_value"),
    [
        pytest.param("name", "attachment.txt", id="name"),
        pytest.param("data", base64.b64encode(b"second").decode("ascii"), id="payload"),
        pytest.param("mime_type", "application/json", id="mime-type"),
        pytest.param(
            "object_marking_refs",
            ["marking-definition--2"],
            id="object-marking-refs",
        ),
        pytest.param("no_trigger_import", True, id="no-trigger-import"),
        pytest.param("embedded", True, id="embedded"),
    ],
)
def test_import_bundle_keeps_changed_file_upload_external_reference_uncached(
    field_name, second_value
):
    opencti = _external_reference_opencti()
    opencti_stix2 = OpenCTIStix2(opencti)
    first_file = {
        "name": "payload.txt",
        "data": base64.b64encode(b"payload").decode("ascii"),
        "mime_type": "text/plain",
        "object_marking_refs": ["marking-definition--1"],
        "no_trigger_import": False,
        "embedded": False,
    }
    second_file = dict(first_file)
    second_file[field_name] = second_value
    objects = [
        {
            "id": f"malware--{index}",
            "type": "malware",
            "external_references": [
                {
                    "source_name": "benchmark",
                    "url": "https://example.test/reference",
                    "external_id": "REF-1",
                    "x_opencti_files": [file_obj],
                }
            ],
        }
        for index, file_obj in enumerate((first_file, second_file))
    ]

    _import_bundle_extracting_relationships(opencti_stix2, objects)

    assert opencti.external_reference.create_calls == 2


def test_import_bundle_does_not_reuse_file_upload_external_reference_across_bundles():
    opencti = _external_reference_opencti()
    opencti_stix2 = OpenCTIStix2(opencti)
    objects = [
        {
            "id": "malware--shared",
            "type": "malware",
            "external_references": [
                {
                    "source_name": "benchmark",
                    "url": "https://example.test/reference",
                    "external_id": "REF-1",
                    "x_opencti_files": [
                        {
                            "name": "payload.txt",
                            "data": base64.b64encode(b"payload").decode("ascii"),
                            "mime_type": "text/plain",
                        }
                    ],
                }
            ],
        }
    ]

    _import_bundle_extracting_relationships(opencti_stix2, objects)
    _import_bundle_extracting_relationships(opencti_stix2, objects)

    assert opencti.external_reference.create_calls == 2


class _ExternalReferenceReportRecorder:
    def __init__(self, return_value=True):
        self.create_calls = []
        self.return_value = return_value

    @staticmethod
    def generate_fixed_fake_id(name, published=None):
        return f"report--{name}|{published}"

    def create(self, **kwargs):
        self.create_calls.append(kwargs)
        if self.return_value is None:
            return None
        return {"id": kwargs["id"]}


class _MarkingDefinitionRecorder:
    def __init__(self):
        self.read_calls = 0

    def read(self, **_kwargs):
        self.read_calls += 1
        return {"id": "marking-definition--tlp-clear"}


def _external_reference_report_opencti(return_value=True):
    opencti = _external_reference_opencti()
    opencti.report = _ExternalReferenceReportRecorder(return_value=return_value)
    opencti.marking_definition = _MarkingDefinitionRecorder()
    return opencti


def _external_reference_report_objects(descriptions):
    return [
        {
            "id": f"malware--{index}",
            "type": "malware",
            "external_references": [
                {
                    "source_name": "benchmark",
                    "url": "https://example.test/reference",
                    **({"description": description} if description is not None else {}),
                }
            ],
        }
        for index, description in enumerate(descriptions)
    ]


def test_import_bundle_reuses_exact_external_reference_report_mutation():
    opencti = _external_reference_report_opencti()
    opencti_stix2 = OpenCTIStix2(opencti)

    _import_bundle_extracting_relationships(
        opencti_stix2,
        _external_reference_report_objects([None, None]),
        ["external-reference-as-report"],
    )

    assert len(opencti.report.create_calls) == 1
    assert opencti.marking_definition.read_calls == 1


def test_import_bundle_keeps_changed_external_reference_report_mutations():
    opencti = _external_reference_report_opencti()
    opencti_stix2 = OpenCTIStix2(opencti)

    _import_bundle_extracting_relationships(
        opencti_stix2,
        _external_reference_report_objects(["first", "second"]),
        ["external-reference-as-report"],
    )

    assert [call["description"] for call in opencti.report.create_calls] == [
        "first",
        "second",
    ]


def test_import_bundle_does_not_reuse_external_reference_report_across_bundles():
    opencti = _external_reference_report_opencti()
    opencti_stix2 = OpenCTIStix2(opencti)
    objects = _external_reference_report_objects([None])

    _import_bundle_extracting_relationships(
        opencti_stix2, objects, ["external-reference-as-report"]
    )
    _import_bundle_extracting_relationships(
        opencti_stix2, objects, ["external-reference-as-report"]
    )

    assert len(opencti.report.create_calls) == 2


def test_extract_embedded_relationships_keeps_external_reference_report_uncached():
    opencti = _external_reference_report_opencti()
    opencti_stix2 = OpenCTIStix2(opencti)
    stix_object = _external_reference_report_objects([None])[0]

    opencti_stix2.extract_embedded_relationships(
        dict(stix_object), ["external-reference-as-report"]
    )
    opencti_stix2.extract_embedded_relationships(
        dict(stix_object), ["external-reference-as-report"]
    )

    assert len(opencti.report.create_calls) == 2


def test_import_bundle_keeps_unhashable_external_reference_report_inputs_uncached():
    opencti = _external_reference_report_opencti()
    opencti_stix2 = OpenCTIStix2(opencti)
    opencti_stix2.resolve_author = lambda _title: {"id": ["identity--1"]}

    _import_bundle_extracting_relationships(
        opencti_stix2,
        _external_reference_report_objects([None, None]),
        ["external-reference-as-report"],
    )

    assert len(opencti.report.create_calls) == 2


def test_import_bundle_keeps_none_external_reference_report_results_uncached():
    opencti = _external_reference_report_opencti(return_value=None)
    opencti_stix2 = OpenCTIStix2(opencti)

    _import_bundle_extracting_relationships(
        opencti_stix2,
        _external_reference_report_objects([None, None]),
        ["external-reference-as-report"],
    )

    assert len(opencti.report.create_calls) == 2


class _ReportRelationRecorder:
    def __init__(self, add_result=True):
        self.add_calls = []
        self.add_result = add_result

    def add_stix_object_or_stix_relationship(self, **kwargs):
        self.add_calls.append((kwargs["id"], kwargs["stixObjectOrStixRelationshipId"]))
        return self.add_result


class _StixCoreRelationshipImportRecorder:
    @staticmethod
    def import_from_stix2(**kwargs):
        stix_relation = kwargs["stixRelation"]
        return {
            "id": stix_relation["id"],
            "entity_type": "stix-core-relationship",
        }


def _build_report_relation_importer(add_result=True, report_id="report--shared"):
    opencti = SimpleNamespace(
        report=_ReportRelationRecorder(add_result=add_result),
        stix_core_relationship=_StixCoreRelationshipImportRecorder(),
        app_logger=SimpleNamespace(warning=lambda *args, **kwargs: None),
        get_draft_id=lambda: "",
        get_attribute_in_extension=lambda _attribute, _entity: None,
        query=lambda _query: {"data": {"vocabularyCategories": []}},
        logger_class=lambda _name: SimpleNamespace(warning=lambda *args: None),
    )
    opencti_stix2 = OpenCTIStix2(opencti)
    opencti_stix2.extract_embedded_relationships = lambda *_args, **_kwargs: {
        "created_by": None,
        "object_marking": None,
        "object_label": [],
        "open_vocabs": {},
        "granted_refs": [],
        "kill_chain_phases": [],
        "object_refs": [],
        "external_references": ["external-reference--shared"],
        "reports": {"external-reference--shared": {"id": report_id}},
        "sample_refs": [],
    }
    opencti_stix2._prepare_bundle_from_backend_plan = lambda bundle, _plan: (
        0,
        [],
        bundle["objects"],
    )
    return opencti, opencti_stix2


def _shared_report_relationships(count):
    return [
        {
            "id": f"relationship--{index}",
            "type": "relationship",
            "source_ref": "malware--shared-source",
            "target_ref": "indicator--shared-target",
        }
        for index in range(count)
    ]


def _import_bundle_relationships(opencti_stix2, relationships, types=None):
    def import_item_with_retries(item, *_args, **_kwargs):
        opencti_stix2.import_relationship(item, types=types)
        return None

    opencti_stix2.import_item_with_retries = import_item_with_retries
    return opencti_stix2.import_bundle(
        {
            "type": "bundle",
            "id": "bundle--benchmark",
            "objects": relationships,
        },
        types=types,
    )


def test_import_bundle_reuses_report_relation_adds_for_shared_endpoints():
    opencti, opencti_stix2 = _build_report_relation_importer()

    _import_bundle_relationships(
        opencti_stix2,
        _shared_report_relationships(2),
        ["external-reference-as-report"],
    )

    assert opencti.report.add_calls == [
        ("report--shared", "relationship--0"),
        ("report--shared", "malware--shared-source"),
        ("report--shared", "indicator--shared-target"),
        ("report--shared", "relationship--1"),
    ]


def test_import_bundle_does_not_reuse_report_relation_adds_across_bundles():
    opencti, opencti_stix2 = _build_report_relation_importer()
    relationships = _shared_report_relationships(1)

    _import_bundle_relationships(
        opencti_stix2, relationships, ["external-reference-as-report"]
    )
    _import_bundle_relationships(
        opencti_stix2, relationships, ["external-reference-as-report"]
    )

    assert len(opencti.report.add_calls) == 6


def test_import_relationship_keeps_report_relation_adds_uncached_without_bundle_scope():
    opencti, opencti_stix2 = _build_report_relation_importer()
    relationship = _shared_report_relationships(1)[0]

    opencti_stix2.import_relationship(
        relationship, types=["external-reference-as-report"]
    )
    opencti_stix2.import_relationship(
        relationship, types=["external-reference-as-report"]
    )

    assert len(opencti.report.add_calls) == 6


def test_import_bundle_keeps_false_report_relation_adds_uncached():
    opencti, opencti_stix2 = _build_report_relation_importer(add_result=False)

    _import_bundle_relationships(
        opencti_stix2,
        _shared_report_relationships(2),
        ["external-reference-as-report"],
    )

    assert len(opencti.report.add_calls) == 6


def test_import_bundle_keeps_report_relation_adds_distinct_across_drafts():
    opencti, opencti_stix2 = _build_report_relation_importer()
    draft_id = {"value": "draft--one"}
    opencti.get_draft_id = lambda: draft_id["value"]
    relationships = [_shared_report_relationships(1)[0]] * 2
    draft_ids = iter(["draft--one", "draft--two"])

    def import_item_with_retries(item, *_args, **_kwargs):
        draft_id["value"] = next(draft_ids)
        opencti_stix2.import_relationship(item, types=["external-reference-as-report"])
        return None

    opencti_stix2.import_item_with_retries = import_item_with_retries
    opencti_stix2.import_bundle(
        {
            "type": "bundle",
            "id": "bundle--benchmark",
            "objects": relationships,
        },
        types=["external-reference-as-report"],
    )

    assert len(opencti.report.add_calls) == 6


def test_import_bundle_keeps_unhashable_report_relation_adds_uncached():
    opencti, opencti_stix2 = _build_report_relation_importer(
        report_id=["report--shared"]
    )

    _import_bundle_relationships(
        opencti_stix2,
        _shared_report_relationships(2),
        ["external-reference-as-report"],
    )

    assert len(opencti.report.add_calls) == 6


def test_import_bundle_keeps_report_relation_adds_uncached_without_report_mode():
    opencti, opencti_stix2 = _build_report_relation_importer()

    _import_bundle_relationships(opencti_stix2, _shared_report_relationships(2))

    assert len(opencti.report.add_calls) == 6


@pytest.mark.parametrize(
    "field_name", ["external_references", "x_opencti_external_references"]
)
def test_import_bundle_prefetches_existing_external_references_before_item_import(
    field_name,
):
    opencti = _external_reference_prefetch_opencti()
    opencti_stix2 = OpenCTIStix2(opencti)
    objects = [
        {
            "id": f"malware--{index}",
            "type": "malware",
            field_name: [
                {
                    "source_name": "benchmark",
                    "url": f"https://example.test/reference/{index}",
                }
            ],
        }
        for index in range(3)
    ]

    _import_bundle_extracting_relationships(opencti_stix2, objects)

    assert opencti.external_reference.list_filters == [
        [
            "external-reference--https://example.test/reference/0",
            "external-reference--https://example.test/reference/1",
            "external-reference--https://example.test/reference/2",
        ]
    ]
    assert opencti.external_reference.create_calls == 0


def test_import_bundle_keeps_single_external_reference_on_per_item_create():
    opencti = _external_reference_prefetch_opencti()
    opencti_stix2 = OpenCTIStix2(opencti)

    _import_bundle_extracting_relationships(
        opencti_stix2,
        [
            {
                "id": "malware--1",
                "type": "malware",
                "external_references": [
                    {
                        "source_name": "benchmark",
                        "url": "https://example.test/reference/1",
                    }
                ],
            }
        ],
    )

    assert opencti.external_reference.list_filters == []
    assert opencti.external_reference.create_calls == 1


def test_import_bundle_prefetches_existing_external_references_in_bounded_chunks():
    opencti = _external_reference_prefetch_opencti()
    opencti_stix2 = OpenCTIStix2(opencti)
    objects = [
        {
            "id": f"malware--{index}",
            "type": "malware",
            "external_references": [
                {
                    "source_name": "benchmark",
                    "url": f"https://example.test/reference/{index}",
                }
            ],
        }
        for index in range(IMPORT_PREFETCH_BATCH_SIZE + 1)
    ]

    _import_bundle_extracting_relationships(opencti_stix2, objects)

    assert opencti.external_reference.list_filters[0] == [
        f"external-reference--https://example.test/reference/{index}"
        for index in range(IMPORT_PREFETCH_BATCH_SIZE)
    ]
    assert opencti.external_reference.list_filters[1] == [
        f"external-reference--https://example.test/reference/{IMPORT_PREFETCH_BATCH_SIZE}"
    ]
    assert opencti.external_reference.create_calls == 0


def test_import_bundle_keeps_changed_external_reference_metadata_on_per_item_create():
    opencti = _external_reference_prefetch_opencti()
    opencti_stix2 = OpenCTIStix2(opencti)
    objects = [
        {
            "id": f"malware--{index}",
            "type": "malware",
            "external_references": [
                {
                    "source_name": "benchmark",
                    "url": f"https://example.test/reference/{index}",
                    "description": "changed",
                }
            ],
        }
        for index in range(2)
    ]

    _import_bundle_extracting_relationships(opencti_stix2, objects)

    assert opencti.external_reference.list_filters == [
        [
            "external-reference--https://example.test/reference/0",
            "external-reference--https://example.test/reference/1",
        ]
    ]
    assert [
        payload["description"] for payload in opencti.external_reference.create_payloads
    ] == ["changed", "changed"]


def test_import_bundle_skips_file_external_references_during_prefetch():
    opencti = _external_reference_prefetch_opencti()
    opencti_stix2 = OpenCTIStix2(opencti)
    objects = [
        {
            "id": f"malware--{index}",
            "type": "malware",
            "external_references": [
                {
                    "source_name": "benchmark",
                    "url": f"https://example.test/reference/{index}",
                    "x_opencti_files": [
                        {
                            "name": "payload.txt",
                            "data": base64.b64encode(b"payload").decode("ascii"),
                        }
                    ],
                }
            ],
        }
        for index in range(2)
    ]

    _import_bundle_extracting_relationships(opencti_stix2, objects)

    assert opencti.external_reference.list_filters == []
    assert opencti.external_reference.create_calls == 2


def test_import_bundle_falls_back_to_per_item_external_reference_create_when_prefetch_fails():
    opencti = _external_reference_prefetch_opencti()
    opencti_stix2 = OpenCTIStix2(opencti)
    opencti.external_reference.list = lambda **_kwargs: (_ for _ in ()).throw(
        RuntimeError("prefetch failed")
    )
    objects = [
        {
            "id": f"malware--{index}",
            "type": "malware",
            "external_references": [
                {
                    "source_name": "benchmark",
                    "url": f"https://example.test/reference/{index}",
                }
            ],
        }
        for index in range(2)
    ]

    _import_bundle_extracting_relationships(opencti_stix2, objects)

    assert [
        payload["url"] for payload in opencti.external_reference.create_payloads
    ] == [
        "https://example.test/reference/0",
        "https://example.test/reference/1",
    ]


class _KillChainPhasePrefetchRecorder:
    def __init__(self):
        self.list_filters = []
        self.list_first = []
        self.create_payloads = []
        self.list_order = 0

    @staticmethod
    def generate_id(phase_name, kill_chain_name):
        return f"kill-chain-phase--{kill_chain_name}|{phase_name}"

    def list(self, **kwargs):
        ids = kwargs["filters"]["filters"][0]["values"]
        self.list_filters.append(ids)
        self.list_first.append(kwargs["first"])
        return [
            {
                "id": f"internal--{standard_id}",
                "standard_id": standard_id,
                "entity_type": "Kill-Chain-Phase",
                "kill_chain_name": standard_id.removeprefix("kill-chain-phase--").split(
                    "|", 1
                )[0],
                "phase_name": standard_id.removeprefix("kill-chain-phase--").split(
                    "|", 1
                )[1],
                "x_opencti_order": self.list_order,
            }
            for standard_id in ids
        ]

    def create(self, **kwargs):
        self.create_payloads.append(kwargs)
        standard_id = self.generate_id(kwargs["phase_name"], kwargs["kill_chain_name"])
        return {
            "id": f"internal--{standard_id}",
            "standard_id": standard_id,
            "entity_type": "Kill-Chain-Phase",
        }


def _kill_chain_phase_prefetch_opencti():
    opencti = _external_reference_opencti()
    opencti.kill_chain_phase = _KillChainPhasePrefetchRecorder()
    return opencti


def test_get_import_kill_chain_phases_preserves_field_precedence():
    opencti = _kill_chain_phase_prefetch_opencti()
    extension_phase = [{"kill_chain_name": "extension", "phase_name": "phase"}]
    legacy_phase = [{"kill_chain_name": "legacy", "phase_name": "phase"}]
    top_level_phase = [{"kill_chain_name": "top-level", "phase_name": "phase"}]
    extension_reads = []

    def get_attribute_in_extension(attribute, _entity):
        extension_reads.append(attribute)
        return extension_phase if attribute == "kill_chain_phases" else None

    opencti.get_attribute_in_extension = get_attribute_in_extension
    opencti_stix2 = OpenCTIStix2(opencti)

    assert (
        opencti_stix2._get_import_kill_chain_phases(
            {
                "kill_chain_phases": top_level_phase,
                "x_opencti_kill_chain_phases": legacy_phase,
            }
        )
        == top_level_phase
    )
    assert extension_reads == []
    assert (
        opencti_stix2._get_import_kill_chain_phases(
            {
                "kill_chain_phases": None,
                "x_opencti_kill_chain_phases": legacy_phase,
            }
        )
        == legacy_phase
    )
    assert extension_reads == []
    assert (
        opencti_stix2._get_import_kill_chain_phases(
            {"x_opencti_kill_chain_phases": legacy_phase}
        )
        == extension_phase
    )
    assert extension_reads == ["kill_chain_phases"]

    opencti.get_attribute_in_extension = lambda _attribute, _entity: None
    assert (
        opencti_stix2._get_import_kill_chain_phases(
            {"x_opencti_kill_chain_phases": legacy_phase}
        )
        == legacy_phase
    )


@pytest.mark.parametrize(
    "field_name", ["kill_chain_phases", "x_opencti_kill_chain_phases"]
)
def test_import_bundle_prefetches_existing_kill_chain_phases_before_item_import(
    field_name,
):
    opencti = _kill_chain_phase_prefetch_opencti()
    opencti_stix2 = OpenCTIStix2(opencti)
    objects = [
        {
            "id": f"malware--{index}",
            "type": "malware",
            field_name: [
                {
                    "kill_chain_name": "benchmark",
                    "phase_name": f"phase-{index}",
                }
            ],
        }
        for index in range(3)
    ]

    _import_bundle_extracting_relationships(opencti_stix2, objects)

    assert opencti.kill_chain_phase.list_filters == [
        [
            "kill-chain-phase--benchmark|phase-0",
            "kill-chain-phase--benchmark|phase-1",
            "kill-chain-phase--benchmark|phase-2",
        ]
    ]
    assert opencti.kill_chain_phase.list_first == [3]
    assert opencti.kill_chain_phase.create_payloads == []


def test_import_bundle_keeps_single_kill_chain_phase_on_per_item_create():
    opencti = _kill_chain_phase_prefetch_opencti()
    opencti_stix2 = OpenCTIStix2(opencti)

    _import_bundle_extracting_relationships(
        opencti_stix2,
        [
            {
                "id": "malware--1",
                "type": "malware",
                "kill_chain_phases": [
                    {
                        "kill_chain_name": "benchmark",
                        "phase_name": "phase-1",
                    }
                ],
            }
        ],
    )

    assert opencti.kill_chain_phase.list_filters == []
    assert [
        payload["phase_name"] for payload in opencti.kill_chain_phase.create_payloads
    ] == ["phase-1"]


def test_import_bundle_prefetches_existing_kill_chain_phases_in_bounded_chunks():
    opencti = _kill_chain_phase_prefetch_opencti()
    opencti_stix2 = OpenCTIStix2(opencti)
    objects = [
        {
            "id": f"malware--{index}",
            "type": "malware",
            "kill_chain_phases": [
                {
                    "kill_chain_name": "benchmark",
                    "phase_name": f"phase-{index}",
                }
            ],
        }
        for index in range(IMPORT_PREFETCH_BATCH_SIZE + 1)
    ]

    _import_bundle_extracting_relationships(opencti_stix2, objects)

    assert opencti.kill_chain_phase.list_filters[0] == [
        f"kill-chain-phase--benchmark|phase-{index}"
        for index in range(IMPORT_PREFETCH_BATCH_SIZE)
    ]
    assert opencti.kill_chain_phase.list_filters[1] == [
        f"kill-chain-phase--benchmark|phase-{IMPORT_PREFETCH_BATCH_SIZE}"
    ]
    assert opencti.kill_chain_phase.list_first == [IMPORT_PREFETCH_BATCH_SIZE, 1]
    assert opencti.kill_chain_phase.create_payloads == []


def test_import_bundle_keeps_changed_kill_chain_phase_order_on_per_item_create():
    opencti = _kill_chain_phase_prefetch_opencti()
    opencti_stix2 = OpenCTIStix2(opencti)
    objects = [
        {
            "id": f"malware--{index}",
            "type": "malware",
            "kill_chain_phases": [
                {
                    "kill_chain_name": "benchmark",
                    "phase_name": f"phase-{index}",
                    "x_opencti_order": 1,
                }
            ],
        }
        for index in range(2)
    ]

    _import_bundle_extracting_relationships(opencti_stix2, objects)

    assert opencti.kill_chain_phase.list_filters == [
        [
            "kill-chain-phase--benchmark|phase-0",
            "kill-chain-phase--benchmark|phase-1",
        ]
    ]
    assert [
        payload["x_opencti_order"]
        for payload in opencti.kill_chain_phase.create_payloads
    ] == [1, 1]


def test_import_bundle_keeps_id_bearing_kill_chain_phase_on_per_item_create():
    opencti = _kill_chain_phase_prefetch_opencti()
    opencti_stix2 = OpenCTIStix2(opencti)
    objects = [
        {
            "id": f"malware--{index}",
            "type": "malware",
            "kill_chain_phases": [
                {
                    "id": f"kill-chain-phase--explicit-{index}",
                    "kill_chain_name": "benchmark",
                    "phase_name": f"phase-{index}",
                }
            ],
        }
        for index in range(2)
    ]

    _import_bundle_extracting_relationships(opencti_stix2, objects)

    assert opencti.kill_chain_phase.list_filters == []
    assert [
        payload["stix_id"] for payload in opencti.kill_chain_phase.create_payloads
    ] == [
        "kill-chain-phase--explicit-0",
        "kill-chain-phase--explicit-1",
    ]


def test_import_bundle_falls_back_to_per_item_kill_chain_phase_create_when_prefetch_fails():
    opencti = _kill_chain_phase_prefetch_opencti()
    opencti_stix2 = OpenCTIStix2(opencti)
    opencti.kill_chain_phase.list = lambda **_kwargs: (_ for _ in ()).throw(
        RuntimeError("prefetch failed")
    )
    objects = [
        {
            "id": f"malware--{index}",
            "type": "malware",
            "kill_chain_phases": [
                {
                    "kill_chain_name": "benchmark",
                    "phase_name": f"phase-{index}",
                }
            ],
        }
        for index in range(2)
    ]

    _import_bundle_extracting_relationships(opencti_stix2, objects)

    assert [
        payload["phase_name"] for payload in opencti.kill_chain_phase.create_payloads
    ] == ["phase-0", "phase-1"]


class _LabelPrefetchRecorder:
    def __init__(self):
        self.list_filters = []
        self.read_or_create_calls = []
        self.create_payloads = []
        self.existing_values = None
        self.list_result_values = None

    def list(self, **kwargs):
        values = kwargs["filters"]["filters"][0]["values"]
        self.list_filters.append(values)
        result_values = (
            self.list_result_values if self.list_result_values is not None else values
        )
        return [
            {"id": f"label--{value}", "value": value}
            for value in result_values
            if self.existing_values is None or value in self.existing_values
        ]

    def read_or_create_unchecked(self, **kwargs):
        value = kwargs["value"]
        self.read_or_create_calls.append(value)
        return {"id": f"label--{value}", "value": value}

    def create(self, **kwargs):
        self.create_payloads.append(kwargs)
        value = kwargs["value"]
        return {"id": f"label--{value}", "value": value}


def _label_prefetch_opencti():
    opencti = _external_reference_opencti()
    opencti.label = _LabelPrefetchRecorder()
    return opencti


def test_get_import_label_values_preserves_field_precedence():
    opencti = _label_prefetch_opencti()
    opencti.get_attribute_in_extension = lambda attribute, _entity: (
        ["extension-label"] if attribute == "labels" else None
    )
    opencti_stix2 = OpenCTIStix2(opencti)

    assert opencti_stix2._get_import_label_values(
        {
            "labels": ["label"],
            "x_opencti_labels": ["x-label"],
            "x_opencti_tags": [{"value": "tag"}],
        }
    ) == ["label"]
    assert opencti_stix2._get_import_label_values(
        {
            "x_opencti_labels": ["x-label"],
            "x_opencti_tags": [{"value": "tag"}],
        }
    ) == ["extension-label"]

    opencti.get_attribute_in_extension = lambda _attribute, _entity: None
    assert opencti_stix2._get_import_label_values(
        {
            "x_opencti_labels": ["x-label"],
            "x_opencti_tags": [{"value": "tag"}],
        }
    ) == ["x-label"]
    assert opencti_stix2._get_import_label_values(
        {"x_opencti_tags": [{"value": "tag"}]}
    ) == ["tag"]


def test_import_bundle_prefetches_existing_labels_before_item_import():
    opencti = _label_prefetch_opencti()
    opencti_stix2 = OpenCTIStix2(opencti)
    objects = [
        {
            "id": f"malware--{index}",
            "type": "malware",
            "labels": [f"label-{index}"],
        }
        for index in range(3)
    ]

    _import_bundle_extracting_relationships(opencti_stix2, objects)

    assert opencti.label.list_filters == [["label-0", "label-1", "label-2"]]
    assert opencti.label.read_or_create_calls == []


def test_import_bundle_keeps_single_label_on_per_item_resolution():
    opencti = _label_prefetch_opencti()
    opencti_stix2 = OpenCTIStix2(opencti)

    _import_bundle_extracting_relationships(
        opencti_stix2,
        [
            {
                "id": "malware--1",
                "type": "malware",
                "labels": ["label-1"],
            }
        ],
    )

    assert opencti.label.list_filters == []
    assert opencti.label.read_or_create_calls == ["label-1"]


def test_import_bundle_prefetches_existing_labels_in_bounded_chunks():
    opencti = _label_prefetch_opencti()
    opencti_stix2 = OpenCTIStix2(opencti)
    objects = [
        {
            "id": f"malware--{index}",
            "type": "malware",
            "labels": [f"label-{index}"],
        }
        for index in range(IMPORT_PREFETCH_BATCH_SIZE + 1)
    ]

    _import_bundle_extracting_relationships(opencti_stix2, objects)

    assert opencti.label.list_filters[0] == [
        f"label-{index}" for index in range(IMPORT_PREFETCH_BATCH_SIZE)
    ]
    assert opencti.label.list_filters[1] == [f"label-{IMPORT_PREFETCH_BATCH_SIZE}"]
    assert opencti.label.read_or_create_calls == []


def test_import_bundle_skips_redundant_reads_for_labels_prefetched_as_missing():
    opencti = _label_prefetch_opencti()
    opencti.label.existing_values = set()
    opencti_stix2 = OpenCTIStix2(opencti)
    objects = [
        {
            "id": f"malware--{index}",
            "type": "malware",
            "labels": [f"label-{index}"],
        }
        for index in range(3)
    ]

    _import_bundle_extracting_relationships(opencti_stix2, objects)

    assert opencti.label.list_filters == [["label-0", "label-1", "label-2"]]
    assert opencti.label.read_or_create_calls == []
    assert [payload["value"] for payload in opencti.label.create_payloads] == [
        "label-0",
        "label-1",
        "label-2",
    ]


def test_import_bundle_preserves_tag_color_for_labels_prefetched_as_missing():
    opencti = _label_prefetch_opencti()
    opencti.label.existing_values = set()
    opencti_stix2 = OpenCTIStix2(opencti)

    _import_bundle_extracting_relationships(
        opencti_stix2,
        [
            {
                "id": "malware--1",
                "type": "malware",
                "x_opencti_tags": [
                    {"value": "tag-0", "color": "#123456"},
                    {"value": "tag-1", "color": "#654321"},
                ],
            }
        ],
    )

    assert opencti.label.read_or_create_calls == []
    assert opencti.label.create_payloads == [
        {"value": "tag-0", "color": "#123456"},
        {"value": "tag-1", "color": "#654321"},
    ]


def test_import_bundle_reuses_normalized_label_prefetch_matches():
    opencti = _label_prefetch_opencti()
    opencti.label.list_result_values = ["label-0", "label-1"]
    opencti_stix2 = OpenCTIStix2(opencti)
    objects = [
        {"id": "malware--0", "type": "malware", "labels": ["LABEL-0"]},
        {"id": "malware--1", "type": "malware", "labels": [" label-1 "]},
    ]

    _import_bundle_extracting_relationships(opencti_stix2, objects)

    assert opencti.label.list_filters == [["LABEL-0", " label-1 "]]
    assert opencti.label.read_or_create_calls == []
    assert opencti.label.create_payloads == []
    assert opencti_stix2.get_in_cache("label_LABEL-0")["id"] == "label--label-0"
    assert opencti_stix2.get_in_cache("label_ label-1 ")["id"] == "label--label-1"


def test_import_bundle_does_not_reuse_missing_label_proof_between_imports():
    opencti = _label_prefetch_opencti()
    opencti.label.existing_values = set()
    opencti_stix2 = OpenCTIStix2(opencti)

    _import_bundle_extracting_relationships(
        opencti_stix2,
        [
            {"id": "malware--0", "type": "malware", "labels": ["label-0"]},
            {"id": "malware--1", "type": "malware", "labels": ["label-1"]},
        ],
    )
    _import_bundle_extracting_relationships(
        opencti_stix2,
        [{"id": "malware--2", "type": "malware", "labels": ["label-2"]}],
    )

    assert [payload["value"] for payload in opencti.label.create_payloads] == [
        "label-0",
        "label-1",
    ]
    assert opencti.label.read_or_create_calls == ["label-2"]


def test_import_bundle_falls_back_to_per_item_label_resolution_when_prefetch_fails():
    opencti = _label_prefetch_opencti()
    opencti_stix2 = OpenCTIStix2(opencti)
    opencti.label.list = lambda **_kwargs: (_ for _ in ()).throw(
        RuntimeError("prefetch failed")
    )
    objects = [
        {
            "id": f"malware--{index}",
            "type": "malware",
            "labels": [f"label-{index}"],
        }
        for index in range(2)
    ]

    _import_bundle_extracting_relationships(opencti_stix2, objects)

    assert opencti.label.read_or_create_calls == ["label-0", "label-1"]
    assert opencti.label.create_payloads == []


class _VocabularyPrefetchRecorder:
    def __init__(self):
        self.list_filters = []
        self.list_kwargs = []
        self.read_or_create_calls = []
        self.create_calls = []
        self.existing_values = None
        self.list_results = None

    def list(self, **kwargs):
        self.list_kwargs.append(kwargs)
        values = kwargs["filters"]["filters"][0]["values"]
        self.list_filters.append(values)
        if self.list_results is not None:
            return self.list_results
        return [
            {
                "id": f"vocabulary--{value}",
                "name": value,
                "category": {"key": "malware_type_ov"},
            }
            for value in values
            if self.existing_values is None or value in self.existing_values
        ]

    def read_or_create_unchecked_with_cache(self, vocab, cache, field):
        category = field.get("category", cache.get("category_" + field["key"]))
        vocab_key = (
            f"vocab_{category}_{vocab}" if category is not None else "vocab_" + vocab
        )
        if vocab_key not in cache:
            self.read_or_create_calls.append((vocab, field["required"], category))
            cache[vocab_key] = {
                "id": f"vocabulary--{vocab}",
                "name": vocab,
                "category": {"key": category},
            }
        return cache[vocab_key]

    def create(self, **kwargs):
        self.create_calls.append(
            (kwargs["name"], kwargs["required"], kwargs["category"])
        )
        return {
            "id": f"vocabulary--{kwargs['name']}",
            "name": kwargs["name"],
            "category": {"key": kwargs["category"]},
        }


def _vocabulary_prefetch_opencti(vocabulary_categories=None):
    opencti = _external_reference_opencti()
    opencti.vocabulary = _VocabularyPrefetchRecorder()
    if vocabulary_categories is None:
        vocabulary_categories = [
            {
                "key": "malware_type_ov",
                "entity_types": ["Malware"],
                "fields": [
                    {
                        "key": "malware_types",
                        "required": False,
                        "multiple": True,
                    }
                ],
            }
        ]
    opencti.query = lambda _query: {
        "data": {"vocabularyCategories": vocabulary_categories}
    }
    return opencti


def test_import_bundle_prefetches_existing_vocabularies_before_item_import():
    opencti = _vocabulary_prefetch_opencti()
    opencti_stix2 = OpenCTIStix2(opencti)
    objects = [
        {
            "id": f"malware--{index}",
            "type": "malware",
            "malware_types": [f"vocab-{index}"],
        }
        for index in range(3)
    ]

    _import_bundle_extracting_relationships(opencti_stix2, objects)

    assert opencti.vocabulary.list_filters == [["vocab-0", "vocab-1", "vocab-2"]]
    assert opencti.vocabulary.list_kwargs[0]["first"] == IMPORT_PREFETCH_BATCH_SIZE
    assert opencti.vocabulary.list_kwargs[0]["getAll"] is True
    assert opencti.vocabulary.read_or_create_calls == []


def test_import_bundle_keeps_single_vocabulary_on_per_item_resolution():
    opencti = _vocabulary_prefetch_opencti()
    opencti_stix2 = OpenCTIStix2(opencti)

    _import_bundle_extracting_relationships(
        opencti_stix2,
        [
            {
                "id": "malware--1",
                "type": "malware",
                "malware_types": ["vocab-1"],
            }
        ],
    )

    assert opencti.vocabulary.list_filters == []
    assert opencti.vocabulary.read_or_create_calls == [
        ("vocab-1", False, "malware_type_ov")
    ]


def test_import_bundle_prefetches_existing_vocabularies_in_bounded_chunks():
    opencti = _vocabulary_prefetch_opencti()
    opencti_stix2 = OpenCTIStix2(opencti)
    objects = [
        {
            "id": f"malware--{index}",
            "type": "malware",
            "malware_types": [f"vocab-{index}"],
        }
        for index in range(IMPORT_PREFETCH_BATCH_SIZE + 1)
    ]

    _import_bundle_extracting_relationships(opencti_stix2, objects)

    assert opencti.vocabulary.list_filters[0] == [
        f"vocab-{index}" for index in range(IMPORT_PREFETCH_BATCH_SIZE)
    ]
    assert opencti.vocabulary.list_filters[1] == [f"vocab-{IMPORT_PREFETCH_BATCH_SIZE}"]
    assert opencti.vocabulary.read_or_create_calls == []


def test_import_bundle_reuses_normalized_vocabulary_prefetch_matches():
    opencti = _vocabulary_prefetch_opencti()
    opencti.vocabulary.list_results = [
        {
            "id": "vocabulary--csirt",
            "name": "csirt",
            "category": {"key": "malware_type_ov"},
        },
        {
            "id": "vocabulary--malware",
            "name": "malware",
            "category": {"key": "malware_type_ov"},
        },
    ]
    opencti_stix2 = OpenCTIStix2(opencti)
    objects = [
        {"id": "malware--0", "type": "malware", "malware_types": ["CSIRT"]},
        {"id": "malware--1", "type": "malware", "malware_types": [" malware "]},
    ]

    _import_bundle_extracting_relationships(opencti_stix2, objects)

    assert opencti.vocabulary.list_filters == [["CSIRT", " malware "]]
    assert opencti.vocabulary.read_or_create_calls == []
    assert (
        opencti_stix2.mapping_cache_permanent["vocab_malware_type_ov_CSIRT"]["id"]
        == "vocabulary--csirt"
    )
    assert (
        opencti_stix2.mapping_cache_permanent["vocab_malware_type_ov_ malware "]["id"]
        == "vocabulary--malware"
    )
    assert opencti.vocabulary.create_calls == []


def test_import_bundle_does_not_seed_ambiguous_vocabulary_aliases_without_category():
    opencti = _vocabulary_prefetch_opencti(
        [
            {
                "key": "threat_actor_group_role_ov",
                "entity_types": ["Threat-Actor-Group"],
                "fields": [{"key": "roles", "required": False, "multiple": True}],
            },
            {
                "key": "threat_actor_individual_role_ov",
                "entity_types": ["Threat-Actor-Individual"],
                "fields": [{"key": "roles", "required": False, "multiple": True}],
            },
        ]
    )
    opencti.vocabulary.list_results = [
        {"id": "vocabulary--agent", "name": "agent"},
        {"id": "vocabulary--independent", "name": "independent"},
    ]
    opencti_stix2 = OpenCTIStix2(opencti)
    objects = [
        {
            "id": "threat-actor--0",
            "type": "threat-actor",
            "x_opencti_type": "Threat-Actor-Group",
            "roles": ["agent", "independent"],
        },
        {
            "id": "threat-actor--1",
            "type": "threat-actor",
            "x_opencti_type": "Threat-Actor-Individual",
            "roles": ["agent", "independent"],
        },
    ]

    _import_bundle_extracting_relationships(opencti_stix2, objects)

    assert opencti.vocabulary.list_filters == [["agent", "independent"]]
    assert opencti.vocabulary.read_or_create_calls == [
        ("agent", False, "threat_actor_group_role_ov"),
        ("independent", False, "threat_actor_group_role_ov"),
        ("agent", False, "threat_actor_individual_role_ov"),
        ("independent", False, "threat_actor_individual_role_ov"),
    ]
    assert opencti.vocabulary.create_calls == []


def test_prefetch_import_vocabularies_keeps_batch_cache_entries_scoped():
    opencti = _vocabulary_prefetch_opencti()
    opencti_stix2 = OpenCTIStix2(opencti)
    objects = [
        {"id": "malware--0", "type": "malware", "malware_types": ["vocab-0"]},
        {"id": "malware--1", "type": "malware", "malware_types": ["vocab-1"]},
    ]
    vocab_key = "vocab_malware_type_ov_vocab-0"

    with opencti_stix2.batch_mapping_cache():
        opencti_stix2._prefetch_import_vocabularies(objects)
        assert vocab_key in opencti_stix2._get_mapping_cache_permanent()
        assert vocab_key not in opencti_stix2.mapping_cache_permanent

    assert vocab_key not in opencti_stix2.mapping_cache_permanent


def test_import_bundle_falls_back_to_per_item_vocabulary_resolution_when_prefetch_fails():
    opencti = _vocabulary_prefetch_opencti()
    opencti_stix2 = OpenCTIStix2(opencti)
    opencti.vocabulary.list = lambda **_kwargs: (_ for _ in ()).throw(
        RuntimeError("prefetch failed")
    )
    objects = [
        {
            "id": f"malware--{index}",
            "type": "malware",
            "malware_types": [f"vocab-{index}"],
        }
        for index in range(2)
    ]

    _import_bundle_extracting_relationships(opencti_stix2, objects)

    assert opencti.vocabulary.read_or_create_calls == [
        ("vocab-0", False, "malware_type_ov"),
        ("vocab-1", False, "malware_type_ov"),
    ]
    assert opencti.vocabulary.create_calls == []


def test_import_bundle_skips_redundant_reads_for_vocabularies_prefetched_as_missing():
    opencti = _vocabulary_prefetch_opencti()
    opencti.vocabulary.existing_values = set()
    opencti_stix2 = OpenCTIStix2(opencti)
    objects = [
        {
            "id": f"malware--{index}",
            "type": "malware",
            "malware_types": [f"vocab-{index}"],
        }
        for index in range(3)
    ]

    _import_bundle_extracting_relationships(opencti_stix2, objects)

    assert opencti.vocabulary.list_filters == [["vocab-0", "vocab-1", "vocab-2"]]
    assert opencti.vocabulary.read_or_create_calls == []
    assert opencti.vocabulary.create_calls == [
        ("vocab-0", False, "malware_type_ov"),
        ("vocab-1", False, "malware_type_ov"),
        ("vocab-2", False, "malware_type_ov"),
    ]


def test_import_bundle_does_not_reuse_missing_vocabulary_proof_between_imports():
    opencti = _vocabulary_prefetch_opencti()
    opencti.vocabulary.existing_values = set()
    opencti_stix2 = OpenCTIStix2(opencti)
    objects = [
        {"id": "malware--0", "type": "malware", "malware_types": ["vocab-0"]},
        {"id": "malware--1", "type": "malware", "malware_types": ["vocab-1"]},
    ]

    _import_bundle_extracting_relationships(opencti_stix2, objects)

    opencti_stix2.mapping_cache_permanent.pop("vocab_malware_type_ov_vocab-0")
    opencti.vocabulary.read_or_create_calls.clear()
    opencti.vocabulary.create_calls.clear()
    _import_bundle_extracting_relationships(
        opencti_stix2,
        [{"id": "malware--2", "type": "malware", "malware_types": ["vocab-0"]}],
    )

    assert opencti.vocabulary.read_or_create_calls == [
        ("vocab-0", False, "malware_type_ov")
    ]
    assert opencti.vocabulary.create_calls == []


def test_pick_aliases(opencti_stix2: OpenCTIStix2) -> None:
    stix_object = {}
    assert opencti_stix2.pick_aliases(stix_object) is None
    stix_object["aliases"] = "alias"
    assert opencti_stix2.pick_aliases(stix_object) == "alias"
    stix_object["x_amitt_aliases"] = "amitt_alias"
    assert opencti_stix2.pick_aliases(stix_object) == "amitt_alias"
    stix_object["x_mitre_aliases"] = "mitre_alias"
    assert opencti_stix2.pick_aliases(stix_object) == "mitre_alias"
    stix_object["x_opencti_aliases"] = "opencti_alias"
    assert opencti_stix2.pick_aliases(stix_object) == "opencti_alias"


def test_import_bundle_from_file(opencti_stix2: OpenCTIStix2, caplog) -> None:
    opencti_stix2.import_bundle_from_file("foo.txt")
    for record in caplog.records:
        assert record.levelname == "ERROR"
    assert "The bundle file does not exist" in caplog.text


def test_import_bundle_keeps_original_bundle_id_without_legacy_split(
    monkeypatch,
) -> None:
    opencti_stix2 = OpenCTIStix2(MagicMock())

    def fail_legacy_split(*args, **kwargs):
        raise AssertionError("default import must not materialize legacy child bundles")

    monkeypatch.setattr(
        OpenCTIStix2Splitter, "split_bundle_with_expectations", fail_legacy_split
    )

    imported_calls = []

    def fake_import_item_with_retries(
        item, update, types, work_id, bundle_id, report_expectation=True
    ):
        imported_calls.append((item["id"], bundle_id))
        return None

    monkeypatch.setattr(
        opencti_stix2, "import_item_with_retries", fake_import_item_with_retries
    )

    bundle_id = "bundle--11111111-1111-4111-8111-111111111111"
    imported, rejected = opencti_stix2.import_bundle(
        {
            "type": "bundle",
            "id": bundle_id,
            "objects": [
                {
                    "type": "indicator",
                    "id": "indicator--11111111-1111-4111-8111-111111111111",
                    "created_by_ref": "identity--22222222-2222-4222-8222-222222222222",
                },
                {
                    "type": "identity",
                    "id": "identity--22222222-2222-4222-8222-222222222222",
                },
            ],
        }
    )

    assert rejected == []
    assert imported == [
        {"id": "identity--22222222-2222-4222-8222-222222222222", "type": "identity"},
        {"id": "indicator--11111111-1111-4111-8111-111111111111", "type": "indicator"},
    ]
    assert imported_calls == [
        ("identity--22222222-2222-4222-8222-222222222222", bundle_id),
        ("indicator--11111111-1111-4111-8111-111111111111", bundle_id),
    ]


def test_import_bundle_generates_missing_bundle_id_without_legacy_split(
    monkeypatch,
) -> None:
    opencti_stix2 = OpenCTIStix2(MagicMock())
    imported_calls = []

    def fake_import_item_with_retries(
        item, update, types, work_id, bundle_id, report_expectation=True
    ):
        imported_calls.append((item["id"], bundle_id))
        return None

    monkeypatch.setattr(
        opencti_stix2, "import_item_with_retries", fake_import_item_with_retries
    )

    bundle = {
        "type": "bundle",
        "objects": [
            {
                "type": "indicator",
                "id": "indicator--11111111-1111-4111-8111-111111111111",
            },
        ],
    }
    imported, rejected = opencti_stix2.import_bundle(bundle)

    assert rejected == []
    assert imported == [
        {"id": "indicator--11111111-1111-4111-8111-111111111111", "type": "indicator"}
    ]
    assert bundle["id"].startswith("bundle--")
    assert imported_calls == [
        ("indicator--11111111-1111-4111-8111-111111111111", bundle["id"])
    ]


def test_import_bundle_reports_duplicate_objects_suppressed_during_preparation(
    monkeypatch,
) -> None:
    opencti = MagicMock()
    opencti_stix2 = OpenCTIStix2(opencti)

    imported_calls = []

    def fake_import_item_with_retries(
        item, update, types, work_id, bundle_id, report_expectation=True
    ):
        imported_calls.append((item["id"], bundle_id))
        return None

    monkeypatch.setattr(
        opencti_stix2, "import_item_with_retries", fake_import_item_with_retries
    )

    bundle_id = "bundle--11111111-1111-4111-8111-111111111111"
    imported, rejected = opencti_stix2.import_bundle(
        {
            "type": "bundle",
            "id": bundle_id,
            "objects": [
                {
                    "type": "indicator",
                    "id": "indicator--11111111-1111-4111-8111-111111111111",
                },
                {
                    "type": "indicator",
                    "id": "indicator--11111111-1111-4111-8111-111111111111",
                },
            ],
        },
        work_id="work--1",
    )

    assert rejected == []
    assert imported == [
        {"id": "indicator--11111111-1111-4111-8111-111111111111", "type": "indicator"}
    ]
    assert imported_calls == [
        ("indicator--11111111-1111-4111-8111-111111111111", bundle_id)
    ]
    opencti.work.report_expectation.assert_called_once_with("work--1", None)


def test_import_bundle_can_defer_expectation_reporting_to_batch_boundary(
    monkeypatch,
) -> None:
    opencti = MagicMock()
    opencti_stix2 = OpenCTIStix2(opencti)

    observed_expectation_flags = []

    def fake_import_item_with_retries(
        item, update, types, work_id, bundle_id, report_expectation=True
    ):
        observed_expectation_flags.append(report_expectation)
        return None

    monkeypatch.setattr(
        opencti_stix2, "import_item_with_retries", fake_import_item_with_retries
    )

    imported, rejected = opencti_stix2.import_bundle(
        {
            "type": "bundle",
            "id": "bundle--11111111-1111-4111-8111-111111111111",
            "objects": [
                {
                    "type": "indicator",
                    "id": "indicator--11111111-1111-4111-8111-111111111111",
                },
            ],
        },
        work_id="work--1",
        report_expectations=False,
    )

    assert rejected == []
    assert imported == [
        {"id": "indicator--11111111-1111-4111-8111-111111111111", "type": "indicator"}
    ]
    assert observed_expectation_flags == [False]
    opencti.work.report_expectation.assert_not_called()


def test_import_bundle_batch_executes_one_captured_plan(monkeypatch) -> None:
    opencti = MagicMock()
    opencti_stix2 = OpenCTIStix2(opencti)
    plan = MagicMock()

    @contextmanager
    def batch_mutation_plan(*args, **kwargs):
        yield plan

    opencti.batch_mutation_plan.side_effect = batch_mutation_plan
    monkeypatch.setattr(
        opencti_stix2,
        "import_bundle",
        lambda *args, **kwargs: ([{"id": "indicator--1", "type": "indicator"}], []),
    )

    imported, rejected = opencti_stix2.import_bundle_from_json_batch(
        '{"type":"bundle","id":"bundle--1","objects":[{"type":"indicator","id":"indicator--1"}]}',
        execution_mode="BULK",
        wait_until="COMMITTED",
    )

    assert imported == [{"id": "indicator--1", "type": "indicator"}]
    assert rejected == []
    opencti.execute_batch_mutation_plan.assert_called_once_with(
        plan,
        execution_mode="BULK",
        wait_until="COMMITTED",
    )


def test_import_bundle_batch_forwards_direct_delivery_context(monkeypatch) -> None:
    opencti = MagicMock()
    opencti_stix2 = OpenCTIStix2(opencti)
    plan = MagicMock()
    direct_delivery_context = {
        "submission_id": "batch-submission--1",
        "delivery_id": "batch-delivery--1",
        "parent_delivery_id": None,
        "delivery_kind": "ROOT",
        "delivery_protocol_version": 2,
        "delivery_branch_kind": "ROOT",
        "delivery_branch_sequence": 0,
        "delivery_branch_ordinal": 0,
    }

    @contextmanager
    def batch_mutation_plan(*args, **kwargs):
        yield plan

    opencti.batch_mutation_plan.side_effect = batch_mutation_plan
    monkeypatch.setattr(
        opencti_stix2,
        "import_bundle",
        lambda *args, **kwargs: ([{"id": "indicator--1", "type": "indicator"}], []),
    )

    opencti_stix2.import_bundle_from_json_batch(
        '{"type":"bundle","id":"bundle--1","objects":[{"type":"indicator","id":"indicator--1"}]}',
        execution_mode="BULK",
        wait_until="MATERIALIZED",
        direct_delivery_context=direct_delivery_context,
    )

    assert (
        opencti.batch_mutation_plan.call_args.kwargs["direct_delivery_context"]
        == direct_delivery_context
    )
    opencti.execute_batch_mutation_plan.assert_called_once_with(
        plan,
        execution_mode="BULK",
        wait_until="MATERIALIZED",
        direct_delivery_context=direct_delivery_context,
    )


def test_import_item_with_retries_propagates_batch_plan_size_failures() -> None:
    opencti_stix2 = OpenCTIStix2(MagicMock())
    opencti_stix2.import_item = MagicMock(side_effect=BatchMutationPlanTooLarge(2, 1))

    with pytest.raises(BatchMutationPlanTooLarge):
        opencti_stix2.import_item_with_retries(
            {"id": "indicator--1", "type": "indicator"},
        )


def test_import_bundle_batch_splits_oversized_plan_into_sequential_chunks(
    monkeypatch,
) -> None:
    opencti = MagicMock()
    opencti_stix2 = OpenCTIStix2(opencti)
    executed_object_ids = []
    executed_backend_plans = []

    @contextmanager
    def batch_mutation_plan(*args, **kwargs):
        yield BatchMutationPlan()

    def fake_import_bundle(stix_bundle, *args, **kwargs):
        batch_plan = kwargs["batch_plan"]
        imported = []
        for item in stix_bundle["objects"]:
            with batch_plan.execution_group(0, item["id"]):
                batch_plan.capture(
                    "mutation Record($value: String!) { record(value: $value) }",
                    {"value": item["id"]},
                    [],
                )
            imported.append({"id": item["id"], "type": item["type"]})
        return imported, []

    def execute_batch_mutation_plan(plan, **kwargs):
        object_ids = [operation["object_id"] for operation in plan.operations]
        executed_object_ids.append(object_ids)
        executed_backend_plans.append(kwargs.get("backend_batch_plan"))
        if len(object_ids) > 2:
            raise BatchMutationPlanTooLarge(200, 100)
        return {"data": {"batchMutationsExecute": {"operation_errors": []}}}

    object_ids = [f"indicator--{index}" for index in range(4)]
    backend_batch_plan = {
        "version": 1,
        "ordered_object_ids": object_ids,
        "incompatible_object_ids": [],
        "ignored_object_count": 0,
        "object_normalizations": [],
        "execution_phases": [{"phase": 0, "object_ids": object_ids}],
    }
    opencti.batch_mutation_plan.side_effect = batch_mutation_plan
    opencti.execute_batch_mutation_plan.side_effect = execute_batch_mutation_plan
    monkeypatch.setattr(opencti_stix2, "import_bundle", fake_import_bundle)

    imported, rejected = opencti_stix2.import_bundle_from_json_batch(
        json.dumps(
            {
                "type": "bundle",
                "id": "bundle--1",
                "objects": [
                    {"type": "indicator", "id": object_id} for object_id in object_ids
                ],
            }
        ),
        report_expectations=False,
        execution_mode="BULK",
        wait_until="COMMITTED",
        backend_batch_plan=backend_batch_plan,
        split_oversized_batch_plan=True,
    )

    assert imported == [
        {"id": object_id, "type": "indicator"} for object_id in object_ids
    ]
    assert rejected == []
    assert executed_object_ids == [
        object_ids,
        object_ids[:2],
        object_ids[2:],
    ]
    assert executed_backend_plans[1]["ordered_object_ids"] == object_ids[:2]
    assert executed_backend_plans[2]["ordered_object_ids"] == object_ids[2:]
    opencti.logger_class.return_value.warning.assert_called_once()


def test_import_bundle_batch_does_not_split_oversized_direct_delivery_context(
    monkeypatch,
) -> None:
    opencti = MagicMock()
    opencti_stix2 = OpenCTIStix2(opencti)

    @contextmanager
    def batch_mutation_plan(*args, **kwargs):
        yield BatchMutationPlan()

    opencti.batch_mutation_plan.side_effect = batch_mutation_plan
    opencti.execute_batch_mutation_plan.side_effect = BatchMutationPlanTooLarge(
        200, 100
    )
    monkeypatch.setattr(
        opencti_stix2,
        "import_bundle",
        lambda *args, **kwargs: ([{"id": "indicator--1", "type": "indicator"}], []),
    )

    with pytest.raises(BatchMutationPlanTooLarge):
        opencti_stix2.import_bundle_from_json_batch(
            '{"type":"bundle","id":"bundle--1","objects":[{"type":"indicator","id":"indicator--1"}]}',
            report_expectations=False,
            execution_mode="BULK",
            split_oversized_batch_plan=True,
            direct_delivery_context={
                "submission_id": "batch-submission--1",
                "delivery_id": "batch-delivery--1",
            },
        )


def test_import_bundle_batch_returns_retryable_missing_reference_items(
    monkeypatch,
) -> None:
    opencti = MagicMock()
    opencti_stix2 = OpenCTIStix2(opencti)
    plan = BatchMutationPlan()

    @contextmanager
    def batch_mutation_plan(*args, **kwargs):
        yield plan

    def fake_import_bundle(*args, **kwargs):
        batch_plan = kwargs["batch_plan"]
        with batch_plan.execution_group(0, "relationship--1"):
            batch_plan.capture(
                "mutation Record($value: String!) { record(value: $value) }",
                {"value": "relationship--1"},
                [],
            )
        return ([{"id": "relationship--1", "type": "relationship"}], [])

    opencti.batch_mutation_plan.side_effect = batch_mutation_plan
    opencti.execute_batch_mutation_plan.return_value = {
        "data": {
            "batchMutationsExecute": {
                "operation_errors": [
                    {"object_id": "relationship--1", "retryable": True}
                ]
            }
        }
    }
    monkeypatch.setattr(opencti_stix2, "import_bundle", fake_import_bundle)
    imported, rejected = opencti_stix2.import_bundle_from_json_batch(
        '{"type":"bundle","id":"bundle--1","objects":[{"type":"relationship","id":"relationship--1"}]}',
        execution_mode="BULK",
    )

    assert imported == []
    assert rejected == [
        {
            "type": "relationship",
            "id": "relationship--1",
            "rejection_info": {
                "reject_reason": "MISSING_REFERENCE",
                "retryable": True,
            },
        }
    ]
    opencti.execute_batch_mutation_plan.assert_called_once_with(
        plan,
        execution_mode="BULK",
        wait_until=None,
    )


def test_import_bundle_batch_returns_nonretryable_operation_failures(
    monkeypatch,
) -> None:
    opencti = MagicMock()
    opencti_stix2 = OpenCTIStix2(opencti)
    plan = BatchMutationPlan()

    @contextmanager
    def batch_mutation_plan(*args, **kwargs):
        yield plan

    def fake_import_bundle(*args, **kwargs):
        batch_plan = kwargs["batch_plan"]
        with batch_plan.execution_group(0, "indicator--1"):
            batch_plan.capture(
                "mutation Record($value: String!) { record(value: $value) }",
                {"value": "indicator--1"},
                [],
            )
        return ([{"id": "indicator--1", "type": "indicator"}], [])

    opencti.batch_mutation_plan.side_effect = batch_mutation_plan
    opencti.execute_batch_mutation_plan.return_value = {
        "data": {
            "batchMutationsExecute": {
                "operation_errors": [
                    {
                        "object_id": "indicator--1",
                        "code": "FUNCTIONAL_ERROR",
                        "message": "Batch GraphQL operation failed",
                        "retryable": False,
                    }
                ]
            }
        }
    }
    monkeypatch.setattr(opencti_stix2, "import_bundle", fake_import_bundle)
    imported, rejected = opencti_stix2.import_bundle_from_json_batch(
        '{"type":"bundle","id":"bundle--1","objects":[{"type":"indicator","id":"indicator--1"}]}',
        execution_mode="BULK",
    )

    assert imported == []
    assert rejected == [
        {
            "type": "indicator",
            "id": "indicator--1",
            "rejection_info": {
                "reject_reason": "FUNCTIONAL_ERROR",
                "retryable": False,
                "last_error_msg": "Batch GraphQL operation failed",
            },
        }
    ]


def test_import_bundle_batch_does_not_reuse_synthetic_cache_entries_between_plans(
    monkeypatch,
) -> None:
    opencti = MagicMock()
    opencti.get_draft_id.return_value = ""
    opencti_stix2 = OpenCTIStix2(opencti)
    plans = []

    @contextmanager
    def batch_mutation_plan(*args, **kwargs):
        yield BatchMutationPlan()

    def execute_batch_mutation_plan(plan, **kwargs):
        plans.append(plan)
        return {"data": {"batchMutationsExecute": {"operation_errors": []}}}

    def fake_import_bundle(*args, **kwargs):
        batch_plan = kwargs["batch_plan"]
        label_data = opencti_stix2.get_in_cache("label_OSINT")
        if label_data is None:
            label_result = batch_plan.capture(
                "mutation LabelAdd($input: LabelAddInput!) { labelAdd(input: $input) { id } }",
                {"input": {"value": "OSINT"}},
                [],
            )
            label_data = label_result["data"]["labelAdd"]
            opencti_stix2.set_in_cache("label_OSINT", label_data)
        batch_plan.capture(
            "mutation IndicatorAdd($input: IndicatorAddInput!) { indicatorAdd(input: $input) { id } }",
            {"input": {"objectLabel": [label_data["id"]]}},
            [],
        )
        return ([{"id": "indicator--1", "type": "indicator"}], [])

    opencti.batch_mutation_plan.side_effect = batch_mutation_plan
    opencti.execute_batch_mutation_plan.side_effect = execute_batch_mutation_plan
    monkeypatch.setattr(opencti_stix2, "import_bundle", fake_import_bundle)

    for _ in range(2):
        imported, rejected = opencti_stix2.import_bundle_from_json_batch(
            '{"type":"bundle","id":"bundle--1","objects":[{"type":"indicator","id":"indicator--1"}]}',
            execution_mode="BULK",
        )
        assert imported == [{"id": "indicator--1", "type": "indicator"}]
        assert rejected == []

    assert [len(plan.operations) for plan in plans] == [2, 2]
    assert json.loads(plans[1].operations[1]["variables"]) == {
        "input": {
            "objectLabel": [build_batch_result_token(0, ["labelAdd", "id"])],
        }
    }
    assert opencti_stix2.get_in_cache("label_OSINT") is None


def test_batch_mapping_cache_does_not_reuse_synthetic_tuple_cache_entries_between_plans():
    opencti = MagicMock()
    opencti.get_draft_id.return_value = ""
    opencti_stix2 = OpenCTIStix2(opencti)
    cache_key = opencti_stix2._external_reference_cache_key(
        "external-reference--1",
        "benchmark",
        "https://example.test/reference",
        "REF-1",
        None,
    )
    cache_data = {"id": build_batch_result_token(0, ["externalReferenceAdd", "id"])}

    for _ in range(2):
        with opencti_stix2.batch_mapping_cache():
            assert opencti_stix2.get_in_cache(cache_key) is None
            opencti_stix2.set_in_cache(cache_key, cache_data)
            assert opencti_stix2.get_in_cache(cache_key) == cache_data
        assert opencti_stix2.get_in_cache(cache_key) is None

    opencti_stix2.mapping_cache[opencti_stix2._mapping_cache_key(cache_key)] = (
        cache_data
    )
    assert opencti_stix2.get_in_cache(cache_key) is None


def test_import_bundle_batch_tags_item_mutations_with_dependency_phases(
    monkeypatch,
) -> None:
    opencti_stix2 = OpenCTIStix2(MagicMock())
    plan = BatchMutationPlan()

    def fake_import_item_with_retries(
        item, update, types, work_id, bundle_id, report_expectation=True
    ):
        plan.capture(
            "mutation Record($value: String!) { record(value: $value) }",
            {"value": item["id"]},
            [],
        )
        return None

    monkeypatch.setattr(
        opencti_stix2, "import_item_with_retries", fake_import_item_with_retries
    )

    opencti_stix2.import_bundle(
        {
            "type": "bundle",
            "id": "bundle--11111111-1111-4111-8111-111111111111",
            "objects": [
                {
                    "type": "indicator",
                    "id": "indicator--11111111-1111-4111-8111-111111111111",
                    "created_by_ref": "identity--22222222-2222-4222-8222-222222222222",
                },
                {
                    "type": "identity",
                    "id": "identity--22222222-2222-4222-8222-222222222222",
                },
            ],
        },
        batch_plan=plan,
    )

    assert [
        (
            operation["execution_group"],
            operation["execution_phase"],
            operation["object_id"],
            operation["variables"],
        )
        for operation in plan.operations
    ] == [
        (
            0,
            1,
            "identity--22222222-2222-4222-8222-222222222222",
            '{"value": "identity--22222222-2222-4222-8222-222222222222"}',
        ),
        (
            1,
            2,
            "indicator--11111111-1111-4111-8111-111111111111",
            '{"value": "indicator--11111111-1111-4111-8111-111111111111"}',
        ),
    ]


def test_import_bundle_batch_prefers_backend_execution_phases(
    monkeypatch,
) -> None:
    opencti_stix2 = OpenCTIStix2(MagicMock())
    plan = BatchMutationPlan()

    def fake_import_item_with_retries(
        item, update, types, work_id, bundle_id, report_expectation=True
    ):
        plan.capture(
            "mutation Record($value: String!) { record(value: $value) }",
            {"value": item["id"]},
            [],
        )
        return None

    monkeypatch.setattr(
        opencti_stix2, "import_item_with_retries", fake_import_item_with_retries
    )

    opencti_stix2.import_bundle(
        {
            "type": "bundle",
            "id": "bundle--11111111-1111-4111-8111-111111111111",
            "objects": [
                {
                    "type": "indicator",
                    "id": "indicator--11111111-1111-4111-8111-111111111111",
                    "created_by_ref": "identity--22222222-2222-4222-8222-222222222222",
                },
                {
                    "type": "identity",
                    "id": "identity--22222222-2222-4222-8222-222222222222",
                },
            ],
        },
        batch_plan=plan,
        backend_batch_plan={
            "version": 1,
            "execution_phases": [
                {
                    "phase": 0,
                    "object_ids": ["identity--22222222-2222-4222-8222-222222222222"],
                },
                {
                    "phase": 7,
                    "object_ids": ["indicator--11111111-1111-4111-8111-111111111111"],
                },
            ],
        },
    )

    assert [
        (operation["execution_phase"], operation["object_id"], operation["variables"])
        for operation in plan.operations
    ] == [
        (
            0,
            "identity--22222222-2222-4222-8222-222222222222",
            '{"value": "identity--22222222-2222-4222-8222-222222222222"}',
        ),
        (
            7,
            "indicator--11111111-1111-4111-8111-111111111111",
            '{"value": "indicator--11111111-1111-4111-8111-111111111111"}',
        ),
    ]


def test_import_bundle_batch_uses_backend_preparation_without_legacy_splitter(
    monkeypatch,
) -> None:
    opencti_stix2 = OpenCTIStix2(MagicMock())

    def fail_prepare_bundle_for_import(*args, **kwargs):
        raise AssertionError(
            "backend-admitted bundles must not rerun legacy preparation"
        )

    monkeypatch.setattr(
        OpenCTIStix2Splitter,
        "prepare_bundle_for_import",
        fail_prepare_bundle_for_import,
    )

    imported_items = []

    def fake_import_item_with_retries(
        item, update, types, work_id, bundle_id, report_expectation=True
    ):
        imported_items.append(item.copy())
        return None

    monkeypatch.setattr(
        opencti_stix2, "import_item_with_retries", fake_import_item_with_retries
    )

    imported, rejected = opencti_stix2.import_bundle(
        {
            "type": "bundle",
            "id": "bundle--11111111-1111-4111-8111-111111111111",
            "objects": [
                {
                    "type": "indicator",
                    "id": "indicator--11111111-1111-4111-8111-111111111111",
                    "created_by_ref": "identity--missing",
                    "external_references": [
                        {"source_name": "feed", "external_id": "1"},
                        {"source_name": "feed", "external_id": "1"},
                        {"url": "https://example.test/a"},
                    ],
                },
                {
                    "type": "identity",
                    "id": "identity--22222222-2222-4222-8222-222222222222",
                },
            ],
        },
        backend_batch_plan={
            "version": 1,
            "ordered_object_ids": [
                "identity--22222222-2222-4222-8222-222222222222",
                "indicator--11111111-1111-4111-8111-111111111111",
            ],
            "ignored_object_count": 0,
            "incompatible_object_ids": [],
            "object_normalizations": [
                {
                    "id": "indicator--11111111-1111-4111-8111-111111111111",
                    "reference_values": {"created_by_ref": None},
                    "external_reference_indexes": [0, 2],
                }
            ],
            "execution_phases": [],
        },
    )

    assert rejected == []
    assert imported == [
        {"id": "identity--22222222-2222-4222-8222-222222222222", "type": "identity"},
        {"id": "indicator--11111111-1111-4111-8111-111111111111", "type": "indicator"},
    ]
    assert [item["id"] for item in imported_items] == [
        "identity--22222222-2222-4222-8222-222222222222",
        "indicator--11111111-1111-4111-8111-111111111111",
    ]
    assert imported_items[1]["created_by_ref"] is None
    assert imported_items[1]["external_references"] == [
        {"source_name": "feed", "external_id": "1"},
        {"url": "https://example.test/a"},
    ]


def test_import_bundle_skips_ref_count_when_limit_is_disabled(monkeypatch):
    opencti_stix2 = OpenCTIStix2(MagicMock())
    compute_calls = []

    monkeypatch.setattr(
        OpenCTIStix2Utils,
        "compute_object_refs_number",
        lambda item: compute_calls.append(item["id"]) or 0,
    )
    monkeypatch.setattr(
        opencti_stix2,
        "import_item_with_retries",
        lambda *_args, **_kwargs: None,
    )

    opencti_stix2.import_bundle(
        {
            "type": "bundle",
            "id": "bundle--disabled-max-refs",
            "objects": [{"id": "malware--disabled", "type": "malware"}],
        },
        objects_max_refs=0,
    )

    assert compute_calls == []


def test_import_bundle_counts_refs_when_limit_is_enabled(monkeypatch):
    opencti_stix2 = OpenCTIStix2(MagicMock())
    compute_calls = []

    monkeypatch.setattr(
        OpenCTIStix2Utils,
        "compute_object_refs_number",
        lambda item: compute_calls.append(item["id"]) or 1,
    )
    monkeypatch.setattr(
        opencti_stix2,
        "import_item_with_retries",
        lambda *_args, **_kwargs: None,
    )

    imported, rejected = opencti_stix2.import_bundle(
        {
            "type": "bundle",
            "id": "bundle--enabled-max-refs",
            "objects": [{"id": "malware--enabled", "type": "malware"}],
        },
        objects_max_refs=1,
    )

    assert compute_calls == ["malware--enabled"]
    assert imported == []
    assert len(rejected) == 1
    assert rejected[0]["id"] == "malware--enabled"
    assert rejected[0]["rejection_info"] == {
        "reject_reason": "ELEMENT_TOO_LARGE",
        "objects_max_refs": 1,
    }


def test_extract_embedded_storage_path_ignores_query_string(
    opencti_stix2: OpenCTIStix2,
):
    uri = "https://remote.example/download?next=/storage/get/embedded/Note/internal-note-id/a.png"

    result = opencti_stix2._extract_embedded_storage_path(uri)

    assert result is None


def test_extract_embedded_storage_path_ignores_fragment(opencti_stix2: OpenCTIStix2):
    uri = "https://remote.example/download#/storage/view/embedded/Note/internal-note-id/a.png"

    result = opencti_stix2._extract_embedded_storage_path(uri)

    assert result is None


def test_extract_embedded_storage_path_from_relative_embedded_path_with_context(
    opencti_stix2: OpenCTIStix2,
):
    uri = "embedded/upload_image_example.png"

    result = opencti_stix2._extract_embedded_storage_path(
        uri,
        entity_type="Report",
        entity_id="internal-report-id",
    )

    assert result == "embedded/Report/internal-report-id/upload_image_example.png"


def test_prepare_export_rewrites_relative_embedded_markdown_image_uri(
    opencti_stix2: OpenCTIStix2, monkeypatch
):
    monkeypatch.setattr(
        opencti_stix2.opencti.stix_nested_ref_relationship,
        "list",
        lambda **kwargs: [],
    )

    fetch_calls = []

    def fake_fetch(url, binary=False, serialize=False):
        fetch_calls.append((url, binary, serialize))
        return "Zm9v"

    monkeypatch.setattr(opencti_stix2.opencti, "fetch_opencti_file", fake_fetch)

    entity = {
        "id": "internal-report-id-embedded",
        "type": "report",
        "entity_type": "Report",
        "x_opencti_id": "internal-report-id-embedded",
        "description": "desc ![img](embedded/upload_image_example.png)",
    }

    result = opencti_stix2.prepare_export(entity=entity, mode="simple")

    assert len(result) == 1
    assert "data:image/png;base64,Zm9v" in result[0]["description"]
    assert len(fetch_calls) == 1
    assert fetch_calls[0][0].endswith(
        "/storage/get/embedded/Report/internal-report-id-embedded/upload_image_example.png"
    )
    assert fetch_calls[0][1] is True
    assert fetch_calls[0][2] is True


def test_bundle_level_rewrite_rewrites_relative_embedded_markdown_image_uri(
    opencti_stix2: OpenCTIStix2, monkeypatch
):
    fetch_calls = []

    def fake_fetch(url, binary=False, serialize=False):
        fetch_calls.append((url, binary, serialize))
        return "Zm9v"

    monkeypatch.setattr(opencti_stix2.opencti, "fetch_opencti_file", fake_fetch)

    bundle = {
        "type": "bundle",
        "id": "bundle--11111111-1111-4111-8111-111111111111",
        "objects": [
            {
                "type": "report",
                "id": "report--392ef26a-4496-50ae-9828-4c3c72328245",
                "x_opencti_type": "Report",
                "x_opencti_id": "bf8359d6-030a-43b3-9fe2-1ba678ecb3ed",
                "description": "![upload_image_example.png](embedded/upload_image_example.png)",
            }
        ],
    }

    opencti_stix2._rewrite_embedded_image_uris_in_bundle_for_export(bundle)

    description = bundle["objects"][0]["description"]
    assert "data:image/png;base64,Zm9v" in description
    assert len(fetch_calls) == 1
    assert fetch_calls[0][0].endswith(
        "/storage/get/embedded/Report/bf8359d6-030a-43b3-9fe2-1ba678ecb3ed/upload_image_example.png"
    )
    assert fetch_calls[0][1] is True
    assert fetch_calls[0][2] is True


def test_import_observable_passes_embedded_flags_to_create(
    opencti_stix2: OpenCTIStix2, monkeypatch
):
    monkeypatch.setattr(
        opencti_stix2,
        "extract_embedded_relationships",
        lambda stix_object, types=None: {
            "created_by": None,
            "object_marking": None,
            "object_label": None,
            "open_vocabs": {},
            "granted_refs": [],
            "kill_chain_phases": [],
            "object_refs": [],
            "external_references": [],
            "reports": {},
            "sample_refs": [],
        },
    )
    monkeypatch.setattr(
        opencti_stix2.opencti,
        "file",
        lambda name, data, mime_type: {
            "name": name,
            "data": data,
            "mime_type": mime_type,
        },
    )

    captured_kwargs = {}

    def fake_create(**kwargs):
        captured_kwargs.update(kwargs)
        return {"id": "observable--1", "entity_type": "Stix-Cyber-Observable"}

    monkeypatch.setattr(
        opencti_stix2.opencti.stix_cyber_observable,
        "create",
        fake_create,
    )

    stix_object = {
        "id": "ipv4-addr--11111111-1111-4111-8111-111111111111",
        "type": "ipv4-addr",
        "value": "1.2.3.4",
        "x_opencti_files": [
            {
                "name": "img.png",
                "data": "Zm9v",
                "mime_type": "image/png",
                "embedded": True,
            }
        ],
    }

    opencti_stix2.import_observable(stix_object, update=False)

    assert captured_kwargs.get("embedded") == [True]


def test_import_observable_closes_large_base64_upload_stream_after_create(monkeypatch):
    captured_data = None
    payload = b"x" * (BASE64_FILE_MEMORY_THRESHOLD + 1)
    encoded_data = base64.b64encode(payload).decode("ascii")

    def fake_create(**kwargs):
        nonlocal captured_data
        captured_data = kwargs["files"][0]["data"]
        assert not captured_data.closed
        assert captured_data.read() == payload
        captured_data.seek(0)
        return {"id": "observable--1", "entity_type": "Stix-Cyber-Observable"}

    fake_opencti = SimpleNamespace(
        file=lambda name, data, mime_type: {
            "name": name,
            "data": data,
            "mime_type": mime_type,
        },
        get_attribute_in_extension=lambda attribute, entity: None,
        get_draft_id=lambda: "",
        stix_cyber_observable=SimpleNamespace(create=fake_create),
        stix_nested_ref_relationship=SimpleNamespace(create=lambda **kwargs: None),
    )
    opencti_stix2 = OpenCTIStix2(fake_opencti)
    monkeypatch.setattr(
        opencti_stix2,
        "extract_embedded_relationships",
        lambda stix_object, types=None: {
            "created_by": None,
            "object_marking": None,
            "object_label": None,
            "open_vocabs": {},
            "granted_refs": [],
            "kill_chain_phases": [],
            "object_refs": [],
            "external_references": [],
            "reports": {},
            "sample_refs": [],
        },
    )

    opencti_stix2.import_observable(
        {
            "id": "ipv4-addr--11111111-1111-4111-8111-111111111111",
            "type": "ipv4-addr",
            "value": "1.2.3.4",
            "x_opencti_files": [
                {
                    "name": "payload.bin",
                    "data": encoded_data,
                    "mime_type": "application/octet-stream",
                }
            ],
        },
        update=False,
    )

    assert captured_data.closed


@pytest.mark.parametrize(
    ("stix_type", "file_overrides", "update", "expected_payload_bin"),
    [
        pytest.param("artifact", {}, False, False, id="exact-duplicate"),
        pytest.param("artifact", {}, True, False, id="exact-duplicate-update"),
        pytest.param(
            "artifact",
            {"name": "attachment.bin"},
            False,
            True,
            id="different-name",
        ),
        pytest.param(
            "artifact",
            {"mime_type": "text/plain"},
            False,
            True,
            id="different-mime-type",
        ),
        pytest.param(
            "artifact",
            {"data": "YmFy"},
            False,
            True,
            id="different-payload",
        ),
        pytest.param(
            "artifact",
            {"embedded": True},
            False,
            True,
            id="embedded-file",
        ),
        pytest.param(
            "artifact",
            {"no_trigger_import": True},
            False,
            True,
            id="no-trigger-import-file",
        ),
        pytest.param("file", {}, False, True, id="non-artifact"),
    ],
)
def test_import_observable_skips_only_redundant_artifact_payload_upload(
    monkeypatch, stix_type, file_overrides, update, expected_payload_bin
):
    captured_kwargs = {}

    def fake_create(**kwargs):
        captured_kwargs.update(kwargs)
        return {"id": "artifact--1", "entity_type": "Artifact"}

    fake_opencti = SimpleNamespace(
        file=lambda name, data, mime_type: {
            "name": name,
            "data": data,
            "mime_type": mime_type,
        },
        get_attribute_in_extension=lambda attribute, entity: None,
        get_draft_id=lambda: "",
        stix_cyber_observable=SimpleNamespace(create=fake_create),
        stix_nested_ref_relationship=SimpleNamespace(create=lambda **kwargs: None),
    )
    opencti_stix2 = OpenCTIStix2(fake_opencti)
    monkeypatch.setattr(
        opencti_stix2,
        "extract_embedded_relationships",
        lambda stix_object, types=None: {
            "created_by": None,
            "object_marking": None,
            "object_label": None,
            "open_vocabs": {},
            "granted_refs": [],
            "kill_chain_phases": [],
            "object_refs": [],
            "external_references": [],
            "reports": {},
            "sample_refs": [],
        },
    )

    x_opencti_file = {
        "name": "payload.bin",
        "data": "Zm9v",
        "mime_type": "application/octet-stream",
    }
    x_opencti_file.update(file_overrides)
    stix_object = {
        "id": "artifact--11111111-1111-4111-8111-111111111111",
        "type": stix_type,
        "mime_type": "application/octet-stream",
        "x_opencti_additional_names": ["nested/payload.bin"],
        "payload_bin": "Zm9v",
        "x_opencti_files": [x_opencti_file],
    }

    opencti_stix2.import_observable(stix_object, update=update)

    assert ("payload_bin" in captured_kwargs["observableData"]) is expected_payload_bin
    assert captured_kwargs["update"] is update
    assert stix_object["payload_bin"] == "Zm9v"
    assert len(captured_kwargs["files"]) == 1


def test_import_observable_batches_nested_ref_relationship_creates(monkeypatch):
    class _NestedRefCollection:
        def __init__(self):
            self.add_many_calls = []
            self.create_calls = []

        def add_many_to_stix_core_object(self, from_id, to_ids, relationship_type):
            self.add_many_calls.append((from_id, list(to_ids), relationship_type))

        def create(self, **kwargs):
            self.create_calls.append(kwargs)

    nested_ref_collection = _NestedRefCollection()
    fake_opencti = SimpleNamespace(
        get_attribute_in_extension=lambda attribute, entity: None,
        get_draft_id=lambda: "",
        stix_cyber_observable=SimpleNamespace(
            create=lambda **kwargs: {
                "id": "observable--1",
                "entity_type": "Stix-Cyber-Observable",
            }
        ),
        stix_nested_ref_relationship=nested_ref_collection,
    )
    opencti_stix2 = OpenCTIStix2(fake_opencti)
    monkeypatch.setattr(
        opencti_stix2,
        "extract_embedded_relationships",
        lambda stix_object, types=None: {
            "created_by": None,
            "object_marking": None,
            "object_label": None,
            "open_vocabs": {},
            "granted_refs": [],
            "kill_chain_phases": [],
            "object_refs": [],
            "external_references": [],
            "reports": {},
            "sample_refs": [],
        },
    )

    ref_count = (NESTED_REF_RELATIONSHIP_CREATE_BATCH_SIZE * 2) + 1
    opencti_stix2.import_observable(
        {
            "id": "directory--1",
            "type": "directory",
            "path": "/benchmark",
            "contains_refs": [f"artifact--{index}" for index in range(ref_count)],
            "x_opencti_custom_ref": "identity--custom",
        },
        update=False,
    )

    assert [len(call[1]) for call in nested_ref_collection.add_many_calls] == [
        NESTED_REF_RELATIONSHIP_CREATE_BATCH_SIZE,
        NESTED_REF_RELATIONSHIP_CREATE_BATCH_SIZE,
    ]
    assert nested_ref_collection.add_many_calls[0] == (
        "observable--1",
        [
            f"artifact--{index}"
            for index in range(NESTED_REF_RELATIONSHIP_CREATE_BATCH_SIZE)
        ],
        "contains",
    )
    assert nested_ref_collection.create_calls == [
        {
            "fromId": "observable--1",
            "toId": f"artifact--{ref_count - 1}",
            "relationship_type": "contains",
        },
        {
            "fromId": "observable--1",
            "toId": "identity--custom",
            "relationship_type": "x_opencti_custom",
        },
    ]


def test_import_observable_falls_back_to_singular_nested_ref_creates_without_bulk_helper(
    monkeypatch,
):
    class _NestedRefCollection:
        def __init__(self):
            self.create_calls = []

        def create(self, **kwargs):
            self.create_calls.append(kwargs)

    nested_ref_collection = _NestedRefCollection()
    fake_opencti = SimpleNamespace(
        get_attribute_in_extension=lambda attribute, entity: None,
        get_draft_id=lambda: "",
        stix_cyber_observable=SimpleNamespace(
            create=lambda **kwargs: {
                "id": "observable--1",
                "entity_type": "Stix-Cyber-Observable",
            }
        ),
        stix_nested_ref_relationship=nested_ref_collection,
    )
    opencti_stix2 = OpenCTIStix2(fake_opencti)
    monkeypatch.setattr(
        opencti_stix2,
        "extract_embedded_relationships",
        lambda stix_object, types=None: {
            "created_by": None,
            "object_marking": None,
            "object_label": None,
            "open_vocabs": {},
            "granted_refs": [],
            "kill_chain_phases": [],
            "object_refs": [],
            "external_references": [],
            "reports": {},
            "sample_refs": [],
        },
    )

    opencti_stix2.import_observable(
        {
            "id": "directory--1",
            "type": "directory",
            "path": "/benchmark",
            "contains_refs": ["artifact--1", "artifact--2"],
        },
        update=False,
    )

    assert nested_ref_collection.create_calls == [
        {
            "fromId": "observable--1",
            "toId": "artifact--1",
            "relationship_type": "contains",
        },
        {
            "fromId": "observable--1",
            "toId": "artifact--2",
            "relationship_type": "contains",
        },
    ]


def test_import_observable_preserves_mixed_nested_ref_relationship_order(monkeypatch):
    class _NestedRefCollection:
        def __init__(self):
            self.calls = []

        def add_many_to_stix_core_object(self, from_id, to_ids, relationship_type):
            self.calls.append(("add_many", from_id, list(to_ids), relationship_type))

        def create(self, **kwargs):
            self.calls.append(("create", kwargs))

    nested_ref_collection = _NestedRefCollection()
    fake_opencti = SimpleNamespace(
        get_attribute_in_extension=lambda attribute, entity: None,
        get_draft_id=lambda: "",
        stix_cyber_observable=SimpleNamespace(
            create=lambda **kwargs: {
                "id": "observable--1",
                "entity_type": "Stix-Cyber-Observable",
            }
        ),
        stix_nested_ref_relationship=nested_ref_collection,
    )
    opencti_stix2 = OpenCTIStix2(fake_opencti)
    monkeypatch.setattr(
        opencti_stix2,
        "extract_embedded_relationships",
        lambda stix_object, types=None: {
            "created_by": None,
            "object_marking": None,
            "object_label": None,
            "open_vocabs": {},
            "granted_refs": [],
            "kill_chain_phases": [],
            "object_refs": [],
            "external_references": [],
            "reports": {},
            "sample_refs": [],
        },
    )

    opencti_stix2.import_observable(
        {
            "id": "directory--1",
            "type": "directory",
            "path": "/benchmark",
            "contains_refs": ["artifact--1", "artifact--2"],
            "x_opencti_custom_ref": "identity--custom",
            "parent_refs": ["directory--2", "directory--3"],
        },
        update=False,
    )

    assert nested_ref_collection.calls == [
        (
            "add_many",
            "observable--1",
            ["artifact--1", "artifact--2"],
            "contains",
        ),
        (
            "create",
            {
                "fromId": "observable--1",
                "toId": "identity--custom",
                "relationship_type": "x_opencti_custom",
            },
        ),
        (
            "add_many",
            "observable--1",
            ["directory--2", "directory--3"],
            "parent",
        ),
    ]


def test_prepare_export_prefers_x_opencti_type_for_relative_embedded_markdown_image_uri(
    opencti_stix2: OpenCTIStix2, monkeypatch
):
    monkeypatch.setattr(
        opencti_stix2.opencti.stix_nested_ref_relationship,
        "list",
        lambda **kwargs: [],
    )

    fetch_calls = []

    def fake_fetch(url, binary=False, serialize=False):
        fetch_calls.append((url, binary, serialize))
        return "Zm9v"

    monkeypatch.setattr(opencti_stix2.opencti, "fetch_opencti_file", fake_fetch)

    entity = {
        "id": "internal-report-id-embedded",
        "type": "report",
        "entity_type": "Note",
        "x_opencti_type": "Report",
        "x_opencti_id": "internal-report-id-embedded",
        "description": "desc ![img](embedded/upload_image_example.png)",
    }

    result = opencti_stix2.prepare_export(entity=entity, mode="simple")

    assert len(result) == 1
    assert "data:image/png;base64,Zm9v" in result[0]["description"]
    assert len(fetch_calls) == 1
    assert fetch_calls[0][0].endswith(
        "/storage/get/embedded/Report/internal-report-id-embedded/upload_image_example.png"
    )
    assert fetch_calls[0][1] is True
    assert fetch_calls[0][2] is True


def test_extract_embedded_relationships_resolves_open_vocab_by_entity_type(
    opencti_stix2: OpenCTIStix2, monkeypatch
):
    def fake_query(_query):
        return {
            "data": {
                "vocabularyCategories": [
                    {
                        "key": "threat_actor_group_role_ov",
                        "entity_types": ["Threat-Actor-Group"],
                        "fields": [
                            {"key": "roles", "required": False, "multiple": True}
                        ],
                    },
                    {
                        "key": "threat_actor_individual_role_ov",
                        "entity_types": ["Threat-Actor-Individual"],
                        "fields": [
                            {"key": "roles", "required": False, "multiple": True}
                        ],
                    },
                ]
            }
        }

    monkeypatch.setattr(opencti_stix2.opencti, "query", fake_query)

    resolved_categories = []

    def fake_read_or_create_unchecked_with_cache(vocab, cache, field):
        resolved_categories.append(field["category"])
        if field["category"] == "threat_actor_group_role_ov":
            return {"name": vocab}
        return None

    monkeypatch.setattr(
        opencti_stix2.opencti.vocabulary,
        "read_or_create_unchecked_with_cache",
        fake_read_or_create_unchecked_with_cache,
    )

    stix_object = {
        "id": "threat-actor--11111111-1111-4111-8111-111111111111",
        "type": "threat-actor",
        "x_opencti_type": "Threat-Actor-Group",
        "name": "TA_20250505",
        "roles": ["agent", "independent"],
    }

    embedded = opencti_stix2.extract_embedded_relationships(stix_object)

    assert embedded["open_vocabs"]["roles"] == ["agent", "independent"]
    assert resolved_categories == [
        "threat_actor_group_role_ov",
        "threat_actor_group_role_ov",
    ]


def test_extract_embedded_relationships_resolves_open_vocab_with_lowercase_entity_type(
    opencti_stix2: OpenCTIStix2, monkeypatch
):
    def fake_query(_query):
        return {
            "data": {
                "vocabularyCategories": [
                    {
                        "key": "threat_actor_group_role_ov",
                        "entity_types": ["Threat-Actor-Group"],
                        "fields": [
                            {"key": "roles", "required": False, "multiple": True}
                        ],
                    },
                ]
            }
        }

    monkeypatch.setattr(opencti_stix2.opencti, "query", fake_query)

    monkeypatch.setattr(
        opencti_stix2.opencti.vocabulary,
        "read_or_create_unchecked_with_cache",
        lambda vocab, cache, field: {"name": vocab},
    )

    stix_object = {
        "id": "threat-actor--11111111-1111-4111-8111-111111111111",
        "type": "threat-actor",
        "x_opencti_type": "threat-actor-group",
        "name": "TA_20250505",
        "roles": ["agent", "independent"],
    }

    embedded = opencti_stix2.extract_embedded_relationships(stix_object)

    assert embedded["open_vocabs"]["roles"] == ["agent", "independent"]


def test_prepare_export_does_not_rewrite_markdown_image_uri_in_descriptions_list(
    opencti_stix2: OpenCTIStix2, monkeypatch
):
    monkeypatch.setattr(
        opencti_stix2.opencti.stix_nested_ref_relationship,
        "list",
        lambda **kwargs: [],
    )

    fetch_calls = []

    def fake_fetch(url, binary=False, serialize=False):
        fetch_calls.append((url, binary, serialize))
        return "Zm9v"

    monkeypatch.setattr(opencti_stix2.opencti, "fetch_opencti_file", fake_fetch)

    entity = {
        "id": "report--66666666-6666-4666-8666-666666666666",
        "type": "report",
        "x_opencti_id": "internal-report-id-6",
        "descriptions": [
            "first ![img](/storage/view/embedded/Report/internal-report-id-6/a.png)",
            "second no image",
        ],
    }

    result = opencti_stix2.prepare_export(entity=entity, mode="simple")

    assert len(result) == 1
    assert (
        result[0]["descriptions"][0]
        == "first ![img](/storage/view/embedded/Report/internal-report-id-6/a.png)"
    )
    assert result[0]["descriptions"][1] == "second no image"
    assert len(fetch_calls) == 0


def test_prepare_export_does_not_corrupt_malformed_markdown_image_syntax(
    opencti_stix2: OpenCTIStix2, monkeypatch
):
    monkeypatch.setattr(
        opencti_stix2.opencti.stix_nested_ref_relationship,
        "list",
        lambda **kwargs: [],
    )

    # Intentionally malformed markdown image (missing ] before the URL destination).
    malformed = (
        "![02 osint vulnerability triage queue "
        "(/storage/get/embedded/Report/internal-report-id/markdown-image-abc.pngTkSuQmCC)"
    )

    entity = {
        "id": "report--22222222-2222-4222-8222-222222222222",
        "type": "report",
        "x_opencti_id": "internal-report-id",
        "description": malformed,
    }

    result = opencti_stix2.prepare_export(entity=entity, mode="simple")

    assert len(result) == 1
    assert result[0]["description"] == malformed


def test_prepare_export_keeps_non_embedded_markdown_image_uri(
    opencti_stix2: OpenCTIStix2, monkeypatch
):
    monkeypatch.setattr(
        opencti_stix2.opencti.stix_nested_ref_relationship,
        "list",
        lambda **kwargs: [],
    )

    fetch_calls = []

    def fake_fetch(url, binary=False, serialize=False):
        fetch_calls.append((url, binary, serialize))
        return "Zm9v"

    monkeypatch.setattr(opencti_stix2.opencti, "fetch_opencti_file", fake_fetch)

    entity = {
        "id": "note--22222222-2222-4222-8222-222222222222",
        "type": "note",
        "x_opencti_id": "internal-note-id-2",
        "description": "desc ![img](/storage/get/import/global/a.png)",
    }

    result = opencti_stix2.prepare_export(entity=entity, mode="simple")

    assert len(result) == 1
    assert result[0]["description"] == "desc ![img](/storage/get/import/global/a.png)"
    assert len(fetch_calls) == 0


def test_generate_export_fetches_external_reference_files_by_id(
    opencti_stix2: OpenCTIStix2, monkeypatch
):
    """External reference importFiles must be fetched via fetch_opencti_file_by_id,
    passing the raw file id rather than a manually crafted storage URL."""
    fetch_by_id_calls = []

    def fake_fetch_by_id(file_id, binary=False, serialize=False):
        fetch_by_id_calls.append((file_id, binary, serialize))
        return "Zm9v"

    monkeypatch.setattr(
        opencti_stix2.opencti, "fetch_opencti_file_by_id", fake_fetch_by_id
    )

    entity = {
        "id": "internal-report-id-ext-ref",
        "standard_id": "report--33333333-3333-4333-8333-333333333333",
        "entity_type": "Report",
        "parent_types": ["Stix-Domain-Object"],
        "externalReferencesIds": ["ext-ref-id"],
        "externalReferences": [
            {
                "source_name": "acme-source",
                "description": "",
                "url": "https://example.com/report",
                "hash": "",
                "external_id": "",
                "importFiles": [
                    {
                        "id": "import/External-Reference/ext-ref-id/report.pdf",
                        "name": "report.pdf",
                        "metaData": {"mimetype": "application/pdf", "version": "v1"},
                    }
                ],
            }
        ],
    }

    result = opencti_stix2.generate_export(entity=entity)

    assert len(fetch_by_id_calls) == 1
    assert fetch_by_id_calls[0] == (
        "import/External-Reference/ext-ref-id/report.pdf",
        True,
        True,
    )
    x_opencti_files = result["external_references"][0]["x_opencti_files"]
    assert x_opencti_files == [
        {
            "name": "report.pdf",
            "data": "Zm9v",
            "mime_type": "application/pdf",
            "version": "v1",
        }
    ]


def test_prepare_export_fetches_artifact_payload_bin_by_id(
    opencti_stix2: OpenCTIStix2, monkeypatch
):
    """Artifact payload_bin must be fetched via fetch_opencti_file_by_id using the
    first importFiles entry's id, not a manually crafted storage URL."""
    monkeypatch.setattr(
        opencti_stix2.opencti.stix_nested_ref_relationship,
        "list",
        lambda **kwargs: [],
    )

    fetch_by_id_calls = []

    def fake_fetch_by_id(file_id, binary=False, serialize=False):
        fetch_by_id_calls.append((file_id, binary, serialize))
        return "Zm9v"

    monkeypatch.setattr(
        opencti_stix2.opencti, "fetch_opencti_file_by_id", fake_fetch_by_id
    )

    entity = {
        "id": "artifact--44444444-4444-4444-8444-444444444444",
        "type": "artifact",
        "x_opencti_id": "internal-artifact-id",
        "importFilesIds": ["import/Artifact/internal-artifact-id/sample.bin"],
        "importFiles": [
            {
                "id": "import/Artifact/internal-artifact-id/sample.bin",
                "name": "sample.bin",
                "metaData": {"mimetype": "application/octet-stream", "version": None},
            }
        ],
    }

    result = opencti_stix2.prepare_export(entity=entity, mode="simple")

    assert len(result) == 1
    assert result[0]["payload_bin"] == "Zm9v"
    # An artifact entity with importFiles goes through both the "Artifact"
    # (payload_bin) and generic "Files" (x_opencti_files) branches. Assert on
    # the fetched file id/flags rather than the exact call count, so this
    # test doesn't lock in that implementation detail.
    assert len(fetch_by_id_calls) >= 1
    for call in fetch_by_id_calls:
        assert call == ("import/Artifact/internal-artifact-id/sample.bin", True, True)


def test_prepare_export_fetches_generic_import_files_by_id(
    opencti_stix2: OpenCTIStix2, monkeypatch
):
    """Generic entity importFiles (x_opencti_files) must be fetched via
    fetch_opencti_file_by_id, not a manually crafted storage URL."""
    monkeypatch.setattr(
        opencti_stix2.opencti.stix_nested_ref_relationship,
        "list",
        lambda **kwargs: [],
    )

    fetch_by_id_calls = []

    def fake_fetch_by_id(file_id, binary=False, serialize=False):
        fetch_by_id_calls.append((file_id, binary, serialize))
        return "Zm9v"

    monkeypatch.setattr(
        opencti_stix2.opencti, "fetch_opencti_file_by_id", fake_fetch_by_id
    )

    entity = {
        "id": "intrusion-set--55555555-5555-4555-8555-555555555555",
        "type": "intrusion-set",
        "x_opencti_id": "internal-intrusion-set-id",
        "importFilesIds": ["import/Intrusion-Set/internal-intrusion-set-id/notes.txt"],
        "importFiles": [
            {
                "id": "import/Intrusion-Set/internal-intrusion-set-id/notes.txt",
                "name": "notes.txt",
                "metaData": {"mimetype": "text/plain", "version": "v2"},
            }
        ],
    }

    result = opencti_stix2.prepare_export(entity=entity, mode="simple")

    assert len(result) == 1
    x_opencti_files = result[0]["x_opencti_files"]
    assert len(x_opencti_files) == 1
    assert x_opencti_files[0]["name"] == "notes.txt"
    assert x_opencti_files[0]["data"] == "Zm9v"
    assert x_opencti_files[0]["mime_type"] == "text/plain"
    assert x_opencti_files[0]["version"] == "v2"
    assert fetch_by_id_calls == [
        ("import/Intrusion-Set/internal-intrusion-set-id/notes.txt", True, True)
    ]

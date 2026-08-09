import pytest

from pycti.utils.opencti_stix2_update import OpenCTIStix2Update


class _ExternalReference:
    def __init__(self):
        self.calls = []

    def create(self, **kwargs):
        self.calls.append(kwargs)
        return {"id": f"external-reference--{len(self.calls) - 1}"}


class _RelationAdder:
    def __init__(self):
        self.calls = []

    def add_external_reference(self, id, external_reference_id):
        self.calls.append((id, external_reference_id))
        return True


class _NestedRefRelationship:
    def __init__(self):
        self.object_calls = []
        self.relationship_calls = []

    def add_many_to_stix_core_object(self, from_id, to_ids, relationship_type):
        self.object_calls.append((from_id, list(to_ids), relationship_type))
        return True

    def add_many_to_stix_core_relationship(self, from_id, to_ids, relationship_type):
        self.relationship_calls.append((from_id, list(to_ids), relationship_type))
        return True


class _ObjectOnlyNestedRefRelationship:
    def __init__(self):
        self.object_calls = []

    def add_many_to_stix_core_object(self, from_id, to_ids, relationship_type):
        self.object_calls.append((from_id, list(to_ids), relationship_type))
        return True


class _OpenCTI:
    def __init__(self, nested_ref_relationship=True):
        self.external_reference = _ExternalReference()
        self.stix_domain_object = _RelationAdder()
        self.stix_cyber_observable = _RelationAdder()
        self.stix_core_relationship = _RelationAdder()
        if nested_ref_relationship is True:
            self.stix_nested_ref_relationship = _NestedRefRelationship()
        elif nested_ref_relationship is not False:
            self.stix_nested_ref_relationship = nested_ref_relationship


class _OpenCTIWithoutRelationTargets:
    def __init__(self):
        self.external_reference = _ExternalReference()


class _OrderedExternalReference:
    def __init__(self, events):
        self.calls = []
        self.events = events

    def create(self, **kwargs):
        self.calls.append(kwargs)
        external_reference_id = f"external-reference--{len(self.calls) - 1}"
        self.events.append(("create", external_reference_id))
        return {"id": external_reference_id}


class _OrderedRelationAdder:
    def __init__(self, events):
        self.calls = []
        self.events = events

    def add_external_reference(self, id, external_reference_id):
        self.calls.append((id, external_reference_id))
        self.events.append(("attach", external_reference_id))
        return True


class _OrderedOpenCTI:
    def __init__(self):
        self.events = []
        self.external_reference = _OrderedExternalReference(self.events)
        self.stix_domain_object = _OrderedRelationAdder(self.events)


def _external_references(count, version=2):
    values = [
        {
            "source_name": f"source-{index}",
            "url": f"https://example.test/{index}",
        }
        for index in range(count)
    ]
    if version == 1:
        return values
    return [{"value": value} for value in values]


def test_add_external_references_batches_domain_object_relations_in_bounded_chunks():
    opencti = _OpenCTI()
    updater = OpenCTIStix2Update(opencti)

    updater.add_external_references(
        "indicator", "indicator--1", _external_references(201)
    )

    assert [call["source_name"] for call in opencti.external_reference.calls] == [
        f"source-{index}" for index in range(201)
    ]
    assert opencti.stix_nested_ref_relationship.object_calls == [
        (
            "indicator--1",
            [f"external-reference--{index}" for index in range(100)],
            "external-reference",
        ),
        (
            "indicator--1",
            [f"external-reference--{index}" for index in range(100, 200)],
            "external-reference",
        ),
    ]
    assert opencti.stix_domain_object.calls == [
        ("indicator--1", "external-reference--200")
    ]


def test_add_external_references_uses_relationship_bulk_edit_path():
    opencti = _OpenCTI()
    updater = OpenCTIStix2Update(opencti)

    updater.add_external_references(
        "relationship", "relationship--1", _external_references(2)
    )

    assert opencti.stix_nested_ref_relationship.relationship_calls == [
        (
            "relationship--1",
            ["external-reference--0", "external-reference--1"],
            "external-reference",
        )
    ]
    assert opencti.stix_core_relationship.calls == []


@pytest.mark.parametrize(
    ("entity_type", "target_attribute", "version"),
    [
        ("indicator", "stix_domain_object", 2),
        ("ipv4-addr", "stix_cyber_observable", 1),
        ("relationship", "stix_core_relationship", 2),
    ],
)
def test_add_external_references_keeps_singleton_entity_dispatch(
    entity_type, target_attribute, version
):
    opencti = _OpenCTI()
    updater = OpenCTIStix2Update(opencti)

    updater.add_external_references(
        entity_type,
        "entity--1",
        _external_references(1, version=version),
        version=version,
    )

    assert getattr(opencti, target_attribute).calls == [
        ("entity--1", "external-reference--0")
    ]
    assert opencti.stix_nested_ref_relationship.object_calls == []
    assert opencti.stix_nested_ref_relationship.relationship_calls == []


def test_add_external_references_falls_back_to_single_mutations_without_bulk_helper():
    opencti = _OpenCTI(nested_ref_relationship=False)
    updater = OpenCTIStix2Update(opencti)

    updater.add_external_references(
        "indicator", "indicator--1", _external_references(2)
    )

    assert opencti.stix_domain_object.calls == [
        ("indicator--1", "external-reference--0"),
        ("indicator--1", "external-reference--1"),
    ]


def test_add_external_references_keeps_no_helper_create_attach_order():
    opencti = _OrderedOpenCTI()
    updater = OpenCTIStix2Update(opencti)

    updater.add_external_references(
        "indicator", "indicator--1", _external_references(2)
    )

    assert opencti.events == [
        ("create", "external-reference--0"),
        ("attach", "external-reference--0"),
        ("create", "external-reference--1"),
        ("attach", "external-reference--1"),
    ]


def test_add_external_references_falls_back_when_relationship_bulk_helper_is_missing():
    opencti = _OpenCTI(nested_ref_relationship=_ObjectOnlyNestedRefRelationship())
    updater = OpenCTIStix2Update(opencti)

    updater.add_external_references(
        "relationship", "relationship--1", _external_references(2)
    )

    assert opencti.stix_core_relationship.calls == [
        ("relationship--1", "external-reference--0"),
        ("relationship--1", "external-reference--1"),
    ]


def test_add_external_references_keeps_empty_and_all_invalid_values_as_noops():
    opencti = _OpenCTIWithoutRelationTargets()
    updater = OpenCTIStix2Update(opencti)

    updater.add_external_references("indicator", "indicator--1", [])
    updater.add_external_references(
        "indicator",
        "indicator--1",
        [
            {"value": {"source_name": "missing-url"}},
            {"value": {"url": "https://example.test/missing-source"}},
        ],
    )

    assert opencti.external_reference.calls == []


def test_add_external_references_skips_invalid_values_without_reordering_valid_ids():
    opencti = _OpenCTI()
    updater = OpenCTIStix2Update(opencti)

    updater.add_external_references(
        "indicator",
        "indicator--1",
        [
            {"value": {"source_name": "source-0", "url": "https://example.test/0"}},
            {"value": {"source_name": "missing-url"}},
            {"value": {"source_name": "source-1", "url": "https://example.test/1"}},
        ],
    )

    assert [call["source_name"] for call in opencti.external_reference.calls] == [
        "source-0",
        "source-1",
    ]
    assert opencti.stix_nested_ref_relationship.object_calls == [
        (
            "indicator--1",
            ["external-reference--0", "external-reference--1"],
            "external-reference",
        )
    ]

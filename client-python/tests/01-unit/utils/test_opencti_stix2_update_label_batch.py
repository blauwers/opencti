import pytest

from pycti.utils.opencti_stix2_update import (
    LABEL_PREFETCH_BATCH_SIZE,
    LABEL_RELATION_CREATE_BATCH_SIZE,
    OpenCTIStix2Update,
)


class _Label:
    def __init__(self, existing=None, fail_list=False):
        self.existing = {} if existing is None else dict(existing)
        self.fail_list = fail_list
        self.list_calls = []
        self.create_calls = []

    def list(self, filters, first=None, getAll=True):
        self.list_calls.append((filters, first, getAll))
        if self.fail_list:
            raise RuntimeError("prefetch failed")
        values = filters["filters"][0]["values"]
        return [
            self.existing[value.lower().strip()]
            for value in values
            if value.lower().strip() in self.existing
        ]

    def create(self, value):
        self.create_calls.append(value)
        label_data = {"id": f"label--created-{len(self.create_calls)}", "value": value}
        self.existing[value.lower().strip()] = label_data
        return label_data


class _RelationAdder:
    def __init__(self):
        self.calls = []

    def add_label(self, id, label_name=None, label_id=None):
        self.calls.append((id, label_name, label_id))
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
    def __init__(self, existing=None, fail_list=False, nested_ref_relationship=True):
        self.label = _Label(existing=existing, fail_list=fail_list)
        self.stix_domain_object = _RelationAdder()
        self.stix_cyber_observable = _RelationAdder()
        self.stix_core_relationship = _RelationAdder()
        if nested_ref_relationship is True:
            self.stix_nested_ref_relationship = _NestedRefRelationship()
        elif nested_ref_relationship is not False:
            self.stix_nested_ref_relationship = nested_ref_relationship


class _OpenCTIWithoutRelationTargets:
    def __init__(self):
        self.label = _Label()


class _OpenCTIWithExplodingBulkLookup:
    def __init__(self):
        self.label = _Label(existing=_existing_labels(1))
        self.stix_domain_object = _RelationAdder()

    @property
    def stix_nested_ref_relationship(self):
        raise AssertionError("singleton path must not inspect bulk helpers")


def _labels(count, version=2):
    values = [f"label-{index}" for index in range(count)]
    if version == 1:
        return values
    return [{"value": value} for value in values]


def _existing_labels(count):
    return {
        f"label-{index}": {"id": f"label--{index}", "value": f"label-{index}"}
        for index in range(count)
    }


def test_add_labels_prefetches_in_bounded_chunks_and_batches_relations():
    label_count = LABEL_PREFETCH_BATCH_SIZE + 1
    opencti = _OpenCTI(existing=_existing_labels(label_count))
    updater = OpenCTIStix2Update(opencti)

    updater.add_labels("indicator", "indicator--1", _labels(label_count))

    assert [
        len(call[0]["filters"][0]["values"]) for call in opencti.label.list_calls
    ] == [LABEL_PREFETCH_BATCH_SIZE, 1]
    assert [call[1] for call in opencti.label.list_calls] == [
        LABEL_PREFETCH_BATCH_SIZE,
        1,
    ]
    assert opencti.label.list_calls[0][0]["filters"][0]["values"][:2] == [
        "label-0",
        "label-1",
    ]
    assert opencti.label.list_calls[-1][0]["filters"][0]["values"] == ["label-1000"]
    assert opencti.label.create_calls == []
    assert len(opencti.stix_nested_ref_relationship.object_calls) == (
        LABEL_PREFETCH_BATCH_SIZE // LABEL_RELATION_CREATE_BATCH_SIZE
    )
    assert opencti.stix_nested_ref_relationship.object_calls[0] == (
        "indicator--1",
        [f"label--{index}" for index in range(LABEL_RELATION_CREATE_BATCH_SIZE)],
        "object-label",
    )
    assert opencti.stix_nested_ref_relationship.object_calls[-1] == (
        "indicator--1",
        [
            f"label--{index}"
            for index in range(
                LABEL_PREFETCH_BATCH_SIZE - LABEL_RELATION_CREATE_BATCH_SIZE,
                LABEL_PREFETCH_BATCH_SIZE,
            )
        ],
        "object-label",
    )
    assert opencti.stix_domain_object.calls == [("indicator--1", None, "label--1000")]


def test_add_labels_reuses_normalized_prefetch_matches_and_created_misses():
    opencti = _OpenCTI(existing={"known": {"id": "label--known", "value": "Known"}})
    updater = OpenCTIStix2Update(opencti)

    updater.add_labels(
        "indicator",
        "indicator--1",
        [{"value": " known "}, {"value": " Missing "}, {"value": "MISSING"}],
    )

    assert opencti.label.list_calls[0][0]["filters"][0]["values"] == [
        "known",
        "missing",
    ]
    assert opencti.label.create_calls == [" Missing "]
    assert opencti.stix_nested_ref_relationship.object_calls == [
        (
            "indicator--1",
            ["label--known", "label--created-1", "label--created-1"],
            "object-label",
        )
    ]


def test_add_labels_uses_relationship_bulk_edit_path():
    opencti = _OpenCTI(existing=_existing_labels(2))
    updater = OpenCTIStix2Update(opencti)

    updater.add_labels("relationship", "relationship--1", _labels(2))

    assert opencti.stix_nested_ref_relationship.relationship_calls == [
        ("relationship--1", ["label--0", "label--1"], "object-label")
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
def test_add_labels_keeps_singleton_entity_dispatch(
    entity_type, target_attribute, version
):
    opencti = _OpenCTI(existing=_existing_labels(1))
    updater = OpenCTIStix2Update(opencti)

    updater.add_labels(
        entity_type,
        "entity--1",
        _labels(1, version=version),
        version=version,
    )

    assert getattr(opencti, target_attribute).calls == [("entity--1", "label-0", None)]
    assert opencti.label.list_calls == []
    assert opencti.stix_nested_ref_relationship.object_calls == []
    assert opencti.stix_nested_ref_relationship.relationship_calls == []


def test_add_labels_keeps_singleton_path_independent_from_bulk_helper_lookup():
    opencti = _OpenCTIWithExplodingBulkLookup()
    updater = OpenCTIStix2Update(opencti)

    updater.add_labels("indicator", "indicator--1", [{"value": "label-0"}])

    assert opencti.stix_domain_object.calls == [("indicator--1", "label-0", None)]


def test_add_labels_falls_back_to_existing_helper_when_prefetch_fails():
    opencti = _OpenCTI(existing=_existing_labels(2), fail_list=True)
    updater = OpenCTIStix2Update(opencti)

    updater.add_labels("indicator", "indicator--1", _labels(2))

    assert opencti.stix_domain_object.calls == [
        ("indicator--1", "label-0", None),
        ("indicator--1", "label-1", None),
    ]
    assert opencti.stix_nested_ref_relationship.object_calls == []


def test_add_labels_keeps_existing_helper_path_without_bulk_helper():
    opencti = _OpenCTI(existing=_existing_labels(2), nested_ref_relationship=False)
    updater = OpenCTIStix2Update(opencti)

    updater.add_labels("indicator", "indicator--1", _labels(2))

    assert opencti.label.list_calls == []
    assert opencti.stix_domain_object.calls == [
        ("indicator--1", "label-0", None),
        ("indicator--1", "label-1", None),
    ]


def test_add_labels_keeps_existing_helper_path_for_non_string_values():
    opencti = _OpenCTI(existing=_existing_labels(1))
    updater = OpenCTIStix2Update(opencti)

    updater.add_labels(
        "indicator",
        "indicator--1",
        [{"value": "label-0"}, {"value": None}],
    )

    assert opencti.label.list_calls == []
    assert opencti.stix_domain_object.calls == [
        ("indicator--1", "label-0", None),
        ("indicator--1", None, None),
    ]


def test_add_labels_falls_back_when_relationship_bulk_helper_is_missing():
    opencti = _OpenCTI(
        existing=_existing_labels(2),
        nested_ref_relationship=_ObjectOnlyNestedRefRelationship(),
    )
    updater = OpenCTIStix2Update(opencti)

    updater.add_labels("relationship", "relationship--1", _labels(2))

    assert opencti.label.list_calls == []
    assert opencti.stix_core_relationship.calls == [
        ("relationship--1", "label-0", None),
        ("relationship--1", "label-1", None),
    ]


def test_add_labels_keeps_empty_values_as_noop():
    updater = OpenCTIStix2Update(_OpenCTIWithoutRelationTargets())

    updater.add_labels("indicator", "indicator--1", [])

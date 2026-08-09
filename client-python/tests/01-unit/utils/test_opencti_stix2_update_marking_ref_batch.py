import pytest

from pycti.utils.opencti_stix2_update import (
    BULK_REF_RELATION_VALIDATION_API_FEATURE,
    OBJECT_MARKING_REF_CREATE_BATCH_SIZE,
    OpenCTIStix2Update,
)


class _RelationAdder:
    def __init__(self):
        self.calls = []

    def add_marking_definition(self, id, marking_definition_id):
        self.calls.append((id, marking_definition_id))
        return True


class _NestedRefRelationship:
    def __init__(self):
        self.object_calls = []
        self.relationship_calls = []
        self.sighting_calls = []

    def add_many_to_stix_core_object(self, from_id, to_ids, relationship_type):
        self.object_calls.append((from_id, list(to_ids), relationship_type))
        return True

    def add_many_to_stix_core_relationship(self, from_id, to_ids, relationship_type):
        self.relationship_calls.append((from_id, list(to_ids), relationship_type))
        return True

    def add_many_to_stix_sighting_relationship(
        self, from_id, to_ids, relationship_type
    ):
        self.sighting_calls.append((from_id, list(to_ids), relationship_type))
        return True


class _ObjectOnlyNestedRefRelationship:
    def __init__(self):
        self.object_calls = []

    def add_many_to_stix_core_object(self, from_id, to_ids, relationship_type):
        self.object_calls.append((from_id, list(to_ids), relationship_type))
        return True


class _OpenCTI:
    def __init__(
        self,
        nested_ref_relationship=True,
        supports_bulk_validation=True,
        feature_error=None,
    ):
        self._supports_bulk_validation = supports_bulk_validation
        self._feature_error = feature_error
        self.feature_calls = []
        self.stix_domain_object = _RelationAdder()
        self.stix_cyber_observable = _RelationAdder()
        self.stix_core_relationship = _RelationAdder()
        self.stix_sighting_relationship = _RelationAdder()
        if nested_ref_relationship is True:
            self.stix_nested_ref_relationship = _NestedRefRelationship()
        elif nested_ref_relationship is not False:
            self.stix_nested_ref_relationship = nested_ref_relationship

    def supports_api_feature(self, feature):
        self.feature_calls.append(feature)
        if self._feature_error is not None:
            raise self._feature_error
        return (
            feature == BULK_REF_RELATION_VALIDATION_API_FEATURE
            and self._supports_bulk_validation
        )


class _OpenCTIWithoutFeatureProbe:
    def __init__(self):
        self.stix_domain_object = _RelationAdder()
        self.stix_cyber_observable = _RelationAdder()
        self.stix_core_relationship = _RelationAdder()
        self.stix_sighting_relationship = _RelationAdder()
        self.stix_nested_ref_relationship = _NestedRefRelationship()


class _OpenCTIWithoutRelationTargets:
    pass


class _OpenCTIWithExplodingBulkLookup:
    def __init__(self):
        self.stix_domain_object = _RelationAdder()

    @property
    def stix_nested_ref_relationship(self):
        raise AssertionError("singleton path must not inspect bulk helpers")


def _marking_refs(count, version=2):
    values = [f"marking-definition--{index}" for index in range(count)]
    if version == 1:
        return values
    return [{"value": value} for value in values]


def test_add_object_marking_refs_batches_domain_object_relations_in_bounded_chunks():
    ref_count = OBJECT_MARKING_REF_CREATE_BATCH_SIZE * 2 + 1
    opencti = _OpenCTI()
    updater = OpenCTIStix2Update(opencti)

    updater.add_object_marking_refs(
        "indicator", "indicator--1", _marking_refs(ref_count)
    )

    assert opencti.stix_nested_ref_relationship.object_calls == [
        (
            "indicator--1",
            [
                f"marking-definition--{index}"
                for index in range(OBJECT_MARKING_REF_CREATE_BATCH_SIZE)
            ],
            "object-marking",
        ),
        (
            "indicator--1",
            [
                f"marking-definition--{index}"
                for index in range(
                    OBJECT_MARKING_REF_CREATE_BATCH_SIZE,
                    OBJECT_MARKING_REF_CREATE_BATCH_SIZE * 2,
                )
            ],
            "object-marking",
        ),
    ]
    assert opencti.stix_domain_object.calls == [
        ("indicator--1", f"marking-definition--{ref_count - 1}")
    ]
    assert opencti.feature_calls == [BULK_REF_RELATION_VALIDATION_API_FEATURE]


def test_add_object_marking_refs_uses_relationship_bulk_edit_path():
    opencti = _OpenCTI()
    updater = OpenCTIStix2Update(opencti)

    updater.add_object_marking_refs("relationship", "relationship--1", _marking_refs(2))

    assert opencti.stix_nested_ref_relationship.relationship_calls == [
        (
            "relationship--1",
            ["marking-definition--0", "marking-definition--1"],
            "object-marking",
        )
    ]
    assert opencti.stix_core_relationship.calls == []


def test_add_object_marking_refs_uses_sighting_bulk_edit_path():
    opencti = _OpenCTI()
    updater = OpenCTIStix2Update(opencti)

    updater.add_object_marking_refs("sighting", "sighting--1", _marking_refs(2))

    assert opencti.stix_nested_ref_relationship.sighting_calls == [
        (
            "sighting--1",
            ["marking-definition--0", "marking-definition--1"],
            "object-marking",
        )
    ]
    assert opencti.stix_sighting_relationship.calls == []


@pytest.mark.parametrize(
    ("entity_type", "target_attribute", "version"),
    [
        ("indicator", "stix_domain_object", 2),
        ("ipv4-addr", "stix_cyber_observable", 1),
        ("relationship", "stix_core_relationship", 2),
        ("sighting", "stix_sighting_relationship", 2),
    ],
)
def test_add_object_marking_refs_keeps_singleton_entity_dispatch(
    entity_type, target_attribute, version
):
    opencti = _OpenCTI()
    updater = OpenCTIStix2Update(opencti)

    updater.add_object_marking_refs(
        entity_type,
        "entity--1",
        _marking_refs(1, version=version),
        version=version,
    )

    assert getattr(opencti, target_attribute).calls == [
        ("entity--1", "marking-definition--0")
    ]
    assert opencti.feature_calls == []
    assert opencti.stix_nested_ref_relationship.object_calls == []
    assert opencti.stix_nested_ref_relationship.relationship_calls == []
    assert opencti.stix_nested_ref_relationship.sighting_calls == []


def test_add_object_marking_refs_keeps_singleton_path_independent_from_bulk_lookup():
    opencti = _OpenCTIWithExplodingBulkLookup()
    updater = OpenCTIStix2Update(opencti)

    updater.add_object_marking_refs(
        "indicator", "indicator--1", [{"value": "marking-definition--1"}]
    )

    assert opencti.stix_domain_object.calls == [
        ("indicator--1", "marking-definition--1")
    ]


def test_add_object_marking_refs_keeps_empty_input_as_no_op():
    updater = OpenCTIStix2Update(_OpenCTIWithoutRelationTargets())

    updater.add_object_marking_refs("indicator", "indicator--1", [])


def test_add_object_marking_refs_keeps_non_string_values_on_scalar_path():
    opencti = _OpenCTI()
    updater = OpenCTIStix2Update(opencti)

    updater.add_object_marking_refs(
        "indicator",
        "indicator--1",
        [{"value": "marking-definition--1"}, {"value": None}],
    )

    assert opencti.stix_domain_object.calls == [
        ("indicator--1", "marking-definition--1"),
        ("indicator--1", None),
    ]
    assert opencti.feature_calls == []
    assert opencti.stix_nested_ref_relationship.object_calls == []


def test_add_object_marking_refs_falls_back_without_bulk_helper():
    opencti = _OpenCTI(nested_ref_relationship=False)
    updater = OpenCTIStix2Update(opencti)

    updater.add_object_marking_refs("indicator", "indicator--1", _marking_refs(2))

    assert opencti.stix_domain_object.calls == [
        ("indicator--1", "marking-definition--0"),
        ("indicator--1", "marking-definition--1"),
    ]
    assert opencti.feature_calls == []


def test_add_object_marking_refs_falls_back_when_entity_bulk_helper_is_missing():
    opencti = _OpenCTI(nested_ref_relationship=_ObjectOnlyNestedRefRelationship())
    updater = OpenCTIStix2Update(opencti)

    updater.add_object_marking_refs("relationship", "relationship--1", _marking_refs(2))

    assert opencti.stix_core_relationship.calls == [
        ("relationship--1", "marking-definition--0"),
        ("relationship--1", "marking-definition--1"),
    ]
    assert opencti.feature_calls == []


def test_add_object_marking_refs_falls_back_without_feature_probe():
    opencti = _OpenCTIWithoutFeatureProbe()
    updater = OpenCTIStix2Update(opencti)

    updater.add_object_marking_refs("indicator", "indicator--1", _marking_refs(2))

    assert opencti.stix_domain_object.calls == [
        ("indicator--1", "marking-definition--0"),
        ("indicator--1", "marking-definition--1"),
    ]
    assert opencti.stix_nested_ref_relationship.object_calls == []


def test_add_object_marking_refs_falls_back_when_platform_does_not_advertise_validation():
    opencti = _OpenCTI(supports_bulk_validation=False)
    updater = OpenCTIStix2Update(opencti)

    updater.add_object_marking_refs("indicator", "indicator--1", _marking_refs(2))

    assert opencti.stix_domain_object.calls == [
        ("indicator--1", "marking-definition--0"),
        ("indicator--1", "marking-definition--1"),
    ]
    assert opencti.stix_nested_ref_relationship.object_calls == []
    assert opencti.feature_calls == [BULK_REF_RELATION_VALIDATION_API_FEATURE]


def test_add_object_marking_refs_falls_back_when_feature_probe_fails():
    opencti = _OpenCTI(feature_error=RuntimeError("temporary failure"))
    updater = OpenCTIStix2Update(opencti)

    updater.add_object_marking_refs("indicator", "indicator--1", _marking_refs(2))

    assert opencti.stix_domain_object.calls == [
        ("indicator--1", "marking-definition--0"),
        ("indicator--1", "marking-definition--1"),
    ]
    assert opencti.stix_nested_ref_relationship.object_calls == []
    assert opencti.feature_calls == [BULK_REF_RELATION_VALIDATION_API_FEATURE]

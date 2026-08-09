import pytest

from pycti.utils.opencti_stix2_update import (
    BULK_REF_RELATION_DELETE_API_FEATURE,
    LABEL_PREFETCH_BATCH_SIZE,
    REF_RELATION_DELETE_BATCH_SIZE,
    OpenCTIStix2Update,
)


class _RelationRemover:
    def __init__(self):
        self.marking_calls = []
        self.external_reference_calls = []
        self.kill_chain_phase_calls = []
        self.label_calls = []

    def remove_marking_definition(self, id, marking_definition_id):
        self.marking_calls.append((id, marking_definition_id))
        return True

    def remove_external_reference(self, id, external_reference_id):
        self.external_reference_calls.append((id, external_reference_id))
        return True

    def remove_kill_chain_phase(self, id, kill_chain_phase_id):
        self.kill_chain_phase_calls.append((id, kill_chain_phase_id))
        return True

    def remove_label(self, id, label_id=None, label_name=None):
        self.label_calls.append((id, label_id, label_name))
        return True


class _ObjectRefRemover:
    def __init__(self):
        self.calls = []

    def remove_stix_object_or_stix_relationship(
        self, id, stixObjectOrStixRelationshipId
    ):
        self.calls.append((id, stixObjectOrStixRelationshipId))
        return True


class _NestedRefRelationship:
    def __init__(self):
        self.object_calls = []
        self.relationship_calls = []
        self.sighting_calls = []

    def remove_many_to_stix_core_object(self, from_id, to_ids, relationship_type):
        self.object_calls.append((from_id, list(to_ids), relationship_type))
        return True

    def remove_many_to_stix_core_relationship(self, from_id, to_ids, relationship_type):
        self.relationship_calls.append((from_id, list(to_ids), relationship_type))
        return True

    def remove_many_to_stix_sighting_relationship(
        self, from_id, to_ids, relationship_type
    ):
        self.sighting_calls.append((from_id, list(to_ids), relationship_type))
        return True


class _ObjectOnlyNestedRefRelationship:
    def __init__(self):
        self.object_calls = []

    def remove_many_to_stix_core_object(self, from_id, to_ids, relationship_type):
        self.object_calls.append((from_id, list(to_ids), relationship_type))
        return True


class _Label:
    def __init__(self, existing=None, fail_list=False):
        self.existing = {} if existing is None else dict(existing)
        self.fail_list = fail_list
        self.list_calls = []

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


class _OpenCTI:
    def __init__(
        self,
        existing_labels=None,
        fail_label_list=False,
        nested_ref_relationship=True,
        supports_bulk_delete=True,
        feature_error=None,
    ):
        self._supports_bulk_delete = supports_bulk_delete
        self._feature_error = feature_error
        self.feature_calls = []
        self.label = _Label(existing=existing_labels, fail_list=fail_label_list)
        self.stix_domain_object = _RelationRemover()
        self.stix_cyber_observable = _RelationRemover()
        self.stix_core_relationship = _RelationRemover()
        self.stix_sighting_relationship = _RelationRemover()
        self.report = _ObjectRefRemover()
        self.note = _ObjectRefRemover()
        self.observed_data = _ObjectRefRemover()
        self.opinion = _ObjectRefRemover()
        self.grouping = _ObjectRefRemover()
        self.case_incident = _ObjectRefRemover()
        self.case_rfi = _ObjectRefRemover()
        self.case_rft = _ObjectRefRemover()
        self.feedback = _ObjectRefRemover()
        self.task = _ObjectRefRemover()
        if nested_ref_relationship is True:
            self.stix_nested_ref_relationship = _NestedRefRelationship()
        elif nested_ref_relationship is not False:
            self.stix_nested_ref_relationship = nested_ref_relationship

    def supports_api_feature(self, feature):
        self.feature_calls.append(feature)
        if self._feature_error is not None:
            raise self._feature_error
        return (
            feature == BULK_REF_RELATION_DELETE_API_FEATURE
            and self._supports_bulk_delete
        )


class _OpenCTIWithoutFeatureProbe:
    def __init__(self):
        self.report = _ObjectRefRemover()
        self.stix_nested_ref_relationship = _NestedRefRelationship()


class _OpenCTIWithoutRelationTargets:
    pass


class _OpenCTIWithExplodingBulkLookup:
    def __init__(self):
        self.report = _ObjectRefRemover()
        self.label = _Label(existing=_existing_labels(1))
        self.stix_domain_object = _RelationRemover()

    @property
    def stix_nested_ref_relationship(self):
        raise AssertionError("singleton path must not inspect bulk helpers")


def _object_refs(count, version=2):
    values = [f"indicator--{index}" for index in range(count)]
    if version == 1:
        return values
    return [{"value": value} for value in values]


def _marking_refs(count, version=2):
    values = [f"marking-definition--{index}" for index in range(count)]
    if version == 1:
        return values
    return [{"value": value} for value in values]


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


def test_remove_object_refs_batches_multiple_refs_in_bounded_chunks():
    ref_count = REF_RELATION_DELETE_BATCH_SIZE * 2 + 1
    opencti = _OpenCTI()
    updater = OpenCTIStix2Update(opencti)

    updater.remove_object_refs("report", "report--1", _object_refs(ref_count))

    assert opencti.stix_nested_ref_relationship.object_calls == [
        (
            "report--1",
            [f"indicator--{index}" for index in range(REF_RELATION_DELETE_BATCH_SIZE)],
            "object",
        ),
        (
            "report--1",
            [
                f"indicator--{index}"
                for index in range(
                    REF_RELATION_DELETE_BATCH_SIZE,
                    REF_RELATION_DELETE_BATCH_SIZE * 2,
                )
            ],
            "object",
        ),
    ]
    assert opencti.report.calls == [
        ("report--1", f"indicator--{REF_RELATION_DELETE_BATCH_SIZE * 2}")
    ]
    assert opencti.feature_calls == [BULK_REF_RELATION_DELETE_API_FEATURE]


@pytest.mark.parametrize(
    ("entity_type", "entity_attribute"),
    [
        ("report", "report"),
        ("note", "note"),
        ("observed-data", "observed_data"),
        ("opinion", "opinion"),
        ("grouping", "grouping"),
        ("case-incident", "case_incident"),
        ("case-rfi", "case_rfi"),
        ("case-rft", "case_rft"),
        ("feedback", "feedback"),
        ("task", "task"),
    ],
)
def test_remove_object_refs_keeps_single_ref_on_entity_specific_path(
    entity_type, entity_attribute
):
    opencti = _OpenCTI()
    updater = OpenCTIStix2Update(opencti)

    updater.remove_object_refs(entity_type, f"{entity_type}--1", _object_refs(1, 1), 1)

    assert getattr(opencti, entity_attribute).calls == [
        (f"{entity_type}--1", "indicator--0")
    ]
    assert opencti.feature_calls == []
    assert opencti.stix_nested_ref_relationship.object_calls == []


def test_remove_object_refs_keeps_singleton_path_independent_from_bulk_lookup():
    opencti = _OpenCTIWithExplodingBulkLookup()
    updater = OpenCTIStix2Update(opencti)

    updater.remove_object_refs("report", "report--1", [{"value": "indicator--1"}])

    assert opencti.report.calls == [("report--1", "indicator--1")]


def test_remove_object_refs_keeps_empty_supported_refs_as_noop_without_helpers():
    updater = OpenCTIStix2Update(_OpenCTIWithoutRelationTargets())

    updater.remove_object_refs("report", "report--1", [])


def test_remove_object_refs_ignores_unsupported_entity_types():
    opencti = _OpenCTI()
    updater = OpenCTIStix2Update(opencti)

    updater.remove_object_refs(
        "unsupported", "unsupported--1", [{"value": "indicator--1"}]
    )

    assert opencti.report.calls == []
    assert opencti.feature_calls == []
    assert opencti.stix_nested_ref_relationship.object_calls == []


def test_remove_object_refs_keeps_non_string_values_on_scalar_path():
    opencti = _OpenCTI()
    updater = OpenCTIStix2Update(opencti)

    updater.remove_object_refs(
        "report",
        "report--1",
        [{"value": "indicator--1"}, {"value": None}],
    )

    assert opencti.report.calls == [
        ("report--1", "indicator--1"),
        ("report--1", None),
    ]
    assert opencti.feature_calls == []
    assert opencti.stix_nested_ref_relationship.object_calls == []


def test_remove_object_refs_falls_back_without_bulk_helper():
    opencti = _OpenCTI(nested_ref_relationship=False)
    updater = OpenCTIStix2Update(opencti)

    updater.remove_object_refs("report", "report--1", _object_refs(2))

    assert opencti.report.calls == [
        ("report--1", "indicator--0"),
        ("report--1", "indicator--1"),
    ]
    assert opencti.feature_calls == []


def test_remove_object_refs_falls_back_without_feature_probe():
    opencti = _OpenCTIWithoutFeatureProbe()
    updater = OpenCTIStix2Update(opencti)

    updater.remove_object_refs("report", "report--1", _object_refs(2))

    assert opencti.report.calls == [
        ("report--1", "indicator--0"),
        ("report--1", "indicator--1"),
    ]
    assert opencti.stix_nested_ref_relationship.object_calls == []


@pytest.mark.parametrize(
    "opencti",
    [
        _OpenCTI(supports_bulk_delete=False),
        _OpenCTI(feature_error=RuntimeError("temporary failure")),
    ],
)
def test_remove_object_refs_falls_back_when_bulk_delete_is_unavailable(opencti):
    updater = OpenCTIStix2Update(opencti)

    updater.remove_object_refs("report", "report--1", _object_refs(2))

    assert opencti.report.calls == [
        ("report--1", "indicator--0"),
        ("report--1", "indicator--1"),
    ]
    assert opencti.stix_nested_ref_relationship.object_calls == []
    assert opencti.feature_calls == [BULK_REF_RELATION_DELETE_API_FEATURE]


def test_remove_relation_helpers_use_bulk_delete_for_known_ref_ids():
    opencti = _OpenCTI()
    updater = OpenCTIStix2Update(opencti)

    updater.remove_object_marking_refs(
        "relationship",
        "relationship--1",
        [{"value": "marking-definition--1"}, {"value": "marking-definition--2"}],
    )
    updater.remove_object_marking_refs(
        "sighting",
        "sighting--1",
        [{"value": "marking-definition--3"}, {"value": "marking-definition--4"}],
    )
    updater.remove_external_references(
        "relationship",
        "relationship--1",
        [{"id": "external-reference--1"}, {"id": "external-reference--2"}],
    )
    updater.remove_kill_chain_phases(
        "relationship",
        "relationship--1",
        [{"id": "kill-chain-phase--1"}, {"id": "kill-chain-phase--2"}],
    )

    assert opencti.stix_nested_ref_relationship.relationship_calls == [
        (
            "relationship--1",
            ["marking-definition--1", "marking-definition--2"],
            "object-marking",
        ),
        (
            "relationship--1",
            ["external-reference--1", "external-reference--2"],
            "external-reference",
        ),
        (
            "relationship--1",
            ["kill-chain-phase--1", "kill-chain-phase--2"],
            "kill-chain-phase",
        ),
    ]
    assert opencti.stix_nested_ref_relationship.sighting_calls == [
        (
            "sighting--1",
            ["marking-definition--3", "marking-definition--4"],
            "object-marking",
        )
    ]
    assert opencti.stix_core_relationship.marking_calls == []
    assert opencti.stix_core_relationship.external_reference_calls == []
    assert opencti.stix_core_relationship.kill_chain_phase_calls == []
    assert opencti.stix_sighting_relationship.marking_calls == []


@pytest.mark.parametrize(
    ("entity_type", "target_attribute", "version"),
    [
        ("indicator", "stix_domain_object", 2),
        ("ipv4-addr", "stix_cyber_observable", 1),
        ("relationship", "stix_core_relationship", 2),
        ("sighting", "stix_sighting_relationship", 2),
    ],
)
def test_remove_object_marking_refs_keeps_singleton_entity_dispatch(
    entity_type, target_attribute, version
):
    opencti = _OpenCTI()
    updater = OpenCTIStix2Update(opencti)

    updater.remove_object_marking_refs(
        entity_type,
        "entity--1",
        _marking_refs(1, version=version),
        version=version,
    )

    assert getattr(opencti, target_attribute).marking_calls == [
        ("entity--1", "marking-definition--0")
    ]
    assert opencti.feature_calls == []


def test_remove_non_marking_sighting_relations_keep_existing_scalar_dispatch():
    opencti = _OpenCTI()
    updater = OpenCTIStix2Update(opencti)

    updater.remove_external_references(
        "sighting",
        "sighting--1",
        [{"id": "external-reference--1"}, {"id": "external-reference--2"}],
    )

    assert opencti.stix_domain_object.external_reference_calls == [
        ("sighting--1", "external-reference--1"),
        ("sighting--1", "external-reference--2"),
    ]
    assert opencti.feature_calls == []
    assert opencti.stix_nested_ref_relationship.sighting_calls == []


def test_remove_labels_prefetches_in_bounded_chunks_and_batches_relations():
    label_count = LABEL_PREFETCH_BATCH_SIZE + 1
    opencti = _OpenCTI(existing_labels=_existing_labels(label_count))
    updater = OpenCTIStix2Update(opencti)

    updater.remove_labels("indicator", "indicator--1", _labels(label_count))

    assert [
        len(call[0]["filters"][0]["values"]) for call in opencti.label.list_calls
    ] == [LABEL_PREFETCH_BATCH_SIZE, 1]
    assert [call[1] for call in opencti.label.list_calls] == [
        LABEL_PREFETCH_BATCH_SIZE,
        1,
    ]
    assert len(opencti.stix_nested_ref_relationship.object_calls) == (
        LABEL_PREFETCH_BATCH_SIZE // REF_RELATION_DELETE_BATCH_SIZE
    )
    assert opencti.stix_nested_ref_relationship.object_calls[0] == (
        "indicator--1",
        [f"label--{index}" for index in range(REF_RELATION_DELETE_BATCH_SIZE)],
        "object-label",
    )
    assert opencti.stix_domain_object.label_calls == [
        ("indicator--1", "label--1000", None)
    ]


def test_remove_labels_reuses_normalized_prefetch_matches_and_skips_missing_labels():
    opencti = _OpenCTI(
        existing_labels={
            "existing-one": {"id": "label--existing-one", "value": "Existing-One"},
            "existing-two": {"id": "label--existing-two", "value": "existing-two"},
        }
    )
    updater = OpenCTIStix2Update(opencti)

    updater.remove_labels(
        "indicator",
        "indicator--1",
        [
            {"value": " Existing-One "},
            {"value": "existing-two"},
            {"value": "missing"},
            {"value": "EXISTING-TWO"},
        ],
    )

    assert opencti.label.list_calls[0][0]["filters"][0]["values"] == [
        "existing-one",
        "existing-two",
        "missing",
    ]
    assert opencti.stix_nested_ref_relationship.object_calls == [
        (
            "indicator--1",
            ["label--existing-one", "label--existing-two", "label--existing-two"],
            "object-label",
        )
    ]
    assert opencti.stix_domain_object.label_calls == []


def test_remove_labels_keeps_all_missing_prefetch_results_as_noop():
    opencti = _OpenCTI(existing_labels={})
    updater = OpenCTIStix2Update(opencti)

    updater.remove_labels(
        "indicator",
        "indicator--1",
        [{"value": "missing-one"}, {"value": "missing-two"}],
    )

    assert len(opencti.label.list_calls) == 1
    assert opencti.stix_nested_ref_relationship.object_calls == []
    assert opencti.stix_domain_object.label_calls == []


def test_remove_labels_falls_back_to_existing_helper_when_prefetch_fails():
    opencti = _OpenCTI(existing_labels=_existing_labels(2), fail_label_list=True)
    updater = OpenCTIStix2Update(opencti)

    updater.remove_labels("indicator", "indicator--1", _labels(2))

    assert opencti.stix_domain_object.label_calls == [
        ("indicator--1", None, "label-0"),
        ("indicator--1", None, "label-1"),
    ]
    assert opencti.stix_nested_ref_relationship.object_calls == []


def test_remove_labels_keeps_singleton_path_independent_from_bulk_lookup():
    opencti = _OpenCTIWithExplodingBulkLookup()
    updater = OpenCTIStix2Update(opencti)

    updater.remove_labels("indicator", "indicator--1", [{"value": "label-0"}])

    assert opencti.stix_domain_object.label_calls == [("indicator--1", None, "label-0")]


def test_remove_labels_keeps_non_string_values_on_scalar_path():
    opencti = _OpenCTI(existing_labels=_existing_labels(1))
    updater = OpenCTIStix2Update(opencti)

    updater.remove_labels(
        "indicator",
        "indicator--1",
        [{"value": "label-0"}, {"value": None}],
    )

    assert opencti.label.list_calls == []
    assert opencti.stix_domain_object.label_calls == [
        ("indicator--1", None, "label-0"),
        ("indicator--1", None, None),
    ]
    assert opencti.feature_calls == []


def test_remove_labels_falls_back_without_relationship_bulk_helper():
    opencti = _OpenCTI(
        existing_labels=_existing_labels(2),
        nested_ref_relationship=_ObjectOnlyNestedRefRelationship(),
    )
    updater = OpenCTIStix2Update(opencti)

    updater.remove_labels("relationship", "relationship--1", _labels(2))

    assert opencti.label.list_calls == []
    assert opencti.stix_core_relationship.label_calls == [
        ("relationship--1", None, "label-0"),
        ("relationship--1", None, "label-1"),
    ]
    assert opencti.feature_calls == []

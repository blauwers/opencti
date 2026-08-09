import pytest

from pycti.utils.opencti_stix2_update import (
    OBJECT_REF_CREATE_BATCH_SIZE,
    OpenCTIStix2Update,
)


class _RelationAdder:
    def __init__(self):
        self.calls = []

    def add_stix_object_or_stix_relationship(self, id, stixObjectOrStixRelationshipId):
        self.calls.append((id, stixObjectOrStixRelationshipId))
        return True


class _NestedRefRelationship:
    def __init__(self):
        self.calls = []

    def add_many_to_stix_core_object(self, from_id, to_ids, relationship_type):
        self.calls.append((from_id, list(to_ids), relationship_type))
        return True


class _OpenCTI:
    def __init__(self, with_bulk=True):
        self.report = _RelationAdder()
        self.note = _RelationAdder()
        self.observed_data = _RelationAdder()
        self.opinion = _RelationAdder()
        self.grouping = _RelationAdder()
        self.case_incident = _RelationAdder()
        self.case_rfi = _RelationAdder()
        self.case_rft = _RelationAdder()
        self.feedback = _RelationAdder()
        self.task = _RelationAdder()
        if with_bulk:
            self.stix_nested_ref_relationship = _NestedRefRelationship()


def test_add_object_refs_batches_multiple_refs_in_bounded_chunks():
    opencti = _OpenCTI()
    updater = OpenCTIStix2Update(opencti)
    object_refs = [
        {"value": f"indicator--{index}"}
        for index in range(OBJECT_REF_CREATE_BATCH_SIZE * 2 + 1)
    ]

    updater.add_object_refs("report", "report--1", object_refs)

    assert opencti.stix_nested_ref_relationship.calls == [
        (
            "report--1",
            [f"indicator--{index}" for index in range(OBJECT_REF_CREATE_BATCH_SIZE)],
            "object",
        ),
        (
            "report--1",
            [
                f"indicator--{index}"
                for index in range(
                    OBJECT_REF_CREATE_BATCH_SIZE, OBJECT_REF_CREATE_BATCH_SIZE * 2
                )
            ],
            "object",
        ),
    ]
    assert opencti.report.calls == [
        ("report--1", f"indicator--{OBJECT_REF_CREATE_BATCH_SIZE * 2}")
    ]


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
def test_add_object_refs_keeps_single_ref_on_entity_specific_path(
    entity_type, entity_attribute
):
    opencti = _OpenCTI()
    updater = OpenCTIStix2Update(opencti)

    updater.add_object_refs(
        entity_type, f"{entity_type}--1", ["indicator--1"], version=1
    )

    assert getattr(opencti, entity_attribute).calls == [
        (f"{entity_type}--1", "indicator--1")
    ]
    assert opencti.stix_nested_ref_relationship.calls == []


def test_add_object_refs_falls_back_to_single_mutations_without_bulk_helper():
    opencti = _OpenCTI(with_bulk=False)
    updater = OpenCTIStix2Update(opencti)

    updater.add_object_refs(
        "report",
        "report--1",
        [{"value": "indicator--1"}, {"value": "indicator--2"}],
    )

    assert opencti.report.calls == [
        ("report--1", "indicator--1"),
        ("report--1", "indicator--2"),
    ]


def test_add_object_refs_keeps_empty_supported_refs_as_noop_without_helper():
    updater = OpenCTIStix2Update(object())

    updater.add_object_refs("report", "report--1", [])


def test_add_object_refs_ignores_unsupported_entity_types():
    opencti = _OpenCTI()
    updater = OpenCTIStix2Update(opencti)

    updater.add_object_refs(
        "unsupported", "unsupported--1", [{"value": "indicator--1"}]
    )

    assert opencti.report.calls == []
    assert opencti.stix_nested_ref_relationship.calls == []

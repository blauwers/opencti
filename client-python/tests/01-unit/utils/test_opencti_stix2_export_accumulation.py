from types import SimpleNamespace

from pycti.utils.opencti_stix2 import (
    EXPORT_ACCESS_LISTER_ATTRIBUTES,
    EXPORT_PREFETCH_BATCH_SIZE,
    OpenCTIStix2,
)


class _StaticCollection:
    def __init__(self, items):
        self.items = items

    def list(self, **_kwargs):
        return self.items


class _CountingCollection(_StaticCollection):
    def __init__(self, items):
        super().__init__(items)
        self.list_calls = 0

    def list(self, **kwargs):
        self.list_calls += 1
        return super().list(**kwargs)


class _RelationshipCollection:
    def __init__(self, relationships_by_root):
        self.relationships_by_root = relationships_by_root

    def list(self, **kwargs):
        from_or_to_id = kwargs["fromOrToId"]
        if not isinstance(from_or_to_id, list):
            return self.relationships_by_root.get(from_or_to_id, [])

        relationships = []
        seen_relationship_ids = set()
        for entity_id in from_or_to_id:
            for relationship in self.relationships_by_root.get(entity_id, []):
                if relationship["id"] not in seen_relationship_ids:
                    seen_relationship_ids.add(relationship["id"])
                    relationships.append(relationship)
        return relationships


class _CountingCoreRelationshipCollection(_RelationshipCollection):
    def __init__(self, relationships_by_root=None):
        super().__init__(relationships_by_root or {})
        self.kwargs = []

    def list(self, **kwargs):
        self.kwargs.append(kwargs)
        return super().list(**kwargs)


class _CountingSightingRelationshipCollection(_RelationshipCollection):
    def __init__(self, relationships_by_root=None):
        super().__init__(relationships_by_root or {})
        self.kwargs = []

    def list(self, **kwargs):
        self.kwargs.append(kwargs)
        if "fromOrToId" in kwargs:
            return super().list(**kwargs)

        from_or_to_ids = kwargs["filters"]["filters"][0]["values"]
        return super().list(fromOrToId=from_or_to_ids)


class _CountingNestedRefRelationshipCollection:
    def __init__(self, relationships_by_source=None):
        self.relationships_by_source = relationships_by_source or {}
        self.kwargs = []

    def list(self, **kwargs):
        self.kwargs.append(kwargs)
        source_ids = kwargs.get("fromId")
        if source_ids is None:
            source_ids = kwargs["filters"]["filters"][0]["values"]
        if not isinstance(source_ids, list):
            source_ids = [source_ids]

        relationships = []
        seen_relationship_ids = set()
        for source_id in source_ids:
            for relationship in self.relationships_by_source.get(source_id, []):
                if relationship["id"] not in seen_relationship_ids:
                    seen_relationship_ids.add(relationship["id"])
                    relationships.append(relationship)
        return relationships


class _CountingRelatedObjectLister:
    def __init__(self, targets_by_id):
        self.targets_by_id = targets_by_id
        self.list_calls = 0
        self.filters = []
        self.firsts = []
        self.kwargs = []

    def list(self, **kwargs):
        self.list_calls += 1
        self.filters.append(kwargs["filters"])
        self.firsts.append(kwargs["first"])
        self.kwargs.append(kwargs)
        return [self.targets_by_id[target_id] for target_id in kwargs["filters"]]


class _CountingAccessCollection:
    def __init__(self, visible_ids=None):
        self.list_calls = 0
        self.kwargs = []
        self.visible_ids = set(visible_ids) if visible_ids is not None else None

    def list(self, **kwargs):
        self.list_calls += 1
        self.kwargs.append(kwargs)
        entity_ids = kwargs["filters"]
        if isinstance(entity_ids, str):
            entity_ids = [entity_ids]
        return [
            {"id": entity_id}
            for entity_id in entity_ids
            if self.visible_ids is None or entity_id in self.visible_ids
        ]


def _helper(entities=None):
    helper = OpenCTIStix2.__new__(OpenCTIStix2)
    helper.generate_export = lambda entity: entity
    helper.prepare_export = lambda entity, mode, access_filter: [entity]
    if entities is not None:
        helper.export_entities_list = lambda **_kwargs: entities
    return helper


def _relationship(identifier, target_identifier=None):
    target_identifier = target_identifier or identifier
    return {
        "id": identifier,
        "type": "uses",
        "x_opencti_id": f"internal-{identifier}",
        "from": {
            "id": "root",
            "standard_id": "indicator--root",
            "entity_type": "Indicator",
            "parent_types": ["Stix-Domain-Object"],
        },
        "to": {
            "id": f"target-{target_identifier}",
            "standard_id": f"malware--{target_identifier}",
            "entity_type": "Malware",
            "parent_types": ["Stix-Domain-Object"],
        },
    }


def _nested_ref_relationship(identifier, from_identifier, target_identifier):
    relationship = _relationship(identifier, target_identifier)
    relationship["relationship_type"] = "sample"
    relationship["from"]["id"] = from_identifier
    relationship["from"]["standard_id"] = f"indicator--{from_identifier}"
    return relationship


def _relationship_from_root(identifier, root_identifier, target_identifier):
    relationship = _relationship(identifier, target_identifier)
    relationship["from"]["id"] = root_identifier
    relationship["from"]["standard_id"] = f"indicator--{root_identifier}"
    return relationship


def _relationship_root(identifier):
    return {
        "id": f"relationship--root-{identifier}",
        "type": "uses",
        "x_opencti_id": f"relationship-root-{identifier}",
        "from": {
            "id": f"source-{identifier}",
            "standard_id": f"malware--source-{identifier}",
            "entity_type": "Malware",
            "parent_types": ["Stix-Domain-Object"],
        },
        "to": {
            "id": f"target-{identifier}",
            "standard_id": f"malware--target-{identifier}",
            "entity_type": "Malware",
            "parent_types": ["Stix-Domain-Object"],
        },
    }


def _root_with_already_emitted_refs(identifier):
    return {
        "id": f"indicator--root-{identifier}",
        "type": "indicator",
        "x_opencti_id": f"root-{identifier}",
        "createdBy": {
            "id": f"creator-{identifier}",
            "standard_id": f"identity--creator-{identifier}",
            "entity_type": "Identity",
            "parent_types": ["Stix-Domain-Object"],
        },
        "createdById": f"creator-{identifier}",
        "dataSource": {
            "id": f"data-source-{identifier}",
            "standard_id": f"data-source--{identifier}",
            "entity_type": "Data-Source",
            "parent_types": ["Stix-Domain-Object"],
        },
        "dataSourceId": f"data-source-{identifier}",
        "objectMarking": [
            {
                "id": f"marking-{identifier}",
                "standard_id": f"marking-definition--{identifier}",
                "definition_type": "TLP",
                "definition": "TLP:CLEAR",
                "created": "2017-01-20T00:00:00.000Z",
            }
        ],
        "objectMarkingIds": [f"marking-{identifier}"],
        "related_ref": f"malware--unseen-{identifier}",
    }


def _container_root(identifier):
    return {
        "id": f"report--root-{identifier}",
        "type": "report",
        "x_opencti_id": f"root-{identifier}",
        "objects": [
            {
                "id": f"target-{identifier}",
                "standard_id": f"malware--target-{identifier}",
                "entity_type": "Malware",
                "parent_types": ["Stix-Domain-Object"],
            }
        ],
        "objectsIds": [f"target-{identifier}"],
    }


def _related_object_data(target_identifier):
    return {
        "id": f"target-{target_identifier}",
        "standard_id": f"malware--{target_identifier}",
        "entity_type": "Malware",
        "parent_types": ["Stix-Domain-Object"],
    }


def _configure_related_object_export_conversion(helper):
    helper.prepare_id_filters_export = lambda entity_id, access_filter: entity_id
    helper.generate_export = lambda entity: (
        {
            "id": entity["standard_id"],
            "type": entity["entity_type"].lower(),
            "x_opencti_id": entity["id"],
        }
        if "standard_id" in entity
        else entity.copy()
    )


def _full_helper(relationships, access_collection=None):
    helper = OpenCTIStix2.__new__(OpenCTIStix2)
    helper.opencti = SimpleNamespace(
        stix_nested_ref_relationship=_StaticCollection([]),
        stix_core_relationship=_StaticCollection(relationships),
        stix_sighting_relationship=_StaticCollection([]),
        opencti_stix_object_or_stix_relationship=access_collection
        or _StaticCollection([{}]),
    )
    helper.generate_export = lambda entity: entity
    helper.prepare_id_filters_export = lambda entity_id, access_filter: None
    helper.get_reader = lambda resolve_type: lambda filters: None
    helper.get_lister = lambda resolve_type: None
    return helper


def test_export_entities_list_uses_normalized_lister_lookup():
    calls = []

    def list_entities(**kwargs):
        calls.append(kwargs)
        return [{"id": "container--1"}]

    helper = OpenCTIStix2.__new__(OpenCTIStix2)
    helper.opencti = SimpleNamespace(
        stix_domain_object=SimpleNamespace(list=list_entities),
    )

    result = helper.export_entities_list(
        entity_type="Container",
        filters={"mode": "and"},
        getAll=False,
        withFiles=True,
    )

    assert result == [{"id": "container--1"}]
    assert calls == [
        {
            "search": None,
            "filters": {"mode": "and"},
            "orderBy": None,
            "orderMode": None,
            "getAll": False,
            "withFiles": True,
        }
    ]


def _shared_root_relationships():
    relationships_by_root = {
        f"root-{index}": [_relationship(f"relationship--{index}", "shared")]
        for index in range(1, 4)
    }
    for index, relationships in enumerate(relationships_by_root.values(), start=1):
        relationships[0]["from"]["id"] = f"root-{index}"
        relationships[0]["from"]["standard_id"] = f"indicator--root-{index}"
    return relationships_by_root


def _shared_root_entities():
    return [
        {
            "id": f"indicator--root-{index}",
            "type": "indicator",
            "x_opencti_id": f"root-{index}",
        }
        for index in range(1, 4)
    ]


def _root_entities(count):
    return [
        {
            "id": f"indicator--root-{index}",
            "type": "indicator",
            "x_opencti_id": f"root-{index}",
        }
        for index in range(1, count + 1)
    ]


def test_export_selected_deduplicates_and_rewrites_bundle_once():
    helper = _helper()
    rewrite_sizes = []
    helper._rewrite_embedded_image_uris_in_bundle_for_export = (
        lambda bundle: rewrite_sizes.append(len(bundle["objects"]))
    )
    entities = [
        {"id": "indicator--1", "type": "indicator"},
        {"id": "indicator--2", "type": "indicator"},
        {"id": "indicator--1", "type": "indicator"},
    ]

    bundle = helper.export_selected(entities)

    assert [item["id"] for item in bundle["objects"]] == [
        "indicator--1",
        "indicator--2",
    ]
    assert rewrite_sizes == [2]


def test_prepare_export_full_deduplicates_relationship_bundles():
    helper = _full_helper(
        [
            _relationship("relationship--1"),
            _relationship("relationship--2"),
            _relationship("relationship--1"),
        ]
    )
    entity = {
        "id": "indicator--root",
        "type": "indicator",
        "x_opencti_id": "root",
    }

    result = helper.prepare_export(entity=entity, mode="full")

    assert [item["id"] for item in result] == [
        "indicator--root",
        "relationship--1",
        "relationship--2",
    ]


def test_prepare_export_full_checks_only_unseen_relation_endpoints_once():
    access_collection = _CountingAccessCollection()
    helper = _full_helper(
        [
            _relationship("relationship--1", "shared"),
            _relationship("relationship--2", "shared"),
            _relationship("relationship--3", "shared"),
        ],
        access_collection=access_collection,
    )
    helper.prepare_id_filters_export = lambda entity_id, access_filter: entity_id
    entity = {
        "id": "indicator--root",
        "type": "indicator",
        "x_opencti_id": "root",
    }

    helper.prepare_export(entity=entity, mode="full")

    assert access_collection.list_calls == 1
    assert access_collection.kwargs == [
        {
            "filters": "target-shared",
            "first": 1,
            "customAttributes": EXPORT_ACCESS_LISTER_ATTRIBUTES,
        }
    ]


def test_prepare_export_full_batches_unique_relation_endpoint_access_checks():
    access_collection = _CountingAccessCollection()
    helper = _full_helper(
        [
            _relationship("relationship--1", "target-1"),
            _relationship("relationship--2", "target-2"),
            _relationship("relationship--3", "target-3"),
        ],
        access_collection=access_collection,
    )
    helper.prepare_id_filters_export = lambda entity_id, access_filter: entity_id
    entity = {
        "id": "indicator--root",
        "type": "indicator",
        "x_opencti_id": "root",
    }

    helper.prepare_export(entity=entity, mode="full")

    assert access_collection.list_calls == 1
    assert access_collection.kwargs == [
        {
            "filters": ["target-target-1", "target-target-2", "target-target-3"],
            "first": EXPORT_PREFETCH_BATCH_SIZE,
            "getAll": True,
            "customAttributes": EXPORT_ACCESS_LISTER_ATTRIBUTES,
        }
    ]


def test_prepare_export_full_chunks_unique_relation_endpoint_access_checks():
    access_collection = _CountingAccessCollection()
    helper = _full_helper(
        [
            _relationship(f"relationship--{index}", f"target-{index}")
            for index in range(EXPORT_PREFETCH_BATCH_SIZE + 1)
        ],
        access_collection=access_collection,
    )
    helper.prepare_id_filters_export = lambda entity_id, access_filter: entity_id
    entity = {
        "id": "indicator--root",
        "type": "indicator",
        "x_opencti_id": "root",
    }

    helper.prepare_export(entity=entity, mode="full")

    assert access_collection.list_calls == 2
    assert [len(kwargs["filters"]) for kwargs in access_collection.kwargs] == [
        EXPORT_PREFETCH_BATCH_SIZE,
        1,
    ]
    assert [kwargs["first"] for kwargs in access_collection.kwargs] == [
        EXPORT_PREFETCH_BATCH_SIZE,
        EXPORT_PREFETCH_BATCH_SIZE,
    ]
    assert all(kwargs["getAll"] is True for kwargs in access_collection.kwargs)
    assert all(
        kwargs["customAttributes"] == EXPORT_ACCESS_LISTER_ATTRIBUTES
        for kwargs in access_collection.kwargs
    )


def test_prepare_export_full_reads_repeated_related_object_once():
    helper = _full_helper(
        [
            _relationship("relationship--1", "shared"),
            _relationship("relationship--2", "shared"),
            _relationship("relationship--3", "shared"),
        ]
    )
    read_calls = []

    def read(filters):
        read_calls.append(filters)
        return {
            "id": "malware--shared",
            "type": "malware",
            "x_opencti_id": "target-shared",
        }

    helper.get_reader = lambda resolve_type: read
    helper.generate_export = lambda entity: entity.copy()
    entity = {
        "id": "indicator--root",
        "type": "indicator",
        "x_opencti_id": "root",
    }

    result = helper.prepare_export(entity=entity, mode="full")

    assert len(read_calls) == 1
    assert [item["id"] for item in result] == [
        "indicator--root",
        "relationship--1",
        "relationship--2",
        "relationship--3",
        "malware--shared",
    ]


def test_prepare_export_full_batches_unique_related_object_reads_by_type():
    helper = _full_helper(
        [
            _relationship("relationship--1", "target-1"),
            _relationship("relationship--2", "target-2"),
            _relationship("relationship--3", "target-3"),
        ]
    )
    lister = _CountingRelatedObjectLister(
        {
            f"target-target-{index}": {
                "id": f"target-target-{index}",
                "standard_id": f"malware--target-{index}",
                "entity_type": "Malware",
                "parent_types": ["Stix-Domain-Object"],
            }
            for index in range(1, 4)
        }
    )
    helper.get_lister = lambda resolve_type: lister.list
    helper.prepare_id_filters_export = lambda entity_id, access_filter: entity_id
    helper.generate_export = lambda entity: (
        {
            "id": entity["standard_id"],
            "type": entity["entity_type"].lower(),
            "x_opencti_id": entity["id"],
        }
        if "standard_id" in entity
        else entity.copy()
    )
    helper.get_reader = lambda resolve_type: lambda filters: (_ for _ in ()).throw(
        AssertionError("batchable related objects should not use the reader")
    )
    entity = {
        "id": "indicator--root",
        "type": "indicator",
        "x_opencti_id": "root",
    }

    result = helper.prepare_export(entity=entity, mode="full")

    assert lister.list_calls == 1
    assert lister.filters == [["target-target-1", "target-target-2", "target-target-3"]]
    assert lister.firsts == [EXPORT_PREFETCH_BATCH_SIZE]
    assert [item["id"] for item in result] == [
        "indicator--root",
        "relationship--1",
        "relationship--2",
        "relationship--3",
        "malware--target-1",
        "malware--target-2",
        "malware--target-3",
    ]


def test_prepare_export_full_chunks_unique_related_object_reads():
    relationships = [
        _relationship(f"relationship--{index}", f"target-{index}")
        for index in range(EXPORT_PREFETCH_BATCH_SIZE + 1)
    ]
    helper = _full_helper(relationships)
    lister = _CountingRelatedObjectLister(
        {
            f"target-target-{index}": {
                "id": f"target-target-{index}",
                "standard_id": f"malware--target-{index}",
                "entity_type": "Malware",
                "parent_types": ["Stix-Domain-Object"],
            }
            for index in range(EXPORT_PREFETCH_BATCH_SIZE + 1)
        }
    )
    helper.get_lister = lambda resolve_type: lister.list
    helper.prepare_id_filters_export = lambda entity_id, access_filter: entity_id
    helper.generate_export = lambda entity: (
        {
            "id": entity["standard_id"],
            "type": entity["entity_type"].lower(),
            "x_opencti_id": entity["id"],
        }
        if "standard_id" in entity
        else entity.copy()
    )
    entity = {
        "id": "indicator--root",
        "type": "indicator",
        "x_opencti_id": "root",
    }

    result = helper.prepare_export(entity=entity, mode="full")

    assert lister.list_calls == 2
    assert [len(filters) for filters in lister.filters] == [
        EXPORT_PREFETCH_BATCH_SIZE,
        1,
    ]
    assert lister.firsts == [EXPORT_PREFETCH_BATCH_SIZE, EXPORT_PREFETCH_BATCH_SIZE]
    assert len(result) == (EXPORT_PREFETCH_BATCH_SIZE + 1) * 2 + 1


def test_export_selected_reuses_related_endpoint_access_across_roots():
    access_collection = _CountingCollection([{}])
    helper = _full_helper([], access_collection=access_collection)
    helper.opencti.stix_core_relationship = _RelationshipCollection(
        _shared_root_relationships()
    )

    helper.export_selected(entities_list=_shared_root_entities(), mode="full")

    assert access_collection.list_calls == 1


def test_export_list_reuses_related_endpoint_access_across_roots():
    access_collection = _CountingCollection([{}])
    helper = _full_helper([], access_collection=access_collection)
    helper.opencti.stix_core_relationship = _RelationshipCollection(
        _shared_root_relationships()
    )
    helper.export_entities_list = lambda **_kwargs: _shared_root_entities()

    helper.export_list(entity_type="Indicator", mode="full")

    assert access_collection.list_calls == 1


def test_export_selected_prefetches_unique_top_level_endpoint_access_across_roots():
    access_collection = _CountingAccessCollection()
    helper = _full_helper([], access_collection=access_collection)
    helper.opencti.stix_core_relationship = _RelationshipCollection(
        {
            f"root-{index}": [
                _relationship_from_root(
                    f"relationship--{index}", f"root-{index}", f"target-{index}"
                )
            ]
            for index in range(1, 4)
        }
    )
    helper.prepare_id_filters_export = lambda entity_id, access_filter: entity_id

    helper.export_selected(entities_list=_root_entities(3), mode="full")

    assert access_collection.kwargs == [
        {
            "filters": ["target-target-1", "target-target-2", "target-target-3"],
            "first": EXPORT_PREFETCH_BATCH_SIZE,
            "getAll": True,
            "customAttributes": EXPORT_ACCESS_LISTER_ATTRIBUTES,
        }
    ]


def test_export_list_prefetches_unique_top_level_endpoint_access_across_roots():
    access_collection = _CountingAccessCollection()
    helper = _full_helper([], access_collection=access_collection)
    helper.opencti.stix_core_relationship = _RelationshipCollection(
        {
            f"root-{index}": [
                _relationship_from_root(
                    f"relationship--{index}", f"root-{index}", f"target-{index}"
                )
            ]
            for index in range(1, 4)
        }
    )
    helper.prepare_id_filters_export = lambda entity_id, access_filter: entity_id
    helper.export_entities_list = lambda **_kwargs: _root_entities(3)

    helper.export_list(entity_type="Indicator", mode="full")

    assert access_collection.kwargs == [
        {
            "filters": ["target-target-1", "target-target-2", "target-target-3"],
            "first": EXPORT_PREFETCH_BATCH_SIZE,
            "getAll": True,
            "customAttributes": EXPORT_ACCESS_LISTER_ATTRIBUTES,
        }
    ]


def test_export_selected_prefetches_top_level_endpoint_access_from_core_and_sighting():
    access_collection = _CountingAccessCollection()
    helper = _full_helper([], access_collection=access_collection)
    helper.opencti.stix_core_relationship = _RelationshipCollection(
        {"root-1": [_relationship_from_root("relationship--1", "root-1", "target-1")]}
    )
    helper.opencti.stix_sighting_relationship = _CountingSightingRelationshipCollection(
        {"root-2": [_relationship_from_root("sighting--2", "root-2", "target-2")]}
    )
    helper.prepare_id_filters_export = lambda entity_id, access_filter: entity_id

    helper.export_selected(entities_list=_root_entities(2), mode="full")

    assert access_collection.kwargs == [
        {
            "filters": ["target-target-1", "target-target-2"],
            "first": EXPORT_PREFETCH_BATCH_SIZE,
            "getAll": True,
            "customAttributes": EXPORT_ACCESS_LISTER_ATTRIBUTES,
        }
    ]


def test_export_selected_prefetches_top_level_endpoint_access_in_bounded_chunks():
    access_collection = _CountingAccessCollection()
    helper = _full_helper([], access_collection=access_collection)
    helper.opencti.stix_core_relationship = _RelationshipCollection(
        {
            f"root-{index}": [
                _relationship_from_root(
                    f"relationship--{index}", f"root-{index}", f"target-{index}"
                )
            ]
            for index in range(1, EXPORT_PREFETCH_BATCH_SIZE + 2)
        }
    )
    helper.prepare_id_filters_export = lambda entity_id, access_filter: entity_id

    helper.export_selected(
        entities_list=_root_entities(EXPORT_PREFETCH_BATCH_SIZE + 1), mode="full"
    )

    assert [len(kwargs["filters"]) for kwargs in access_collection.kwargs] == [
        EXPORT_PREFETCH_BATCH_SIZE,
        1,
    ]
    assert [kwargs["first"] for kwargs in access_collection.kwargs] == [
        EXPORT_PREFETCH_BATCH_SIZE,
        EXPORT_PREFETCH_BATCH_SIZE,
    ]


def test_export_selected_prefetches_selected_roots_as_visible():
    access_collection = _CountingAccessCollection()
    helper = _full_helper([], access_collection=access_collection)
    relationship = _relationship_from_root("relationship--1", "root-1", "root-2")
    relationship["to"]["id"] = "root-2"
    relationship["to"]["standard_id"] = "indicator--root-2"
    helper.opencti.stix_core_relationship = _RelationshipCollection(
        {"root-1": [relationship]}
    )
    helper.prepare_id_filters_export = lambda entity_id, access_filter: entity_id

    helper.export_selected(entities_list=_root_entities(2), mode="full")

    assert access_collection.kwargs == []


def test_export_selected_prefetches_unique_top_level_related_object_exports_across_roots():
    helper = _full_helper([], access_collection=_CountingAccessCollection())
    helper.opencti.stix_core_relationship = _RelationshipCollection(
        {
            f"root-{index}": [
                _relationship_from_root(
                    f"relationship--{index}", f"root-{index}", f"target-{index}"
                )
            ]
            for index in range(1, 4)
        }
    )
    lister = _CountingRelatedObjectLister(
        {
            f"target-target-{index}": _related_object_data(f"target-{index}")
            for index in range(1, 4)
        }
    )
    helper.get_lister = lambda resolve_type: lister.list
    helper.get_reader = lambda resolve_type: lambda filters: (_ for _ in ()).throw(
        AssertionError("batchable related objects should not use the reader")
    )
    _configure_related_object_export_conversion(helper)

    result = helper.export_selected(entities_list=_root_entities(3), mode="full")

    assert lister.kwargs == [
        {
            "filters": ["target-target-1", "target-target-2", "target-target-3"],
            "first": EXPORT_PREFETCH_BATCH_SIZE,
            "getAll": True,
        }
    ]
    assert [item["id"] for item in result["objects"]] == [
        "indicator--root-1",
        "relationship--1",
        "malware--target-1",
        "indicator--root-2",
        "relationship--2",
        "malware--target-2",
        "indicator--root-3",
        "relationship--3",
        "malware--target-3",
    ]


def test_export_list_prefetches_unique_top_level_related_object_exports_across_roots():
    helper = _full_helper([], access_collection=_CountingAccessCollection())
    helper.opencti.stix_core_relationship = _RelationshipCollection(
        {
            f"root-{index}": [
                _relationship_from_root(
                    f"relationship--{index}", f"root-{index}", f"target-{index}"
                )
            ]
            for index in range(1, 4)
        }
    )
    lister = _CountingRelatedObjectLister(
        {
            f"target-target-{index}": _related_object_data(f"target-{index}")
            for index in range(1, 4)
        }
    )
    helper.get_lister = lambda resolve_type: lister.list
    helper.get_reader = lambda resolve_type: lambda filters: (_ for _ in ()).throw(
        AssertionError("batchable related objects should not use the reader")
    )
    helper.export_entities_list = lambda **_kwargs: _root_entities(3)
    _configure_related_object_export_conversion(helper)

    helper.export_list(entity_type="Indicator", mode="full")

    assert lister.kwargs == [
        {
            "filters": ["target-target-1", "target-target-2", "target-target-3"],
            "first": EXPORT_PREFETCH_BATCH_SIZE,
            "getAll": True,
        }
    ]


def test_export_selected_prefetches_top_level_related_object_exports_in_bounded_chunks():
    helper = _full_helper([], access_collection=_CountingAccessCollection())
    helper.opencti.stix_core_relationship = _RelationshipCollection(
        {
            f"root-{index}": [
                _relationship_from_root(
                    f"relationship--{index}", f"root-{index}", f"target-{index}"
                )
            ]
            for index in range(1, EXPORT_PREFETCH_BATCH_SIZE + 2)
        }
    )
    lister = _CountingRelatedObjectLister(
        {
            f"target-target-{index}": _related_object_data(f"target-{index}")
            for index in range(1, EXPORT_PREFETCH_BATCH_SIZE + 2)
        }
    )
    helper.get_lister = lambda resolve_type: lister.list
    helper.get_reader = lambda resolve_type: lambda filters: (_ for _ in ()).throw(
        AssertionError("batchable related objects should not use the reader")
    )
    _configure_related_object_export_conversion(helper)

    helper.export_selected(
        entities_list=_root_entities(EXPORT_PREFETCH_BATCH_SIZE + 1), mode="full"
    )

    assert [len(kwargs["filters"]) for kwargs in lister.kwargs] == [
        EXPORT_PREFETCH_BATCH_SIZE,
        1,
    ]
    assert [kwargs["first"] for kwargs in lister.kwargs] == [
        EXPORT_PREFETCH_BATCH_SIZE,
        EXPORT_PREFETCH_BATCH_SIZE,
    ]
    assert all(kwargs["getAll"] is True for kwargs in lister.kwargs)


def test_export_selected_prefetches_only_proven_visible_top_level_related_object_exports():
    helper = _full_helper(
        [],
        access_collection=_CountingAccessCollection(
            visible_ids={"target-target-1", "target-target-2"}
        ),
    )
    helper.opencti.stix_core_relationship = _RelationshipCollection(
        {
            f"root-{index}": [
                _relationship_from_root(
                    f"relationship--{index}", f"root-{index}", f"target-{index}"
                )
            ]
            for index in range(1, 4)
        }
    )
    lister = _CountingRelatedObjectLister(
        {
            f"target-target-{index}": _related_object_data(f"target-{index}")
            for index in range(1, 4)
        }
    )
    helper.get_lister = lambda resolve_type: lister.list
    read_calls = []

    def read(filters):
        read_calls.append(filters)
        return None

    helper.get_reader = lambda resolve_type: read
    _configure_related_object_export_conversion(helper)

    result = helper.export_selected(entities_list=_root_entities(3), mode="full")

    assert lister.filters == [["target-target-1", "target-target-2"]]
    assert read_calls == ["target-target-3"]
    assert "malware--target-3" not in {item["id"] for item in result["objects"]}


def test_export_selected_keeps_multi_root_related_object_reader_fallback_without_lister():
    helper = _full_helper([], access_collection=_CountingAccessCollection())
    helper.opencti.stix_core_relationship = _RelationshipCollection(
        {
            f"root-{index}": [
                _relationship_from_root(
                    f"relationship--{index}", f"root-{index}", f"target-{index}"
                )
            ]
            for index in range(1, 3)
        }
    )
    targets_by_id = {
        f"target-target-{index}": _related_object_data(f"target-{index}")
        for index in range(1, 3)
    }
    read_calls = []

    def read(filters):
        read_calls.append(filters)
        return targets_by_id[filters]

    helper.get_lister = lambda resolve_type: None
    helper.get_reader = lambda resolve_type: read
    _configure_related_object_export_conversion(helper)

    helper.export_selected(entities_list=_root_entities(2), mode="full")

    assert read_calls == ["target-target-1", "target-target-2"]


def test_export_selected_prefetches_relationship_root_endpoint_work_across_roots():
    helper = _full_helper([], access_collection=_CountingAccessCollection())
    nested_ref_relationships = _CountingNestedRefRelationshipCollection()
    helper.opencti.stix_nested_ref_relationship = nested_ref_relationships
    lister = _CountingRelatedObjectLister(
        {
            f"{side}-{index}": {
                "id": f"{side}-{index}",
                "standard_id": f"malware--{side}-{index}",
                "entity_type": "Malware",
                "parent_types": ["Stix-Domain-Object"],
            }
            for index in range(1, 4)
            for side in ("source", "target")
        }
    )
    helper.get_lister = lambda resolve_type: lister.list
    helper.get_reader = lambda resolve_type: lambda filters: (_ for _ in ()).throw(
        AssertionError(
            "batchable relationship-root endpoints should not use the reader"
        )
    )
    _configure_related_object_export_conversion(helper)

    result = helper.export_selected(
        entities_list=[_relationship_root(index) for index in range(1, 4)],
        mode="full",
    )

    assert helper.opencti.opencti_stix_object_or_stix_relationship.kwargs == [
        {
            "filters": [
                "source-1",
                "target-1",
                "source-2",
                "target-2",
                "source-3",
                "target-3",
            ],
            "first": EXPORT_PREFETCH_BATCH_SIZE,
            "getAll": True,
            "customAttributes": EXPORT_ACCESS_LISTER_ATTRIBUTES,
        }
    ]
    assert nested_ref_relationships.kwargs == [
        {
            "filters": {
                "mode": "and",
                "filters": [
                    {
                        "key": "fromId",
                        "values": [
                            "relationship-root-1",
                            "relationship-root-2",
                            "relationship-root-3",
                        ],
                    }
                ],
                "filterGroups": [],
            },
            "first": EXPORT_PREFETCH_BATCH_SIZE,
            "getAll": True,
        },
        {
            "filters": {
                "mode": "and",
                "filters": [
                    {
                        "key": "fromId",
                        "values": [
                            "source-1",
                            "target-1",
                            "source-2",
                            "target-2",
                            "source-3",
                            "target-3",
                        ],
                    }
                ],
                "filterGroups": [],
            },
            "first": EXPORT_PREFETCH_BATCH_SIZE,
            "getAll": True,
        },
    ]
    assert lister.kwargs == [
        {
            "filters": [
                "source-1",
                "target-1",
                "source-2",
                "target-2",
                "source-3",
                "target-3",
            ],
            "first": EXPORT_PREFETCH_BATCH_SIZE,
            "getAll": True,
        }
    ]
    assert [item["id"] for item in result["objects"]] == [
        "relationship--root-1",
        "malware--source-1",
        "malware--target-1",
        "relationship--root-2",
        "malware--source-2",
        "malware--target-2",
        "relationship--root-3",
        "malware--source-3",
        "malware--target-3",
    ]


def test_export_list_prefetches_relationship_root_endpoint_work_across_roots():
    helper = _full_helper([], access_collection=_CountingAccessCollection())
    helper.opencti.stix_nested_ref_relationship = (
        _CountingNestedRefRelationshipCollection()
    )
    lister = _CountingRelatedObjectLister(
        {
            f"{side}-{index}": {
                "id": f"{side}-{index}",
                "standard_id": f"malware--{side}-{index}",
                "entity_type": "Malware",
                "parent_types": ["Stix-Domain-Object"],
            }
            for index in range(1, 4)
            for side in ("source", "target")
        }
    )
    helper.get_lister = lambda resolve_type: lister.list
    helper.get_reader = lambda resolve_type: lambda filters: (_ for _ in ()).throw(
        AssertionError(
            "batchable relationship-root endpoints should not use the reader"
        )
    )
    helper.export_entities_list = lambda **_kwargs: [
        _relationship_root(index) for index in range(1, 4)
    ]
    _configure_related_object_export_conversion(helper)

    result = helper.export_list(entity_type="stix-core-relationship", mode="full")

    assert lister.filters == [
        ["source-1", "target-1", "source-2", "target-2", "source-3", "target-3"]
    ]
    assert [item["id"] for item in result["objects"]] == [
        "relationship--root-1",
        "malware--source-1",
        "malware--target-1",
        "relationship--root-2",
        "malware--source-2",
        "malware--target-2",
        "relationship--root-3",
        "malware--source-3",
        "malware--target-3",
    ]


def test_export_selected_prefetches_relationship_root_endpoints_in_bounded_chunks():
    helper = _full_helper([], access_collection=_CountingAccessCollection())
    helper.opencti.stix_nested_ref_relationship = (
        _CountingNestedRefRelationshipCollection()
    )
    lister = _CountingRelatedObjectLister(
        {
            f"{side}-{index}": {
                "id": f"{side}-{index}",
                "standard_id": f"malware--{side}-{index}",
                "entity_type": "Malware",
                "parent_types": ["Stix-Domain-Object"],
            }
            for index in range(1, (EXPORT_PREFETCH_BATCH_SIZE // 2) + 2)
            for side in ("source", "target")
        }
    )
    helper.get_lister = lambda resolve_type: lister.list
    helper.get_reader = lambda resolve_type: lambda filters: (_ for _ in ()).throw(
        AssertionError(
            "batchable relationship-root endpoints should not use the reader"
        )
    )
    _configure_related_object_export_conversion(helper)
    root_count = (EXPORT_PREFETCH_BATCH_SIZE // 2) + 1

    helper.export_selected(
        entities_list=[_relationship_root(index) for index in range(1, root_count + 1)],
        mode="full",
    )

    assert [
        len(kwargs["filters"])
        for kwargs in helper.opencti.opencti_stix_object_or_stix_relationship.kwargs
    ] == [EXPORT_PREFETCH_BATCH_SIZE, 2]
    assert [len(kwargs["filters"]) for kwargs in lister.kwargs] == [
        EXPORT_PREFETCH_BATCH_SIZE,
        2,
    ]
    assert [kwargs["first"] for kwargs in lister.kwargs] == [
        EXPORT_PREFETCH_BATCH_SIZE,
        EXPORT_PREFETCH_BATCH_SIZE,
    ]
    assert all(kwargs["getAll"] is True for kwargs in lister.kwargs)


def test_prepare_export_full_does_not_reread_relationship_root_endpoints_as_refs():
    helper = _full_helper([], access_collection=_CountingAccessCollection())
    targets_by_id = {
        f"{side}-1": {
            "id": f"{side}-1",
            "standard_id": f"malware--{side}-1",
            "entity_type": "Malware",
            "parent_types": ["Stix-Domain-Object"],
        }
        for side in ("source", "target")
    }
    read_calls = []

    def read(filters):
        read_calls.append(filters)
        return targets_by_id[filters]

    helper.get_lister = lambda resolve_type: None
    helper.get_reader = lambda resolve_type: read
    _configure_related_object_export_conversion(helper)

    result = helper.prepare_export(entity=_relationship_root(1), mode="full")

    assert read_calls == ["source-1", "target-1"]
    assert [item["id"] for item in result] == [
        "relationship--root-1",
        "malware--source-1",
        "malware--target-1",
    ]


def test_prepare_export_full_does_not_reread_refs_already_emitted_in_result():
    helper = _full_helper([], access_collection=_CountingAccessCollection())
    read_calls = []

    def read(filters):
        read_calls.append(filters)
        return {
            "id": "unseen-1",
            "standard_id": "malware--unseen-1",
            "entity_type": "Malware",
            "parent_types": ["Stix-Domain-Object"],
        }

    helper.get_lister = lambda resolve_type: None
    helper.get_reader = lambda resolve_type: read
    _configure_related_object_export_conversion(helper)

    result = helper.prepare_export(
        entity=_root_with_already_emitted_refs(1), mode="full"
    )

    assert read_calls == ["malware--unseen-1"]
    assert [item["id"] for item in result] == [
        "identity--creator-1",
        "data-source--1",
        "marking-definition--1",
        "indicator--root-1",
        "malware--unseen-1",
    ]


def test_export_selected_prefetches_container_objects_across_roots():
    helper = _full_helper([], access_collection=_CountingAccessCollection())
    nested_ref_relationships = _CountingNestedRefRelationshipCollection()
    helper.opencti.stix_nested_ref_relationship = nested_ref_relationships
    lister = _CountingRelatedObjectLister(
        {
            f"target-{index}": {
                "id": f"target-{index}",
                "standard_id": f"malware--target-{index}",
                "entity_type": "Malware",
                "parent_types": ["Stix-Domain-Object"],
            }
            for index in range(1, 4)
        }
    )
    helper.get_lister = lambda resolve_type: lister.list
    helper.get_reader = lambda resolve_type: lambda filters: (_ for _ in ()).throw(
        AssertionError("batchable container objects should not use the reader")
    )
    _configure_related_object_export_conversion(helper)

    result = helper.export_selected(
        entities_list=[_container_root(index) for index in range(1, 4)],
        mode="full",
    )

    assert nested_ref_relationships.kwargs == [
        {
            "filters": {
                "mode": "and",
                "filters": [
                    {
                        "key": "fromId",
                        "values": ["root-1", "root-2", "root-3"],
                    }
                ],
                "filterGroups": [],
            },
            "first": EXPORT_PREFETCH_BATCH_SIZE,
            "getAll": True,
        },
        {
            "filters": {
                "mode": "and",
                "filters": [
                    {
                        "key": "fromId",
                        "values": ["target-1", "target-2", "target-3"],
                    }
                ],
                "filterGroups": [],
            },
            "first": EXPORT_PREFETCH_BATCH_SIZE,
            "getAll": True,
        },
    ]
    assert lister.kwargs == [
        {
            "filters": ["target-1", "target-2", "target-3"],
            "first": EXPORT_PREFETCH_BATCH_SIZE,
            "getAll": True,
        }
    ]
    assert [item["id"] for item in result["objects"]] == [
        "report--root-1",
        "malware--target-1",
        "report--root-2",
        "malware--target-2",
        "report--root-3",
        "malware--target-3",
    ]


def test_export_list_prefetches_container_objects_across_roots():
    helper = _full_helper([], access_collection=_CountingAccessCollection())
    helper.opencti.stix_nested_ref_relationship = (
        _CountingNestedRefRelationshipCollection()
    )
    lister = _CountingRelatedObjectLister(
        {
            f"target-{index}": {
                "id": f"target-{index}",
                "standard_id": f"malware--target-{index}",
                "entity_type": "Malware",
                "parent_types": ["Stix-Domain-Object"],
            }
            for index in range(1, 4)
        }
    )
    helper.get_lister = lambda resolve_type: lister.list
    helper.get_reader = lambda resolve_type: lambda filters: (_ for _ in ()).throw(
        AssertionError("batchable container objects should not use the reader")
    )
    helper.export_entities_list = lambda **_kwargs: [
        _container_root(index) for index in range(1, 4)
    ]
    _configure_related_object_export_conversion(helper)

    result = helper.export_list(entity_type="Report", mode="full")

    assert lister.filters == [["target-1", "target-2", "target-3"]]
    assert [item["id"] for item in result["objects"]] == [
        "report--root-1",
        "malware--target-1",
        "report--root-2",
        "malware--target-2",
        "report--root-3",
        "malware--target-3",
    ]


def test_export_selected_prefetches_container_objects_in_bounded_chunks():
    helper = _full_helper([], access_collection=_CountingAccessCollection())
    helper.opencti.stix_nested_ref_relationship = (
        _CountingNestedRefRelationshipCollection()
    )
    lister = _CountingRelatedObjectLister(
        {
            f"target-{index}": {
                "id": f"target-{index}",
                "standard_id": f"malware--target-{index}",
                "entity_type": "Malware",
                "parent_types": ["Stix-Domain-Object"],
            }
            for index in range(1, EXPORT_PREFETCH_BATCH_SIZE + 2)
        }
    )
    helper.get_lister = lambda resolve_type: lister.list
    helper.get_reader = lambda resolve_type: lambda filters: (_ for _ in ()).throw(
        AssertionError("batchable container objects should not use the reader")
    )
    _configure_related_object_export_conversion(helper)
    root_count = EXPORT_PREFETCH_BATCH_SIZE + 1

    helper.export_selected(
        entities_list=[_container_root(index) for index in range(1, root_count + 1)],
        mode="full",
    )

    assert [len(kwargs["filters"]) for kwargs in lister.kwargs] == [
        EXPORT_PREFETCH_BATCH_SIZE,
        1,
    ]
    assert [kwargs["first"] for kwargs in lister.kwargs] == [
        EXPORT_PREFETCH_BATCH_SIZE,
        EXPORT_PREFETCH_BATCH_SIZE,
    ]
    assert all(kwargs["getAll"] is True for kwargs in lister.kwargs)


def test_prepare_export_full_does_not_reread_contained_objects_as_object_refs():
    helper = _full_helper([], access_collection=_CountingAccessCollection())
    read_calls = []

    def read(filters):
        read_calls.append(filters)
        return {
            "id": "target-1",
            "standard_id": "malware--target-1",
            "entity_type": "Malware",
            "parent_types": ["Stix-Domain-Object"],
        }

    helper.get_lister = lambda resolve_type: None
    helper.get_reader = lambda resolve_type: read
    _configure_related_object_export_conversion(helper)

    result = helper.prepare_export(entity=_container_root(1), mode="full")

    assert read_calls == ["target-1"]
    assert [item["id"] for item in result] == [
        "report--root-1",
        "malware--target-1",
    ]


def test_export_selected_reuses_related_object_reads_across_roots():
    helper = _full_helper([])
    helper.opencti.stix_core_relationship = _RelationshipCollection(
        _shared_root_relationships()
    )
    read_calls = []

    def read(filters):
        read_calls.append(filters)
        return {
            "id": "malware--shared",
            "type": "malware",
            "x_opencti_id": "target-shared",
        }

    helper.get_reader = lambda resolve_type: read
    helper.generate_export = lambda entity: entity.copy()

    helper.export_selected(entities_list=_shared_root_entities(), mode="full")

    assert len(read_calls) == 1


def test_export_list_reuses_related_object_reads_across_roots():
    helper = _full_helper([])
    helper.opencti.stix_core_relationship = _RelationshipCollection(
        _shared_root_relationships()
    )
    read_calls = []

    def read(filters):
        read_calls.append(filters)
        return {
            "id": "malware--shared",
            "type": "malware",
            "x_opencti_id": "target-shared",
        }

    helper.get_reader = lambda resolve_type: read
    helper.generate_export = lambda entity: entity.copy()
    helper.export_entities_list = lambda **_kwargs: _shared_root_entities()

    helper.export_list(entity_type="Indicator", mode="full")

    assert len(read_calls) == 1


def test_export_selected_prefetches_core_relationships_across_roots():
    helper = _full_helper([])
    core_relationships = _CountingCoreRelationshipCollection()
    helper.opencti.stix_core_relationship = core_relationships

    helper.export_selected(entities_list=_root_entities(3), mode="full")

    assert core_relationships.kwargs == [
        {
            "fromOrToId": ["root-1", "root-2", "root-3"],
            "first": EXPORT_PREFETCH_BATCH_SIZE,
            "getAll": True,
            "filters": None,
        }
    ]


def test_export_list_prefetches_core_relationships_across_roots():
    helper = _full_helper([])
    core_relationships = _CountingCoreRelationshipCollection()
    helper.opencti.stix_core_relationship = core_relationships
    helper.export_entities_list = lambda **_kwargs: _root_entities(3)

    helper.export_list(entity_type="Indicator", mode="full")

    assert core_relationships.kwargs == [
        {
            "fromOrToId": ["root-1", "root-2", "root-3"],
            "first": EXPORT_PREFETCH_BATCH_SIZE,
            "getAll": True,
            "filters": None,
        }
    ]


def test_export_selected_keeps_single_root_core_relationship_fallback():
    helper = _full_helper([])
    core_relationships = _CountingCoreRelationshipCollection()
    helper.opencti.stix_core_relationship = core_relationships

    helper.export_selected(entities_list=_root_entities(1), mode="full")

    assert core_relationships.kwargs == [
        {
            "fromOrToId": "root-1",
            "getAll": True,
            "filters": None,
        }
    ]


def test_export_selected_chunks_core_relationship_prefetch():
    helper = _full_helper([])
    core_relationships = _CountingCoreRelationshipCollection()
    helper.opencti.stix_core_relationship = core_relationships

    helper.export_selected(
        entities_list=_root_entities(EXPORT_PREFETCH_BATCH_SIZE + 1), mode="full"
    )

    assert [len(kwargs["fromOrToId"]) for kwargs in core_relationships.kwargs] == [
        EXPORT_PREFETCH_BATCH_SIZE,
        1,
    ]
    assert [kwargs["first"] for kwargs in core_relationships.kwargs] == [
        EXPORT_PREFETCH_BATCH_SIZE,
        EXPORT_PREFETCH_BATCH_SIZE,
    ]


def test_export_selected_copies_shared_core_relationships_for_each_root():
    shared_relationship = _relationship("relationship--shared", "root-2")
    shared_relationship["from"]["id"] = "root-1"
    shared_relationship["from"]["standard_id"] = "indicator--root-1"
    shared_relationship["to"]["id"] = "root-2"
    shared_relationship["to"]["standard_id"] = "indicator--root-2"
    helper = _full_helper([])
    helper.opencti.stix_core_relationship = _CountingCoreRelationshipCollection(
        {
            "root-1": [shared_relationship],
            "root-2": [shared_relationship],
        }
    )

    result = helper.export_selected(entities_list=_root_entities(2), mode="full")

    assert [item["id"] for item in result["objects"]] == [
        "indicator--root-1",
        "relationship--shared",
        "indicator--root-2",
    ]


def test_export_selected_prefetches_sighting_relationships_across_roots():
    helper = _full_helper([])
    sighting_relationships = _CountingSightingRelationshipCollection()
    helper.opencti.stix_sighting_relationship = sighting_relationships

    helper.export_selected(entities_list=_root_entities(3), mode="full")

    assert sighting_relationships.kwargs == [
        {
            "filters": {
                "mode": "and",
                "filters": [
                    {"key": "fromOrToId", "values": ["root-1", "root-2", "root-3"]}
                ],
                "filterGroups": [],
            },
            "first": EXPORT_PREFETCH_BATCH_SIZE,
            "getAll": True,
        }
    ]


def test_export_list_prefetches_sighting_relationships_across_roots():
    helper = _full_helper([])
    sighting_relationships = _CountingSightingRelationshipCollection()
    helper.opencti.stix_sighting_relationship = sighting_relationships
    helper.export_entities_list = lambda **_kwargs: _root_entities(3)
    access_filter = {
        "mode": "or",
        "filters": [{"key": "objectLabel", "values": ["label--1"]}],
        "filterGroups": [],
    }

    helper.export_list(
        entity_type="Indicator", mode="full", access_filter=access_filter
    )

    assert sighting_relationships.kwargs == [
        {
            "filters": {
                "mode": "and",
                "filters": [
                    {"key": "fromOrToId", "values": ["root-1", "root-2", "root-3"]}
                ],
                "filterGroups": [access_filter],
            },
            "first": EXPORT_PREFETCH_BATCH_SIZE,
            "getAll": True,
        }
    ]


def test_export_selected_keeps_single_root_sighting_relationship_fallback():
    helper = _full_helper([])
    sighting_relationships = _CountingSightingRelationshipCollection()
    helper.opencti.stix_sighting_relationship = sighting_relationships

    helper.export_selected(entities_list=_root_entities(1), mode="full")

    assert sighting_relationships.kwargs == [
        {
            "fromOrToId": "root-1",
            "getAll": True,
            "filters": None,
        }
    ]


def test_export_selected_chunks_sighting_relationship_prefetch():
    helper = _full_helper([])
    sighting_relationships = _CountingSightingRelationshipCollection()
    helper.opencti.stix_sighting_relationship = sighting_relationships

    helper.export_selected(
        entities_list=_root_entities(EXPORT_PREFETCH_BATCH_SIZE + 1), mode="full"
    )

    assert [
        len(kwargs["filters"]["filters"][0]["values"])
        for kwargs in sighting_relationships.kwargs
    ] == [EXPORT_PREFETCH_BATCH_SIZE, 1]
    assert [kwargs["first"] for kwargs in sighting_relationships.kwargs] == [
        EXPORT_PREFETCH_BATCH_SIZE,
        EXPORT_PREFETCH_BATCH_SIZE,
    ]


def test_export_selected_copies_shared_sighting_relationships_for_each_root():
    shared_relationship = _relationship("sighting--shared", "root-2")
    shared_relationship["from"]["id"] = "root-1"
    shared_relationship["from"]["standard_id"] = "indicator--root-1"
    shared_relationship["to"]["id"] = "root-2"
    shared_relationship["to"]["standard_id"] = "indicator--root-2"
    helper = _full_helper([])
    helper.opencti.stix_sighting_relationship = _CountingSightingRelationshipCollection(
        {
            "root-1": [shared_relationship],
            "root-2": [shared_relationship],
        }
    )

    result = helper.export_selected(entities_list=_root_entities(2), mode="full")

    assert [item["id"] for item in result["objects"]] == [
        "indicator--root-1",
        "sighting--shared",
        "indicator--root-2",
    ]


def test_export_selected_prefetches_nested_ref_relationships_across_roots():
    helper = _full_helper([])
    nested_ref_relationships = _CountingNestedRefRelationshipCollection()
    helper.opencti.stix_nested_ref_relationship = nested_ref_relationships

    helper.export_selected(entities_list=_root_entities(3), mode="full")

    assert nested_ref_relationships.kwargs == [
        {
            "filters": {
                "mode": "and",
                "filters": [
                    {"key": "fromId", "values": ["root-1", "root-2", "root-3"]}
                ],
                "filterGroups": [],
            },
            "first": EXPORT_PREFETCH_BATCH_SIZE,
            "getAll": True,
        }
    ]


def test_export_list_prefetches_nested_ref_relationships_across_roots():
    helper = _full_helper([])
    nested_ref_relationships = _CountingNestedRefRelationshipCollection()
    helper.opencti.stix_nested_ref_relationship = nested_ref_relationships
    helper.export_entities_list = lambda **_kwargs: _root_entities(3)
    access_filter = {
        "mode": "or",
        "filters": [{"key": "objectLabel", "values": ["label--1"]}],
        "filterGroups": [],
    }

    helper.export_list(
        entity_type="Indicator", mode="full", access_filter=access_filter
    )

    assert nested_ref_relationships.kwargs == [
        {
            "filters": {
                "mode": "and",
                "filters": [
                    {"key": "fromId", "values": ["root-1", "root-2", "root-3"]}
                ],
                "filterGroups": [access_filter],
            },
            "first": EXPORT_PREFETCH_BATCH_SIZE,
            "getAll": True,
        }
    ]


def test_export_selected_keeps_single_root_nested_ref_relationship_fallback():
    helper = _full_helper([])
    nested_ref_relationships = _CountingNestedRefRelationshipCollection()
    helper.opencti.stix_nested_ref_relationship = nested_ref_relationships

    helper.export_selected(entities_list=_root_entities(1), mode="full")

    assert nested_ref_relationships.kwargs == [
        {
            "fromId": "root-1",
            "filters": None,
            "getAll": True,
        }
    ]


def test_prepare_export_simple_paginates_nested_ref_relationships():
    helper = _full_helper([])
    nested_ref_relationships = _CountingNestedRefRelationshipCollection()
    helper.opencti.stix_nested_ref_relationship = nested_ref_relationships

    helper.prepare_export(entity=_root_entities(1)[0], mode="simple")

    assert nested_ref_relationships.kwargs == [
        {
            "fromId": "root-1",
            "filters": None,
            "getAll": True,
        }
    ]


def test_export_selected_simple_prefetches_nested_ref_relationships_across_roots():
    helper = _full_helper([])
    nested_ref_relationships = _CountingNestedRefRelationshipCollection(
        {
            "root-1": [_nested_ref_relationship("nested-ref--1", "root-1", "target-1")],
            "root-2": [_nested_ref_relationship("nested-ref--2", "root-2", "target-2")],
            "root-3": [_nested_ref_relationship("nested-ref--3", "root-3", "target-3")],
        }
    )
    helper.opencti.stix_nested_ref_relationship = nested_ref_relationships

    result = helper.export_selected(entities_list=_root_entities(3), mode="simple")

    assert nested_ref_relationships.kwargs == [
        {
            "filters": {
                "mode": "and",
                "filters": [
                    {"key": "fromId", "values": ["root-1", "root-2", "root-3"]}
                ],
                "filterGroups": [],
            },
            "first": EXPORT_PREFETCH_BATCH_SIZE,
            "getAll": True,
        }
    ]
    assert [item["sample_refs"] for item in result["objects"]] == [
        ["malware--target-1"],
        ["malware--target-2"],
        ["malware--target-3"],
    ]


def test_export_list_simple_prefetches_nested_ref_relationships_across_roots():
    helper = _full_helper([])
    nested_ref_relationships = _CountingNestedRefRelationshipCollection(
        {
            "root-1": [_nested_ref_relationship("nested-ref--1", "root-1", "target-1")],
            "root-2": [_nested_ref_relationship("nested-ref--2", "root-2", "target-2")],
            "root-3": [_nested_ref_relationship("nested-ref--3", "root-3", "target-3")],
        }
    )
    helper.opencti.stix_nested_ref_relationship = nested_ref_relationships
    helper.export_entities_list = lambda **_kwargs: _root_entities(3)

    result = helper.export_list(entity_type="Indicator", mode="simple")

    assert nested_ref_relationships.kwargs == [
        {
            "filters": {
                "mode": "and",
                "filters": [
                    {"key": "fromId", "values": ["root-1", "root-2", "root-3"]}
                ],
                "filterGroups": [],
            },
            "first": EXPORT_PREFETCH_BATCH_SIZE,
            "getAll": True,
        }
    ]
    assert [item["sample_refs"] for item in result["objects"]] == [
        ["malware--target-1"],
        ["malware--target-2"],
        ["malware--target-3"],
    ]


def test_export_selected_simple_keeps_single_root_nested_ref_relationship_fallback():
    helper = _full_helper([])
    nested_ref_relationships = _CountingNestedRefRelationshipCollection()
    helper.opencti.stix_nested_ref_relationship = nested_ref_relationships

    helper.export_selected(entities_list=_root_entities(1), mode="simple")

    assert nested_ref_relationships.kwargs == [
        {
            "fromId": "root-1",
            "filters": None,
            "getAll": True,
        }
    ]


def test_export_selected_chunks_nested_ref_relationship_prefetch():
    helper = _full_helper([])
    nested_ref_relationships = _CountingNestedRefRelationshipCollection()
    helper.opencti.stix_nested_ref_relationship = nested_ref_relationships

    helper.export_selected(
        entities_list=_root_entities(EXPORT_PREFETCH_BATCH_SIZE + 1), mode="full"
    )

    assert [
        len(kwargs["filters"]["filters"][0]["values"])
        for kwargs in nested_ref_relationships.kwargs
    ] == [EXPORT_PREFETCH_BATCH_SIZE, 1]
    assert [kwargs["first"] for kwargs in nested_ref_relationships.kwargs] == [
        EXPORT_PREFETCH_BATCH_SIZE,
        EXPORT_PREFETCH_BATCH_SIZE,
    ]


def test_export_selected_applies_nested_ref_relationships_only_to_source_roots():
    relationship = _nested_ref_relationship("nested-ref--1", "root-1", "root-2")
    relationship["to"]["id"] = "root-2"
    relationship["to"]["standard_id"] = "indicator--root-2"
    relationship["to"]["entity_type"] = "Indicator"
    helper = _full_helper([])
    helper.opencti.stix_nested_ref_relationship = (
        _CountingNestedRefRelationshipCollection({"root-1": [relationship]})
    )

    result = helper.export_selected(entities_list=_root_entities(2), mode="full")
    root_objects = {
        item["id"]: item
        for item in result["objects"]
        if item["id"].startswith("indicator--root-")
    }

    assert root_objects["indicator--root-1"]["sample_refs"] == ["indicator--root-2"]
    assert "sample_refs" not in root_objects["indicator--root-2"]


def test_prepare_export_full_prefetches_recursive_nested_refs():
    helper = _full_helper(
        [
            _relationship("relationship--1", "target-1"),
            _relationship("relationship--2", "target-2"),
            _relationship("relationship--3", "target-3"),
        ]
    )
    nested_ref_relationships = _CountingNestedRefRelationshipCollection(
        {
            "internal-relationship--1": [
                _nested_ref_relationship(
                    "nested-ref--relationship-1",
                    "internal-relationship--1",
                    "relationship-sample",
                )
            ],
            "target-target-1": [
                _nested_ref_relationship(
                    "nested-ref--target-1", "target-target-1", "target-sample"
                )
            ],
        }
    )
    helper.opencti.stix_nested_ref_relationship = nested_ref_relationships
    lister = _CountingRelatedObjectLister(
        {
            f"target-target-{index}": {
                "id": f"target-target-{index}",
                "standard_id": f"malware--target-{index}",
                "entity_type": "Malware",
                "parent_types": ["Stix-Domain-Object"],
            }
            for index in range(1, 4)
        }
    )
    helper.get_lister = lambda resolve_type: lister.list
    helper.prepare_id_filters_export = lambda entity_id, access_filter: entity_id
    helper.generate_export = lambda entity: (
        {
            "id": entity["standard_id"],
            "type": entity["entity_type"].lower(),
            "x_opencti_id": entity["id"],
        }
        if "standard_id" in entity
        else entity.copy()
    )
    helper.get_reader = lambda resolve_type: lambda filters: (_ for _ in ()).throw(
        AssertionError("batchable related objects should not use the reader")
    )

    result = helper.prepare_export(entity=_root_entities(1)[0], mode="full")
    objects_by_id = {item["id"]: item for item in result}

    assert nested_ref_relationships.kwargs == [
        {
            "fromId": "root-1",
            "filters": None,
            "getAll": True,
        },
        {
            "filters": {
                "mode": "and",
                "filters": [
                    {
                        "key": "fromId",
                        "values": [
                            "internal-relationship--1",
                            "internal-relationship--2",
                            "internal-relationship--3",
                            "target-target-1",
                            "target-target-2",
                            "target-target-3",
                        ],
                    }
                ],
                "filterGroups": [],
            },
            "first": EXPORT_PREFETCH_BATCH_SIZE,
            "getAll": True,
        },
    ]
    assert objects_by_id["relationship--1"]["sample_refs"] == [
        "malware--relationship-sample"
    ]
    assert objects_by_id["malware--target-1"]["sample_refs"] == [
        "malware--target-sample"
    ]


def test_recursive_nested_ref_prefetch_extends_existing_map_without_refetching_known_ids():
    helper = _full_helper([])
    nested_ref_relationships = _CountingNestedRefRelationshipCollection()
    helper.opencti.stix_nested_ref_relationship = nested_ref_relationships
    existing_map = {"known": []}

    result = helper._prefetch_export_nested_ref_relationships_by_entity_ids(
        ["known", "new-1", "new-2"],
        access_filter=None,
        relationships_by_entity_id=existing_map,
    )

    assert result is existing_map
    assert sorted(result) == ["known", "new-1", "new-2"]
    assert nested_ref_relationships.kwargs == [
        {
            "filters": {
                "mode": "and",
                "filters": [{"key": "fromId", "values": ["new-1", "new-2"]}],
                "filterGroups": [],
            },
            "first": EXPORT_PREFETCH_BATCH_SIZE,
            "getAll": True,
        }
    ]


def test_recursive_nested_ref_prefetch_keeps_singleton_fallback():
    helper = _full_helper([])
    nested_ref_relationships = _CountingNestedRefRelationshipCollection()
    helper.opencti.stix_nested_ref_relationship = nested_ref_relationships
    existing_map = {"known": []}

    result = helper._prefetch_export_nested_ref_relationships_by_entity_ids(
        ["known", "new-1"],
        access_filter=None,
        relationships_by_entity_id=existing_map,
    )
    helper.prepare_export(
        entity={"id": "indicator--new-1", "type": "indicator", "x_opencti_id": "new-1"},
        mode="simple",
        stix_nested_ref_relationships_by_entity_id=result,
    )

    assert result is existing_map
    assert nested_ref_relationships.kwargs == [
        {
            "fromId": "new-1",
            "filters": None,
            "getAll": True,
        }
    ]


def test_recursive_nested_ref_prefetch_chunks_new_ids():
    helper = _full_helper([])
    nested_ref_relationships = _CountingNestedRefRelationshipCollection()
    helper.opencti.stix_nested_ref_relationship = nested_ref_relationships
    existing_map = {"known": []}

    helper._prefetch_export_nested_ref_relationships_by_entity_ids(
        ["known"] + [f"new-{index}" for index in range(EXPORT_PREFETCH_BATCH_SIZE + 1)],
        access_filter=None,
        relationships_by_entity_id=existing_map,
    )

    assert [
        len(kwargs["filters"]["filters"][0]["values"])
        for kwargs in nested_ref_relationships.kwargs
    ] == [EXPORT_PREFETCH_BATCH_SIZE, 1]
    assert [kwargs["first"] for kwargs in nested_ref_relationships.kwargs] == [
        EXPORT_PREFETCH_BATCH_SIZE,
        EXPORT_PREFETCH_BATCH_SIZE,
    ]


def test_export_list_deduplicates_objects_and_rewrites_bundle_once():
    entities = [
        {"id": "indicator--1", "type": "indicator"},
        {"id": "indicator--2", "type": "indicator"},
        {"id": "indicator--1", "type": "indicator"},
    ]
    helper = _helper(entities)
    rewrite_sizes = []
    helper._rewrite_embedded_image_uris_in_bundle_for_export = (
        lambda bundle: rewrite_sizes.append(len(bundle["objects"]))
    )

    bundle = helper.export_list(entity_type="Indicator")

    assert [item["id"] for item in bundle["objects"]] == [
        "indicator--1",
        "indicator--2",
    ]
    assert rewrite_sizes == [2]

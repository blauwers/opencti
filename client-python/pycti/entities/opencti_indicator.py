# coding: utf-8

import json
import uuid

from stix2.canonicalization.Canonicalize import canonicalize

from .indicator.opencti_indicator_properties import (
    INDICATOR_PROPERTIES,
    INDICATOR_PROPERTIES_WITH_FILES,
)

_INDICATOR_EXTENSION_FIELDS = (
    ("x_opencti_score", "score"),
    ("x_opencti_detection", "detection"),
    ("x_opencti_create_observables", "create_observables"),
    ("x_opencti_stix_ids", "stix_ids"),
    ("x_opencti_granted_refs", "granted_refs"),
    ("x_opencti_workflow_id", "workflow_id"),
    ("x_opencti_modified_at", "modified_at"),
    ("opencti_upsert_operations", "opencti_upsert_operations"),
)


class Indicator:
    """Main Indicator class for OpenCTI

    Manages threat indicators and detection patterns in the OpenCTI platform.

    :param opencti: instance of :py:class:`~pycti.api.opencti_api_client.OpenCTIApiClient`
    :type opencti: OpenCTIApiClient
    """

    def __init__(self, opencti):
        """Initialize the Indicator instance.

        :param opencti: OpenCTI API client instance
        :type opencti: OpenCTIApiClient
        """
        self.opencti = opencti
        self.properties = INDICATOR_PROPERTIES
        self.properties_with_files = INDICATOR_PROPERTIES_WITH_FILES

    @staticmethod
    def generate_id(pattern):
        """Generate a STIX ID for an Indicator.

        :param pattern: The STIX pattern
        :type pattern: str
        :return: STIX ID for the indicator
        :rtype: str
        """
        data = {"pattern": pattern.strip()}
        data = canonicalize(data, utf8=False)
        id = str(uuid.uuid5(uuid.UUID("00abedb4-aa42-466c-9c01-fed23315a9b7"), data))
        return "indicator--" + id

    @staticmethod
    def generate_id_from_data(data):
        """Generate a STIX ID from indicator data.

        :param data: Dictionary containing 'pattern' key
        :type data: dict
        :return: STIX ID for the indicator
        :rtype: str
        """
        return Indicator.generate_id(data["pattern"])

    def list(self, **kwargs):
        """List Indicator objects.

        :param filters: (optional) the filters to apply
        :type filters: dict
        :param search: (optional) a search keyword to apply for the listing
        :type search: str
        :param first: (optional) return the first n rows from the `after` ID or the beginning if not set
        :type first: int
        :param after: (optional) OpenCTI object ID of the first row for pagination
        :type after: str
        :param orderBy: (optional) the field to order the response on
        :type orderBy: str
        :param orderMode: (optional) either "asc" or "desc"
        :type orderMode: str
        :param customAttributes: (optional) list of attributes keys to return
        :type customAttributes: str
        :param getAll: (optional) switch to return all entries (be careful to use this without any other filters)
        :type getAll: bool
        :param withPagination: (optional) switch to use pagination
        :type withPagination: bool
        :param withFiles: (optional) include files in response
        :type withFiles: bool
        :param toStix: (optional) get in STIX format
        :type toStix: bool
        :return: List of Indicators
        :rtype: list
        """

        filters = kwargs.get("filters", None)
        search = kwargs.get("search", None)
        first = kwargs.get("first", 500)
        after = kwargs.get("after", None)
        order_by = kwargs.get("orderBy", None)
        order_mode = kwargs.get("orderMode", None)
        custom_attributes = kwargs.get("customAttributes", None)
        get_all = kwargs.get("getAll", False)
        with_pagination = kwargs.get("withPagination", False)
        with_files = kwargs.get("withFiles", False)
        to_stix = kwargs.get("toStix", False)

        self.opencti.app_logger.info(
            "Listing Indicators with filters",
            lambda: {"filters": json.dumps(filters)},
        )
        query = (
            """
                query Indicators($filters: FilterGroup, $search: String, $first: Int, $after: ID, $orderBy: IndicatorsOrdering, $orderMode: OrderingMode, $toStix: Boolean) {
                    indicators(filters: $filters, search: $search, first: $first, after: $after, orderBy: $orderBy, orderMode: $orderMode, toStix: $toStix) {
                        edges {
                            node {
                                """
            + (
                "toStix"
                if to_stix
                else (
                    custom_attributes
                    if custom_attributes is not None
                    else (self.properties_with_files if with_files else self.properties)
                )
            )
            + """
                        }
                    }
                    pageInfo {
                        startCursor
                        endCursor
                        hasNextPage
                        hasPreviousPage
                        globalCount
                    }
                }
            }
        """
        )
        result = self.opencti.query(
            query,
            {
                "filters": filters,
                "search": search,
                "first": first,
                "after": after,
                "orderBy": order_by,
                "orderMode": order_mode,
                "toStix": to_stix,
            },
        )
        if get_all:
            final_data = []
            data = self.opencti.process_multiple(result["data"]["indicators"])
            final_data.extend(data)
            while result["data"]["indicators"]["pageInfo"]["hasNextPage"]:
                after = result["data"]["indicators"]["pageInfo"]["endCursor"]
                self.opencti.app_logger.debug("Listing Indicators", {"after": after})
                result = self.opencti.query(
                    query,
                    {
                        "filters": filters,
                        "search": search,
                        "first": first,
                        "after": after,
                        "orderBy": order_by,
                        "orderMode": order_mode,
                        "toStix": to_stix,
                    },
                )
                data = self.opencti.process_multiple(result["data"]["indicators"])
                final_data.extend(data)
            return final_data
        else:
            return self.opencti.process_multiple(
                result["data"]["indicators"], with_pagination
            )

    def read(self, **kwargs):
        """Read an Indicator object.

        Read can be either used with a known OpenCTI entity `id` or by using a
        valid filter to search and return a single Indicator entity or None.

        Note: either `id` or `filters` is required.

        :param id: the id of the Indicator
        :type id: str
        :param filters: the filters to apply if no id provided
        :type filters: dict
        :param customAttributes: custom attributes to return
        :type customAttributes: str
        :param withFiles: whether to include files
        :type withFiles: bool
        :return: Indicator object
        :rtype: dict or None
        """

        id = kwargs.get("id", None)
        filters = kwargs.get("filters", None)
        custom_attributes = kwargs.get("customAttributes", None)
        with_files = kwargs.get("withFiles", False)
        if id is not None:
            self.opencti.app_logger.info("Reading Indicator", {"id": id})
            query = (
                """
                    query Indicator($id: String!) {
                        indicator(id: $id) {
                            """
                + (
                    custom_attributes
                    if custom_attributes is not None
                    else (self.properties_with_files if with_files else self.properties)
                )
                + """
                    }
                }
             """
            )
            result = self.opencti.query(query, {"id": id})
            return self.opencti.process_multiple_fields(result["data"]["indicator"])
        elif filters is not None:
            result = self.list(filters=filters, customAttributes=custom_attributes)
            if len(result) > 0:
                return result[0]
            else:
                return None
        else:
            self.opencti.app_logger.error(
                "[opencti_indicator] Missing parameters: id or filters"
            )
            return None

    def create(self, **kwargs):
        """Create an Indicator object.

        :param stix_id: (optional) the STIX ID
        :type stix_id: str
        :param createdBy: (optional) the author ID
        :type createdBy: str
        :param objectMarking: (optional) list of marking definition IDs
        :type objectMarking: list
        :param objectLabel: (optional) list of label IDs
        :type objectLabel: list
        :param externalReferences: (optional) list of external reference IDs
        :type externalReferences: list
        :param revoked: (optional) whether the indicator is revoked
        :type revoked: bool
        :param confidence: (optional) confidence level (0-100)
        :type confidence: int
        :param lang: (optional) language
        :type lang: str
        :param created: (optional) creation date
        :type created: str
        :param modified: (optional) modification date
        :type modified: str
        :param pattern_type: the pattern type (required)
        :type pattern_type: str
        :param pattern_version: (optional) the pattern version
        :type pattern_version: str
        :param pattern: the indicator pattern (required)
        :type pattern: str
        :param name: the name of the Indicator (defaults to pattern)
        :type name: str
        :param description: (optional) description
        :type description: str
        :param indicator_types: (optional) list of indicator types
        :type indicator_types: list
        :param valid_from: (optional) valid from date
        :type valid_from: str
        :param valid_until: (optional) valid until date
        :type valid_until: str
        :param x_opencti_score: (optional) score (default: 50)
        :type x_opencti_score: int
        :param x_opencti_detection: (optional) detection flag (default: False)
        :type x_opencti_detection: bool
        :param x_opencti_main_observable_type: the main observable type (required)
        :type x_opencti_main_observable_type: str
        :param x_mitre_platforms: (optional) list of MITRE platforms
        :type x_mitre_platforms: list
        :param killChainPhases: (optional) list of kill chain phase IDs
        :type killChainPhases: list
        :param x_opencti_stix_ids: (optional) list of additional STIX IDs
        :type x_opencti_stix_ids: list
        :param x_opencti_create_observables: (optional) create observables (default: False)
        :type x_opencti_create_observables: bool
        :param objectOrganization: (optional) list of organization IDs
        :type objectOrganization: list
        :param x_opencti_workflow_id: (optional) workflow ID
        :type x_opencti_workflow_id: str
        :param x_opencti_modified_at: (optional) custom modification date
        :type x_opencti_modified_at: str
        :param update: (optional) whether to update if exists (default: False)
        :type update: bool
        :param files: (optional) list of File objects to attach
        :type files: list
        :param filesMarkings: (optional) list of lists of marking definition IDs for each file
        :type filesMarkings: list
        :return: Indicator object
        :rtype: dict or None
        """
        input_variables = self._build_create_input(**kwargs)
        if input_variables is None:
            self.opencti.app_logger.error(
                "[opencti_indicator] Missing parameters: "
                "name or pattern or pattern_type or x_opencti_main_observable_type"
            )
            return None
        self.opencti.app_logger.info(
            "Creating Indicator", {"name": input_variables["name"]}
        )
        query = """
            mutation IndicatorAdd($input: IndicatorAddInput!) {
                indicatorAdd(input: $input) {
                    id
                    standard_id
                    entity_type
                    parent_types
                    observables {
                        edges {
                            node {
                                id
                                standard_id
                                entity_type
                            }
                        }
                    }
                }
            }
        """
        result = self.opencti.query(query, {"input": input_variables})
        return self.opencti.process_multiple_fields(result["data"]["indicatorAdd"])

    def _build_create_input(self, **kwargs):
        name = kwargs.get("name", kwargs.get("pattern", None))
        pattern = kwargs.get("pattern", None)
        pattern_type = kwargs.get("pattern_type", None)
        x_opencti_main_observable_type = kwargs.get(
            "x_opencti_main_observable_type", None
        )
        if (
            name is None
            or pattern is None
            or pattern_type is None
            or x_opencti_main_observable_type is None
        ):
            return None
        if x_opencti_main_observable_type == "File":
            x_opencti_main_observable_type = "StixFile"
        return {
            "stix_id": kwargs.get("stix_id", None),
            "createdBy": kwargs.get("createdBy", None),
            "objectMarking": kwargs.get("objectMarking", None),
            "objectLabel": kwargs.get("objectLabel", None),
            "objectOrganization": kwargs.get("objectOrganization", None),
            "externalReferences": kwargs.get("externalReferences", None),
            "revoked": kwargs.get("revoked", None),
            "confidence": kwargs.get("confidence", None),
            "lang": kwargs.get("lang", None),
            "created": kwargs.get("created", None),
            "modified": kwargs.get("modified", None),
            "pattern_type": pattern_type,
            "pattern_version": kwargs.get("pattern_version", None),
            "pattern": pattern,
            "name": name,
            "description": kwargs.get("description", None),
            "indicator_types": kwargs.get("indicator_types", None),
            "valid_until": kwargs.get("valid_until", None),
            "valid_from": kwargs.get("valid_from", None),
            "x_opencti_score": kwargs.get("x_opencti_score", 50),
            "x_opencti_detection": kwargs.get("x_opencti_detection", False),
            "x_opencti_main_observable_type": x_opencti_main_observable_type,
            "x_mitre_platforms": kwargs.get("x_mitre_platforms", None),
            "x_opencti_stix_ids": kwargs.get("x_opencti_stix_ids", None),
            "killChainPhases": kwargs.get("killChainPhases", None),
            "createObservables": kwargs.get("x_opencti_create_observables", False),
            "x_opencti_workflow_id": kwargs.get("x_opencti_workflow_id", None),
            "x_opencti_modified_at": kwargs.get("x_opencti_modified_at", None),
            "update": kwargs.get("update", False),
            "files": kwargs.get("files", None),
            "filesMarkings": kwargs.get("filesMarkings", None),
            "noTriggerImport": kwargs.get("noTriggerImport", None),
            "embedded": kwargs.get("embedded", None),
            "upsertOperations": kwargs.get("upsert_operations", None),
        }

    def create_many(self, items, with_observables=True):
        input_variables = [self._build_create_input(**item) for item in items]
        if any(input_variable is None for input_variable in input_variables):
            self.opencti.app_logger.error(
                "[opencti_indicator] Missing parameters in bulk create input"
            )
            return None
        self.opencti.app_logger.info(
            "Creating Indicator batch", {"count": len(input_variables)}
        )
        observables_selection = (
            """
                    observables {
                        edges {
                            node {
                                id
                                standard_id
                                entity_type
                            }
                        }
                    }
            """
            if with_observables
            else ""
        )
        query = (
            """
            mutation IndicatorsAdd($inputs: [IndicatorAddInput!]!) {
                indicatorsAdd(inputs: $inputs) {
                    id
                    standard_id
                    entity_type
                    parent_types
            """
            + observables_selection
            + """
                }
            }
        """
        )
        result = self.opencti.query(query, {"inputs": input_variables})
        return [
            self.opencti.process_multiple_fields(indicator)
            for indicator in result["data"]["indicatorsAdd"]
        ]

    def update_field(self, **kwargs):
        """Update an Indicator object field.

        :param id: the Indicator id
        :type id: str
        :param input: the input of the field
        :type input: list
        :return: Updated indicator object
        :rtype: dict or None
        """
        id = kwargs.get("id", None)
        input = kwargs.get("input", None)
        if id is not None and input is not None:
            self.opencti.app_logger.info("Updating Indicator", {"id": id})
            query = """
                        mutation IndicatorFieldPatch($id: ID!, $input: [EditInput!]!) {
                            indicatorFieldPatch(id: $id, input: $input) {
                                id
                                standard_id
                                entity_type
                            }
                        }
                    """
            result = self.opencti.query(
                query,
                {
                    "id": id,
                    "input": input,
                },
            )
            return self.opencti.process_multiple_fields(
                result["data"]["indicatorFieldPatch"]
            )
        else:
            self.opencti.app_logger.error(
                "[opencti_indicator] Cannot update indicator field, missing parameters: id and input"
            )
            return None

    def add_stix_cyber_observable(self, **kwargs):
        """Add a Stix-Cyber-Observable object to Indicator object (based-on).

        :param id: the id of the Indicator
        :type id: str
        :param indicator: Indicator object
        :type indicator: dict
        :param stix_cyber_observable_id: the id of the Stix-Observable
        :type stix_cyber_observable_id: str
        :return: True if there has been no import error
        :rtype: bool
        """
        id = kwargs.get("id", None)
        indicator = kwargs.get("indicator", None)
        stix_cyber_observable_id = kwargs.get("stix_cyber_observable_id", None)
        if id is not None and stix_cyber_observable_id is not None:
            if indicator is None:
                indicator = self.read(id=id)
            if indicator is None:
                self.opencti.app_logger.error(
                    "[opencti_indicator] Cannot add Object Ref, indicator not found"
                )
                return False
            if stix_cyber_observable_id in indicator["observablesIds"]:
                return True
            else:
                self.opencti.app_logger.info(
                    "Adding Stix-Observable to Indicator",
                    {"observable": stix_cyber_observable_id, "indicator": id},
                )
                query = """
                    mutation StixCoreRelationshipAdd($input: StixCoreRelationshipAddInput!) {
                        stixCoreRelationshipAdd(input: $input) {
                            id
                        }
                    }
                """
                self.opencti.query(
                    query,
                    {
                        "id": id,
                        "input": {
                            "fromId": id,
                            "toId": stix_cyber_observable_id,
                            "relationship_type": "based-on",
                        },
                    },
                )
                return True
        else:
            self.opencti.app_logger.error(
                "[opencti_indicator] Missing parameters: id and stix cyber_observable_id"
            )
            return False

    def _build_import_kwargs(self, stix_object, extras, update):
        self.opencti.copy_attributes_from_extension(
            _INDICATOR_EXTENSION_FIELDS, stix_object
        )
        if "x_opencti_main_observable_type" not in stix_object:
            extension_main_observable_type = self.opencti.get_attribute_in_extension(
                "main_observable_type", stix_object
            )
            if extension_main_observable_type is not None:
                stix_object["x_opencti_main_observable_type"] = (
                    extension_main_observable_type
                )
        if "x_mitre_platforms" not in stix_object:
            stix_object["x_mitre_platforms"] = (
                self.opencti.get_attribute_in_mitre_extension("platforms", stix_object)
            )
        return {
            "stix_id": stix_object["id"],
            "createdBy": extras.get("created_by_id"),
            "objectMarking": extras.get("object_marking_ids"),
            "objectLabel": extras.get("object_label_ids"),
            "externalReferences": extras.get("external_references_ids"),
            "revoked": stix_object.get("revoked"),
            "confidence": stix_object.get("confidence"),
            "lang": stix_object.get("lang"),
            "created": stix_object.get("created"),
            "modified": stix_object.get("modified"),
            "pattern_type": stix_object.get("pattern_type"),
            "pattern_version": stix_object.get("pattern_version"),
            "pattern": stix_object.get("pattern", ""),
            "name": stix_object.get("name", stix_object.get("pattern")),
            "description": (
                self.opencti.stix2.convert_markdown(stix_object["description"])
                if "description" in stix_object
                else None
            ),
            "indicator_types": stix_object.get("indicator_types"),
            "valid_from": stix_object.get("valid_from"),
            "valid_until": stix_object.get("valid_until"),
            "x_opencti_score": stix_object.get("x_opencti_score", 50),
            "x_opencti_detection": stix_object.get("x_opencti_detection", False),
            "x_mitre_platforms": stix_object.get("x_mitre_platforms"),
            "x_opencti_main_observable_type": stix_object.get(
                "x_opencti_main_observable_type", "Unknown"
            ),
            "killChainPhases": extras.get("kill_chain_phases_ids"),
            "x_opencti_stix_ids": stix_object.get("x_opencti_stix_ids"),
            "x_opencti_create_observables": stix_object.get(
                "x_opencti_create_observables", False
            ),
            "objectOrganization": stix_object.get("x_opencti_granted_refs"),
            "x_opencti_workflow_id": stix_object.get("x_opencti_workflow_id"),
            "x_opencti_modified_at": stix_object.get("x_opencti_modified_at"),
            "update": update,
            "files": extras.get("files"),
            "filesMarkings": extras.get("filesMarkings"),
            "noTriggerImport": extras.get("noTriggerImport"),
            "embedded": extras.get("embedded"),
            "upsert_operations": stix_object.get("opencti_upsert_operations"),
        }

    def import_from_stix2(self, **kwargs):
        """Import an Indicator object from a STIX2 object."""
        stix_object = kwargs.get("stixObject", None)
        if stix_object is None:
            self.opencti.app_logger.error(
                "[opencti_indicator] Missing parameters: stixObject"
            )
            return None
        extras = kwargs.get("extras", {})
        update = kwargs.get("update", False)
        return self.create(**self._build_import_kwargs(stix_object, extras, update))

    def import_many_from_stix2(self, stix_objects, extras, update=False):
        if len(stix_objects) != len(extras):
            raise ValueError("stix_objects and extras must have the same length")
        items = [
            self._build_import_kwargs(stix_object, item_extras, update)
            for stix_object, item_extras in zip(stix_objects, extras)
        ]
        # Batched STIX imports are only used for indicators that cannot create linked
        # observables, so avoid resolving the relation-backed response field.
        return self.create_many(items, with_observables=False)

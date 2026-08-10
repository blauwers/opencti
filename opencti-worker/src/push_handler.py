import base64
import datetime
import hashlib
import json
import random
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Literal, Optional, Union

import pika
from pika.adapters.blocking_connection import BlockingChannel
from pika.exceptions import NackError, UnroutableError
from pycti import OpenCTIApiClient, OpenCTIStix2, OpenCTIStix2Splitter, __version__
from pycti.api.opencti_api_batch import (
    BatchMutationPlanLimitExceeded,
    BatchMutationPlanTooLarge,
    BatchMutationPlanTooManyExecutionGroups,
)
from requests import RequestException, Timeout

BATCH_REPLAY_COUNT_KEY = "batch_replay_count"
BATCH_REPLAY_LIMIT = 4
BATCH_DELIVERY_PROTOCOL_V2 = 2
BATCH_DELIVERY_PREFIX = "batch-delivery--"
BATCH_DELIVERY_KIND_ROOT = "ROOT"
BATCH_DELIVERY_KIND_CHILD = "CHILD"
BATCH_DELIVERY_BRANCH_ROOT = "ROOT"
BATCH_DELIVERY_BRANCH_LEGACY_SPLIT = "LEGACY_SPLIT"
BATCH_DELIVERY_BRANCH_OVERSIZED_CHUNK = "OVERSIZED_CHUNK"
BATCH_DELIVERY_BRANCH_INTACT_REPLAY = "INTACT_REPLAY"
BATCH_DELIVERY_BRANCH_TERMINAL_DEAD_LETTER = "TERMINAL_DEAD_LETTER"
BATCH_DELIVERY_BRANCH_KINDS = {
    BATCH_DELIVERY_BRANCH_ROOT,
    BATCH_DELIVERY_BRANCH_LEGACY_SPLIT,
    BATCH_DELIVERY_BRANCH_OVERSIZED_CHUNK,
    BATCH_DELIVERY_BRANCH_INTACT_REPLAY,
    BATCH_DELIVERY_BRANCH_TERMINAL_DEAD_LETTER,
}
BATCH_DELIVERY_HANDOFF_NONE = "NONE"
BATCH_DELIVERY_HANDOFF_PLANNED = "PLANNED"
BATCH_DELIVERY_HANDOFF_CHILDREN_RESERVED = "CHILDREN_RESERVED"
BATCH_DELIVERY_HANDOFF_CHILDREN_PUBLISHED = "CHILDREN_PUBLISHED"


class BatchDeliveryChildPublishRetryable(Exception):
    pass


@dataclass(frozen=True)
class BatchDeliveryEnvelope:
    delivery_id: str
    parent_delivery_id: Optional[str]
    delivery_kind: str
    delivery_protocol_version: int
    delivery_branch_kind: str
    delivery_branch_sequence: int
    delivery_branch_ordinal: int


def _is_non_negative_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 0


def _build_delivery_id(parts: List[Any]) -> str:
    encoded = json.dumps(parts, separators=(",", ":"))
    return (
        f"{BATCH_DELIVERY_PREFIX}{hashlib.sha256(encoded.encode('utf-8')).hexdigest()}"
    )


def build_root_delivery_id(submission_id: str) -> str:
    return _build_delivery_id([submission_id, BATCH_DELIVERY_BRANCH_ROOT, 0, 0])


def build_child_delivery_id(
    parent_delivery_id: str,
    branch_kind: str,
    branch_sequence: int,
    branch_ordinal: int,
) -> str:
    if (
        not isinstance(parent_delivery_id, str)
        or len(parent_delivery_id) == 0
        or branch_kind not in BATCH_DELIVERY_BRANCH_KINDS
        or branch_kind == BATCH_DELIVERY_BRANCH_ROOT
        or not _is_non_negative_int(branch_sequence)
        or not _is_non_negative_int(branch_ordinal)
    ):
        raise ValueError("Invalid batch delivery child lineage")
    return _build_delivery_id(
        [parent_delivery_id, branch_kind, branch_sequence, branch_ordinal]
    )


def parse_batch_delivery_envelope(
    data: Dict[str, Any],
) -> Optional[BatchDeliveryEnvelope]:
    envelope_fields = {
        "delivery_id",
        "parent_delivery_id",
        "delivery_kind",
        "delivery_protocol_version",
        "delivery_branch_kind",
        "delivery_branch_sequence",
        "delivery_branch_ordinal",
    }
    if not any(field in data for field in envelope_fields):
        return None
    if data.get("delivery_protocol_version") != BATCH_DELIVERY_PROTOCOL_V2:
        raise ValueError("Unsupported batch delivery protocol")
    delivery_id = data.get("delivery_id")
    submission_id = data.get("submission_id")
    parent_delivery_id = data.get("parent_delivery_id")
    delivery_kind = data.get("delivery_kind")
    branch_kind = data.get("delivery_branch_kind")
    branch_sequence = data.get("delivery_branch_sequence")
    branch_ordinal = data.get("delivery_branch_ordinal")
    if (
        not isinstance(delivery_id, str)
        or len(delivery_id) == 0
        or not isinstance(submission_id, str)
        or len(submission_id) == 0
        or delivery_kind not in {BATCH_DELIVERY_KIND_ROOT, BATCH_DELIVERY_KIND_CHILD}
        or branch_kind not in BATCH_DELIVERY_BRANCH_KINDS
        or not _is_non_negative_int(branch_sequence)
        or not _is_non_negative_int(branch_ordinal)
    ):
        raise ValueError("Invalid batch delivery envelope")
    if delivery_kind == BATCH_DELIVERY_KIND_ROOT:
        if (
            parent_delivery_id is not None
            or branch_kind != BATCH_DELIVERY_BRANCH_ROOT
            or branch_sequence != 0
            or branch_ordinal != 0
            or delivery_id != build_root_delivery_id(submission_id)
        ):
            raise ValueError("Invalid root batch delivery envelope")
    elif (
        not isinstance(parent_delivery_id, str)
        or len(parent_delivery_id) == 0
        or branch_kind == BATCH_DELIVERY_BRANCH_ROOT
        or delivery_id
        != build_child_delivery_id(
            parent_delivery_id,
            branch_kind,
            branch_sequence,
            branch_ordinal,
        )
    ):
        raise ValueError("Invalid child batch delivery envelope")
    return BatchDeliveryEnvelope(
        delivery_id=delivery_id,
        parent_delivery_id=parent_delivery_id,
        delivery_kind=delivery_kind,
        delivery_protocol_version=BATCH_DELIVERY_PROTOCOL_V2,
        delivery_branch_kind=branch_kind,
        delivery_branch_sequence=branch_sequence,
        delivery_branch_ordinal=branch_ordinal,
    )


def build_direct_delivery_context(
    data: Dict[str, Any], envelope: Optional[BatchDeliveryEnvelope]
) -> Optional[Dict[str, Any]]:
    if envelope is None:
        return None
    return {
        "submission_id": data["submission_id"],
        "delivery_id": envelope.delivery_id,
        "parent_delivery_id": envelope.parent_delivery_id,
        "delivery_kind": envelope.delivery_kind,
        "delivery_protocol_version": envelope.delivery_protocol_version,
        "delivery_branch_kind": envelope.delivery_branch_kind,
        "delivery_branch_sequence": envelope.delivery_branch_sequence,
        "delivery_branch_ordinal": envelope.delivery_branch_ordinal,
    }


def build_child_delivery_message(
    data: Dict[str, Any],
    branch_kind: str,
    branch_sequence: int,
    branch_ordinal: int,
) -> Dict[str, Any]:
    child_data = dict(data)
    parent_envelope = parse_batch_delivery_envelope(data)
    if parent_envelope is None:
        return child_data
    child_data.update(
        {
            "delivery_id": build_child_delivery_id(
                parent_envelope.delivery_id,
                branch_kind,
                branch_sequence,
                branch_ordinal,
            ),
            "parent_delivery_id": parent_envelope.delivery_id,
            "delivery_kind": BATCH_DELIVERY_KIND_CHILD,
            "delivery_protocol_version": BATCH_DELIVERY_PROTOCOL_V2,
            "delivery_branch_kind": branch_kind,
            "delivery_branch_sequence": branch_sequence,
            "delivery_branch_ordinal": branch_ordinal,
        }
    )
    return child_data


def build_bundle_queue_message(data: Dict[str, Any], bundle: Any) -> Dict[str, Any]:
    queue_message = dict(data)
    text_bundle = json.dumps(bundle)
    queue_message["content"] = base64.b64encode(
        text_bundle.encode("utf-8", "escape")
    ).decode("utf-8")
    return queue_message


def build_child_delivery_queue_message(
    data: Dict[str, Any],
    bundle: Any,
    branch_kind: str,
    branch_sequence: int,
    branch_ordinal: int,
) -> Dict[str, Any]:
    return build_bundle_queue_message(
        build_child_delivery_message(
            data,
            branch_kind,
            branch_sequence,
            branch_ordinal,
        ),
        bundle,
    )


def should_split_bundles(data: Dict[str, Any], content: Dict[str, Any]) -> bool:
    return len(content["objects"]) > 1 and data.get("split_bundles") is True


def should_add_legacy_default_split_expectations(
    data: Dict[str, Any], content: Dict[str, Any]
) -> bool:
    # Old producers omitted split_bundles and relied on the worker split branch
    # to add expectations unless they explicitly sent no_split=True.
    return (
        len(content["objects"]) > 1
        and "split_bundles" not in data
        and data.get("no_split") is not True
    )


def should_report_batch_expectation(data: Dict[str, Any]) -> bool:
    return data.get("split_bundles") is False


def uses_platform_split_expectation_accounting(data: Dict[str, Any]) -> bool:
    return data.get("delivery_protocol_version") == BATCH_DELIVERY_PROTOCOL_V2


def batch_expectation_work_ids(
    data: Dict[str, Any], work_id: Optional[str]
) -> List[str]:
    """Return every logical Work attributed to one physical batch bundle."""

    additional_work_ids = data.get("additional_work_ids", [])
    if additional_work_ids is None:
        additional_work_ids = []
    if not isinstance(additional_work_ids, list) or any(
        not isinstance(additional_work_id, str) or len(additional_work_id) == 0
        for additional_work_id in additional_work_ids
    ):
        raise ValueError("Invalid additional_work_ids")
    work_ids = []
    for current_work_id in [work_id, *additional_work_ids]:
        if (
            isinstance(current_work_id, str)
            and len(current_work_id) > 0
            and current_work_id not in work_ids
        ):
            work_ids.append(current_work_id)
    return work_ids


def enrichment_batch_output_owners(
    data: Dict[str, Any],
) -> Optional[Dict[str, List[str]]]:
    """Return output object ownership from a validated enrichment result envelope."""

    serialized_result = data.get("enrichment_batch_result")
    if serialized_result is None:
        return None
    if not isinstance(serialized_result, str) or len(serialized_result) == 0:
        raise ValueError("Invalid enrichment_batch_result")
    try:
        result_envelope = json.loads(serialized_result)
    except json.JSONDecodeError as err:
        raise ValueError("Invalid enrichment_batch_result JSON") from err
    if not isinstance(result_envelope, dict) or not isinstance(
        result_envelope.get("results"), list
    ):
        raise ValueError("Invalid enrichment_batch_result")
    output_owners: Dict[str, List[str]] = {}
    for result in result_envelope["results"]:
        if not isinstance(result, dict):
            raise ValueError("Invalid enrichment_batch_result item")
        work_id = result.get("work_id")
        output_object_ids = result.get("output_object_ids")
        if not isinstance(work_id, str) or len(work_id) == 0:
            raise ValueError("Invalid enrichment_batch_result work_id")
        if not isinstance(output_object_ids, list) or any(
            not isinstance(output_object_id, str) or len(output_object_id) == 0
            for output_object_id in output_object_ids
        ):
            raise ValueError("Invalid enrichment_batch_result output_object_ids")
        for output_object_id in output_object_ids:
            owners = output_owners.setdefault(output_object_id, [])
            if work_id not in owners:
                owners.append(work_id)
    return output_owners


def build_batch_expectation_error(
    content: Dict[str, Any], rejected_items: List[Dict[str, Any]]
) -> Optional[Dict[str, str]]:
    if len(rejected_items) == 0:
        return None
    return {
        "error": f"{len(rejected_items)} element(s) failed during batch import",
        "source": f"Bundle {content.get('id', 'unknown')}",
    }


def should_dead_letter_rejected_item(item: Dict[str, Any]) -> bool:
    rejection_info = item.get("rejection_info")
    return (
        isinstance(rejection_info, dict) and rejection_info.get("retryable") is not True
    )


def should_replay_rejected_item(item: Dict[str, Any]) -> bool:
    rejection_info = item.get("rejection_info")
    return isinstance(rejection_info, dict) and rejection_info.get("retryable") is True


def batch_replay_count(data: Dict[str, Any]) -> int:
    count = data.get(BATCH_REPLAY_COUNT_KEY)
    return (
        count
        if isinstance(count, int) and not isinstance(count, bool) and count >= 0
        else 0
    )


def should_replay_intact_bundle(
    data: Dict[str, Any], rejected_items: List[Dict[str, Any]]
) -> bool:
    return batch_replay_count(data) < BATCH_REPLAY_LIMIT and any(
        should_replay_rejected_item(item) for item in rejected_items
    )


def is_batch_plan_limit_error(error: Exception) -> bool:
    if isinstance(error, BatchMutationPlanLimitExceeded):
        return True
    error_message = str(error).lower()
    return (
        "request entity too large" in error_message
        or "payload too large" in error_message
    )


@dataclass(unsafe_hash=True)
class PushHandler:  # pylint: disable=too-many-instance-attributes
    logger: Any
    log_level: str
    json_logging: bool
    opencti_url: str
    opencti_token: str
    ssl_verify: Union[bool, str]
    connector_id: str
    push_exchange: str
    listen_exchange: str
    push_routing: str
    dead_letter_routing: str
    pika_parameters: pika.ConnectionParameters
    bundles_global_counter: Any
    bundles_processing_time_gauge: Any
    objects_max_refs: int
    requests_timeout: int = 300
    batch_requests_timeout: Optional[int] = None
    batch_requests_max_payload_size: Optional[int] = None
    batch_requests_max_execution_groups: Optional[int] = None
    custom_headers: Optional[str] = None

    def __post_init__(self) -> None:
        self.api = OpenCTIApiClient(
            url=self.opencti_url,
            token=self.opencti_token,
            log_level=self.log_level,
            json_logging=self.json_logging,
            ssl_verify=self.ssl_verify,
            custom_headers=self.custom_headers,
            requests_timeout=self.requests_timeout,
            batch_requests_timeout=self.batch_requests_timeout,
            provider="worker/" + __version__,
            batch_requests_max_payload_size=self.batch_requests_max_payload_size,
            batch_requests_max_execution_groups=self.batch_requests_max_execution_groups,
        )

    def send_bundle_to_specific_queue(
        self,
        push_channel: BlockingChannel,
        exchange: str,
        routing_key: str,
        data: Any,
        bundle: Any,
        is_split_bundle=False,
    ):
        self.send_queue_message_to_specific_queue(
            push_channel,
            exchange,
            routing_key,
            build_bundle_queue_message(data, bundle),
            is_split_bundle,
        )

    def send_queue_message_to_specific_queue(
        self,
        push_channel: BlockingChannel,
        exchange: str,
        routing_key: str,
        data: Dict[str, Any],
        is_split_bundle=False,
    ):
        # Send the message
        retry_count = 0
        while True:
            try:
                push_channel.basic_publish(
                    exchange=exchange,
                    routing_key=routing_key,
                    body=json.dumps(data),
                    properties=pika.BasicProperties(
                        delivery_mode=2,
                        content_encoding="utf-8",  # make message persistent
                    ),
                )
                return
            except (UnroutableError, NackError) as err:
                retry_count = retry_count + 1
                self.logger.info(
                    "Unable to send bundle, retrying...",
                    {
                        "retry_count": retry_count,
                        "routing_key": routing_key,
                        "is_split_bundle": is_split_bundle,
                    },
                )
                self.logger.debug("Unable to send bundle error", {"error": str(err)})
                time.sleep(10)

    def _confirm_delivery(self, push_channel: BlockingChannel, data: Dict[str, Any]):
        try:
            push_channel.confirm_delivery()
        except Exception as err:  # pylint: disable=broad-except
            if parse_batch_delivery_envelope(data) is not None:
                raise BatchDeliveryChildPublishRetryable(
                    "Unable to enable publisher confirms for durable child handoff"
                ) from err
            self.logger.warning(str(err))

    @staticmethod
    def _build_child_reservations(
        child_queue_messages: List[Dict[str, Any]],
    ) -> List[Dict[str, Any]]:
        return [
            {
                "branch_kind": child_queue_message["delivery_branch_kind"],
                "branch_sequence": child_queue_message["delivery_branch_sequence"],
                "branch_ordinal": child_queue_message["delivery_branch_ordinal"],
                "queue_payload": json.dumps(child_queue_message),
            }
            for child_queue_message in child_queue_messages
        ]

    @staticmethod
    def _handoff_matches_branch(
        handoff: Dict[str, Any],
        expected_branch_kind: str,
    ) -> bool:
        children = handoff.get("children")
        if children is None:
            children = handoff.get("pending_children")
        if not isinstance(children, list):
            raise ValueError("Invalid durable child handoff state")
        if len(children) == 0:
            if handoff.get("child_count") == 0:
                return False
            raise ValueError("Invalid durable child handoff state")
        branch_kind = None
        for child in children:
            queue_payload = child.get("queue_payload")
            if not isinstance(queue_payload, str) or len(queue_payload) == 0:
                raise ValueError("Invalid durable child handoff payload")
            queue_message = json.loads(queue_payload)
            child_envelope = parse_batch_delivery_envelope(queue_message)
            if child_envelope is None:
                raise ValueError("Invalid durable child handoff payload")
            if branch_kind is None:
                branch_kind = child_envelope.delivery_branch_kind
            elif branch_kind != child_envelope.delivery_branch_kind:
                raise ValueError("Invalid durable child handoff branch set")
            if child_envelope.delivery_branch_kind != expected_branch_kind:
                return False
        return True

    @staticmethod
    def _handoff_branch_kind(handoff: Dict[str, Any]) -> Optional[str]:
        children = handoff.get("children")
        if children is None:
            children = handoff.get("pending_children")
        if not isinstance(children, list) or len(children) == 0:
            raise ValueError("Invalid durable child handoff state")
        branch_kind = None
        for child in children:
            queue_payload = child.get("queue_payload")
            if not isinstance(queue_payload, str) or len(queue_payload) == 0:
                raise ValueError("Invalid durable child handoff payload")
            child_envelope = parse_batch_delivery_envelope(json.loads(queue_payload))
            if child_envelope is None:
                raise ValueError("Invalid durable child handoff payload")
            if branch_kind is None:
                branch_kind = child_envelope.delivery_branch_kind
            elif branch_kind != child_envelope.delivery_branch_kind:
                raise ValueError("Invalid durable child handoff branch set")
        return branch_kind

    def _publish_reserved_child_handoff(
        self,
        push_channel: BlockingChannel,
        exchange: str,
        routing_key: str,
        parent_delivery_id: str,
        handoff: Dict[str, Any],
        is_split_bundle=False,
    ) -> None:
        pending_children = handoff.get("pending_children")
        if not isinstance(pending_children, list):
            raise ValueError("Invalid durable child handoff state")
        published_child_ids = []
        try:
            for child in pending_children:
                queue_payload = child.get("queue_payload")
                child_delivery_id = child.get("delivery_id")
                if (
                    not isinstance(queue_payload, str)
                    or len(queue_payload) == 0
                    or not isinstance(child_delivery_id, str)
                    or len(child_delivery_id) == 0
                ):
                    raise ValueError("Invalid durable child handoff payload")
                queue_message = json.loads(queue_payload)
                try:
                    self.send_queue_message_to_specific_queue(
                        push_channel,
                        exchange,
                        routing_key,
                        queue_message,
                        is_split_bundle,
                    )
                except Exception as err:  # pylint: disable=broad-except
                    raise BatchDeliveryChildPublishRetryable(
                        "Unable to publish durable child handoff"
                    ) from err
                published_child_ids.append(child_delivery_id)
        except BatchDeliveryChildPublishRetryable:
            if published_child_ids:
                self.api.mark_batch_delivery_children_published(
                    parent_delivery_id,
                    published_child_ids,
                )
            raise
        if published_child_ids or (
            len(pending_children) == 0
            and handoff.get("handoff_evidence")
            == BATCH_DELIVERY_HANDOFF_CHILDREN_RESERVED
        ):
            self.api.mark_batch_delivery_children_published(
                parent_delivery_id,
                published_child_ids,
            )

    def _reserve_and_publish_child_handoff(
        self,
        push_channel: BlockingChannel,
        exchange: str,
        routing_key: str,
        data: Dict[str, Any],
        child_queue_messages: List[Dict[str, Any]],
        is_split_bundle=False,
    ) -> None:
        parent_envelope = parse_batch_delivery_envelope(data)
        if parent_envelope is None:
            for child_queue_message in child_queue_messages:
                self.send_queue_message_to_specific_queue(
                    push_channel,
                    exchange,
                    routing_key,
                    child_queue_message,
                    is_split_bundle,
                )
            return
        handoff = self.api.reserve_batch_delivery_children(
            parent_envelope.delivery_id,
            self._build_child_reservations(child_queue_messages),
        )
        self._publish_reserved_child_handoff(
            push_channel,
            exchange,
            routing_key,
            parent_envelope.delivery_id,
            handoff,
            is_split_bundle,
        )

    def _resume_reserved_child_handoff(
        self,
        push_channel: BlockingChannel,
        exchange: str,
        routing_key: str,
        data: Dict[str, Any],
        expected_branch_kind: str,
        is_split_bundle=False,
    ) -> bool:
        parent_envelope = parse_batch_delivery_envelope(data)
        if parent_envelope is None:
            return False
        handoff = self.api.batch_delivery_handoff(parent_envelope.delivery_id)
        if handoff.get("handoff_evidence") not in {
            BATCH_DELIVERY_HANDOFF_CHILDREN_RESERVED,
            BATCH_DELIVERY_HANDOFF_CHILDREN_PUBLISHED,
        }:
            return False
        if not self._handoff_matches_branch(handoff, expected_branch_kind):
            return False
        self._publish_reserved_child_handoff(
            push_channel,
            exchange,
            routing_key,
            parent_envelope.delivery_id,
            handoff,
            is_split_bundle,
        )
        return True

    def _resume_reserved_unsplit_child_handoff(
        self,
        data: Dict[str, Any],
    ) -> Optional[str]:
        parent_envelope = parse_batch_delivery_envelope(data)
        if parent_envelope is None:
            return None
        handoff = self.api.batch_delivery_handoff(parent_envelope.delivery_id)
        if handoff.get("handoff_evidence") not in {
            BATCH_DELIVERY_HANDOFF_CHILDREN_RESERVED,
            BATCH_DELIVERY_HANDOFF_CHILDREN_PUBLISHED,
        }:
            return None
        branch_kind = self._handoff_branch_kind(handoff)
        route = {
            BATCH_DELIVERY_BRANCH_LEGACY_SPLIT: (
                self.push_exchange,
                self.push_routing,
                True,
            ),
            BATCH_DELIVERY_BRANCH_OVERSIZED_CHUNK: (
                self.push_exchange,
                self.push_routing,
                False,
            ),
            BATCH_DELIVERY_BRANCH_INTACT_REPLAY: (
                self.push_exchange,
                self.push_routing,
                False,
            ),
            BATCH_DELIVERY_BRANCH_TERMINAL_DEAD_LETTER: (
                self.listen_exchange,
                self.dead_letter_routing,
                False,
            ),
        }.get(branch_kind)
        if route is None:
            return None
        with pika.BlockingConnection(self.pika_parameters) as push_pika_connection:
            with push_pika_connection.channel() as push_channel:
                self._confirm_delivery(push_channel, data)
                self._publish_reserved_child_handoff(
                    push_channel,
                    route[0],
                    route[1],
                    parent_envelope.delivery_id,
                    handoff,
                    route[2],
                )
        return branch_kind

    def _report_expectations(
        self, work_ids: List[str], error: Optional[Dict[str, str]]
    ) -> None:
        for work_id in work_ids:
            self.api.work.report_expectation(work_id, error)

    def _report_bundle_expectations(
        self,
        data: Dict[str, Any],
        work_ids: List[str],
        content: Dict[str, Any],
        rejected_items: List[Dict[str, Any]],
    ) -> None:
        output_owners = enrichment_batch_output_owners(data)
        if output_owners is None:
            self._report_expectations(
                work_ids, build_batch_expectation_error(content, rejected_items)
            )
            return
        owner_work_ids = {
            owner_work_id
            for owners in output_owners.values()
            for owner_work_id in owners
        }
        if not owner_work_ids.issubset(set(work_ids)):
            raise ValueError("Enrichment batch result references an untracked Work")
        rejected_items_by_work_id = {work_id: [] for work_id in work_ids}
        for rejected_item in rejected_items:
            owner_work_ids = output_owners.get(rejected_item.get("id"))
            if owner_work_ids is None:
                raise ValueError(
                    "Rejected enrichment batch output object has no owning Work"
                )
            for owner_work_id in owner_work_ids:
                if owner_work_id in rejected_items_by_work_id:
                    rejected_items_by_work_id[owner_work_id].append(rejected_item)
        for work_id in work_ids:
            self.api.work.report_expectation(
                work_id,
                build_batch_expectation_error(
                    content, rejected_items_by_work_id[work_id]
                ),
            )

    def _add_expectations(self, work_ids: List[str], expectations: int) -> bool:
        if len(work_ids) == 0:
            return True
        has_alive_work = False
        for work_id in work_ids:
            if self.api.work.add_expectations(work_id, expectations):
                has_alive_work = True
        return has_alive_work

    def split_and_requeue_bundle(
        self,
        data: Dict[str, Any],
        content: Dict[str, Any],
        work_id: Optional[str],
        *,
        add_expectations: bool,
        report_parent_expectation: bool,
    ) -> Literal["ack"]:
        work_ids = batch_expectation_work_ids(data, work_id)
        platform_accounts_split_expectations = (
            uses_platform_split_expectation_accounting(data)
        )
        with pika.BlockingConnection(self.pika_parameters) as push_pika_connection:
            with push_pika_connection.channel() as push_channel:
                self._confirm_delivery(push_channel, data)
                if self._resume_reserved_child_handoff(
                    push_channel,
                    self.push_exchange,
                    self.push_routing,
                    data,
                    BATCH_DELIVERY_BRANCH_LEGACY_SPLIT,
                    True,
                ):
                    if (
                        report_parent_expectation
                        and not platform_accounts_split_expectations
                    ):
                        self._report_bundle_expectations(data, work_ids, content, [])
                    return "ack"
                event_version = content.get("x_opencti_event_version")
                stix2_splitter = OpenCTIStix2Splitter()
                expectations, _, bundles = (
                    stix2_splitter.split_bundle_with_expectations(
                        content,
                        False,
                        event_version,
                        data.get("cleanup_inconsistent_bundle", False),
                    )
                )
                if (
                    add_expectations
                    and not platform_accounts_split_expectations
                    and not self._add_expectations(work_ids, expectations)
                ):
                    return "ack"
                split_queue_messages = []
                for bundle_ordinal, bundle in enumerate(bundles):
                    split_data = build_child_delivery_queue_message(
                        data,
                        bundle,
                        BATCH_DELIVERY_BRANCH_LEGACY_SPLIT,
                        0,
                        bundle_ordinal,
                    )
                    split_data["split_bundles"] = True
                    split_data["no_split"] = False
                    split_data.pop("batch_plan", None)
                    split_queue_messages.append(split_data)
                self._reserve_and_publish_child_handoff(
                    push_channel,
                    self.push_exchange,
                    self.push_routing,
                    data,
                    split_queue_messages,
                    True,
                )
                if (
                    report_parent_expectation
                    and not platform_accounts_split_expectations
                ):
                    self._report_bundle_expectations(data, work_ids, content, [])
        return "ack"

    def split_and_requeue_batch_chunks(
        self,
        data: Dict[str, Any],
        content: Dict[str, Any],
        work_id: Optional[str],
        error: Exception,
    ) -> Optional[Literal["ack"]]:
        work_ids = batch_expectation_work_ids(data, work_id)
        platform_accounts_split_expectations = (
            uses_platform_split_expectation_accounting(data)
        )
        with pika.BlockingConnection(self.pika_parameters) as push_pika_connection:
            with push_pika_connection.channel() as push_channel:
                self._confirm_delivery(push_channel, data)
                if self._resume_reserved_child_handoff(
                    push_channel,
                    self.push_exchange,
                    self.push_routing,
                    data,
                    BATCH_DELIVERY_BRANCH_OVERSIZED_CHUNK,
                ):
                    if not platform_accounts_split_expectations:
                        self._report_bundle_expectations(data, work_ids, content, [])
                    return "ack"
                chunks = OpenCTIStix2.build_oversized_batch_plan_chunks(
                    content,
                    data.get("cleanup_inconsistent_bundle", False),
                    data.get("batch_plan"),
                )
                if chunks is None:
                    return None

                log_context = {
                    "bundle_id": content.get("id"),
                    "object_count": len(content["objects"]),
                    "chunk_object_counts": [
                        len(chunk_bundle["objects"]) for chunk_bundle, _ in chunks
                    ],
                }
                if isinstance(error, BatchMutationPlanTooLarge):
                    log_context["actual_size"] = error.actual_size
                    log_context["max_size"] = error.max_size
                elif isinstance(error, BatchMutationPlanTooManyExecutionGroups):
                    log_context["actual_execution_groups"] = error.actual_count
                    log_context["max_execution_groups"] = error.max_count
                self.logger.warning(
                    (
                        "Splitting bounded batch mutation plan into "
                        "durable child batches"
                    ),
                    log_context,
                )
                if (
                    not platform_accounts_split_expectations
                    and not self._add_expectations(work_ids, len(chunks))
                ):
                    return "ack"
                chunk_queue_messages = []
                for chunk_ordinal, (
                    chunk_bundle,
                    chunk_backend_batch_plan,
                ) in enumerate(chunks):
                    chunk_data = build_child_delivery_queue_message(
                        data,
                        chunk_bundle,
                        BATCH_DELIVERY_BRANCH_OVERSIZED_CHUNK,
                        0,
                        chunk_ordinal,
                    )
                    chunk_data["split_bundles"] = False
                    chunk_data["no_split"] = True
                    chunk_data["batch_plan"] = chunk_backend_batch_plan
                    chunk_queue_messages.append(chunk_data)
                self._reserve_and_publish_child_handoff(
                    push_channel,
                    self.push_exchange,
                    self.push_routing,
                    data,
                    chunk_queue_messages,
                )
                if not platform_accounts_split_expectations:
                    self._report_bundle_expectations(data, work_ids, content, [])
        return "ack"

    def handle_message(
        self,
        body: str,
    ) -> Literal["ack", "nack", "requeue"]:
        try:
            data: Dict[str, Any] = json.loads(body)
        except Exception as e:
            self.logger.error(
                "Could not process message",
                {"body": body, "exception": e},
            )
            # Nack message, no requeue for this unprocessed message
            return "nack"

        imported_items = []
        too_large_items_bundles = []
        start_processing = datetime.datetime.now()
        try:
            # Set the API headers
            delivery_envelope = parse_batch_delivery_envelope(data)
            self.api.set_applicant_id_header(data.get("applicant_id"))
            self.api.set_playbook_id_header(data.get("playbook_id"))
            self.api.set_event_id(data.get("event_id"))
            self.api.set_draft_id(data.get("draft_id"))
            self.api.set_synchronized_upsert_header(data.get("synchronized", False))
            self.api.set_previous_standard_header(data.get("previous_standard"))
            self.api.set_batch_wait_until(data.get("batch_wait_until"))
            replay_count = batch_replay_count(data)
            self.api.set_retry_number(replay_count if replay_count > 0 else None)
            work_id = data.get("work_id")
            work_ids = batch_expectation_work_ids(data, work_id)
            self.api.set_work_id(work_id)

            # Execute the import
            types = (
                data["entities_types"]
                if "entities_types" in data and len(data["entities_types"]) > 0
                else None
            )
            raw_content = base64.b64decode(data["content"]).decode("utf-8")
            content = json.loads(raw_content)
            event_type = data.get("type", "bundle")
            if event_type == "bundle":
                # Event type bundle
                # Standard event with STIX information
                if "objects" not in content or len(content["objects"]) == 0:
                    raise ValueError("JSON data type is not a STIX2 bundle")
                if not should_split_bundles(data, content):
                    resumed_handoff_branch = (
                        self._resume_reserved_unsplit_child_handoff(data)
                    )
                    if resumed_handoff_branch is not None:
                        return "ack"
                    report_bundle_expectation = len(work_ids) > 0 and (
                        should_report_batch_expectation(data)
                        or (data.get("split_bundles") is True and len(work_ids) > 1)
                    )
                    if len(
                        work_ids
                    ) > 0 and should_add_legacy_default_split_expectations(
                        data, content
                    ):
                        if not self._add_expectations(
                            work_ids, len(content["objects"])
                        ):
                            return "ack"
                    update = data.get("update", False)
                    import_bundle = (
                        self.api.stix2.import_bundle_from_json
                        if data.get("split_bundles") is True
                        else self.api.stix2.import_bundle_from_json_batch
                    )
                    import_kwargs = {
                        "report_expectations": not report_bundle_expectation,
                    }
                    if data.get("split_bundles") is not True:
                        import_kwargs["execution_mode"] = data.get(
                            "batch_execution_mode"
                        )
                        import_kwargs["wait_until"] = data.get("batch_wait_until")
                        import_kwargs["backend_batch_plan"] = data.get("batch_plan")
                        import_kwargs["split_oversized_batch_plan"] = False
                        import_kwargs["direct_delivery_context"] = (
                            build_direct_delivery_context(data, delivery_envelope)
                        )
                    try:
                        imported_items, too_large_items_bundles = import_bundle(
                            raw_content,
                            update,
                            types,
                            work_id,
                            self.objects_max_refs,
                            data.get("cleanup_inconsistent_bundle", False),
                            **import_kwargs,
                        )
                    except Exception as err:
                        if data.get(
                            "split_bundles"
                        ) is not True and is_batch_plan_limit_error(err):
                            if data.get("split_bundles") is False:
                                durable_batch_split_result = (
                                    self.split_and_requeue_batch_chunks(
                                        data,
                                        content,
                                        work_id,
                                        err,
                                    )
                                )
                                if durable_batch_split_result is not None:
                                    return durable_batch_split_result
                            log_context = {
                                "bundle_id": content.get("id"),
                                "object_count": len(content["objects"]),
                            }
                            if isinstance(err, BatchMutationPlanTooLarge):
                                log_context["actual_size"] = err.actual_size
                                log_context["max_size"] = err.max_size
                            elif isinstance(
                                err, BatchMutationPlanTooManyExecutionGroups
                            ):
                                log_context["actual_execution_groups"] = (
                                    err.actual_count
                                )
                                log_context["max_execution_groups"] = err.max_count
                            self.logger.warning(
                                (
                                    "Falling back to split bundle transport for "
                                    "bounded batch mutation request"
                                ),
                                log_context,
                            )
                            return self.split_and_requeue_bundle(
                                data,
                                content,
                                work_id,
                                add_expectations=data.get("split_bundles") is False,
                                report_parent_expectation=data.get("split_bundles")
                                is False,
                            )
                        raise
                    if should_replay_intact_bundle(data, too_large_items_bundles):
                        next_replay_count = replay_count + 1
                        self.logger.warning(
                            (
                                "Deferring intact bundle replay for retryable "
                                "batch failures"
                            ),
                            {
                                "bundle_id": content.get("id"),
                                "count": sum(
                                    1
                                    for item in too_large_items_bundles
                                    if should_replay_rejected_item(item)
                                ),
                                "retry_number": next_replay_count,
                            },
                        )
                        replay_data = build_child_delivery_queue_message(
                            data,
                            content,
                            BATCH_DELIVERY_BRANCH_INTACT_REPLAY,
                            next_replay_count,
                            0,
                        )
                        replay_data[BATCH_REPLAY_COUNT_KEY] = next_replay_count
                        with pika.BlockingConnection(
                            self.pika_parameters
                        ) as push_pika_connection:
                            with push_pika_connection.channel() as push_channel:
                                self._confirm_delivery(push_channel, data)
                                self._reserve_and_publish_child_handoff(
                                    push_channel,
                                    self.push_exchange,
                                    self.push_routing,
                                    data,
                                    [replay_data],
                                )
                        imported_items = []
                        return "ack"
                    if report_bundle_expectation:
                        self._report_bundle_expectations(
                            data,
                            work_ids,
                            content,
                            too_large_items_bundles,
                        )
                    dead_letter_items = [
                        item
                        for item in too_large_items_bundles
                        if should_dead_letter_rejected_item(item)
                    ]
                    if len(dead_letter_items) > 0:
                        with pika.BlockingConnection(
                            self.pika_parameters
                        ) as push_pika_connection:
                            with push_pika_connection.channel() as push_channel:
                                self._confirm_delivery(push_channel, data)
                                dead_letter_queue_messages = []
                                for (
                                    dead_letter_ordinal,
                                    too_large_item_bundle,
                                ) in enumerate(dead_letter_items):
                                    rejection_info = too_large_item_bundle.setdefault(
                                        "rejection_info", {}
                                    )
                                    rejection_info["original_connector_id"] = (
                                        self.connector_id
                                    )
                                    self.logger.warning(
                                        (
                                            "Detected a rejected batch item, "
                                            "sending it to dead letter queue..."
                                        ),
                                        {
                                            "bundle_id": too_large_item_bundle["id"],
                                            "connector_id": self.connector_id,
                                        },
                                    )
                                    dead_letter_queue_messages.append(
                                        build_child_delivery_queue_message(
                                            data,
                                            too_large_item_bundle,
                                            BATCH_DELIVERY_BRANCH_TERMINAL_DEAD_LETTER,
                                            replay_count,
                                            dead_letter_ordinal,
                                        )
                                    )
                                self._reserve_and_publish_child_handoff(
                                    push_channel,
                                    self.listen_exchange,
                                    self.dead_letter_routing,
                                    data,
                                    dead_letter_queue_messages,
                                )
                else:
                    # Bundle splitting was explicitly requested, split and requeue.
                    return self.split_and_requeue_bundle(
                        data,
                        content,
                        work_id,
                        add_expectations=True,
                        report_parent_expectation=False,
                    )
            # Event type event
            # Specific OpenCTI event operation with specific operation
            elif event_type == "event":
                match content["type"]:
                    # Standard knowledge
                    case "create" | "update":
                        bundle = {
                            "type": "bundle",
                            "objects": [content["data"]],
                        }
                        imported_items = self.api.stix2.import_bundle(
                            bundle, True, types, work_id
                        )
                    # Specific knowledge merge
                    case "merge":
                        # Start with a merge
                        target_id = content["data"]["id"]
                        source_ids = list(
                            map(
                                lambda source: source["id"],
                                content["context"]["sources"],
                            )
                        )
                        merge_object = content["data"]
                        merge_object["opencti_operation"] = content["type"]
                        merge_object["merge_target_id"] = target_id
                        merge_object["merge_source_ids"] = source_ids
                        bundle = {
                            "type": "bundle",
                            "objects": [merge_object],
                        }
                        imported_items = self.api.stix2.import_bundle(
                            bundle, True, types, work_id
                        )
                    # All standard operations
                    case (
                        "delete"  # Standard delete
                        | "restore"  # Restore an operation from trash
                        | "delete_force"  # Delete with no trash
                        | "share"  # Share an element
                        | "unshare"  # Unshare an element
                        | "rule_apply"  # Applying a rule (start engine)
                        | "rule_clear"  # Clearing a rule (stop engine)
                        | "rules_rescan"  # Rescan a rule (massive operation in UI)
                        | "enrichment"  # Ask for enrichment (massive operation in UI)
                        | "clear_access_restriction"  # Clear access members
                        | "revert_draft"  # Cancel draft modification
                    ):
                        data_object = content["data"]
                        data_object["opencti_operation"] = content["type"]
                        bundle = {
                            "type": "bundle",
                            "objects": [data_object],
                        }
                        imported_items = self.api.stix2.import_bundle(
                            bundle, True, types, work_id
                        )
                    case _:
                        raise ValueError(
                            "Unsupported operation type", {"event_type": event_type}
                        )
            else:
                raise ValueError("Unsupported event type", {"event_type": event_type})

            return "ack"
        except (RequestException, Timeout, BatchDeliveryChildPublishRetryable):
            self.logger.error(
                "Error executing data handling, a connection error or timeout occurred"
            )
            sleep_jitter = round(random.uniform(10, 30), 2)
            time.sleep(sleep_jitter)
            return "requeue"
        except Exception as ex:
            # Technical unmanaged exception
            self.logger.error("Error executing data handling", {"reason": str(ex)})
            # Nack message and discard
            return "nack"
        finally:
            self.api.set_retry_number(None)
            self.bundles_global_counter.add(len(imported_items))
            processing_delta = datetime.datetime.now() - start_processing
            self.bundles_processing_time_gauge.record(processing_delta.seconds)

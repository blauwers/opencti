import uuid

from pycti.api.opencti_api_client import API_FEATURE_BULK_REF_RELATION_DELETE


def test_stix2_update_labels_support_bulk_relation_removals(api_client):
    assert api_client.supports_api_feature(API_FEATURE_BULK_REF_RELATION_DELETE)

    suffix = uuid.uuid4().hex
    intrusion_set = api_client.intrusion_set.create(
        name=f"Bulk label intrusion set {suffix}",
        description="Bulk label regression source",
    )
    label_values = [f"bulk-domain-one-{suffix}", f"bulk-domain-two-{suffix}"]
    label_ids = []

    try:
        api_client.stix2.stix2_update.add_labels(
            "intrusion-set",
            intrusion_set["id"],
            [{"value": label_value} for label_value in label_values],
        )

        updated_intrusion_set = api_client.intrusion_set.read(id=intrusion_set["id"])
        label_ids.extend(updated_intrusion_set["objectLabelIds"])
        assert {
            label["value"] for label in updated_intrusion_set["objectLabel"]
        } == set(label_values)

        api_client.stix2.stix2_update.remove_labels(
            "intrusion-set",
            intrusion_set["id"],
            [{"value": label_value} for label_value in label_values],
        )

        updated_intrusion_set = api_client.intrusion_set.read(id=intrusion_set["id"])
        assert updated_intrusion_set["objectLabel"] == []
    finally:
        api_client.stix_domain_object.delete(id=intrusion_set["id"])
        for label_id in label_ids:
            api_client.label.delete(id=label_id)

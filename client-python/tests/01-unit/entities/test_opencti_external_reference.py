from pycti.entities.opencti_external_reference import ExternalReference


def test_generate_id_ignores_blank_url_without_fallback_identity() -> None:
    assert ExternalReference.generate_id(url="", source_name="Unknown") is None


def test_generate_id_uses_source_and_external_id_when_url_is_blank() -> None:
    assert ExternalReference.generate_id(
        url="",
        source_name="feed",
        external_id="1",
    ) == ExternalReference.generate_id(source_name="feed", external_id="1")

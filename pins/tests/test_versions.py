from pins.versions import Version


def test_version_from_string():
    version = Version.from_string("20220209T220116Z-baf3f")
    assert str(version.created) == "2022-02-09 22:01:16"
    assert version.hash == "baf3f"

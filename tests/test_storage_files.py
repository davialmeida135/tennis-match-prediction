import json

import pytest

from tennis_match_prediction.storage.files import write_json


@pytest.mark.parametrize("invalid", [float("nan"), object()])
def test_invalid_json_preserves_publication_and_removes_temporary_file(tmp_path, invalid):
    publication = tmp_path / "report.json"
    publication.write_text('{"status": "previous"}', encoding="utf-8")

    with pytest.raises((ValueError, TypeError)):
        write_json(publication, {"invalid": invalid})

    assert json.loads(publication.read_text(encoding="utf-8")) == {"status": "previous"}
    assert list(tmp_path.iterdir()) == [publication]


def test_json_publication_creates_parent_directories_and_replaces_existing_file(tmp_path):
    publication = tmp_path / "nested" / "report.json"

    write_json(publication, {"status": "first"})
    write_json(publication, {"status": "updated", "player": "João"})

    assert json.loads(publication.read_text(encoding="utf-8")) == {
        "status": "updated",
        "player": "João",
    }
    assert list(publication.parent.iterdir()) == [publication]

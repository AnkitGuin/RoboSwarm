import pytest
from robostream.storage.files import atomic_bytes, safe_relative_path


def test_atomic_write_and_no_implicit_overwrite(tmp_path):
    path = tmp_path / "test.txt"
    atomic_bytes(path, b"raw")
    with pytest.raises(FileExistsError):
        atomic_bytes(path, b"altered")
    assert path.read_bytes() == b"raw"


def test_path_traversal_rejected():
    for input_path in ("../secret", "/tmp/secret", "safe/../../secret"):
        with pytest.raises(ValueError):
            safe_relative_path(input_path)

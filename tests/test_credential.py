import os

import pytest

from dasstore.utils.credential import (
    add_credential,
    get_credential,
    remove_credential,
    replace_credential,
)

paths = ["~/.dasstore/test", "~/.dasstore_tmp/test"]
endpoints = ["test1", "test2"]


@pytest.mark.parametrize("path", paths)
@pytest.mark.parametrize("endpoint", endpoints)
def test_add_credential(endpoint, path):
    key = f"{endpoint}_key"
    secret = f"{endpoint}_secret"
    add_credential(endpoint, credential_path=path, key=key, secret=secret)
    cred = get_credential(endpoint, credential_path=path)
    assert cred["aws_access_key_id"] == key
    assert cred["aws_secret_access_key"] == secret


@pytest.mark.parametrize("path", paths)
@pytest.mark.parametrize("endpoint", endpoints)
def test_replace_credential(endpoint, path):
    key = f"{endpoint}_key_update"
    secret = f"{endpoint}_secret_update"
    replace_credential(endpoint, credential_path=path, key=key, secret=secret)
    cred = get_credential(endpoint, credential_path=path)
    assert cred["aws_access_key_id"] == key
    assert cred["aws_secret_access_key"] == secret


@pytest.mark.parametrize("path", paths)
@pytest.mark.parametrize("endpoint", endpoints)
def test_remove_credential(endpoint, path):
    remove_credential(endpoint, credential_path=path)
    with pytest.raises(KeyError):
        get_credential(endpoint, credential_path=path)


@pytest.mark.parametrize("path", paths)
@pytest.mark.parametrize("endpoint", endpoints)
def test_remove_undefined_credential(endpoint, path):
    remove_credential(endpoint, credential_path=path)


@pytest.mark.parametrize("path", paths)
def test_remove_credential_file(path):
    os.system(f"rm {path}")
    assert not os.path.exists(path)

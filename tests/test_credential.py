import os

from dasstore.utils.credential import (
    add_credential,
    get_credential,
    replace_credential,
    remove_credential,
)

## test existing credential
add_credential("test", key="test_key", secret="test_secret")
cred = get_credential("test")
assert cred["aws_access_key_id"] == "test_key"
assert cred["aws_secret_access_key"] == "test_secret"

replace_credential("test", key="test_key2", secret="test_secret2")
cred = get_credential("test")
assert cred["aws_access_key_id"] == "test_key2"
assert cred["aws_secret_access_key"] == "test_secret2"

remove_credential("test")

## test new credential file
add_credential(
    "test", credential_path="~/.dasstore/test", key="test_key", secret="test_secret"
)

## add for the second time
add_credential(
    "test", credential_path="~/.dasstore/test", key="test_key", secret="test_secret"
)
cred = get_credential("test", credential_path="~/.dasstore/test")
assert cred["aws_access_key_id"] == "test_key"
assert cred["aws_secret_access_key"] == "test_secret"

replace_credential(
    "test", credential_path="~/.dasstore/test", key="test_key2", secret="test_secret2"
)
try:
    replace_credential(
        "test2",
        credential_path="~/.dasstore/test",
        key="test_key2",
        secret="test_secret2",
    )
except ValueError:
    pass

try:
    replace_credential(
        "test",
        credential_path="~/.dasstore/test2",
        key="test_key2",
        secret="test_secret2",
    )
except FileNotFoundError:
    pass

cred = get_credential("test", credential_path="~/.dasstore/test")
assert cred["aws_access_key_id"] == "test_key2"
assert cred["aws_secret_access_key"] == "test_secret2"

## remove credential
remove_credential("test", credential_path="~/.dasstore/test")

## remove for the second time
remove_credential("test", credential_path="~/.dasstore/test")

try:
    remove_credential("test", credential_path="~/.dasstore/test2")
except FileNotFoundError:
    pass

## test non-exist directory
add_credential(
    "test", "~/.dasstore_tmp/credentials", key="testk_ey", secret="test_secret"
)
os.system(f"rm -rf {os.path.expanduser('~/')}.dasstore_tmp")

## test non-exist endpoint
try:
    get_credential("test", credential_path="~/.dasstore/test")
except KeyError:
    pass

## test non-exist file
try:
    get_credential("test", "~/test")
except FileNotFoundError:
    pass

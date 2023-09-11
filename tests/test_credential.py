from dasstore.utils.credential import add_credential, get_credential, replace_credential, remove_credential

## test existing credential
add_credential("test", key="test_key", secret="test_secret")
cred = get_credential("test")
assert cred['aws_access_key_id'] == "test_key"
assert cred['aws_secret_access_key'] == "test_secret"

replace_credential("test", key="test_key2", secret="test_secret2")
cred = get_credential("test")
assert cred['aws_access_key_id'] == "test_key2"
assert cred['aws_secret_access_key'] == "test_secret2"

remove_credential("test")

## test new credential file
add_credential("test", credential_path="~/.dasstore/test", key="test_key", secret="test_secret")
cred = get_credential("test", credential_path="~/.dasstore/test")
assert cred['aws_access_key_id'] == "test_key"
assert cred['aws_secret_access_key'] == "test_secret"

replace_credential("test", credential_path="~/.dasstore/test", key="test_key2", secret="test_secret2")
cred = get_credential("test", credential_path="~/.dasstore/test")
assert cred['aws_access_key_id'] == "test_key2"
assert cred['aws_secret_access_key'] == "test_secret2"

remove_credential("test", credential_path="~/.dasstore/test")
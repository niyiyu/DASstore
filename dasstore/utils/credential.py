import os


def get_credential(endpoint, credential_path="~/.dasstore/credentials"):
    endpoint = endpoint.rstrip("/")
    if "~" in credential_path:
        credential_path = credential_path.replace("~", os.path.expanduser("~"))

    try:
        assert os.path.exists(credential_path)
    except AssertionError:
        raise FileNotFoundError(f"Check credential file:\t {credential_path}")

    creds = _parse_credential(credential_path)

    if endpoint in creds:
        return creds[endpoint]
    else:
        raise KeyError(f"No credential found for endpoint [{endpoint}]")


def add_credential(
    endpoint, credential_path="~/.dasstore/credentials", key=None, secret=None
):
    endpoint = endpoint.rstrip("/")
    if "~" in credential_path:
        credential_path = credential_path.replace("~", os.path.expanduser("~"))

    if not os.path.exists(credential_path):
        os.makedirs(os.path.dirname(credential_path), exist_ok=True)

        if key is None and secret is None:
            print(f"Enter credential for [{endpoint}]:")
            key = input("Input access key ID:        \t")
            secret = input("Input secret access key: \t")

        with open(credential_path, "w") as f:
            f.write(f"[{endpoint}]\n")
            f.write(f"aws_access_key_id = {key}\n")
            f.write(f"aws_secret_access_key = {secret}\n")

        print(f"Credential added to [{credential_path}]")
    else:
        creds = _parse_credential(credential_path)

        if endpoint not in creds:
            if key is None and secret is None:
                print(f"Enter credential for [{endpoint}]:")
                key = input("Input access key ID:        \t")
                secret = input("Input secret access key: \t")

            with open(credential_path, "a") as f:
                f.write(f"[{endpoint}]\n")
                f.write(f"aws_access_key_id = {key}\n")
                f.write(f"aws_secret_access_key = {secret}\n")

            print(f"Credential added to [{credential_path}]")
        else:
            print(f"Credential for [{endpoint}] already exist at [{credential_path}]")


def replace_credential(
    endpoint, credential_path="~/.dasstore/credentials", key=None, secret=None
):
    endpoint = endpoint.rstrip("/")
    if "~" in credential_path:
        credential_path = credential_path.replace("~", os.path.expanduser("~"))

    if os.path.exists(credential_path):
        creds = _parse_credential(credential_path)
        if endpoint in creds:
            if key is None and secret is None:
                print(f"Replacing credential for [{endpoint}]:")
                key = input("Input access key ID:        \t")
                secret = input("Input secret access key: \t")

            creds[endpoint]["aws_access_key_id"] = key
            creds[endpoint]["aws_secret_access_key"] = secret
            _save_credential(creds, credential_path)
        else:
            raise ValueError(f"Credential for [{endpoint}] does not exist.")
    else:
        raise FileNotFoundError("Credential does not exist.")


def remove_credential(endpoint, credential_path="~/.dasstore/credentials"):
    endpoint = endpoint.rstrip("/")
    if "~" in credential_path:
        credential_path = credential_path.replace("~", os.path.expanduser("~"))

    if os.path.exists(credential_path):
        creds = _parse_credential(credential_path)
    if os.path.exists(credential_path):
        creds = _parse_credential(credential_path)
        if endpoint in creds:
            creds.pop(endpoint, None)
            _save_credential(creds, credential_path)
        else:
            print(f"Endpoint [{endpoint}] not found. Skipping")
    else:
        raise FileNotFoundError("Credential does not exist.")


def _save_credential(creds, credential_path):
    with open(credential_path, "w") as f:
        for endpoint in creds:
            f.write(f"[{endpoint}]\n")
            f.write(f"aws_access_key_id = {creds[endpoint]['aws_access_key_id']}\n")
            f.write(
                f"aws_secret_access_key = {creds[endpoint]['aws_secret_access_key']}\n\n"
            )

    _chmod_credential(credential_path)


def _parse_credential(credential_path):
    # parse the credential file if exist
    with open(credential_path, "r") as f:
        lines = f.readlines()
    lines = [i.split("\n")[0] for i in lines]

    # parse credentials by their endpoint url
    creds = {}
    for idl, l in enumerate(lines):
        if (l == "") or ("=" in l):
            continue

        if l[0] == "[" and l[-1] == "]":
            creds[l[1:-1]] = dict([i.split(" = ") for i in lines[idl + 1 : idl + 3]])

    return creds


def _chmod_credential(credential_path):
    os.chmod(credential_path, 0o600)

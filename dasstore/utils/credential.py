import os


def get_credential(endpoint, credential_path="~/.dasstore/credentials"):
    if "~" in credential_path:
        home = os.environ["HOME"]
        credential_path = credential_path.replace("~", home)

    try:
        assert os.path.exists(credential_path)
    except AssertionError:
        raise FileNotFoundError(f"Check credential file:\t {credential_path}")

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

    if endpoint in creds:
        return creds[endpoint]
    else:
        raise KeyError(f"No credential found for endpoint {endpoint}")

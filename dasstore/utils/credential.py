import os


def get_credential(credential_path="~/.dasstore/credentials"):
    if "~" in credential_path:
        home = os.environ["HOME"]
        credential_path = credential_path.replace("~", home)

    try:
        assert os.path.exists(credential_path)
    except:
        print(f"Check credential file:\t {credential_path}")

    # parse the credential file if exist
    with open(credential_path, "r") as f:
        lines = f.readlines()
    lines = [i.split("\n")[0] for i in lines]
    lines = [i.split(" = ") for i in lines]

    return dict(lines)

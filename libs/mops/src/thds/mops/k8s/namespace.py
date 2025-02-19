from getpass import getuser


def parse_namespace(input_str: str) -> str:
    # lowercase and replace all non-alphanumeric characters with dashes
    return "".join(c if c.isalnum() else "-" for c in input_str.lower())


def user_namespace() -> str:
    try:
        return getuser()
    except OSError:
        return "CICD-Runner"


def main() -> None:
    print(user_namespace())

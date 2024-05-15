import socket


def friendly() -> str:
    hn = socket.gethostname()
    if hn.endswith(".local"):
        hn = hn[: -len(".local")]
    if hn.startswith("MBP-"):
        hn = hn[len("MBP-") :]
    return hn

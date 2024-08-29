# thanks to https://gist.github.com/minism/1590432
# and https://gist.github.com/fnky/458719343aabd01cfb17a3a4f7296797


class fg:
    BLACK = "\033[30m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"

    ERROR_RED = "\033[38;5;196m"

    RESET = "\033[39m"  # a.k.a. DEFAULT


class bg:
    BLACK = "\033[40m"
    RED = "\033[41m"
    GREEN = "\033[42m"
    YELLOW = "\033[43m"
    BLUE = "\033[44m"
    MAGENTA = "\033[45m"
    CYAN = "\033[46m"
    WHITE = "\033[47m"

    ERROR_RED = "\033[48;5;196m"

    RESET = "\033[49m"  # a.k.a. DEFAULT


class style:
    BRIGHT = "\033[1m"
    DIM = "\033[2m"
    NORMAL = "\033[22m"

    BLINK = "\033[5m"
    NO_BLINK = "\033[25m"

    ITALIC = "\033[3m"
    NO_ITALIC = "\033[23m"

    RESET_ALL = "\033[0m"

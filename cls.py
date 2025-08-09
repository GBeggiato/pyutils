
import os


CMD = "cls" if os.name == "nt" else "clear"


def clear_screen() -> int:
    return os.system(CMD)

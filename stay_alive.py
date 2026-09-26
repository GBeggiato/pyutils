import ctypes
import sys
import time
from typing import Literal


_PHONY = 100
MOUSEEVENTF_LEFTDOWN  = 0x0002
MOUSEEVENTF_LEFTUP    = 0x0004
MOUSEEVENTF_LEFTCLICK = MOUSEEVENTF_LEFTDOWN + MOUSEEVENTF_LEFTUP


def _click(ev: Literal[6], dwData: int=0) -> None:
    if not hasattr(ctypes, "windll"):
        raise OSError("only windows !")
    width  = ctypes.windll.user32.GetSystemMetrics(0)
    height = ctypes.windll.user32.GetSystemMetrics(1)
    convertedX = ctypes.c_long(65536 * _PHONY // width + 1)
    convertedY = ctypes.c_long(65536 * _PHONY // height + 1)
    ctypes.windll.user32.mouse_event(ev, convertedX, convertedY, dwData, 0)
    if ctypes.windll.kernel32.GetLastError() != 0:
       if not hasattr(ctypes, "WinError"):
           raise OSError("only windows !")
       raise ctypes.WinError()


def main() -> None:
    if sys.platform != "win32":
        raise OSError("only windows !")
    while 1:
        _click()
        time.sleep(20)


if __name__ == "__main__":
    main()


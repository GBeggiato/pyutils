import ctypes
import sys
import time
from typing import Literal


_PHONY = 100
MOUSEEVENTF_LEFTDOWN  = 0x0002
MOUSEEVENTF_LEFTUP    = 0x0004
MOUSEEVENTF_LEFTCLICK = MOUSEEVENTF_LEFTDOWN + MOUSEEVENTF_LEFTUP


def _sendMouseEvent(ev: Literal[6], x: int, y: int, dwData: int=0):
    width  = ctypes.windll.user32.GetSystemMetrics(0)
    height = ctypes.windll.user32.GetSystemMetrics(1)
    convertedX = ctypes.c_long(65536 * x // width + 1)
    convertedY = ctypes.c_long(65536 * y // height + 1)
    ctypes.windll.user32.mouse_event(ev, convertedX, convertedY, dwData, 0)
    if ctypes.windll.kernel32.GetLastError() != 0:
       raise ctypes.WinError()

def _click():
    _sendMouseEvent(ev=MOUSEEVENTF_LEFTCLICK, x=_PHONY, y=_PHONY)


def main():
    if sys.platform != "win32":
        raise OSError("only windows !")
    while 1:
        _click()
        time.sleep(20)


if __name__ == "__main__":
    main()


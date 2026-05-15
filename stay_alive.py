import ctypes
import sys
import time
from typing import Literal


_PHONY = 100
MOUSEEVENTF_LEFTDOWN  = 0x0002
MOUSEEVENTF_LEFTUP    = 0x0004
MOUSEEVENTF_LEFTCLICK = MOUSEEVENTF_LEFTDOWN + MOUSEEVENTF_LEFTUP


def _sendMouseEvent(ev: Literal[6], x: int, y: int, dwData: int=0):
    assert x != None and y != None, 'x and y cannot be set to None'
    width  = ctypes.windll.user32.GetSystemMetrics(0)
    height = ctypes.windll.user32.GetSystemMetrics(1)
    convertedX = 65536 * x // width + 1
    convertedY = 65536 * y // height + 1
    ctypes.windll.user32.mouse_event(ev, ctypes.c_long(convertedX), ctypes.c_long(convertedY), dwData, 0)
    if ctypes.windll.kernel32.GetLastError() != 0:
       raise ctypes.WinError()

        
def _left_click():
    try:
        _sendMouseEvent(MOUSEEVENTF_LEFTCLICK, _PHONY, _PHONY)
    except (PermissionError, OSError):
        pass


def main():
    if sys.platform != "win32":
        raise OSError("only windows !")
    while 1:
        _left_click()
        time.sleep(20)


if __name__ == "__main__":
    main()


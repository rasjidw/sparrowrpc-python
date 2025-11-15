from __future__ import annotations

import signal
import socket
import sys
from typing import Optional, Iterable, Callable


class SignalHandlerInstaller:
    def __init__(self, signals: Optional[Iterable[signal.Signals]] = ()):
        if not signals:
            if sys.platform == 'win32':
                signals = [signal.SIGINT, signal.SIGTERM, signal.SIGBREAK]
            else:
                signals = [signal.SIGINT, signal.SIGTERM]
        self.signals = signals
        self.saved_handlers = dict()  # signal -> handler

    def install(self, handler: Callable):
        for sig in self.signals:
            self.saved_handlers[sig] = signal.getsignal(sig)
            signal.signal(sig, handler)
    
    def remove(self):
        for sig in self.signals:
            signal.signal(sig, self.saved_handlers[sig])



def detect_unix_socket_in_use(socket_path):
    detecting_socket = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    try:
        detecting_socket.connect(socket_path)
        detecting_socket.close()
        return True
    except Exception:
        return False

from __future__ import annotations

import signal
import sys
from typing import Optional, Iterable


class SignalHandlerInstaller:
    def __init__(self, signals: Optional[Iterable[signal.Signals]] = ()):
        if not signals:
            if sys.platform == 'win32':
                signals = [signal.SIGINT, signals.SIGBREAK]
            else:
                signals = [signal.SIGINT, signal.SIGTERM]
        self.signals = signals
        self.saved_handlers = dict()  # signal -> handler

    def install(self, handler: callable):
        for sig in self.signals:
            self.saved_handlers[sig] = signal.getsignal(sig)
            signal.signal(sig, handler)
    
    def remove(self):
        for sig in self.signals:
            signal.signal(sig, self.saved_handlers[sig])

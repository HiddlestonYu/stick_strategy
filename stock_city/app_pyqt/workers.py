from PyQt5 import QtCore


class FunctionWorker(QtCore.QThread):
    """以背景執行緒執行任意函式，避免 UI 阻塞。"""

    succeeded = QtCore.pyqtSignal(object)
    failed = QtCore.pyqtSignal(str)

    def __init__(self, func, *args, **kwargs):
        super().__init__()
        self._func = func
        self._args = args
        self._kwargs = kwargs

    def run(self):
        try:
            result = self._func(*self._args, **self._kwargs)
            self.succeeded.emit(result)
        except Exception as exc:
            self.failed.emit(str(exc))

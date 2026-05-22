import sys

from PyQt5 import QtWidgets

from stock_city.app_pyqt.main_window import MainWindow


def main():
    app = QtWidgets.QApplication(sys.argv)
    app.setStyle("Fusion")
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()

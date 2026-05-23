import sys
from PyQt5 import QtWidgets
from stock_city.app_pyqt.main_window import MainWindow

DARK_STYLE = """
QWidget { background-color: #16213e; color: #e0e0e0; font-size: 13px; }
QGroupBox { border: 1px solid #2a3560; border-radius: 5px; margin-top: 8px; font-weight: bold; color: #7ec8e3; }
QGroupBox::title { subcontrol-origin: margin; left: 8px; padding: 0 4px; }
QPushButton { background-color: #1e2d55; border: 1px solid #2d4080; border-radius: 4px; padding: 5px 12px; color: #d0d8f0; min-height: 26px; }
QPushButton:hover { background-color: #2d3f70; }
QPushButton:disabled { color: #4a4a5a; background-color: #141e35; border-color: #202840; }
QPushButton#btn_buy { background-color: #8b1a1a; border-color: #cc2222; color: #fff; font-weight: bold; font-size: 14px; }
QPushButton#btn_buy:hover { background-color: #bb2222; }
QPushButton#btn_buy:disabled { background-color: #3a1a1a; border-color: #441a1a; color: #555; }
QPushButton#btn_sell { background-color: #1a5a2a; border-color: #22aa44; color: #fff; font-weight: bold; font-size: 14px; }
QPushButton#btn_sell:hover { background-color: #22884a; }
QPushButton#btn_sell:disabled { background-color: #1a3a22; border-color: #1a4422; color: #555; }
QPushButton#btn_close_pos { background-color: #5a3a00; border-color: #cc8800; color: #ffe066; font-weight: bold; }
QPushButton#btn_close_pos:hover { background-color: #885500; }
QPushButton#btn_close_pos:disabled { background-color: #2a2000; color: #555; }
QComboBox, QSpinBox, QDoubleSpinBox, QLineEdit { background-color: #1a2545; border: 1px solid #2d4080; border-radius: 4px; padding: 3px 6px; color: #d0d8f0; }
QComboBox QAbstractItemView { background-color: #1a2545; color: #d0d8f0; selection-background-color: #2d3f70; }
QTabWidget::pane { border: 1px solid #2a3560; background-color: #16213e; }
QTabBar::tab { background-color: #141e35; color: #8090b0; padding: 6px 14px; margin-right: 2px; border-top-left-radius: 4px; border-top-right-radius: 4px; }
QTabBar::tab:selected { background-color: #1e2d55; color: #d0d8f0; font-weight: bold; border-top: 2px solid #4a6aaa; }
QTableWidget { background-color: #16213e; alternate-background-color: #1a2848; gridline-color: #1e2d55; color: #d0d8f0; border: none; }
QHeaderView::section { background-color: #1a2545; color: #7ec8e3; border: none; padding: 4px 8px; font-weight: bold; }
QScrollBar:vertical { background-color: #141e35; width: 8px; border-radius: 4px; }
QScrollBar::handle:vertical { background-color: #2d4080; border-radius: 4px; min-height: 20px; }
QScrollBar::add-line:vertical, QScrollBar::sub-line:vertical { height: 0; }
QCheckBox { color: #d0d8f0; spacing: 6px; }
QCheckBox::indicator { width: 14px; height: 14px; border: 1px solid #2d4080; border-radius: 3px; background-color: #1a2545; }
QCheckBox::indicator:checked { background-color: #2d5aaa; border-color: #4a7aee; }
QRadioButton { color: #d0d8f0; spacing: 6px; }
QRadioButton::indicator { width: 13px; height: 13px; border: 1px solid #2d4080; border-radius: 7px; background-color: #1a2545; }
QRadioButton::indicator:checked { background-color: #2d5aaa; border-color: #4a7aee; }
QSplitter::handle { background-color: #1e2d55; }
QStatusBar { background-color: #0f1629; color: #7090b0; border-top: 1px solid #1e2d55; }
"""


def main():
    app = QtWidgets.QApplication(sys.argv)
    app.setStyle("Fusion")
    app.setStyleSheet(DARK_STYLE)
    window = MainWindow()
    window.show()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()

import sys
import urllib3

from PyQt6.QtWidgets import QApplication
from PyQt6.QtGui import QFont

from app.main_window import MainWindow

# Suppress insecure HTTPS warnings for self-signed certs
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def main():
    app = QApplication(sys.argv)
    app.setApplicationName("Kibana Alert Simulator")
    app.setOrganizationName("KibanaAlertSim")

    # Apply a clean default style
    app.setStyleSheet("""
        QMainWindow {
            background-color: #f5f6fa;
        }
        QGroupBox {
            font-weight: bold;
            border: 1px solid #dcdde1;
            border-radius: 6px;
            margin-top: 10px;
            padding-top: 15px;
        }
        QGroupBox::title {
            subcontrol-origin: margin;
            left: 10px;
            padding: 0 5px;
        }
        QListWidget {
            border: 1px solid #dcdde1;
            border-radius: 4px;
            background-color: white;
        }
        QListWidget::item {
            padding: 4px;
        }
        QListWidget::item:selected {
            background-color: #3498db;
            color: white;
        }
        QTreeWidget {
            border: 1px solid #dcdde1;
            border-radius: 4px;
            background-color: white;
        }
        QLineEdit, QComboBox, QSpinBox {
            border: 1px solid #dcdde1;
            border-radius: 4px;
            padding: 5px;
            background-color: white;
        }
        QLineEdit:focus, QComboBox:focus {
            border-color: #3498db;
        }
        QTextEdit {
            border: 1px solid #dcdde1;
            border-radius: 4px;
            background-color: #fafafa;
        }
        QPushButton {
            border: 1px solid #dcdde1;
            border-radius: 4px;
            padding: 6px 14px;
            background-color: white;
        }
        QPushButton:hover {
            background-color: #ecf0f1;
        }
        QPushButton:pressed {
            background-color: #dcdde1;
        }
        QStatusBar {
            background-color: #ecf0f1;
        }
        QTabWidget::pane {
            border: 1px solid #dcdde1;
            border-radius: 4px;
        }
        QTabBar::tab {
            padding: 8px 16px;
            margin-right: 2px;
            border: 1px solid #dcdde1;
            border-bottom: none;
            border-top-left-radius: 4px;
            border-top-right-radius: 4px;
            background-color: #ecf0f1;
        }
        QTabBar::tab:selected {
            background-color: white;
        }
    """)

    window = MainWindow()
    window.show()

    # Auto-show connection dialog on start
    window._show_connect_dialog()

    sys.exit(app.exec())


if __name__ == "__main__":
    main()

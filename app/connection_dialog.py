from PyQt6.QtWidgets import (
    QDialog, QVBoxLayout, QHBoxLayout, QTabWidget, QWidget,
    QLabel, QLineEdit, QPushButton, QMessageBox, QFormLayout,
    QGroupBox,
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal

from .models import ConnectionConfig
from .client import ElasticKibanaClient


class ConnectionTestThread(QThread):
    """Tests connection in a background thread."""
    finished = pyqtSignal(str)  # success message
    error = pyqtSignal(str)     # error message

    def __init__(self, client: ElasticKibanaClient, config: ConnectionConfig):
        super().__init__()
        self.client = client
        self.config = config

    def run(self):
        try:
            info = self.client.connect(self.config)
            self.finished.emit(info)
        except Exception as e:
            self.error.emit(str(e))


class ConnectionDialog(QDialog):
    """Dialog for connecting to Elasticsearch/Kibana."""

    def __init__(self, client: ElasticKibanaClient, parent=None):
        super().__init__(parent)
        self.client = client
        self.setWindowTitle("Connect to Elasticsearch / Kibana")
        self.setMinimumWidth(500)
        self.setMinimumHeight(350)
        self._test_thread = None
        self._setup_ui()

    def _setup_ui(self):
        layout = QVBoxLayout(self)

        # Tab widget for auth methods
        self.tabs = QTabWidget()

        # Tab 1: Cloud ID + API Key
        cloud_tab = QWidget()
        cloud_layout = QFormLayout(cloud_tab)
        self.cloud_id_input = QLineEdit()
        self.cloud_id_input.setPlaceholderText("deployment:base64encoded...")
        self.cloud_api_key_input = QLineEdit()
        self.cloud_api_key_input.setPlaceholderText("API key or id:key format")
        self.cloud_api_key_input.setEchoMode(QLineEdit.EchoMode.Password)
        cloud_layout.addRow("Cloud ID:", self.cloud_id_input)
        cloud_layout.addRow("API Key:", self.cloud_api_key_input)
        self.tabs.addTab(cloud_tab, "Cloud ID + API Key")

        # Tab 2: URL + Username/Password
        basic_tab = QWidget()
        basic_layout = QFormLayout(basic_tab)
        self.basic_es_url = QLineEdit()
        self.basic_es_url.setPlaceholderText("https://localhost:9200")
        self.basic_kibana_url = QLineEdit()
        self.basic_kibana_url.setPlaceholderText("https://localhost:5601 (optional, auto-derived)")
        self.basic_username = QLineEdit()
        self.basic_username.setPlaceholderText("elastic")
        self.basic_password = QLineEdit()
        self.basic_password.setEchoMode(QLineEdit.EchoMode.Password)
        basic_layout.addRow("ES URL:", self.basic_es_url)
        basic_layout.addRow("Kibana URL:", self.basic_kibana_url)
        basic_layout.addRow("Username:", self.basic_username)
        basic_layout.addRow("Password:", self.basic_password)
        self.tabs.addTab(basic_tab, "URL + Basic Auth")

        # Tab 3: URL + API Key
        apikey_tab = QWidget()
        apikey_layout = QFormLayout(apikey_tab)
        self.apikey_es_url = QLineEdit()
        self.apikey_es_url.setPlaceholderText("https://localhost:9200")
        self.apikey_kibana_url = QLineEdit()
        self.apikey_kibana_url.setPlaceholderText("https://localhost:5601 (optional, auto-derived)")
        self.apikey_api_key = QLineEdit()
        self.apikey_api_key.setPlaceholderText("API key or id:key format")
        self.apikey_api_key.setEchoMode(QLineEdit.EchoMode.Password)
        apikey_layout.addRow("ES URL:", self.apikey_es_url)
        apikey_layout.addRow("Kibana URL:", self.apikey_kibana_url)
        apikey_layout.addRow("API Key:", self.apikey_api_key)
        self.tabs.addTab(apikey_tab, "URL + API Key")

        layout.addWidget(self.tabs)

        # Status
        self.status_label = QLabel("")
        self.status_label.setWordWrap(True)
        layout.addWidget(self.status_label)

        # Buttons
        btn_layout = QHBoxLayout()
        self.test_btn = QPushButton("Test Connection")
        self.test_btn.clicked.connect(self._test_connection)
        self.connect_btn = QPushButton("Connect")
        self.connect_btn.clicked.connect(self._connect)
        self.connect_btn.setDefault(True)
        self.cancel_btn = QPushButton("Cancel")
        self.cancel_btn.clicked.connect(self.reject)

        btn_layout.addWidget(self.test_btn)
        btn_layout.addStretch()
        btn_layout.addWidget(self.cancel_btn)
        btn_layout.addWidget(self.connect_btn)
        layout.addLayout(btn_layout)

    def _get_config(self) -> ConnectionConfig:
        tab = self.tabs.currentIndex()

        if tab == 0:
            return ConnectionConfig(
                method="cloud_id",
                cloud_id=self.cloud_id_input.text().strip(),
                api_key=self.cloud_api_key_input.text().strip(),
            )
        elif tab == 1:
            return ConnectionConfig(
                method="url_basic",
                url=self.basic_es_url.text().strip(),
                username=self.basic_username.text().strip(),
                password=self.basic_password.text().strip(),
                kibana_url_override=self.basic_kibana_url.text().strip() or None,
            )
        else:
            return ConnectionConfig(
                method="url_apikey",
                url=self.apikey_es_url.text().strip(),
                api_key=self.apikey_api_key.text().strip(),
                kibana_url_override=self.apikey_kibana_url.text().strip() or None,
            )

    def _validate(self) -> bool:
        tab = self.tabs.currentIndex()
        if tab == 0:
            if not self.cloud_id_input.text().strip():
                self.status_label.setText("Cloud ID is required.")
                return False
            if not self.cloud_api_key_input.text().strip():
                self.status_label.setText("API Key is required.")
                return False
        elif tab == 1:
            if not self.basic_es_url.text().strip():
                self.status_label.setText("Elasticsearch URL is required.")
                return False
            if not self.basic_username.text().strip():
                self.status_label.setText("Username is required.")
                return False
        else:
            if not self.apikey_es_url.text().strip():
                self.status_label.setText("Elasticsearch URL is required.")
                return False
            if not self.apikey_api_key.text().strip():
                self.status_label.setText("API Key is required.")
                return False
        return True

    def _set_busy(self, busy: bool):
        self.test_btn.setEnabled(not busy)
        self.connect_btn.setEnabled(not busy)
        if busy:
            self.status_label.setText("Connecting...")
            self.status_label.setStyleSheet("color: #888;")

    def _test_connection(self):
        if not self._validate():
            return
        config = self._get_config()
        self._set_busy(True)

        self._test_thread = ConnectionTestThread(self.client, config)
        self._test_thread.finished.connect(lambda msg: self._on_test_success(msg))
        self._test_thread.error.connect(lambda msg: self._on_test_error(msg))
        self._test_thread.start()

    def _on_test_success(self, msg: str):
        self._set_busy(False)
        self.status_label.setText(f"Connected: {msg}")
        self.status_label.setStyleSheet("color: green;")
        self.client.disconnect()

    def _on_test_error(self, msg: str):
        self._set_busy(False)
        self.status_label.setText(f"Error: {msg}")
        self.status_label.setStyleSheet("color: red;")

    def _connect(self):
        if not self._validate():
            return
        config = self._get_config()
        self._set_busy(True)

        self._test_thread = ConnectionTestThread(self.client, config)
        self._test_thread.finished.connect(lambda msg: self._on_connect_success(msg))
        self._test_thread.error.connect(lambda msg: self._on_test_error(msg))
        self._test_thread.start()

    def _on_connect_success(self, msg: str):
        self._set_busy(False)
        self.accept()

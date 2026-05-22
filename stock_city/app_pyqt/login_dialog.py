import os
import pathlib

try:
    import tomllib  # Python 3.11+
except ImportError:
    tomllib = None

from PyQt5 import QtCore, QtWidgets

from stock_city.app_pyqt import services
from stock_city.app_pyqt.workers import FunctionWorker

# 專案根目錄（services.py 的上兩層）
_PROJECT_ROOT = pathlib.Path(__file__).resolve().parent.parent.parent
_SECRETS_PATH = _PROJECT_ROOT / ".streamlit" / "secrets.toml"
_DEFAULT_CERT_PATH = _PROJECT_ROOT / "Sinopac.pfx"


def _load_secrets() -> dict:
    """讀取 .streamlit/secrets.toml，回傳 dict（失敗時回空 dict）。"""
    if tomllib is None or not _SECRETS_PATH.exists():
        return {}
    try:
        with open(_SECRETS_PATH, "rb") as f:
            return tomllib.load(f)
    except Exception:
        return {}


class LoginDialog(QtWidgets.QDialog):
    def __init__(self, parent=None):
        super().__init__(parent)
        self.api = None
        self._worker = None
        self.setWindowTitle("Shioaji 登入")
        self.resize(500, 320)
        self._secrets = _load_secrets()
        self._build_ui()
        self._autofill()

    # ─── 自動帶入憑證 ────────────────────────────────────────────
    def _autofill(self):
        """從 secrets.toml → 環境變數，依序嘗試帶入。"""
        api_key = (
            self._secrets.get("SHIOAJI_API_KEY")
            or self._secrets.get("shioaji", {}).get("api_key")
            or os.getenv("SHIOAJI_API_KEY", "")
        )
        secret_key = (
            self._secrets.get("SHIOAJI_SECRET_KEY")
            or self._secrets.get("shioaji", {}).get("secret_key")
            or os.getenv("SHIOAJI_SECRET_KEY", "")
        )

        hints = []
        if api_key:
            self.api_key_edit.setText(api_key)
        if secret_key:
            self.secret_key_edit.setText(secret_key)
        if api_key and secret_key:
            hints.append("API Key / Secret Key 已自動帶入")
            # 自動選擇 API Key 模式
            self.mode_radio_apikey.setChecked(True)

        # 若找到 Sinopac.pfx 則自動帶入憑證路徑
        if _DEFAULT_CERT_PATH.exists():
            self.cert_path_edit.setText(str(_DEFAULT_CERT_PATH))
            hints.append(f"已偵測到 {_DEFAULT_CERT_PATH.name}")

        if hints:
            self.status_label.setText("💡 " + "　" + "、".join(hints) + "，直接按「登入」即可。")
        else:
            self.status_label.setText("請輸入 API Key + Secret Key，或使用憑證檔案登入。")

    # ─── 建立 UI ─────────────────────────────────────────────────
    def _build_ui(self):
        layout = QtWidgets.QVBoxLayout(self)
        layout.setSpacing(8)

        # 登入模式選擇
        mode_group = QtWidgets.QGroupBox("登入方式")
        mode_layout = QtWidgets.QHBoxLayout(mode_group)
        self.mode_radio_apikey = QtWidgets.QRadioButton("API Key / Secret Key")
        self.mode_radio_cert = QtWidgets.QRadioButton("憑證檔案 (.pfx)")
        self.mode_radio_apikey.setChecked(True)
        mode_layout.addWidget(self.mode_radio_apikey)
        mode_layout.addWidget(self.mode_radio_cert)
        layout.addWidget(mode_group)

        # API Key 面板
        self.apikey_panel = QtWidgets.QWidget()
        apikey_form = QtWidgets.QFormLayout(self.apikey_panel)
        apikey_form.setContentsMargins(0, 0, 0, 0)
        self.api_key_edit = QtWidgets.QLineEdit()
        self.api_key_edit.setEchoMode(QtWidgets.QLineEdit.Password)
        self.api_key_edit.setPlaceholderText("永豐 API Key")
        self.secret_key_edit = QtWidgets.QLineEdit()
        self.secret_key_edit.setEchoMode(QtWidgets.QLineEdit.Password)
        self.secret_key_edit.setPlaceholderText("永豐 Secret Key")
        apikey_form.addRow("API Key", self.api_key_edit)
        apikey_form.addRow("Secret Key", self.secret_key_edit)
        layout.addWidget(self.apikey_panel)

        # 憑證面板
        self.cert_panel = QtWidgets.QWidget()
        cert_form = QtWidgets.QFormLayout(self.cert_panel)
        cert_form.setContentsMargins(0, 0, 0, 0)
        self.person_id_edit = QtWidgets.QLineEdit()
        self.person_id_edit.setPlaceholderText("身分證字號")
        self.cert_path_edit = QtWidgets.QLineEdit()
        self.cert_path_edit.setPlaceholderText("Sinopac.pfx 路徑")
        browse_button = QtWidgets.QPushButton("瀏覽…")
        browse_button.setFixedWidth(64)
        browse_button.clicked.connect(self._browse_cert)
        cert_path_row = QtWidgets.QHBoxLayout()
        cert_path_row.setContentsMargins(0, 0, 0, 0)
        cert_path_row.addWidget(self.cert_path_edit)
        cert_path_row.addWidget(browse_button)
        cert_path_widget = QtWidgets.QWidget()
        cert_path_widget.setLayout(cert_path_row)
        self.cert_password_edit = QtWidgets.QLineEdit()
        self.cert_password_edit.setEchoMode(QtWidgets.QLineEdit.Password)
        self.cert_password_edit.setPlaceholderText("憑證密碼")
        cert_form.addRow("身分證字號", self.person_id_edit)
        cert_form.addRow("憑證檔案", cert_path_widget)
        cert_form.addRow("憑證密碼", self.cert_password_edit)
        self.cert_panel.setVisible(False)
        layout.addWidget(self.cert_panel)

        # 切換面板
        self.mode_radio_apikey.toggled.connect(self._on_mode_changed)

        # 合約選項
        self.fetch_contract_checkbox = QtWidgets.QCheckBox("登入時下載合約資料（較慢但功能完整）")
        self.fetch_contract_checkbox.setChecked(True)
        layout.addWidget(self.fetch_contract_checkbox)

        # 狀態文字
        self.status_label = QtWidgets.QLabel("")
        self.status_label.setWordWrap(True)
        layout.addWidget(self.status_label)

        # 按鈕列
        button_row = QtWidgets.QHBoxLayout()
        button_row.addStretch(1)
        self.login_button = QtWidgets.QPushButton("登入")
        self.login_button.setDefault(True)
        self.login_button.setMinimumWidth(80)
        self.cancel_button = QtWidgets.QPushButton("取消")
        self.login_button.clicked.connect(self._login)
        self.cancel_button.clicked.connect(self.reject)
        button_row.addWidget(self.login_button)
        button_row.addWidget(self.cancel_button)
        layout.addLayout(button_row)

    def _on_mode_changed(self, apikey_checked):
        self.apikey_panel.setVisible(apikey_checked)
        self.cert_panel.setVisible(not apikey_checked)

    def _browse_cert(self):
        start_dir = str(_PROJECT_ROOT)
        file_path, _ = QtWidgets.QFileDialog.getOpenFileName(
            self, "選擇憑證檔案", start_dir, "Certificate Files (*.pfx *.p12)"
        )
        if file_path:
            self.cert_path_edit.setText(file_path)

    # ─── 登入邏輯 ────────────────────────────────────────────────
    def _login(self):
        fetch_contract = self.fetch_contract_checkbox.isChecked()

        if self.mode_radio_apikey.isChecked():
            # API Key 模式
            api_key = self.api_key_edit.text().strip() or None
            secret_key = self.secret_key_edit.text().strip() or None
            if not api_key:
                self.status_label.setText("請輸入 API Key。")
                return
            if not secret_key:
                self.status_label.setText("請輸入 Secret Key。")
                return
            cert_path = None
            cert_password = None
        else:
            # 憑證模式
            api_key = self.person_id_edit.text().strip() or None
            cert_path = self.cert_path_edit.text().strip() or None
            cert_password = self.cert_password_edit.text().strip() or None
            secret_key = None
            if not api_key:
                self.status_label.setText("請輸入身分證字號。")
                return
            if not cert_path:
                self.status_label.setText("請選擇憑證檔案（.pfx）。")
                return
            if not cert_password:
                self.status_label.setText("請輸入憑證密碼。")
                return

        self.status_label.setText("登入中，請稍候…")
        self.login_button.setEnabled(False)
        self._worker = FunctionWorker(
            services.login_shioaji,
            api_key=api_key,
            secret_key=secret_key,
            cert_path=cert_path,
            cert_password=cert_password,
            fetch_contract=fetch_contract,
        )
        self._worker.succeeded.connect(self._on_login_done)
        self._worker.failed.connect(self._on_login_error)
        self._worker.start()

    @QtCore.pyqtSlot(object)
    def _on_login_done(self, result):
        api, error = result
        self.login_button.setEnabled(True)
        if error:
            self.status_label.setText(f"登入失敗：{error}")
            return
        self.api = api
        self.accept()

    @QtCore.pyqtSlot(str)
    def _on_login_error(self, message):
        self.login_button.setEnabled(True)
        self.status_label.setText(f"登入失敗：{message}")

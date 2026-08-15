import os

import pandas as pd
from PyQt5 import QtCore, QtGui, QtWidgets

from stock_city.app_pyqt.chart_widget import StrategyChartWidget
from stock_city.app_pyqt.login_dialog import LoginDialog
from stock_city.app_pyqt import services
from stock_city.app_pyqt.workers import FunctionWorker


class TickSignalBridge(QtCore.QObject):
    """Shioaji tick callback -> Qt signal (执行绪安全桥接)。"""
    tick_received = QtCore.pyqtSignal(dict)


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.api = None
        self.current_df = None
        self.current_daily_df = None
        self.current_5m_df = None
        self.current_top_chart_df = None
        self.current_bottom_chart_df = None
        self.top_chart_interval = "1d"
        self.bottom_chart_interval = "5m"
        self.current_trades = []
        self.current_backtest_trades = []
        self.current_backtest_df = None
        self.current_backtest_result = None
        self.current_optimization_results = []
        self._applied_optimization_risk_params = None
        self._workers = []
        self._active_task_count = 0
        self._refresh_in_progress = False
        self._manual_refresh = True
        self._strategy_worker = None
        self.backtest_dialog = None
        self._last_price = None
        self._tick_bridge = TickSignalBridge()
        self._tick_bridge.tick_received.connect(self._on_tick_received)
        self._build_ui()
        self._setup_timer()
        self.refresh_data()

    def _build_ui(self):
        self.setWindowTitle("股票城市 - PyQt 看盤交易室")
        self.resize(1680, 980)
        central = QtWidgets.QWidget()
        root_layout = QtWidgets.QHBoxLayout(central)
        root_layout.setSpacing(0)
        root_layout.setContentsMargins(0, 0, 0, 0)
        self.setCentralWidget(central)

        control_panel = QtWidgets.QScrollArea()
        control_panel.setWidgetResizable(True)
        control_panel.setFixedWidth(340)
        control_body = QtWidgets.QWidget()
        control_layout = QtWidgets.QVBoxLayout(control_body)
        control_layout.setContentsMargins(8, 8, 8, 8)
        control_layout.setSpacing(8)
        control_panel.setWidget(control_body)
        root_layout.addWidget(control_panel)

        self.login_button = QtWidgets.QPushButton("登入 Shioaji")
        self.logout_button = QtWidgets.QPushButton("登出")
        self.logout_button.setEnabled(False)
        self.login_button.clicked.connect(self.open_login_dialog)
        self.logout_button.clicked.connect(self.logout)
        btn_row = QtWidgets.QHBoxLayout()
        btn_row.addWidget(self.login_button)
        btn_row.addWidget(self.logout_button)
        control_layout.addLayout(btn_row)

        data_group = QtWidgets.QGroupBox("資料設定")
        data_form = QtWidgets.QFormLayout(data_group)
        data_form.setSpacing(6)
        self.interval_combo = QtWidgets.QComboBox()
        self.interval_combo.addItems(["1m", "5m", "15m", "30m", "60m", "1d"])
        self.interval_combo.setCurrentText("5m")
        self.interval_combo.setVisible(False)
        self.session_combo = QtWidgets.QComboBox()
        self.session_combo.addItems(["日盤", "夜盤", "全盤"])
        self.kbars_spin = QtWidgets.QSpinBox()
        self.kbars_spin.setRange(200, 3000)
        self.kbars_spin.setValue(1200)
        self.auto_refresh_checkbox = QtWidgets.QCheckBox("自動刷新")
        self.auto_refresh_checkbox.setChecked(True)
        self.refresh_interval_spin = QtWidgets.QSpinBox()
        self.refresh_interval_spin.setRange(1, 60)
        self.refresh_interval_spin.setValue(5)
        self.refresh_button = QtWidgets.QPushButton("重新載入")
        self.refresh_button.clicked.connect(self.refresh_data)
        self.session_combo.currentTextChanged.connect(self._on_session_changed)
        self.chart_interval_label = QtWidgets.QLabel("上方：日 K　下方：5 分 K")
        data_form.addRow("圖表週期", self.chart_interval_label)
        data_form.addRow("時段", self.session_combo)
        data_form.addRow("載入K棒數", self.kbars_spin)
        data_form.addRow(self.auto_refresh_checkbox)
        data_form.addRow("刷新間隔 (秒)", self.refresh_interval_spin)
        data_form.addRow(self.refresh_button)
        control_layout.addWidget(data_group)

        strategy_group = QtWidgets.QGroupBox("策略與風控")
        strategy_form = QtWidgets.QFormLayout(strategy_group)
        strategy_form.setSpacing(6)
        self.enable_strategy_checkbox = QtWidgets.QCheckBox("啟用策略訊號")
        self.enable_strategy_checkbox.setChecked(False)
        self.strategy_combo = QtWidgets.QComboBox()
        for key, name in services.get_strategy_options().items():
            self.strategy_combo.addItem(name, key)
        self.strategy_combo.currentIndexChanged.connect(self._on_strategy_changed)
        self.stop_loss_quantile_spin = QtWidgets.QDoubleSpinBox()
        self.stop_loss_quantile_spin.setRange(0.50, 0.95)
        self.stop_loss_quantile_spin.setSingleStep(0.01)
        self.stop_loss_quantile_spin.setValue(services.AUTO_RISK_STOP_LOSS_QUANTILE)
        self.profit_trigger_quantile_spin = QtWidgets.QDoubleSpinBox()
        self.profit_trigger_quantile_spin.setRange(0.50, 0.95)
        self.profit_trigger_quantile_spin.setSingleStep(0.01)
        self.profit_trigger_quantile_spin.setValue(services.AUTO_RISK_PROFIT_TRIGGER_QUANTILE)
        self.trailing_ratio_spin = QtWidgets.QDoubleSpinBox()
        self.trailing_ratio_spin.setRange(0.20, 1.00)
        self.trailing_ratio_spin.setSingleStep(0.05)
        self.trailing_ratio_spin.setValue(services.AUTO_RISK_TRAILING_RATIO)
        self.min_ma60_slope_spin = QtWidgets.QDoubleSpinBox()
        self.min_ma60_slope_spin.setRange(0, 20)
        self.min_ma60_slope_spin.setSingleStep(1)
        self.min_ma60_slope_spin.setValue(0)
        self.min_body_points_spin = QtWidgets.QDoubleSpinBox()
        self.min_body_points_spin.setRange(0, 100)
        self.min_body_points_spin.setSingleStep(5)
        self.min_body_points_spin.setValue(0)
        self.min_body_atr_ratio_spin = QtWidgets.QDoubleSpinBox()
        self.min_body_atr_ratio_spin.setRange(0, 2)
        self.min_body_atr_ratio_spin.setSingleStep(0.05)
        self.min_body_atr_ratio_spin.setValue(0)
        self.min_volume_ratio_spin = QtWidgets.QDoubleSpinBox()
        self.min_volume_ratio_spin.setRange(0, 5)
        self.min_volume_ratio_spin.setSingleStep(0.1)
        self.min_volume_ratio_spin.setValue(0)
        self.entry_exclude_open_spin = QtWidgets.QSpinBox()
        self.entry_exclude_open_spin.setRange(0, 60)
        self.entry_exclude_open_spin.setSingleStep(5)
        self.entry_exclude_open_spin.setValue(0)
        self.backtest_period_combo = QtWidgets.QComboBox()
        self.backtest_period_combo.addItems(list(services.AUTO_BACKTEST_PERIOD_OPTIONS.keys()))
        self.backtest_period_combo.setCurrentText("1年")
        strategy_form.addRow(self.enable_strategy_checkbox)
        strategy_form.addRow("策略", self.strategy_combo)
        strategy_form.addRow("停損分位", self.stop_loss_quantile_spin)
        strategy_form.addRow("停利分位", self.profit_trigger_quantile_spin)
        strategy_form.addRow("回撤比例", self.trailing_ratio_spin)
        strategy_form.addRow("MA60最小斜率", self.min_ma60_slope_spin)
        strategy_form.addRow("吞噬K最小實體", self.min_body_points_spin)
        strategy_form.addRow("實體/ATR下限", self.min_body_atr_ratio_spin)
        strategy_form.addRow("成交量均量倍數", self.min_volume_ratio_spin)
        strategy_form.addRow("避開開盤(分)", self.entry_exclude_open_spin)
        strategy_form.addRow("回測期間", self.backtest_period_combo)
        self.show_advanced_checkbox = QtWidgets.QCheckBox("顯示進階限制/風控")
        self.show_advanced_checkbox.setChecked(False)
        self.show_advanced_checkbox.toggled.connect(self._set_advanced_controls_visible)
        strategy_form.addRow(self.show_advanced_checkbox)
        self.strategy_group = strategy_group
        self._advanced_controls = (
            self.stop_loss_quantile_spin,
            self.profit_trigger_quantile_spin,
            self.trailing_ratio_spin,
            self.min_ma60_slope_spin,
            self.min_body_points_spin,
            self.min_body_atr_ratio_spin,
            self.min_volume_ratio_spin,
            self.entry_exclude_open_spin,
        )
        self._advanced_form = strategy_form
        self._set_advanced_controls_visible(False)
        for filter_spin in (
            self.stop_loss_quantile_spin,
            self.profit_trigger_quantile_spin,
            self.trailing_ratio_spin,
            self.min_ma60_slope_spin,
            self.min_body_points_spin,
            self.min_body_atr_ratio_spin,
            self.min_volume_ratio_spin,
            self.entry_exclude_open_spin,
        ):
            filter_spin.valueChanged.connect(self._clear_applied_optimization_risk)

        cost_group = QtWidgets.QGroupBox("交易損耗")
        cost_form = QtWidgets.QFormLayout(cost_group)
        cost_form.setSpacing(6)
        self.contract_type_combo = QtWidgets.QComboBox()
        self.contract_type_combo.addItems(list(services.get_contract_cost_presets().keys()))
        self.contract_type_combo.setCurrentText("小台")
        self.commission_spin = QtWidgets.QDoubleSpinBox()
        self.commission_spin.setRange(0, 1000)
        self.commission_spin.setDecimals(0)
        self.commission_spin.setValue(25)
        self.tax_spin = QtWidgets.QDoubleSpinBox()
        self.tax_spin.setRange(0, 1000)
        self.tax_spin.setDecimals(0)
        self.tax_spin.setValue(28)
        self.slippage_spin = QtWidgets.QDoubleSpinBox()
        self.slippage_spin.setRange(0, 20)
        self.slippage_spin.setDecimals(1)
        self.slippage_spin.setSingleStep(0.5)
        self.slippage_spin.setValue(2)
        self.cost_hint_label = QtWidgets.QLabel("")
        self.cost_hint_label.setWordWrap(True)
        cost_form.addRow("商品", self.contract_type_combo)
        cost_form.addRow("手續費/邊", self.commission_spin)
        cost_form.addRow("交易稅/邊", self.tax_spin)
        cost_form.addRow("滑價點/邊", self.slippage_spin)
        cost_form.addRow(self.cost_hint_label)
        self.show_cost_detail_checkbox = QtWidgets.QCheckBox("調整耗損內容")
        self.show_cost_detail_checkbox.setChecked(False)
        self.show_cost_detail_checkbox.toggled.connect(self._set_cost_controls_visible)
        cost_form.addRow(self.show_cost_detail_checkbox)
        self._cost_form = cost_form
        self._cost_detail_controls = (
            self.contract_type_combo,
            self.commission_spin,
            self.tax_spin,
            self.slippage_spin,
        )
        self.contract_type_combo.currentTextChanged.connect(self._on_contract_type_changed)
        for cost_spin in (self.commission_spin, self.tax_spin, self.slippage_spin):
            cost_spin.valueChanged.connect(self._update_cost_hint)
        self._on_contract_type_changed(self.contract_type_combo.currentText())
        self._set_cost_controls_visible(False)
        control_layout.addWidget(cost_group)

        order_group = QtWidgets.QGroupBox("下單  (台指近月 TXFR1)")
        order_layout = QtWidgets.QVBoxLayout(order_group)
        order_layout.setSpacing(6)
        qty_row = QtWidgets.QHBoxLayout()
        qty_row.addWidget(QtWidgets.QLabel("口數"))
        self.order_qty_spin = QtWidgets.QSpinBox()
        self.order_qty_spin.setRange(1, 100)
        self.order_qty_spin.setValue(1)
        self.order_qty_spin.setFixedWidth(68)
        self.order_market_radio = QtWidgets.QRadioButton("市價")
        self.order_limit_radio = QtWidgets.QRadioButton("限價")
        self.order_market_radio.setChecked(True)
        self.order_limit_price = QtWidgets.QLineEdit()
        self.order_limit_price.setPlaceholderText("限價")
        self.order_limit_price.setFixedWidth(76)
        self.order_limit_price.setEnabled(False)
        self.order_market_radio.toggled.connect(
            lambda checked: self.order_limit_price.setEnabled(not checked)
        )
        qty_row.addWidget(self.order_qty_spin)
        qty_row.addStretch()
        qty_row.addWidget(self.order_market_radio)
        qty_row.addWidget(self.order_limit_radio)
        qty_row.addWidget(self.order_limit_price)
        order_layout.addLayout(qty_row)
        trade_btn_row = QtWidgets.QHBoxLayout()
        trade_btn_row.setSpacing(6)
        self.btn_buy = QtWidgets.QPushButton("多單  BUY")
        self.btn_sell = QtWidgets.QPushButton("空單  SELL")
        self.btn_buy.setObjectName("btn_buy")
        self.btn_sell.setObjectName("btn_sell")
        self.btn_buy.setMinimumHeight(42)
        self.btn_sell.setMinimumHeight(42)
        self.btn_buy.setEnabled(False)
        self.btn_sell.setEnabled(False)
        self.btn_buy.clicked.connect(lambda: self._confirm_order("Buy"))
        self.btn_sell.clicked.connect(lambda: self._confirm_order("Sell"))
        trade_btn_row.addWidget(self.btn_buy)
        trade_btn_row.addWidget(self.btn_sell)
        order_layout.addLayout(trade_btn_row)
        self.btn_close_pos = QtWidgets.QPushButton("一鍵平倉  CLOSE ALL")
        self.btn_close_pos.setObjectName("btn_close_pos")
        self.btn_close_pos.setMinimumHeight(36)
        self.btn_close_pos.setEnabled(False)
        self.btn_close_pos.clicked.connect(self._confirm_close)
        order_layout.addWidget(self.btn_close_pos)
        self.order_status_label = QtWidgets.QLabel("登入後才能下單")
        self.order_status_label.setWordWrap(True)
        order_layout.addWidget(self.order_status_label)
        control_layout.addWidget(order_group)

        self.run_backtest_button = QtWidgets.QPushButton("執行回測")
        self.optimize_filters_button = QtWidgets.QPushButton("自動最佳化限制")
        self.apply_best_filter_button = QtWidgets.QPushButton("套用最佳限制")
        self.export_current_button = QtWidgets.QPushButton("匯出策略紀錄")
        self.export_backtest_button = QtWidgets.QPushButton("匯出回測結果")
        self.run_backtest_button.clicked.connect(self.run_backtest)
        self.optimize_filters_button.clicked.connect(self.optimize_entry_filters)
        self.apply_best_filter_button.clicked.connect(self.apply_best_entry_filter)
        self.export_current_button.clicked.connect(self.export_current_trades)
        self.export_backtest_button.clicked.connect(self.export_backtest_results)
        self.apply_best_filter_button.setEnabled(False)
        self.export_current_button.setEnabled(False)
        self.export_backtest_button.setEnabled(False)
        self.open_backtest_dialog_button = QtWidgets.QPushButton("策略回測")
        self.open_backtest_dialog_button.clicked.connect(self.open_backtest_dialog)
        control_layout.addWidget(self.open_backtest_dialog_button)

        status_group = QtWidgets.QGroupBox("連線 / 資料狀態")
        status_layout = QtWidgets.QVBoxLayout(status_group)
        status_layout.setSpacing(3)
        self.login_status_label = QtWidgets.QLabel("未登入")
        self.market_status_label = QtWidgets.QLabel("市場狀態：-")
        self.data_status_label = QtWidgets.QLabel("資料來源：-")
        self.db_status_label = QtWidgets.QLabel("DB 最新：-")
        for w in (self.login_status_label, self.market_status_label,
                  self.data_status_label, self.db_status_label):
            w.setWordWrap(True)
            status_layout.addWidget(w)
        control_layout.addWidget(status_group)
        control_layout.addStretch(1)

        right_widget = QtWidgets.QWidget()
        right_layout = QtWidgets.QVBoxLayout(right_widget)
        right_layout.setContentsMargins(0, 0, 0, 0)
        right_layout.setSpacing(0)
        root_layout.addWidget(right_widget, 1)

        right_layout.addWidget(self._build_price_ticker())

        chart_splitter = QtWidgets.QSplitter(QtCore.Qt.Vertical)
        self.daily_chart_widget = StrategyChartWidget("日 K")
        self.five_min_chart_widget = StrategyChartWidget("5 分 K")
        self.chart_widget = self.five_min_chart_widget
        self.daily_chart_widget.interval_requested.connect(
            lambda interval: self._set_chart_interval("top", interval)
        )
        self.five_min_chart_widget.interval_requested.connect(
            lambda interval: self._set_chart_interval("bottom", interval)
        )
        self._update_chart_interval_label()
        chart_splitter.addWidget(self.daily_chart_widget)
        chart_splitter.addWidget(self.five_min_chart_widget)
        chart_splitter.setStretchFactor(0, 1)
        chart_splitter.setStretchFactor(1, 1)
        chart_splitter.setSizes([460, 460])
        right_layout.addWidget(chart_splitter, 1)

        self.backtest_dialog = QtWidgets.QDialog(self)
        self.backtest_dialog.setWindowTitle("策略回測")
        self.backtest_dialog.resize(1280, 860)
        backtest_dialog_layout = QtWidgets.QVBoxLayout(self.backtest_dialog)
        backtest_dialog_layout.setContentsMargins(8, 8, 8, 8)
        backtest_dialog_layout.setSpacing(8)
        backtest_dialog_layout.addWidget(self.strategy_group)

        backtest_button_row = QtWidgets.QHBoxLayout()
        backtest_button_row.addWidget(self.run_backtest_button)
        backtest_button_row.addWidget(self.optimize_filters_button)
        backtest_button_row.addWidget(self.apply_best_filter_button)
        backtest_button_row.addWidget(self.export_current_button)
        backtest_button_row.addWidget(self.export_backtest_button)
        backtest_dialog_layout.addLayout(backtest_button_row)

        bottom_tabs = QtWidgets.QTabWidget()
        backtest_dialog_layout.addWidget(bottom_tabs, 1)

        trade_tab = QtWidgets.QWidget()
        trade_layout = QtWidgets.QVBoxLayout(trade_tab)
        trade_layout.setContentsMargins(4, 4, 4, 4)
        self.current_metrics_label = QtWidgets.QLabel("目前交易統計尚未產生")
        self.current_metrics_label.setWordWrap(True)
        self.current_trade_table = QtWidgets.QTableWidget()
        self.current_trade_table.setAlternatingRowColors(True)
        trade_layout.addWidget(self.current_metrics_label)
        trade_layout.addWidget(self.current_trade_table)
        bottom_tabs.addTab(trade_tab, "策略訊號")

        pos_tab = QtWidgets.QWidget()
        pos_layout = QtWidgets.QVBoxLayout(pos_tab)
        pos_layout.setContentsMargins(4, 4, 4, 4)
        pos_top = QtWidgets.QHBoxLayout()
        self.refresh_pos_button = QtWidgets.QPushButton("更新持倉")
        self.refresh_pos_button.clicked.connect(self.refresh_positions)
        self.account_info_label = QtWidgets.QLabel("帳戶資訊：請先登入")
        pos_top.addWidget(self.refresh_pos_button)
        pos_top.addWidget(self.account_info_label, 1)
        pos_layout.addLayout(pos_top)
        self.positions_table = QtWidgets.QTableWidget()
        self.positions_table.setAlternatingRowColors(True)
        pos_layout.addWidget(self.positions_table)
        bottom_tabs.addTab(pos_tab, "持倉")

        backtest_tab = QtWidgets.QWidget()
        backtest_layout = QtWidgets.QVBoxLayout(backtest_tab)
        backtest_layout.setContentsMargins(4, 4, 4, 4)
        self.backtest_metrics_label = QtWidgets.QLabel("尚未執行回測")
        self.backtest_metrics_label.setWordWrap(True)
        self.compare_table = QtWidgets.QTableWidget()
        self.compare_table.setAlternatingRowColors(True)
        self.optimization_table = QtWidgets.QTableWidget()
        self.optimization_table.setAlternatingRowColors(True)
        self.period_analysis_table = QtWidgets.QTableWidget()
        self.period_analysis_table.setAlternatingRowColors(True)
        self.data_health_table = QtWidgets.QTableWidget()
        self.data_health_table.setAlternatingRowColors(True)
        self.backtest_trade_table = QtWidgets.QTableWidget()
        self.backtest_trade_table.setAlternatingRowColors(True)
        backtest_layout.addWidget(self.backtest_metrics_label)
        backtest_layout.addWidget(QtWidgets.QLabel("1年 / 2年 對照"))
        backtest_layout.addWidget(self.compare_table)
        backtest_layout.addWidget(QtWidgets.QLabel("最佳化限制 Top 10"))
        backtest_layout.addWidget(self.optimization_table)
        backtest_layout.addWidget(QtWidgets.QLabel("區間勝率 / 穩健性"))
        backtest_layout.addWidget(self.period_analysis_table)
        backtest_layout.addWidget(QtWidgets.QLabel("資料健康檢查"))
        backtest_layout.addWidget(self.data_health_table)
        backtest_layout.addWidget(QtWidgets.QLabel("最近 20 筆回測交易"))
        backtest_layout.addWidget(self.backtest_trade_table)
        bottom_tabs.addTab(backtest_tab, "回測")

        self.exec_progress = QtWidgets.QProgressBar()
        self.exec_progress.setFixedWidth(300)
        self.exec_progress.setVisible(False)
        self.exec_progress.setTextVisible(True)
        self.statusBar().addPermanentWidget(self.exec_progress)
        self.statusBar().showMessage("就緒")

    def _start_progress(self, text, maximum=0):
        self.exec_progress.setVisible(True)
        if maximum and maximum > 0:
            self.exec_progress.setRange(0, maximum)
            self.exec_progress.setValue(0)
            self.exec_progress.setFormat(f"{text} %v/%m")
        else:
            self.exec_progress.setRange(0, 0)
            self.exec_progress.setFormat(f"{text} 進行中...")
        self.statusBar().showMessage(text)

    def _update_progress(self, value=None, text=None):
        if not self.exec_progress.isVisible():
            return
        if text:
            if self.exec_progress.maximum() > 0:
                self.exec_progress.setFormat(f"{text} %v/%m")
            else:
                self.exec_progress.setFormat(f"{text} 進行中...")
            self.statusBar().showMessage(text)
        if value is not None and self.exec_progress.maximum() > 0:
            self.exec_progress.setValue(max(0, min(int(value), self.exec_progress.maximum())))
        QtWidgets.QApplication.processEvents()

    def _finish_progress(self, text="完成"):
        if self.exec_progress.isVisible():
            if self.exec_progress.maximum() > 0:
                self.exec_progress.setValue(self.exec_progress.maximum())
            self.exec_progress.setVisible(False)
        self.statusBar().showMessage(text, 3000)

    def _build_price_ticker(self):
        frame = QtWidgets.QFrame()
        frame.setFixedHeight(54)
        layout = QtWidgets.QHBoxLayout(frame)
        layout.setContentsMargins(16, 4, 16, 4)
        self.ticker_contract_label = QtWidgets.QLabel("台指近月  TXFR1")
        self.ticker_status_label = QtWidgets.QLabel("未連線")
        self.ticker_price_label = QtWidgets.QLabel("─")
        self.ticker_change_label = QtWidgets.QLabel("")
        self.ticker_volume_label = QtWidgets.QLabel("")
        layout.addWidget(self.ticker_contract_label)
        layout.addWidget(self.ticker_status_label)
        layout.addStretch()
        layout.addWidget(self.ticker_price_label)
        layout.addWidget(self.ticker_change_label)
        layout.addStretch()
        layout.addWidget(self.ticker_volume_label)
        return frame

    def _setup_timer(self):
        self.refresh_timer = QtCore.QTimer(self)
        self.refresh_timer.timeout.connect(self._on_auto_refresh)
        self.refresh_interval_spin.valueChanged.connect(self._sync_refresh_timer)
        self.auto_refresh_checkbox.toggled.connect(self._sync_refresh_timer)
        self._sync_refresh_timer()
        self._position_timer = QtCore.QTimer(self)
        self._position_timer.setInterval(10_000)
        self._position_timer.timeout.connect(self.refresh_positions)

    def _set_advanced_controls_visible(self, visible):
        for widget in getattr(self, "_advanced_controls", ()):
            widget.setVisible(bool(visible))
            label = self._advanced_form.labelForField(widget)
            if label is not None:
                label.setVisible(bool(visible))

    def _set_cost_controls_visible(self, visible):
        for widget in getattr(self, "_cost_detail_controls", ()):
            widget.setVisible(bool(visible))
            label = self._cost_form.labelForField(widget)
            if label is not None:
                label.setVisible(bool(visible))

    def _on_contract_type_changed(self, contract_type):
        preset = services.get_contract_cost_presets().get(contract_type)
        if preset:
            self.commission_spin.blockSignals(True)
            self.tax_spin.blockSignals(True)
            self.commission_spin.setValue(float(preset.get("commission_per_side", 0)))
            self.tax_spin.setValue(float(preset.get("tax_per_side", 0)))
            self.commission_spin.blockSignals(False)
            self.tax_spin.blockSignals(False)
        self._update_cost_hint()

    def _current_point_value(self):
        preset = services.get_contract_cost_presets().get(self.contract_type_combo.currentText(), {})
        return float(preset.get("point_value", 50.0) or 50.0)

    def _update_cost_hint(self):
        point_value = self._current_point_value()
        cash_cost = 2.0 * (float(self.commission_spin.value()) + float(self.tax_spin.value()))
        slippage_points = 2.0 * float(self.slippage_spin.value())
        total_points = cash_cost / point_value + slippage_points
        self.cost_hint_label.setText(
            f"{self.contract_type_combo.currentText()} 每筆進出約扣 {total_points:.1f} 點"
        )

    def _sync_refresh_timer(self):
        if self.auto_refresh_checkbox.isChecked():
            self.refresh_timer.start(int(self.refresh_interval_spin.value() * 1000))
        else:
            self.refresh_timer.stop()

    def _interval_label(self, interval):
        return {
            "1m": "1 分 K",
            "5m": "5 分 K",
            "30m": "30 分 K",
            "60m": "60 分 K",
            "1d": "日 K",
        }.get(interval, str(interval))

    def _update_chart_interval_label(self):
        if hasattr(self, "chart_interval_label"):
            self.chart_interval_label.setText(
                f"上方：{self._interval_label(self.top_chart_interval)}　"
                f"下方：{self._interval_label(self.bottom_chart_interval)}"
            )
        if hasattr(self, "daily_chart_widget"):
            self.daily_chart_widget.set_title(self._interval_label(self.top_chart_interval))
        if hasattr(self, "five_min_chart_widget"):
            self.five_min_chart_widget.set_title(self._interval_label(self.bottom_chart_interval))

    def _set_chart_interval(self, panel, interval):
        if interval not in {"1m", "5m", "30m", "60m", "1d"}:
            return
        if panel == "top":
            self.top_chart_interval = interval
        else:
            self.bottom_chart_interval = interval
        self._update_chart_interval_label()
        self._manual_refresh = True
        self.refresh_data(silent=False)

    def _on_auto_refresh(self):
        self.refresh_data(silent=True)

    def _on_session_changed(self, session_text):
        if self.api is None:
            return
        if session_text != "夜盤":
            return

        # 先標記為手動刷新，確保後續圖表會重算 Y 軸（最高+100 / 最低-100）
        self._manual_refresh = True

        # 若已有資料，先立即重畫一次，讓使用者切換時就看到 Y 軸更新
        if self.bottom_chart_interval == "5m" and self.current_df is not None and not self.current_df.empty:
            self.chart_widget.update_chart(
                self.current_df, trades=self.current_trades, reset_view=True
            )

        # 若目前正在刷新，保留手動刷新旗標，待本輪完成後由策略更新路徑套用
        if self._refresh_in_progress:
            self.statusBar().showMessage("切換夜盤：目前更新中，完成後將自動重算 Y 軸", 2500)
            return

        self.statusBar().showMessage("切換夜盤，重新載入資料並重算 Y 軸", 2500)
        self.refresh_data(silent=False)

    def _on_strategy_changed(self):
        strategy_key = self.strategy_combo.currentData()
        if strategy_key != "masa_bottom_pullback":
            return
        changed = False
        if self.interval_combo.currentText() != "5m":
            self.interval_combo.setCurrentText("5m")
            changed = True
        if int(self.kbars_spin.value()) < 1500:
            self.kbars_spin.setValue(1500)
            changed = True
        if changed:
            self.statusBar().showMessage("麻紗底部拉回使用日K判斷、5分K進出，已切到5m並加載更多K棒", 5000)

    def open_backtest_dialog(self):
        if self.backtest_dialog is None:
            return
        self.backtest_dialog.show()
        self.backtest_dialog.raise_()
        self.backtest_dialog.activateWindow()

    def open_login_dialog(self):
        dialog = LoginDialog(self)
        if dialog.exec_() == QtWidgets.QDialog.Accepted and dialog.api is not None:
            self.api = dialog.api
            self.login_status_label.setText("已登入 Shioaji")
            self.logout_button.setEnabled(True)
            self.login_button.setEnabled(False)
            self.btn_buy.setEnabled(True)
            self.btn_sell.setEnabled(True)
            self.btn_close_pos.setEnabled(True)
            self.order_status_label.setText("已連線，可以下單。")
            self.ticker_status_label.setText("已連線")
            services.subscribe_realtime_tick(self.api, self._tick_bridge.tick_received.emit)
            self._position_timer.start()
            self.refresh_positions()
            self.statusBar().showMessage("登入成功", 3000)
            # 確保登入後必定重設 Y 軸（即使 refresh 被 in-progress 擋住也有效）
            self._manual_refresh = True
            if self.bottom_chart_interval == "5m" and self.current_df is not None and not self.current_df.empty:
                self.chart_widget.update_chart(
                    self.current_df, trades=self.current_trades, reset_view=True
                )
            self.refresh_data()
            self._start_data_backfill()

    def logout(self):
        self._position_timer.stop()
        services.unsubscribe_realtime_tick(self.api)
        services.logout_shioaji(self.api)
        self.api = None
        self._last_price = None
        self.login_status_label.setText("未登入")
        self.logout_button.setEnabled(False)
        self.login_button.setEnabled(True)
        self.btn_buy.setEnabled(False)
        self.btn_sell.setEnabled(False)
        self.btn_close_pos.setEnabled(False)
        self.order_status_label.setText("登入後才能下單")
        self.ticker_status_label.setText("未連線")
        self.account_info_label.setText("帳戶資訊：請先登入")
        self.statusBar().showMessage("已登出", 3000)

    def _start_data_backfill(self):
        if self.api is None:
            return
        self.statusBar().showMessage("登入成功，背景檢查近期 K 線缺口...", 5000)
        self._run_worker(
            services.backfill_missing_kbars_from_shioaji,
            self._on_data_backfill_done,
            self.api,
            lookback_days=180,
            silent=True,
        )

    def _on_data_backfill_done(self, result):
        missing_count = len(result.get("missing_dates", []))
        updated_rows = int(result.get("updated_rows", 0) or 0)
        if updated_rows <= 0:
            if missing_count:
                self.statusBar().showMessage(
                    f"資料缺口檢查完成：發現 {missing_count} 天缺資料，但 Shioaji 未回傳資料",
                    6000,
                )
            else:
                self.statusBar().showMessage("資料缺口檢查完成，近期資料完整", 4000)
            return

        self.statusBar().showMessage(
            f"已補齊 {missing_count} 天缺口，共 {updated_rows:,} 筆，重新載入圖表...",
            6000,
        )
        self._manual_refresh = False
        QtCore.QTimer.singleShot(800, self._refresh_after_backfill)

    def _refresh_after_backfill(self):
        if self._refresh_in_progress:
            QtCore.QTimer.singleShot(800, self._refresh_after_backfill)
            return
        self.refresh_data(silent=False)

    @QtCore.pyqtSlot(dict)
    def _on_tick_received(self, tick: dict):
        price = float(tick.get("close", 0))
        volume = int(tick.get("volume", 0))
        if price <= 0:
            return
        if self._last_price is None:
            self._last_price = price
        prev = self._last_price
        self._last_price = price
        is_up = tick.get("tick_type", "") in ("Buy", "BUY", "1")
        color = "#ff6b6b" if is_up else "#4ecdc4"
        chg = price - prev
        chg_str = f"+{chg:.0f}" if chg >= 0 else f"{chg:.0f}"
        self.ticker_price_label.setText(f"{price:,.0f}")
        self.ticker_change_label.setText(chg_str)
        self.ticker_volume_label.setText(f"量  {volume:,}")

    def _selected_strategy_keys(self):
        return [self.strategy_combo.currentData()]

    def _backtest_interval(self):
        return "5m"

    def _risk_config(self):
        config = services.get_risk_config(
            self.stop_loss_quantile_spin.value(),
            self.profit_trigger_quantile_spin.value(),
            self.trailing_ratio_spin.value(),
            min_ma60_slope_points=self.min_ma60_slope_spin.value(),
            min_body_points=self.min_body_points_spin.value(),
            min_body_atr_ratio=self.min_body_atr_ratio_spin.value(),
            min_volume_ratio=self.min_volume_ratio_spin.value(),
            entry_exclude_open_minutes=self.entry_exclude_open_spin.value(),
            contract_type=self.contract_type_combo.currentText(),
            point_value=self._current_point_value(),
            commission_per_side=self.commission_spin.value(),
            tax_per_side=self.tax_spin.value(),
            slippage_points_per_side=self.slippage_spin.value(),
        )
        if self._applied_optimization_risk_params is not None:
            config["_active_risk_params"] = dict(self._applied_optimization_risk_params)
        return config

    def _clear_applied_optimization_risk(self):
        self._applied_optimization_risk_params = None

    def _run_worker(self, func, on_success, *args, silent=False, **kwargs):
        worker = FunctionWorker(func, *args, **kwargs)
        worker.succeeded.connect(on_success)
        worker.failed.connect(lambda msg: self._on_worker_error(msg, silent=silent))
        worker.finished.connect(lambda: self._cleanup_worker(worker))
        self._workers.append(worker)
        self._active_task_count += 1
        if not silent:
            self._start_progress("背景作業", maximum=0)
        worker.start()

    def _cleanup_worker(self, worker):
        if worker in self._workers:
            self._workers.remove(worker)
        self._active_task_count = max(0, self._active_task_count - 1)
        if worker is getattr(self, "_strategy_worker", None):
            self._strategy_worker = None
        if self._active_task_count == 0:
            self._finish_progress("作業完成")

    def _on_worker_error(self, message, silent=False):
        self._set_busy(False)
        self._refresh_in_progress = False
        self._active_task_count = 0
        self._finish_progress("作業失敗")
        if silent:
            self.statusBar().showMessage(f"更新失敗：{message}", 5000)
        else:
            self.statusBar().showMessage("作業失敗", 5000)
            QtWidgets.QMessageBox.warning(self, "錯誤", message)

    def _set_busy(self, busy):
        self.refresh_button.setEnabled(not busy)
        self.run_backtest_button.setEnabled(not busy)
        self.optimize_filters_button.setEnabled(not busy)
        self.apply_best_filter_button.setEnabled((not busy) and bool(self.current_optimization_results))
        if hasattr(self, "open_backtest_dialog_button"):
            self.open_backtest_dialog_button.setEnabled(not busy)
        self.login_button.setEnabled(not busy and self.api is None)
        self.logout_button.setEnabled(not busy and self.api is not None)

    def refresh_data(self, silent=False):
        if self._refresh_in_progress:
            return
        self._manual_refresh = not silent
        self._refresh_in_progress = True
        self._set_busy(True)
        self._run_worker(
            services.load_dashboard_data,
            self._on_data_loaded,
            self.session_combo.currentText(),
            int(self.kbars_spin.value()),
            api=self.api,
            auto_update=True,
            top_interval=self.top_chart_interval,
            bottom_interval=self.bottom_chart_interval,
            silent=silent,
        )

    def _on_data_loaded(self, result):
        self._set_busy(False)
        self._refresh_in_progress = False
        self.current_df = result.get("df_5m", result["df"])
        self.current_5m_df = self.current_df
        self.current_daily_df = result.get("df_daily")
        self.current_top_chart_df = result.get("df_top")
        self.current_bottom_chart_df = result.get("df_bottom")
        meta = result["meta"]
        self.market_status_label.setText(f"市場：{meta['market_status']}")
        self.data_status_label.setText(f"來源：{meta['data_source']}　{meta['count']} 筆")
        self.db_status_label.setText(
            f"DB 最新：{meta['last_db_ts']}　即時：{'是' if meta['is_realtime'] else '否'}"
        )
        self.statusBar().showMessage("資料已更新", 3000)
        if self.current_top_chart_df is not None and not self.current_top_chart_df.empty:
            self.daily_chart_widget.update_chart(self.current_top_chart_df, trades=[], reset_view=self._manual_refresh)
        bottom_trades = self.current_trades if self.bottom_chart_interval == "5m" else []
        if self.current_bottom_chart_df is not None and not self.current_bottom_chart_df.empty:
            self.five_min_chart_widget.update_chart(self.current_bottom_chart_df, trades=bottom_trades, reset_view=self._manual_refresh)
        self._update_strategy_views(meta["is_realtime"])

    def _update_strategy_views(self, is_realtime):
        if self.current_df is None or self.current_df.empty:
            return
        if not self.enable_strategy_checkbox.isChecked():
            self.current_trades = []
            self.export_current_button.setEnabled(False)
            self.current_metrics_label.setText("未啟用策略")
            self._set_table_dataframe(self.current_trade_table, None)
            return
        if self._strategy_worker is not None and self._strategy_worker.isRunning():
            return
        self.statusBar().showMessage("策略計算中...")
        snapshot_df = self.current_df.copy()
        self._strategy_worker = FunctionWorker(
            services.run_strategy_bundle,
            snapshot_df,
            session=self.session_combo.currentText(),
            strategy_keys=self._selected_strategy_keys(),
            risk_config=self._risk_config(),
            is_realtime=is_realtime,
        )
        self._strategy_worker.succeeded.connect(self._on_strategy_done)
        self._strategy_worker.failed.connect(
            lambda msg: self.statusBar().showMessage(f"策略計算失敗：{msg}", 5000)
        )
        self._strategy_worker.finished.connect(
            lambda: self._cleanup_worker(self._strategy_worker)
        )
        self._workers.append(self._strategy_worker)
        self._strategy_worker.start()

    @QtCore.pyqtSlot(object)
    def _on_strategy_done(self, bundle):
        self.current_trades = bundle["trades"]
        metrics_text = self._format_metrics_html("策略訊號統計", bundle["metrics"])
        self.export_current_button.setEnabled(bool(bundle["trades"]))
        if self.bottom_chart_interval == "5m":
            self.five_min_chart_widget.update_chart(
                self.current_df,
                trades=bundle["trades"],
                reset_view=self._manual_refresh,
            )
        elif self.current_bottom_chart_df is not None and not self.current_bottom_chart_df.empty:
            self.five_min_chart_widget.update_chart(
                self.current_bottom_chart_df,
                trades=[],
                reset_view=self._manual_refresh,
            )
        self._manual_refresh = False
        self.current_metrics_label.setText(metrics_text)
        self._set_table_dataframe(self.current_trade_table, bundle["trade_df"])
        self.statusBar().showMessage(f"策略計算完成，共 {len(bundle['trades'])} 筆交易", 3000)

    def run_backtest(self):
        self._set_busy(True)
        period_label = self.backtest_period_combo.currentText()
        period_days = services.AUTO_BACKTEST_PERIOD_OPTIONS[period_label]
        self._start_progress("回測中", maximum=0)
        self._run_worker(
            services.run_backtest_bundle,
            self._on_backtest_done,
            self._backtest_interval(),
            self.session_combo.currentText(),
            strategy_keys=self._selected_strategy_keys(),
            risk_config=self._risk_config(),
            period_days=period_days,
        )

    def optimize_entry_filters(self):
        self._set_busy(True)
        self.current_optimization_results = []
        self.apply_best_filter_button.setEnabled(False)
        period_label = self.backtest_period_combo.currentText()
        period_days = services.AUTO_BACKTEST_PERIOD_OPTIONS[period_label]
        self._start_progress("最佳化限制中", maximum=0)
        self._run_worker(
            services.run_entry_filter_optimization_bundle,
            self._on_optimization_done,
            self._backtest_interval(),
            self.session_combo.currentText(),
            strategy_keys=self._selected_strategy_keys(),
            base_risk_config=self._risk_config(),
            period_days=period_days,
            top_n=10,
        )

    def _on_optimization_done(self, result):
        self._set_busy(False)
        self.current_optimization_results = result.get("results", [])
        self._set_table_dataframe(self.optimization_table, result.get("result_df"))
        self.apply_best_filter_button.setEnabled(bool(self.current_optimization_results))
        self.statusBar().showMessage(
            f"最佳化完成：測試 {result['tested_count']}/{result['total_count']} 組，合格 {result['qualified_count']} 組",
            5000,
        )

    def apply_best_entry_filter(self):
        if not self.current_optimization_results:
            return
        best_config = self.current_optimization_results[0]["risk_config"]
        self.min_ma60_slope_spin.setValue(float(best_config.get("min_ma60_slope_points", 0)))
        self.min_body_points_spin.setValue(float(best_config.get("min_body_points", 0)))
        self.min_body_atr_ratio_spin.setValue(float(best_config.get("min_body_atr_ratio", 0)))
        self.min_volume_ratio_spin.setValue(float(best_config.get("min_volume_ratio", 0)))
        self.entry_exclude_open_spin.setValue(int(best_config.get("entry_exclude_open_minutes", 0)))
        self._applied_optimization_risk_params = best_config.get("_active_risk_params")
        self.statusBar().showMessage("已套用最佳限制，可重新執行回測確認", 5000)
        if self.current_df is not None and not self.current_df.empty:
            self._update_strategy_views(False)

    def _on_backtest_done(self, result):
        self._set_busy(False)
        self.current_backtest_result = result
        self.current_backtest_trades = result["trades"]
        self.current_backtest_df = result.get("df")
        health = result.get("data_health_summary", {})
        health_text = ""
        if health:
            health_text = (
                f"　資料完整率：{health.get('completeness', 0):.1f}%"
                f"　異常日：{health.get('problem_days', 0)}"
            )
        self.backtest_metrics_label.setText(
            self._format_metrics_html(f"回測（{result['period_days']}天）", result["metrics"]) + health_text
        )
        self._set_table_dataframe(self.compare_table, result.get("compare_df"))
        self._set_table_dataframe(self.period_analysis_table, result.get("period_analysis_df"))
        health_df = result.get("data_health_df")
        if health_df is not None and not health_df.empty:
            health_df = health_df[health_df["狀態"] != "OK"].head(50).reset_index(drop=True)
            if health_df.empty:
                health_df = result.get("data_health_df").head(10).reset_index(drop=True)
        self._set_table_dataframe(self.data_health_table, health_df)
        recent_df = result.get("trade_df")
        if recent_df is not None and not recent_df.empty:
            recent_df = recent_df.tail(20).reset_index(drop=True)
        self._set_table_dataframe(self.backtest_trade_table, recent_df)
        self.export_backtest_button.setEnabled(bool(self.current_backtest_trades))
        self.statusBar().showMessage("回測完成", 3000)

    def _confirm_order(self, action: str):
        qty = int(self.order_qty_spin.value())
        price = None
        if self.order_limit_radio.isChecked():
            try:
                price = float(self.order_limit_price.text())
            except ValueError:
                QtWidgets.QMessageBox.warning(self, "輸入錯誤", "請輸入有效的限價。")
                return
        action_text = "多單 買進" if action == "Buy" else "空單 賣出"
        price_text = "市價" if price is None else f"限價 {price:.0f}"
        reply = QtWidgets.QMessageBox.question(
            self, "確認下單",
            f"確定要下：{action_text}　{qty} 口　{price_text}？",
            QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
            QtWidgets.QMessageBox.No,
        )
        if reply != QtWidgets.QMessageBox.Yes:
            return
        self.order_status_label.setText("委託送出中...")
        self._run_worker(
            services.place_futures_order,
            self._on_order_done,
            self.api, action, qty, price,
        )

    def _confirm_close(self):
        reply = QtWidgets.QMessageBox.question(
            self, "確認平倉",
            "確定要一鍵平倉所有台指期倉位？",
            QtWidgets.QMessageBox.Yes | QtWidgets.QMessageBox.No,
            QtWidgets.QMessageBox.No,
        )
        if reply != QtWidgets.QMessageBox.Yes:
            return
        self.order_status_label.setText("平倉送出中...")
        self._run_worker(services.close_all_positions, self._on_order_done, self.api)

    @QtCore.pyqtSlot(object)
    def _on_order_done(self, result):
        msg = result.get("msg", "委託完成")
        status = result.get("status", "")
        self.order_status_label.setText(f"{msg}")
        self.statusBar().showMessage(f"委託：{msg}　狀態：{status}", 6000)
        QtCore.QTimer.singleShot(1500, self.refresh_positions)

    def refresh_positions(self):
        if self.api is None:
            return
        self._run_worker(
            services.get_positions_and_balance,
            self._on_positions_loaded,
            self.api,
            silent=True,
        )

    @QtCore.pyqtSlot(object)
    def _on_positions_loaded(self, result):
        positions = result.get("positions", [])
        balance = result.get("balance", {})
        if balance:
            avail = balance.get("available_margin", 0)
            unrealized = balance.get("unrealized_pnl", 0)
            self.account_info_label.setText(
                f"可用保證金：{avail:,.0f}　浮動損益：{unrealized:+,.0f}"
            )
        else:
            self.account_info_label.setText("帳戶資訊：無法取得（非交易時段）")
        self._update_positions_table(positions)

    def _update_positions_table(self, positions: list):
        headers = ["商品", "方向", "口數", "成本均價", "現價", "浮動損益"]
        self.positions_table.clear()
        self.positions_table.setColumnCount(len(headers))
        self.positions_table.setHorizontalHeaderLabels(headers)
        self.positions_table.setRowCount(len(positions))
        for row_idx, pos in enumerate(positions):
            pnl = float(pos.get("pnl", 0))
            pnl_color = "#ff6b6b" if pnl >= 0 else "#4ecdc4"
            dir_color = "#ff6b6b" if pos.get("direction") == "多" else "#4ecdc4"
            cells = [
                (pos.get("code", "-"), "#d0d8f0"),
                (pos.get("direction", "-"), dir_color),
                (str(pos.get("quantity", 0)), "#d0d8f0"),
                (f"{float(pos.get('price', 0)):.0f}", "#d0d8f0"),
                (f"{float(pos.get('last_price', 0)):.0f}", "#d0d8f0"),
                (f"{pnl:+,.0f}", pnl_color),
            ]
            for col_idx, (text, fg) in enumerate(cells):
                item = QtWidgets.QTableWidgetItem(text)
                item.setForeground(QtGui.QColor(fg))
                item.setTextAlignment(QtCore.Qt.AlignCenter)
                self.positions_table.setItem(row_idx, col_idx, item)
        self.positions_table.resizeColumnsToContents()
        self.positions_table.horizontalHeader().setStretchLastSection(True)

    def export_current_trades(self):
        if not self.current_trades:
            return
        out_dir, trade_csv, summary_csv, image_dir, report_path = services.export_backtest_results_to_folder(
            self.current_trades,
            interval=self._backtest_interval(),
            session=self.session_combo.currentText(),
            period_label="目前視窗",
            selected_strategy_keys=self._selected_strategy_keys(),
            backtest_df=self.current_df,
            period_analysis_df=services.build_period_analysis_dataframe(self.current_trades),
        )
        image_dir, image_count = self._export_trade_images_from_chart(
            trades=self.current_trades,
            source_df=self.current_df,
            out_dir=out_dir,
            trade_csv_path=trade_csv,
            title_prefix="目前視窗",
        )
        _, masa_image_count = services.export_masa_bottom_pullback_entry_screenshots(
            self.current_trades,
            self.current_df,
            out_dir,
            trade_csv,
        )
        image_count += masa_image_count
        QtWidgets.QMessageBox.information(
            self, "匯出完成",
            f"資料夾：{out_dir}\n報告：{report_path}\n明細：{trade_csv}\n摘要：{summary_csv}\n截圖：{image_dir}\n張數：{image_count}",
        )

    def export_backtest_results(self):
        if not self.current_backtest_trades:
            return
        result = self.current_backtest_result or {}
        out_dir, trade_csv, summary_csv, image_dir, report_path = services.export_backtest_results_to_folder(
            self.current_backtest_trades,
            interval=self._backtest_interval(),
            session=self.session_combo.currentText(),
            period_label=self.backtest_period_combo.currentText(),
            selected_strategy_keys=self._selected_strategy_keys(),
            backtest_df=self.current_backtest_df,
            period_analysis_df=result.get("period_analysis_df"),
            data_health_df=result.get("data_health_df"),
            data_health_summary=result.get("data_health_summary"),
        )
        image_dir, image_count = self._export_trade_images_from_chart(
            trades=self.current_backtest_trades,
            source_df=self.current_backtest_df,
            out_dir=out_dir,
            trade_csv_path=trade_csv,
            title_prefix=f"回測-{self.backtest_period_combo.currentText()}",
        )
        _, masa_image_count = services.export_masa_bottom_pullback_entry_screenshots(
            self.current_backtest_trades,
            self.current_backtest_df,
            out_dir,
            trade_csv,
        )
        image_count += masa_image_count
        QtWidgets.QMessageBox.information(
            self, "匯出完成",
            f"資料夾：{out_dir}\n報告：{report_path}\n明細：{trade_csv}\n摘要：{summary_csv}\n截圖：{image_dir}\n張數：{image_count}",
        )

    def _export_trade_images_from_chart(self, trades, source_df, out_dir, trade_csv_path, title_prefix):
        image_dir = os.path.join(out_dir, "trade_images")
        os.makedirs(image_dir, exist_ok=True)
        if source_df is None or source_df.empty or not trades:
            return image_dir, 0

        backup_df = self.current_df
        backup_trades = self.current_trades

        image_rows = []
        total_steps = len(trades) * 3
        self._start_progress("匯出交易截圖", maximum=total_steps)
        step = 0
        created_count = 0

        for i, trade in enumerate(trades, 1):
            entry_idx = int(trade.get("entry_idx", -1))
            exit_idx = int(trade.get("exit_idx", -1))
            if entry_idx < 0 and exit_idx < 0:
                image_rows.append({"entry_image": "", "exit_image": "", "full_image": ""})
                step += 3
                self._update_progress(step, text=f"匯出交易截圖 ({i}/{len(trades)})")
                continue

            valid_idxs = [x for x in (entry_idx, exit_idx) if x >= 0]
            left_idx = min(valid_idxs)
            right_idx = max(valid_idxs)
            window_left = max(0, left_idx - 80)
            window_right = min(len(source_df) - 1, right_idx + 80)
            window_df = source_df.iloc[window_left:window_right + 1].copy()
            if window_df.empty:
                image_rows.append({"entry_image": "", "exit_image": "", "full_image": ""})
                step += 3
                self._update_progress(step, text=f"匯出交易截圖 ({i}/{len(trades)})")
                continue

            local_trade = dict(trade)
            if entry_idx >= 0:
                local_trade["entry_idx"] = entry_idx - window_left
            if exit_idx >= 0:
                local_trade["exit_idx"] = exit_idx - window_left
            self.chart_widget.update_chart(window_df, trades=[local_trade], reset_view=True)

            # 在截圖上方標示 MA 顏色，對應主圖視覺風格。
            self.chart_widget.ohlc_label.setText(
                "  MA20: <span style='color:#f4a261'><b>橘色</b></span>　"
                "MA60: <span style='color:#8d5cf6'><b>紫色</b></span>　"
                "MA100: <span style='color:#4ecdc4'><b>青綠色</b></span>"
                f"　{title_prefix} Trade {i:04d}"
            )
            self.chart_widget.ohlc_label.setStyleSheet(
                "background:#1a1a2e; color:#e0e0e0; padding:4px 8px; font-size:13px;"
            )
            QtWidgets.QApplication.processEvents()

            entry_rel = ""
            exit_rel = ""
            full_rel = ""

            full_name = f"trade_{i:04d}_full.png"
            full_path = os.path.join(image_dir, full_name)
            if self.chart_widget.grab().save(full_path, "PNG"):
                full_rel = os.path.relpath(full_path, out_dir)
                created_count += 1
            step += 1
            self._update_progress(step, text=f"匯出交易截圖 ({i}/{len(trades)})")

            if entry_idx >= 0:
                self.chart_widget._select_vline.setValue(local_trade["entry_idx"])
                self.chart_widget._select_vline.setVisible(True)
                entry_name = f"trade_{i:04d}_entry.png"
                entry_path = os.path.join(image_dir, entry_name)
                if self.chart_widget.grab().save(entry_path, "PNG"):
                    entry_rel = os.path.relpath(entry_path, out_dir)
                    created_count += 1
            step += 1
            self._update_progress(step, text=f"匯出交易截圖 ({i}/{len(trades)})")

            if exit_idx >= 0:
                self.chart_widget._select_vline.setValue(local_trade["exit_idx"])
                self.chart_widget._select_vline.setVisible(True)
                exit_name = f"trade_{i:04d}_exit.png"
                exit_path = os.path.join(image_dir, exit_name)
                if self.chart_widget.grab().save(exit_path, "PNG"):
                    exit_rel = os.path.relpath(exit_path, out_dir)
                    created_count += 1
            step += 1
            self._update_progress(step, text=f"匯出交易截圖 ({i}/{len(trades)})")

            image_rows.append({"entry_image": entry_rel, "exit_image": exit_rel, "full_image": full_rel})

        try:
            trades_df = pd.read_csv(trade_csv_path, encoding="utf-8-sig")
            trades_df["entry_image"] = ""
            trades_df["exit_image"] = ""
            trades_df["full_image"] = ""
            for idx, row in enumerate(image_rows):
                if idx >= len(trades_df):
                    break
                trades_df.at[idx, "entry_image"] = row.get("entry_image", "")
                trades_df.at[idx, "exit_image"] = row.get("exit_image", "")
                trades_df.at[idx, "full_image"] = row.get("full_image", "")
            trades_df.to_csv(trade_csv_path, index=False, encoding="utf-8-sig")
        except Exception:
            pass

        if backup_df is not None and not backup_df.empty:
            self.chart_widget.update_chart(backup_df, trades=backup_trades, reset_view=False)

        self._finish_progress("截圖匯出完成")
        return image_dir, created_count

    def _format_metrics_html(self, title, metrics):
        pf = metrics["profit_factor"]
        pf_text = f"{pf:.2f}" if pf != float("inf") else "inf"
        return (
            f"<b>{title}</b>　"
            f"交易：{metrics['total_trades']}　"
            f"損益：<b>{metrics['total_pnl']:+.0f}</b>　"
            f"勝率：{metrics['win_rate']:.1f}%　"
            f"最大回撤：-{metrics['max_drawdown']:.0f}　"
            f"獲利因子：{pf_text}"
        )

    def _set_table_dataframe(self, table, df):
        if df is None or df.empty:
            table.clear()
            table.setRowCount(0)
            table.setColumnCount(0)
            return
        table.clear()
        table.setColumnCount(len(df.columns))
        table.setHorizontalHeaderLabels([str(c) for c in df.columns])
        table.setRowCount(len(df.index))
        for row_idx in range(len(df.index)):
            for col_idx, col in enumerate(df.columns):
                val = df.iloc[row_idx, col_idx]
                item = QtWidgets.QTableWidgetItem(str(val))
                item.setTextAlignment(QtCore.Qt.AlignCenter)
                table.setItem(row_idx, col_idx, item)
        table.resizeColumnsToContents()
        table.horizontalHeader().setStretchLastSection(True)

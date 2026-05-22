from PyQt5 import QtCore, QtWidgets

from stock_city.app_pyqt.chart_widget import StrategyChartWidget
from stock_city.app_pyqt.login_dialog import LoginDialog
from stock_city.app_pyqt import services
from stock_city.app_pyqt.workers import FunctionWorker


class MainWindow(QtWidgets.QMainWindow):
    def __init__(self):
        super().__init__()
        self.api = None
        self.current_df = None
        self.current_trades = []
        self.current_backtest_trades = []
        self._workers = []
        self._refresh_in_progress = False  # 防止同時發出多個刷新請求
        self._strategy_worker = None       # 策略計算背景執行緒
        self._build_ui()
        self._setup_timer()
        self.refresh_data()

    def _build_ui(self):
        self.setWindowTitle("股票城市 PyQt 看盤室")
        self.resize(1600, 960)

        central = QtWidgets.QWidget()
        root_layout = QtWidgets.QHBoxLayout(central)
        self.setCentralWidget(central)

        control_panel = QtWidgets.QScrollArea()
        control_panel.setWidgetResizable(True)
        control_panel.setMinimumWidth(320)
        control_panel.setMaximumWidth(360)
        control_body = QtWidgets.QWidget()
        control_layout = QtWidgets.QVBoxLayout(control_body)
        control_panel.setWidget(control_body)
        root_layout.addWidget(control_panel)

        self.login_button = QtWidgets.QPushButton("登入 Shioaji")
        self.logout_button = QtWidgets.QPushButton("登出")
        self.refresh_button = QtWidgets.QPushButton("重新載入資料")
        self.run_backtest_button = QtWidgets.QPushButton("執行回測")
        self.export_current_button = QtWidgets.QPushButton("匯出目前交易紀錄")
        self.export_backtest_button = QtWidgets.QPushButton("匯出回測結果")
        self.logout_button.setEnabled(False)
        self.export_backtest_button.setEnabled(False)
        self.export_current_button.setEnabled(False)

        self.login_button.clicked.connect(self.open_login_dialog)
        self.logout_button.clicked.connect(self.logout)
        self.refresh_button.clicked.connect(self.refresh_data)
        self.run_backtest_button.clicked.connect(self.run_backtest)
        self.export_current_button.clicked.connect(self.export_current_trades)
        self.export_backtest_button.clicked.connect(self.export_backtest_results)

        button_row = QtWidgets.QHBoxLayout()
        button_row.addWidget(self.login_button)
        button_row.addWidget(self.logout_button)
        control_layout.addLayout(button_row)
        control_layout.addWidget(self.refresh_button)

        data_group = QtWidgets.QGroupBox("資料設定")
        data_form = QtWidgets.QFormLayout(data_group)
        self.interval_combo = QtWidgets.QComboBox()
        self.interval_combo.addItems(["1m", "5m", "15m", "30m", "60m", "1d"])
        self.interval_combo.setCurrentText("5m")
        self.session_combo = QtWidgets.QComboBox()
        self.session_combo.addItems(["日盤", "夜盤", "全盤"])
        self.kbars_spin = QtWidgets.QSpinBox()
        self.kbars_spin.setRange(20, 2000)
        self.kbars_spin.setSingleStep(10)
        self.kbars_spin.setValue(200)
        self.auto_refresh_checkbox = QtWidgets.QCheckBox("啟用自動刷新")
        self.auto_refresh_checkbox.setChecked(True)
        self.refresh_interval_spin = QtWidgets.QSpinBox()
        self.refresh_interval_spin.setRange(1, 60)
        self.refresh_interval_spin.setValue(5)
        data_form.addRow("K線週期", self.interval_combo)
        data_form.addRow("時段", self.session_combo)
        data_form.addRow("顯示 K 棒數", self.kbars_spin)
        data_form.addRow(self.auto_refresh_checkbox)
        data_form.addRow("刷新秒數", self.refresh_interval_spin)
        control_layout.addWidget(data_group)

        strategy_group = QtWidgets.QGroupBox("策略與風控")
        strategy_form = QtWidgets.QFormLayout(strategy_group)
        self.enable_strategy_checkbox = QtWidgets.QCheckBox("啟用策略訊號")
        self.enable_strategy_checkbox.setChecked(True)
        self.strategy_combo = QtWidgets.QComboBox()
        strategy_options = services.get_strategy_options()
        for key, name in strategy_options.items():
            self.strategy_combo.addItem(name, key)

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

        self.backtest_period_combo = QtWidgets.QComboBox()
        self.backtest_period_combo.addItems(list(services.AUTO_BACKTEST_PERIOD_OPTIONS.keys()))
        self.backtest_period_combo.setCurrentText("1年")

        strategy_form.addRow(self.enable_strategy_checkbox)
        strategy_form.addRow("策略", self.strategy_combo)
        strategy_form.addRow("停損分位", self.stop_loss_quantile_spin)
        strategy_form.addRow("停利分位", self.profit_trigger_quantile_spin)
        strategy_form.addRow("回撤比例", self.trailing_ratio_spin)
        strategy_form.addRow("回測期間", self.backtest_period_combo)
        control_layout.addWidget(strategy_group)
        control_layout.addWidget(self.run_backtest_button)
        control_layout.addWidget(self.export_current_button)
        control_layout.addWidget(self.export_backtest_button)

        status_group = QtWidgets.QGroupBox("狀態")
        status_layout = QtWidgets.QVBoxLayout(status_group)
        self.login_status_label = QtWidgets.QLabel("未登入")
        self.market_status_label = QtWidgets.QLabel("市場狀態：-")
        self.data_status_label = QtWidgets.QLabel("資料來源：-")
        self.db_status_label = QtWidgets.QLabel("DB 最新：-")
        for widget in (self.login_status_label, self.market_status_label, self.data_status_label, self.db_status_label):
            widget.setWordWrap(True)
            status_layout.addWidget(widget)
        control_layout.addWidget(status_group)
        control_layout.addStretch(1)

        right_splitter = QtWidgets.QSplitter(QtCore.Qt.Vertical)
        root_layout.addWidget(right_splitter, 1)

        self.chart_widget = StrategyChartWidget()
        right_splitter.addWidget(self.chart_widget)

        bottom_tabs = QtWidgets.QTabWidget()
        right_splitter.addWidget(bottom_tabs)
        right_splitter.setStretchFactor(0, 3)
        right_splitter.setStretchFactor(1, 2)

        trade_tab = QtWidgets.QWidget()
        trade_layout = QtWidgets.QVBoxLayout(trade_tab)
        self.current_metrics_label = QtWidgets.QLabel("目前交易統計尚未產生")
        self.current_metrics_label.setWordWrap(True)
        self.current_trade_table = QtWidgets.QTableWidget()
        trade_layout.addWidget(self.current_metrics_label)
        trade_layout.addWidget(self.current_trade_table)
        bottom_tabs.addTab(trade_tab, "交易紀錄")

        backtest_tab = QtWidgets.QWidget()
        backtest_layout = QtWidgets.QVBoxLayout(backtest_tab)
        self.backtest_metrics_label = QtWidgets.QLabel("尚未執行回測")
        self.backtest_metrics_label.setWordWrap(True)
        self.compare_table = QtWidgets.QTableWidget()
        self.backtest_trade_table = QtWidgets.QTableWidget()
        backtest_layout.addWidget(self.backtest_metrics_label)
        backtest_layout.addWidget(QtWidgets.QLabel("1年 / 2年 對照"))
        backtest_layout.addWidget(self.compare_table)
        backtest_layout.addWidget(QtWidgets.QLabel("最近 20 筆回測交易"))
        backtest_layout.addWidget(self.backtest_trade_table)
        bottom_tabs.addTab(backtest_tab, "回測")

        self.statusBar().showMessage("就緒")

    def _setup_timer(self):
        self.refresh_timer = QtCore.QTimer(self)
        self.refresh_timer.timeout.connect(self._on_auto_refresh)
        self.refresh_interval_spin.valueChanged.connect(self._sync_refresh_timer)
        self.auto_refresh_checkbox.toggled.connect(self._sync_refresh_timer)
        self._sync_refresh_timer()

    def _sync_refresh_timer(self):
        if self.auto_refresh_checkbox.isChecked():
            self.refresh_timer.start(int(self.refresh_interval_spin.value() * 1000))
        else:
            self.refresh_timer.stop()

    def _on_auto_refresh(self):
        self.refresh_data(silent=True)

    def _selected_strategy_keys(self):
        return [self.strategy_combo.currentData()]

    def _risk_config(self):
        return services.get_risk_config(
            self.stop_loss_quantile_spin.value(),
            self.profit_trigger_quantile_spin.value(),
            self.trailing_ratio_spin.value(),
        )

    def _run_worker(self, func, on_success, *args, silent=False, **kwargs):
        worker = FunctionWorker(func, *args, **kwargs)
        worker.succeeded.connect(on_success)
        if silent:
            # 靜默模式：只更新狀態列，不彈出對話框
            worker.failed.connect(lambda msg: self._on_worker_error(msg, silent=True))
        else:
            worker.failed.connect(lambda msg: self._on_worker_error(msg, silent=False))
        worker.finished.connect(lambda: self._cleanup_worker(worker))
        self._workers.append(worker)
        if not silent:
            self.statusBar().showMessage("背景作業執行中...")
        worker.start()

    def _cleanup_worker(self, worker):
        if worker in self._workers:
            self._workers.remove(worker)
        if worker is getattr(self, '_strategy_worker', None):
            self._strategy_worker = None

    def _on_worker_error(self, message, silent=False):
        self._set_busy(False)
        self._refresh_in_progress = False
        if silent:
            self.statusBar().showMessage(f"更新失敗：{message}", 5000)
        else:
            self.statusBar().showMessage("作業失敗", 5000)
            QtWidgets.QMessageBox.warning(self, "錯誤", message)

    def _set_busy(self, busy):
        self.refresh_button.setEnabled(not busy)
        self.run_backtest_button.setEnabled(not busy)
        self.login_button.setEnabled(not busy and self.api is None)
        self.logout_button.setEnabled(not busy and self.api is not None)

    def open_login_dialog(self):
        dialog = LoginDialog(self)
        if dialog.exec_() == QtWidgets.QDialog.Accepted and dialog.api is not None:
            self.api = dialog.api
            self.login_status_label.setText("已登入 Shioaji")
            self.logout_button.setEnabled(True)
            self.login_button.setEnabled(False)
            self.statusBar().showMessage("登入成功", 3000)
            self.refresh_data()

    def logout(self):
        services.logout_shioaji(self.api)
        self.api = None
        self.login_status_label.setText("未登入")
        self.logout_button.setEnabled(False)
        self.login_button.setEnabled(True)
        self.statusBar().showMessage("已登出", 3000)

    def refresh_data(self, silent=False):
        if self._refresh_in_progress:
            return  # 已有刷新作業在執行中，略過
        self._refresh_in_progress = True
        self._set_busy(True)
        self._run_worker(
            services.load_display_data,
            self._on_data_loaded,
            self.interval_combo.currentText(),
            self.session_combo.currentText(),
            int(self.kbars_spin.value()),
            api=self.api,
            auto_update=True,
            silent=silent,
        )

    def _on_data_loaded(self, result):
        self._set_busy(False)
        self._refresh_in_progress = False
        self.current_df = result["df"]
        meta = result["meta"]
        self.market_status_label.setText(f"市場狀態：{meta['market_status']}")
        self.data_status_label.setText(f"資料來源：{meta['data_source']}｜筆數：{meta['count']}｜回溯：{meta['days']}天")
        self.db_status_label.setText(f"DB 最新：{meta['last_db_ts']}｜即時：{'是' if meta['is_realtime'] else '否'}")
        self.statusBar().showMessage("資料已更新", 3000)
        # 繪製 K 線圖（即時，無策略訊號）
        self.chart_widget.update_chart(self.current_df, trades=[])
        # 策略計算移至背景執行緒
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

        # 在背景執行緒跑策略，避免 UI 凍結
        if self._strategy_worker is not None and self._strategy_worker.isRunning():
            return  # 已有策略計算執行中
        self.statusBar().showMessage("策略計算中...")
        snapshot_df = self.current_df.copy()
        snapshot_session = self.session_combo.currentText()
        snapshot_keys = self._selected_strategy_keys()
        snapshot_risk = self._risk_config()
        self._strategy_worker = FunctionWorker(
            services.run_strategy_bundle,
            snapshot_df,
            session=snapshot_session,
            strategy_keys=snapshot_keys,
            risk_config=snapshot_risk,
            is_realtime=is_realtime,
        )
        self._strategy_worker.succeeded.connect(self._on_strategy_done)
        self._strategy_worker.failed.connect(
            lambda msg: (
                self.statusBar().showMessage(f"策略計算失敗：{msg}", 5000),
            )
        )
        self._strategy_worker.finished.connect(lambda: self._cleanup_worker(self._strategy_worker))
        self._workers.append(self._strategy_worker)
        self._strategy_worker.start()

    @QtCore.pyqtSlot(object)
    def _on_strategy_done(self, bundle):
        self.current_trades = bundle["trades"]
        trade_df = bundle["trade_df"]
        metrics = bundle["metrics"]
        metrics_text = self._format_metrics_html("目前交易統計", metrics)
        self.export_current_button.setEnabled(bool(bundle["trades"]))
        self.chart_widget.update_chart(self.current_df, trades=bundle["trades"])
        self.current_metrics_label.setText(metrics_text)
        self._set_table_dataframe(self.current_trade_table, trade_df)
        self.statusBar().showMessage(f"策略計算完成，共 {len(bundle['trades'])} 筆交易", 3000)

    def run_backtest(self):
        self._set_busy(True)
        period_label = self.backtest_period_combo.currentText()
        period_days = services.AUTO_BACKTEST_PERIOD_OPTIONS[period_label]
        self._run_worker(
            services.run_backtest_bundle,
            self._on_backtest_done,
            self.interval_combo.currentText(),
            self.session_combo.currentText(),
            strategy_keys=self._selected_strategy_keys(),
            risk_config=self._risk_config(),
            period_days=period_days,
        )

    def _on_backtest_done(self, result):
        self._set_busy(False)
        self.current_backtest_trades = result["trades"]
        self.backtest_metrics_label.setText(self._format_metrics_html(f"自動回測（{result['period_days']}天）", result["metrics"]))
        self._set_table_dataframe(self.compare_table, result.get("compare_df"))
        recent_trade_df = result.get("trade_df")
        if recent_trade_df is not None and not recent_trade_df.empty:
            recent_trade_df = recent_trade_df.tail(20).reset_index(drop=True)
        self._set_table_dataframe(self.backtest_trade_table, recent_trade_df)
        self.export_backtest_button.setEnabled(bool(self.current_backtest_trades))
        self.statusBar().showMessage("回測完成", 3000)

    def export_current_trades(self):
        if not self.current_trades:
            return
        out_dir, trade_csv_path, summary_csv_path = services.export_backtest_results_to_folder(
            self.current_trades,
            interval=self.interval_combo.currentText(),
            session=self.session_combo.currentText(),
            period_label="目前視窗",
            selected_strategy_keys=self._selected_strategy_keys(),
        )
        QtWidgets.QMessageBox.information(
            self,
            "匯出完成",
            f"資料夾：{out_dir}\n明細：{trade_csv_path}\n摘要：{summary_csv_path}",
        )

    def export_backtest_results(self):
        if not self.current_backtest_trades:
            return
        out_dir, trade_csv_path, summary_csv_path = services.export_backtest_results_to_folder(
            self.current_backtest_trades,
            interval=self.interval_combo.currentText(),
            session=self.session_combo.currentText(),
            period_label=self.backtest_period_combo.currentText(),
            selected_strategy_keys=self._selected_strategy_keys(),
        )
        QtWidgets.QMessageBox.information(
            self,
            "匯出完成",
            f"資料夾：{out_dir}\n明細：{trade_csv_path}\n摘要：{summary_csv_path}",
        )

    def _format_metrics_html(self, title, metrics):
        profit_factor = metrics["profit_factor"]
        profit_factor_text = f"{profit_factor:.2f}" if profit_factor != float("inf") else "∞"
        return (
            f"<b>{title}</b><br>"
            f"總交易數：{metrics['total_trades']}｜"
            f"總損益：{metrics['total_pnl']:+.0f}｜"
            f"勝率：{metrics['win_rate']:.1f}%｜"
            f"最大資產回撤：-{metrics['max_drawdown']:.0f}｜"
            f"獲利因子：{profit_factor_text}"
        )

    def _set_table_dataframe(self, table, df):
        if df is None or df.empty:
            table.clear()
            table.setRowCount(0)
            table.setColumnCount(0)
            return

        table.clear()
        table.setColumnCount(len(df.columns))
        table.setHorizontalHeaderLabels([str(col) for col in df.columns])
        table.setRowCount(len(df.index))
        for row_idx in range(len(df.index)):
            for col_idx, column in enumerate(df.columns):
                value = df.iloc[row_idx, col_idx]
                item = QtWidgets.QTableWidgetItem(str(value))
                table.setItem(row_idx, col_idx, item)
        table.resizeColumnsToContents()
        table.horizontalHeader().setStretchLastSection(True)

from PyQt5 import QtCore, QtGui, QtWidgets
import pyqtgraph as pg


class CandlestickItem(pg.GraphicsObject):
    def __init__(self):
        super().__init__()
        self._data = []
        self._picture = QtGui.QPicture()

    def set_data(self, data):
        self._data = data or []
        self._generate_picture()
        self.update()

    def _generate_picture(self):
        self._picture = QtGui.QPicture()
        painter = QtGui.QPainter(self._picture)
        width = 0.35
        for x_pos, open_price, close_price, low_price, high_price in self._data:
            color = QtGui.QColor("#ff6b6b") if close_price >= open_price else QtGui.QColor("#4ecdc4")
            painter.setPen(pg.mkPen(color, width=1.5))
            painter.setBrush(pg.mkBrush(color))
            painter.drawLine(QtCore.QPointF(x_pos, low_price), QtCore.QPointF(x_pos, high_price))
            top = max(open_price, close_price)
            bottom = min(open_price, close_price)
            rect = QtCore.QRectF(x_pos - width, bottom, width * 2, max(top - bottom, 0.5))
            painter.drawRect(rect)
        painter.end()

    def paint(self, painter, *args):
        painter.drawPicture(0, 0, self._picture)

    def boundingRect(self):
        return QtCore.QRectF(self._picture.boundingRect())


class StrategyChartWidget(QtWidgets.QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)
        self._date_labels = []
        self._df_cache = None
        self._build_ui()

    def _build_ui(self):
        layout = QtWidgets.QVBoxLayout(self)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(0)

        # OHLC 資訊列
        self.ohlc_label = QtWidgets.QLabel("  點擊 K 棒查看　開 / 高 / 低 / 收")
        self.ohlc_label.setStyleSheet(
            "background:#1a1a2e; color:#888888; padding:4px 8px; font-size:13px;"
        )
        layout.addWidget(self.ohlc_label)

        self.graphics_layout = pg.GraphicsLayoutWidget()
        self.price_plot = self.graphics_layout.addPlot(row=0, col=0)
        self.volume_plot = self.graphics_layout.addPlot(row=1, col=0)
        self.volume_plot.setMaximumHeight(100)
        self.volume_plot.setXLink(self.price_plot)

        self.price_plot.showGrid(x=True, y=True, alpha=0.2)
        self.volume_plot.showGrid(x=True, y=True, alpha=0.2)
        self.price_plot.setLabel("left", "價格")
        self.volume_plot.setLabel("left", "量")
        self.volume_plot.setLabel("bottom", "時間")

        # 垂直選取線
        self._select_vline = pg.InfiniteLine(
            angle=90, movable=False,
            pen=pg.mkPen(color=(255, 255, 255, 90), width=1,
                         style=QtCore.Qt.DashLine)
        )
        self._select_vline.setVisible(False)
        self.price_plot.addItem(self._select_vline)

        # 點擊事件
        self.price_plot.scene().sigMouseClicked.connect(self._on_chart_clicked)

        layout.addWidget(self.graphics_layout)

    def clear(self):
        self.price_plot.clear()
        self.volume_plot.clear()
        self._date_labels = []
        # price_plot.clear() 會移除所有 item，需重新加入選取線
        self.price_plot.addItem(self._select_vline)
        self._select_vline.setVisible(False)

    def update_chart(self, df, trades=None, reset_view=False):
        # ── 更新前儲存目前視窗範圍（X、Y 均保留使用者設定）──
        try:
            _vr = self.price_plot.vb.viewRange()
            _saved_x = _vr[0]   # [x_min, x_max]
            _saved_y = _vr[1]   # [y_min, y_max]
            _old_len = len(self._df_cache) if self._df_cache is not None else 0
        except Exception:
            _saved_x = None
            _saved_y = None
            _old_len = 0

        self.clear()
        if df is None or df.empty:
            self._df_cache = None
            return
        self._df_cache = df

        self._date_labels = [ts.strftime("%m-%d %H:%M") if hasattr(ts, "strftime") else str(ts) for ts in df.index]
        x_values = list(range(len(df)))
        candle_data = [
            (x_pos, float(row["Open"]), float(row["Close"]), float(row["Low"]), float(row["High"]))
            for x_pos, (_, row) in enumerate(df.iterrows())
        ]

        candle_item = CandlestickItem()
        candle_item.set_data(candle_data)
        self.price_plot.addItem(candle_item)

        self.price_plot.plot(x_values, df["MA20"].astype(float).tolist(), pen=pg.mkPen("#f4a261", width=1.5), name="MA20", connect="finite")
        self.price_plot.plot(x_values, df["MA60"].astype(float).tolist(), pen=pg.mkPen("#8d5cf6", width=1.5), name="MA60", connect="finite")
        self.price_plot.plot(x_values, df["MA100"].astype(float).tolist(), pen=pg.mkPen("#4ecdc4", width=1.5), name="MA100", connect="finite")

        volume_brushes = [pg.mkBrush("#ff6b6b") if float(row["Close"]) >= float(row["Open"]) else pg.mkBrush("#4ecdc4") for _, row in df.iterrows()]
        volume_item = pg.BarGraphItem(x=x_values, height=df["Volume"].fillna(0).astype(float).tolist(), width=0.7, brushes=volume_brushes)
        self.volume_plot.addItem(volume_item)

        if trades:
            entry_x = []
            entry_y = []
            exit_x = []
            exit_y = []
            for trade in trades:
                entry_idx = int(trade.get("entry_idx", 0))
                exit_idx = int(trade.get("exit_idx", 0))
                if 0 <= entry_idx < len(df):
                    entry_x.append(entry_idx)
                    entry_y.append(float(df.iloc[entry_idx]["Close"]))
                if 0 <= exit_idx < len(df):
                    exit_x.append(exit_idx)
                    exit_y.append(float(df.iloc[exit_idx]["Close"]))

            if entry_x:
                entry_item = pg.ScatterPlotItem(entry_x, entry_y, symbol="t1", size=14, brush=pg.mkBrush("#ffd166"), pen=pg.mkPen("#ffffff", width=1.5))
                self.price_plot.addItem(entry_item)
            if exit_x:
                exit_item = pg.ScatterPlotItem(exit_x, exit_y, symbol="o", size=10, brush=pg.mkBrush("#ef476f"), pen=pg.mkPen("#ffee58", width=1.2))
                self.price_plot.addItem(exit_item)

        tick_spacing = max(1, len(df) // 8)
        ticks = [(idx, self._date_labels[idx]) for idx in range(0, len(df), tick_spacing)]
        self.price_plot.getAxis("bottom").setTicks([ticks])
        self.volume_plot.getAxis("bottom").setTicks([ticks])

        # ── 視窗範圍管理 ──
        new_len = len(df)
        if _old_len == 0 or _saved_x is None:
            # 首次載入：顯示全部資料，Y 軸範圍 = 最高+100 / 最低-100
            self.price_plot.setXRange(-0.5, new_len - 0.5, padding=0)
            y_hi = float(df["High"].max()) + 100
            y_lo = float(df["Low"].min()) - 100
            self.price_plot.setYRange(y_lo, y_hi, padding=0)
            vol_max = float(df["Volume"].fillna(0).max())
            self.volume_plot.setYRange(0, vol_max * 1.1, padding=0)
        else:
            # 後續更新：還原 X 軸（跟隨新資料）
            new_bars = new_len - _old_len
            at_right_edge = _saved_x[1] >= _old_len - 2
            if at_right_edge and new_bars > 0:
                x_min = _saved_x[0] + new_bars
                x_max = _saved_x[1] + new_bars
            else:
                x_min = _saved_x[0]
                x_max = _saved_x[1]
            x_min = max(x_min, -0.5)
            x_max = min(x_max, new_len - 0.5)
            self.price_plot.setXRange(x_min, x_max, padding=0)

            if reset_view:
                # 手動重新載入：重設 X 為全範圍，Y 依全部 K 棒重算
                self.price_plot.setXRange(-0.5, new_len - 0.5, padding=0)
                y_hi = float(df["High"].max()) + 100
                y_lo = float(df["Low"].min()) - 100
                self.price_plot.setYRange(y_lo, y_hi, padding=0)
                vol_max = float(df["Volume"].fillna(0).max())
                self.volume_plot.setYRange(0, vol_max * 1.1, padding=0)
            else:
                # 自動刷新：Y 軸完全不動
                self.price_plot.setYRange(_saved_y[0], _saved_y[1], padding=0)
                i0 = max(0, int(x_min))
                i1 = min(new_len - 1, int(x_max) + 1)
                visible_vol = df["Volume"].fillna(0).iloc[i0:i1 + 1]
                if not visible_vol.empty:
                    self.volume_plot.setYRange(0, float(visible_vol.max()) * 1.25, padding=0)

    # ─── K 棒點擊：顯示 OHLC ─────────────────────────────────────
    def _on_chart_clicked(self, event):
        if self._df_cache is None or self._df_cache.empty:
            return
        pos = event.pos()
        if not self.price_plot.sceneBoundingRect().contains(pos):
            return
        view_pos = self.price_plot.vb.mapSceneToView(pos)
        x_idx = int(round(view_pos.x()))
        if x_idx < 0 or x_idx >= len(self._df_cache):
            return

        row = self._df_cache.iloc[x_idx]
        o = float(row["Open"])
        h = float(row["High"])
        l = float(row["Low"])
        c = float(row["Close"])
        v = int(row["Volume"]) if "Volume" in row and row["Volume"] == row["Volume"] else 0
        chg = c - o
        chg_str = f"+{chg:.0f}" if chg >= 0 else f"{chg:.0f}"
        color = "#ff6b6b" if c >= o else "#4ecdc4"
        time_str = self._date_labels[x_idx] if x_idx < len(self._date_labels) else ""

        self.ohlc_label.setText(
            f"  {time_str}　"
            f"開 <b>{o:.0f}</b>　"
            f"高 <b>{h:.0f}</b>　"
            f"低 <b>{l:.0f}</b>　"
            f"收 <b><span style='color:{color}'>{c:.0f}</span></b>　"
            f"漲跌 <b><span style='color:{color}'>{chg_str}</span></b>　"
            f"量 {v:,}"
        )
        self.ohlc_label.setStyleSheet(
            "background:#1a1a2e; color:#cccccc; padding:4px 8px; font-size:13px;"
        )
        self._select_vline.setValue(x_idx)
        self._select_vline.setVisible(True)

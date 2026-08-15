# 台指期看盤與策略回測 APP

這個專案目前以 `pyqt_run_app.py` 作為主入口，核心目標是：

- 查看台指期 K 線資料。
- 登入 Shioaji 後自動更新缺漏資料。
- 依使用者提供的策略進行歷史回測。
- 輸出逐筆交易、區間勝率、資料健康檢查與 Markdown 投資報告。

## 快速啟動

```powershell
.venv\Scripts\activate
python pyqt_run_app.py
```

如果你使用 VS Code，可以直接執行：

```text
Terminal > Run Task > Run PyQt App
```

或按 `Ctrl+Shift+P`，選擇 `Tasks: Run Task`，再選 `Run PyQt App`。

如果需要重新建立環境：

```powershell
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

## 主要資料夾

```text
stick_strategy/
├─ pyqt_run_app.py                  # PyQt APP 入口
├─ requirements.txt                 # Python 套件需求
├─ README.md                        # 專案說明
├─ data/                            # 本機資料庫，主要為 txf_ticks.db
├─ docs/
│  ├─ masa_bottom_pullback_strategy.md
│  └─ reports/
│     └─ latest_backtest_report.md  # 最近一次人工保留的回測報告
└─ stock_city/
   ├─ app_pyqt/                     # PyQt 視窗、圖表、服務層與背景 worker
   ├─ db/                           # SQLite 讀寫與 K 線轉換
   ├─ market/                       # 交易日、結算日與時段工具
   └─ strategy/                     # 策略邏輯與策略 registry
```

## 核心模組

- `stock_city/app_pyqt/main_window.py`：主視窗、雙 K 線圖、回測畫面、匯出流程。
- `stock_city/app_pyqt/chart_widget.py`：K 線圖元件，支援右鍵切換 `5m / 30m / 60m / 1d`。
- `stock_city/app_pyqt/services.py`：資料載入、Shioaji 回補、回測、最佳化、報告匯出。
- `stock_city/db/tick_database.py`：SQLite tick / K 線資料存取。
- `stock_city/market/settlement_utils.py`：台灣工作日與台指期結算日判斷。
- `stock_city/strategy/ma20_ma60.py`：目前策略實作，包含 MA 策略與麻紗底部拉回策略雛形。

## 回測與報告

APP 內可直接執行回測並匯出：

- `trades.csv`：逐筆交易資料。
- `summary.csv`：整體績效摘要。
- `period_analysis.csv`：90 / 180 / 365 / 730 天、月、季的區間勝率。
- `data_quality.csv`：資料健康檢查。
- `report.md`：Markdown 投資報告。
- `trade_images/`：若策略支援截圖，會輸出交易截圖。

回測輸出預設放在 `backtest_outputs/`，此資料夾不進 Git。若有一份報告需要長期保存，可放到 `docs/reports/`。

目前保留的最新報告：

```text
docs/reports/latest_backtest_report.md
```

## 本機資料與 Git 規則

以下內容屬於本機資料或輸出，不進 Git：

- `data/txf_ticks.db`
- `backtest_outputs/`
- `masa_video/`
- `*.log`
- `.venv/`
- 憑證檔，例如 `*.pfx`

這些檔案可能對本機運行有用，但不應放進版本庫。

## Shioaji 設定

登入可以透過 APP 視窗輸入 API key、secret、憑證路徑與密碼。若希望登入視窗自動帶入永豐 API key，請建立本機 secrets 檔：

```powershell
Copy-Item .streamlit\secrets.toml.example .streamlit\secrets.toml
```

接著編輯 `.streamlit\secrets.toml`，填入：

```toml
SHIOAJI_API_KEY = "你的API_KEY"
SHIOAJI_SECRET_KEY = "你的SECRET_KEY"
```

`.streamlit\secrets.toml` 已被 `.gitignore` 排除，請不要把真實金鑰或憑證 commit 到 Git。

重要：`.streamlit/secrets.toml` 雖然沿用 Streamlit 的資料夾名稱，但目前 PyQt 登入視窗仍會讀取這個檔案自動帶入永豐 API Key / Secret。整理專案資料夾時不要刪除 `.streamlit/secrets.toml`；可以刪除舊 Streamlit 程式，但這個 secrets 檔是本機登入設定。

登入視窗的自動帶入順序：

1. `.streamlit/secrets.toml`
2. `SHIOAJI_API_KEY` / `SHIOAJI_SECRET_KEY` 環境變數
3. 上次成功登入後保存於本機的 QSettings

若 `.streamlit/secrets.toml` 遺失，Git 無法還原真實 API Key / Secret，因為它不應被提交。此時需要重新建立該檔並填入金鑰。

常見本機檔案：

```text
Sinopac.pfx
data/txf_ticks.db
.streamlit/secrets.toml
```

## 專案整理原則

目前專案已移除舊 Streamlit 入口、舊 CLI 腳本、舊資料快取與舊輸出。主線只保留 PyQt APP 與回測所需模組，讓後續新增策略時可以集中在：

1. `stock_city/strategy/` 新增策略邏輯。
2. `stock_city/app_pyqt/services.py` 串接回測與報告。
3. `docs/` 保存策略規格與重要報告。

## 驗證

基本語法檢查：

```powershell
.venv\Scripts\python.exe -m py_compile pyqt_run_app.py stock_city\app_pyqt\main_window.py stock_city\app_pyqt\services.py stock_city\app_pyqt\chart_widget.py stock_city\strategy\ma20_ma60.py
```

啟動 APP：

```powershell
python pyqt_run_app.py
```

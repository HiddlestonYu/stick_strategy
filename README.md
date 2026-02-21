# 台指期程式交易看盤室 - 股票城市

## 📊 專案簡介

這是一個基於 Streamlit 開發的台指期貨即時看盤系統，採用 **Shioaji API + SQLite Database** 架構，提供精確的 K 線圖表、技術指標分析和多時段交易視圖。所有數據經過嚴格驗證，確保與券商看盤軟體完全一致。

**主要程式**: `stock_city/app/streamlit_run_app.py`

**專案流程圖（Phase 1~3）**：請見 [docs/PROJECT_FLOW.md](docs/PROJECT_FLOW.md)

### 🎯 核心特色
- ✅ **數據準確性**：所有 OHLC 數據經驗證與券商 APP 完全一致
- 🏦 **Shioaji API 整合**：直接連接永豐證券 API，取得最可靠的期貨數據
- 💾 **本地數據庫**：SQLite 儲存 1 分 K 數據，支援快速查詢與多時段重組
- 📅 **結算日處理**：自動偵測每月第三個星期三結算日，正確處理 13:30 收盤
- 🔄 **智能快取**：3-60 秒可調快取機制，平衡即時性與效能

## ✨ 主要功能

### 🔌 數據架構
- **Shioaji API**：永豐證券官方 API，取得台指期貨 TXFR1 近月合約
  - 使用 `api.kbars()` 方法取得 1 分 K 數據
  - 採用 ±1 天查詢範圍確保數據完整性
  - 自動處理時區轉換（UTC → Asia/Taipei）
- **SQLite Database**：本地 `data/txf_ticks.db` 儲存所有 1 分 K 數據
  - 支援日盤/夜盤/全盤時段篩選
  - 動態重組為 5m/15m/30m/60m/1d K 線
  - 智能結算日處理（第三週三 13:30 收盤）

### 📅 結算日智能處理
- **自動偵測**：`stock_city/market/settlement_utils.py` 判斷每月第三個星期三
- **動態收盤時間**：
  - 一般日盤：08:45 - **13:45**
  - 結算日盤：08:45 - **13:30**（提前 15 分鐘）
- **數據驗證**：所有收盤價與券商 APP 完全一致

### 📈 商品與合約
- **TXFR1**：台指期貨近月連續合約
  - 顯示合約代碼與到期日
  - 自動切換到最新近月合約

### ⏰ 交易時段切換
- **全盤**：顯示日盤 + 夜盤完整資料
- **日盤**：08:45 - 13:45/13:30（依結算日調整）
- **夜盤**：15:00 - 次日 05:00

### 📊 K 線週期選擇
- **1m**：1 分鐘 K 線（直接讀取數據庫）
- **5m/15m/30m/60m**：多分鐘 K 線（由 1 分 K 重組）
- **1d（日 K，預設）**：日線，依「交易時段」彙總（日盤/夜盤/全盤）

### 🔄 自動刷新機制
- **即時模式**：市場開盤時啟用自動刷新
- **刷新間隔**：1-60 秒可調（預設 3 秒）
- **倒數計時**：即時顯示下次刷新時間
- **非盤中模式**：自動切換為歷史資料查詢
 - **每日資料檢查**：自動偵測今日資料是否缺失或過舊並更新

### 📐 技術指標
- **10日均線 (MA10)**：橘色線，短期趨勢指標
- **20日均線 (MA20)**：紫色線，中期趨勢指標
- **成交量柱狀圖**：紅綠配色符合台灣習慣（紅漲綠跌）

### 🎨 互動式圖表
- **Plotly 圖表**：支援拖曳、縮放、懸停查看數據
- **MA 均線**：10 日（橘）、20 日（紫）
- **成交量**：獨立子圖顯示
- **顏色配置**：紅漲綠跌（符合台灣習慣）

### 💾 核心模組

**stock_city/app/streamlit_run_app.py** - 主程式
- Streamlit Web 介面
- 數據查詢與圖表渲染
- 時段篩選與 K 線重組

**stock_city/db/tick_database.py** - 數據庫管理
- SQLite CRUD 操作
- Tick 數據讀取與批次寫入
- K 線重組邏輯（含結算日處理）

**stock_city/market/settlement_utils.py** - 結算日工具
- 判斷每月第三個星期三
- 動態回傳日盤收盤時間（13:30/13:45）

**stock_city/scripts/fetch_kbars_improved.py** - 歷史數據抓取
- 使用 Shioaji `api.kbars()` 取得 1 分 K
- ±1 天查詢法確保數據完整
- 自動偵測並標記結算日

### 🔧 核心技術
- **Streamlit 1.28+**：Web 應用框架
- **Plotly 5.17+**：互動式圖表庫
- **Shioaji 1.3.1**：永豐證券官方 API
- **Pandas 2.3+**：數據處理與分析
- **SQLite3**：本地數據儲存
- **pytz**：時區處理

## 🚀 快速開始

### 環境需求
- Python 3.14+
- Shioaji API 帳號（永豐證券）
- 網路連線

### 安裝步驟

1. **建立虛擬環境**
```bash
python -m venv venv_new
venv_new\Scripts\activate  # Windows
```

2. **安裝依賴套件**
```bash
pip install -r requirements.txt
```

3. **設定 Shioaji 憑證（建議用環境變數）**

Windows PowerShell：
```powershell
$Env:SHIOAJI_API_KEY="你的API_KEY"
$Env:SHIOAJI_SECRET_KEY="你的SECRET_KEY"
```

（請勿把金鑰寫進程式或提交到 Git）

另外也可使用 Streamlit secrets（本機檔案、不提交）：
1) 複製 `.streamlit/secrets.toml.example` → `.streamlit/secrets.toml`
2) 填入你的 `SHIOAJI_API_KEY` / `SHIOAJI_SECRET_KEY`

4. **初始化數據庫**
```bash
python -m stock_city.scripts.fetch_kbars_improved  # 抓取歷史數據
```

### 執行應用程式

```bash
streamlit run stock_city/app/streamlit_run_app.py
```

應用程式會在瀏覽器自動開啟：http://localhost:8501

### Shioaji API 設定

**在側邊欄登入 Shioaji（兩種方式擇一）：**
- 直接在側邊欄輸入 API Key / API Secret
- 或先設定環境變數 `SHIOAJI_API_KEY` / `SHIOAJI_SECRET_KEY`（較安全）

**注意**：需先至永豐證券官網申請 API 使用權限

### DB 固定顯示（與 Shioaji 登入無關）
- App 目前固定顯示本機 `data/txf_ticks.db` 的資料。
- 無論是否勾選或登入 Shioaji，都會使用本地 DB 顯示 K 線。
- 若要取得更長歷史資料，請先回填 DB。

### 策略回測（K 線圖下方勾選）
- **策略選擇**：10/20MA 趨勢 + 觸及 + 吞噬（進場/退場）
- **進場**：MA10 與 MA20 同向趨勢，前一根觸及 MA10/MA20，當前 K 棒吞噬前一根（收盤價高/低於前一根）
- **退場**：出現反向吞噬即退場（補單=退場）
- **標記**：圖上顯示進/出場記號，並列出完整交易紀錄
- **預設回測**：5分K、日盤、300筆

## 📊 核心功能說明

### K 線重組邏輯
數據庫儲存 1 分 K，所有其他週期由 `resample_ticks_to_kbars()` 動態重組：
```python
# stock_city/db/tick_database.py
if session == '日盤':
    for date in pd.unique(dates):
        end_minute = 30 if is_settlement_day(date) else 45
        # 篩選 08:45 - 13:30/13:45
elif session == '夜盤':
    # 篩選 15:00 - 05:00 (跨日)
```

### 結算日偵測
```python
# stock_city/market/settlement_utils.py
def is_settlement_day(date):
  # 以「每月第三個週三」為基準；若遇非工作日，順延至下一個工作日
  ...
```

（本專案使用 `holidays` 近似判斷台灣工作日，用於結算日順延與日盤收盤時間 13:30/13:45。）

### 數據驗證狀態
所有 OHLC 數據已與券商 APP 交叉驗證：
- ✅ 2026-01-21 結算日：收 31245（13:30）
- ✅ 2026-01-27：開 32313 收 32509 高 32532 低 32206
- ✅ 所有 23 個交易日數據一致

## 📂 專案結構

### 為什麼要這樣分層？
- **核心邏輯集中**：把資料庫與交易日工具放進 `stock_city/`，Phase 2/3 加策略與回測時不會把根目錄越堆越亂。
- **DB 路徑固定**：所有模組統一透過 `stock_city/project_paths.py` 取得 `data/txf_ticks.db`，避免檔案搬移後路徑跟著變。
- **腳本一致執行方式**：建議用 `python -m stock_city.scripts.<script>`，可確保 import 穩定。

```
stick_strategy/
├── stock_city/
│   ├── project_paths.py           # 專案根目錄/DB 路徑集中管理
│   ├── app/
│   │   └── streamlit_run_app.py   # Streamlit UI 入口
│   ├── db/
│   │   └── tick_database.py       # SQLite + K 線重組核心
│   ├── market/
│   │   └── settlement_utils.py    # 結算日與工作日工具
│   └── scripts/
│       ├── fetch_kbars_improved.py
│       ├── backfill_kbars.py
│       ├── fetch_full_data.py
│       ├── fetch_real_ticks.py
│       └── realtime_ticks_subscriber.py
├── experiments/                  # 一次性驗證/研究用（不影響正式流程）
├── data/
│   └── txf_ticks.db              # SQLite 數據庫（1 分 K）
├── docs/
├── requirements.txt
└── README.md
```

## 🔍 使用說明

### 1. 登入 Shioaji
- 在側邊欄輸入 API Key 和 Secret
- 點擊「登入 Shioaji」
- 看到「✅ 已連線」表示成功

### 2. 選擇查詢條件
- **K 線週期**：1m / 5m / 15m / 30m / 60m / 1d
- **交易時段**：全盤 / 日盤 / 夜盤（日 K 會依所選時段彙總）
- **刷新間隔**：1-60 秒（市場開盤時自動刷新）

### 3. 查看圖表
- 圖表顯示 K 線、MA10/MA20 均線、成交量
- 支援拖曳、縮放、懸停查看詳細數據
- 側邊欄顯示數據來源、更新時間、合約資訊

## 🐛 疑難排解

### 登入失敗
- 確認 API Key/Secret 正確
- 檢查網路連線
- 確認永豐證券 API 帳號已啟用

### 數據不更新
1. 檢查市場是否開盤
2. 檢查「即時刷新」是否啟用
3. 查看側邊欄錯誤訊息
4. 嘗試「強制重置」並重新登入

### 數據不一致
- 重新抓取歷史數據：`python -m stock_city.scripts.fetch_kbars_improved`
- 檢查結算日處理是否正確
- 查看數據庫 `data/txf_ticks.db` 是否完整

### 「顯示 K 棒數量拉很大，但圖上根數不變」
- 常見原因是：該「交易時段 + 週期（尤其是日 K 的夜盤/全盤）」在 DB 裡的存量不足。
- 側邊欄會顯示「DB 日K存量」與日期範圍；若不足，登入 Shioaji 後會自動分批回填。
- 也可以手動回填：`python -m stock_city.scripts.backfill_kbars --days 500 --session 全盤`

## 📝 開發記錄

### 2026-02-04
- ✅ 實作結算日智能偵測系統
- ✅ 修正 2026-01-21 收盤價（31476 → 31245）
- ✅ 動態調整日盤收盤時間（13:30/13:45）
- ✅ 驗證所有 23 個交易日數據正確

### 2026-01-27
- ✅ 切換至 Shioaji API 單一數據源
- ✅ 建立 SQLite 數據庫架構
- ✅ 實作 K 線重組邏輯
- ✅ 修正開盤價計算錯誤

### 2026-01-23
- ✅ 清除舊數據並完整重新抓取
- ✅ 建立 2026-01-01 至 2026-02-04 完整資料集
- ✅ Streamlit UI 優化（日 K 預設、時段強制）

## 📄 授權

本專案僅供學習與個人使用，請勿用於商業用途。

## 👤 作者

Hiddleston @ 2026

---

**最後更新**: 2026-02-08  
**數據範圍**: 2026-01-01 ~ 2026-02-04 (23 交易日)  
**數據庫記錄**: 24,338 筆 1 分 K  
**驗證狀態**: ✅ 所有數據與券商 APP 一致

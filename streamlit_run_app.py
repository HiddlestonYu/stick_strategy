"""
台指期程式交易看盤室 - 股票城市
=====================================================
本程式提供台指期貨、台積電和台灣加權指數的 K 線圖表分析工具
支援多時段切換（日盤/夜盤/全盤）、多週期 K 線（1分-日線）
並包含移動平均線（MA10/MA20）技術指標

作者: AI Assistant
版本: 3.0 - 使用 Shioaji API
日期: 2026-01-14
"""

import streamlit as st  # Streamlit Web 框架，用於建立互動式網頁應用
import plotly.graph_objects as go  # Plotly 圖表物件，用於繪製互動式圖表
from plotly.subplots import make_subplots  # Plotly 子圖功能，用於建立多軸圖表
import pandas as pd  # Pandas 數據處理庫，用於資料分析和處理
import shioaji as sj  # Shioaji API，用於獲取台灣期貨和股票即時數據
from datetime import datetime, timedelta  # 日期時間處理
import pytz  # 時區處理庫，用於處理不同時區的時間

# ============================================================
# 1. 頁面初始化設定與 Shioaji 連線
# ============================================================
# 設定頁面配置：使用寬版面並自訂標題
st.set_page_config(layout="wide", page_title="台指期程式交易看盤室")

# 顯示主標題
st.title("📈 台指期全盤 K線圖 (含 10MA/20MA)")

# 初始化 Shioaji API
@st.cache_resource
def init_shioaji():
    """
    初始化 Shioaji API
    使用 cache_resource 確保只初始化一次
    
    新版登入方式：使用 API Key 和 Secret
    在永豐證券網站申請 API Key：https://www.sinotrade.com.tw/
    """
    try:
        api = sj.Shioaji()
        return api
    except Exception as e:
        st.error(f"Shioaji 初始化失敗: {e}")
        return None

def login_shioaji(api_key=None, secret_key=None, cert_path=None, cert_password=None, fetch_contract=False):
    """
    登入 Shioaji（每次使用新的實例）
    支援兩種登入方式：
    1. API Key + Secret Key
    2. 憑證檔案 (.pfx) + 密碼
    
    參數:
        fetch_contract (bool): 是否在登入時下載合約資料（預設 False 以加快速度）
    
    返回:
        tuple: (api實例, 錯誤訊息)
    """
    try:
        # 建立新的 API 實例以避免快取問題
        api = sj.Shioaji()
        
        # 決定是否下載合約資料
        contracts_cb = lambda security_type: print(f"{repr(security_type)} fetch done.") if fetch_contract else None
        
        # 根據提供的參數決定登入方式
        if cert_path:
            # 使用憑證檔案登入
            if fetch_contract:
                result = api.login(
                    person_id=api_key,
                    passwd=cert_password,
                    contracts_cb=contracts_cb
                )
            else:
                result = api.login(
                    person_id=api_key,
                    passwd=cert_password
                )
        else:
            # 使用 API Key 登入
            if fetch_contract:
                result = api.login(
                    api_key=api_key, 
                    secret_key=secret_key,
                    contracts_cb=contracts_cb
                )
            else:
                result = api.login(
                    api_key=api_key, 
                    secret_key=secret_key
                )
        
        # 檢查登入結果
        if hasattr(result, 'get'):
            status = result.get('status', {})
            if isinstance(status, dict):
                status_code = status.get('status_code', 0)
                if status_code == 200:
                    return api, None
                else:
                    # 登入失敗，返回詳細錯誤
                    detail = result.get('response', {}).get('detail', '未知錯誤')
                    return None, f"狀態碼: {status_code}, 詳情: {detail}"
        
        # 如果沒有錯誤，視為成功
        return api, None
        
    except Exception as e:
        # 捕捉詳細的異常訊息
        error_msg = str(e)
        if 'Too Many Connections' in error_msg:
            return None, "連線數過多，請稍後再試或先登出其他連線"
        return None, error_msg

# 嘗試初始化 Shioaji
api = init_shioaji()

# ============================================================
# 2. 市場狀態檢查函數
# ============================================================
def get_market_status():
    """
    獲取當前市場狀態（開盤/收盤）
    
    返回:
        tuple: (狀態文字, 是否開盤, 時段名稱)
    
    交易時間:
        - 日盤: 08:45 - 13:45
        - 盤中休息: 13:45 - 15:00
        - 夜盤: 15:00 - 05:00 (次日)
    """
    # 獲取台灣當前時間
    taipei_tz = pytz.timezone('Asia/Taipei')
    now = datetime.now(taipei_tz)
    current_hour = now.hour
    current_minute = now.minute
    current_weekday = now.weekday()  # 0=週一, 6=週日
    
    # 檢查是否為週末
    if current_weekday >= 5:  # 週六或週日
        return "🚫 週末休市", False, "休市"
    
    # 轉換為總分鐘數以便比較
    current_time = current_hour * 60 + current_minute
    
    # 日盤時間: 08:45 - 13:45
    day_start = 8 * 60 + 45   # 525
    day_end = 13 * 60 + 45    # 825
    
    # 夜盤時間: 15:00 - 05:00 (次日)
    night_start = 15 * 60     # 900
    night_end = 5 * 60        # 300
    
    # 判斷當前時段
    if day_start <= current_time <= day_end:
        return "🟢 日盤交易中", True, "日盤"
    elif current_time >= night_start or current_time <= night_end:
        return "🌙 夜盤交易中", True, "夜盤"
    else:
        return "🔴 盤中休息", False, "休息"

# ============================================================
# 3. 侧邊欄控制項
# ============================================================
# 使用 Streamlit 的 sidebar 功能建立參數控制面板
with st.sidebar:
    st.header("參數設定")
    
    # ------------------------------------------------------------
    # 3.0 市場狀態顯示
    # ------------------------------------------------------------
    market_status, is_open, session_name = get_market_status()
    
    # 使用不同顏色顯示狀態
    if is_open:
        st.success(f"📊 **市場狀態**: {market_status}")
        st.info(f"⏱ **數據類型**: 即時數據 ({session_name})")
    else:
        st.warning(f"📊 **市場狀態**: {market_status}")
        st.info(f"⏱ **數據類型**: 歷史數據 (收盤)")
    
    st.divider()  # 分隔線
    
    # ------------------------------------------------------------
    # 3.1 Shioaji 帳號設定
    # ------------------------------------------------------------
    with st.expander("⚙️ Shioaji 帳號設定（選填）", expanded=True):
        use_shioaji = st.checkbox("使用 Shioaji 即時數據", value=True)
        
        # 重要提示
        if use_shioaji:
            st.info("💡 **Shioaji 多合約拼接功能**\n- 自動拼接所有可用期貨合約數據\n- 獲得與 Yahoo Finance 類似的完整歷史數據\n- 首次載入可能需要較長時間")
        
        if use_shioaji:
            # 登入方式選擇
            login_method = st.radio(
                "登入方式",
                ["API Key", "憑證檔案 (.pfx)"],
                index=0  # 預設使用 API Key
            )
            
            if login_method == "憑證檔案 (.pfx)":
                st.info("💡 已偵測到 Sinopac.pfx 憑證檔案")
                person_id = st.text_input("身分證字號", help="您的身分證字號")
                cert_password = st.text_input("憑證密碼", type="password", help="憑證檔案的密碼")
                use_cert = True
            else:
                st.info("💡 請至永豐證券網站申請 API Key: https://www.sinotrade.com.tw/")
                api_key = st.text_input("API Key", type="password", value="F97Uvg5MtkHWLzPzueMkxYYgZwo8h18Qsk6Y3Ah6BBox", help="永豐證券提供的 API Key")
                secret_key = st.text_input("Secret Key", type="password", value="5a1Uenx7KtJN1CxxHC34MDJgHN67ePysroAPGmzTv1zG", help="永豐證券提供的 Secret Key")
                use_cert = False
            
            # 登入選項
            fetch_contract = st.checkbox("登入時下載合約資料", value=False, help="取消勾選可加快登入速度，但部分功能可能受限")
            
            # 顯示登入狀態
            if 'shioaji_logged_in' in st.session_state and st.session_state.get('shioaji_logged_in'):
                st.success("✅ 已登入 Shioaji")
                if st.button("登出"):
                    # 關閉舊的連線
                    if 'shioaji_api' in st.session_state and st.session_state['shioaji_api']:
                        try:
                            st.session_state['shioaji_api'].logout()
                        except:
                            pass
                    st.session_state['shioaji_logged_in'] = False
                    st.session_state.pop('shioaji_api', None)
                    st.rerun()
            
            if st.button("登入 Shioaji"):
                # 檢查必要欄位
                if use_cert:
                    if not person_id or not cert_password:
                        st.warning("請輸入身分證字號和憑證密碼")
                    else:
                        with st.spinner("🔄 使用憑證檔案登入中，請稍候..."):
                            try:
                                # 先關閉舊的連線
                                if 'shioaji_api' in st.session_state and st.session_state['shioaji_api']:
                                    try:
                                        st.session_state['shioaji_api'].logout()
                                    except:
                                        pass
                                    st.session_state.pop('shioaji_api', None)
                                
                                cert_path = "d:\\Hiddleston\\stick_strategy\\Sinopac.pfx"
                                new_api, error = login_shioaji(
                                    api_key=person_id,
                                    cert_password=cert_password,
                                    cert_path=cert_path,
                                    fetch_contract=fetch_contract
                                )
                                if new_api:
                                    st.success("✅ Shioaji 憑證登入成功！")
                                    st.info("� 已啟用多合約拼接功能，可獲取完整歷史數據")
                                    st.session_state['shioaji_logged_in'] = True
                                    st.session_state['shioaji_api'] = new_api
                                    st.rerun()
                                else:
                                    st.error(f"❌ 登入失敗: {error if error else '未知錯誤'}")
                                    st.warning("💡 提示: 如果出現連線數過多，請稍等1-2分鐘或聯繫永豐證券客服")
                                    st.session_state['shioaji_logged_in'] = False
                            except Exception as e:
                                st.error(f"❌ 登入失敗: {str(e)}")
                                st.warning("💡 提示: 請檢查身分證字號和憑證密碼是否正確")
                                st.session_state['shioaji_logged_in'] = False
                else:
                    if not api_key or not secret_key:
                        st.warning("請輸入 API Key 和 Secret Key")
                    else:
                        with st.spinner("🔄 登入中，請稍候..."):
                            try:
                                # 先關閉舊的連線
                                if 'shioaji_api' in st.session_state and st.session_state['shioaji_api']:
                                    try:
                                        st.session_state['shioaji_api'].logout()
                                    except:
                                        pass
                                    st.session_state.pop('shioaji_api', None)
                                
                                new_api, error = login_shioaji(
                                    api_key=api_key, 
                                    secret_key=secret_key,
                                    fetch_contract=fetch_contract
                                )
                                if new_api:
                                    st.success("✅ Shioaji 登入成功！")
                                    st.info("� 已啟用多合約拼接功能，可獲取完整歷史數據")
                                    st.session_state['shioaji_logged_in'] = True
                                    st.session_state['shioaji_api'] = new_api
                                    st.rerun()
                                else:
                                    st.error(f"❌ 登入失敗: {error if error else '未知錯誤'}")
                                    st.warning("💡 提示: 如果出現連線數過多，請稍等1-2分鐘或聯繫永豐證券客服")
                                    st.session_state['shioaji_logged_in'] = False
                            except Exception as e:
                                st.error(f"❌ 登入失敗: {str(e)}")
                                st.warning("💡 提示: 請檢查 API Key 和 Secret Key 是否正確且未過期")
                                st.session_state['shioaji_logged_in'] = False
        else:
            st.info("目前使用 Yahoo Finance 歷史數據")
            if 'shioaji_logged_in' in st.session_state:
                st.session_state['shioaji_logged_in'] = False
    
    st.divider()  # 分隔線
    
    # ------------------------------------------------------------
    # 3.2 商品選擇下拉選單
    # ------------------------------------------------------------
    # 提供三種商品選項供使用者選擇
    # index=0 表示預設選擇第一個選項（台指期模擬）
    product_option = st.selectbox(
        "選擇商品",
        ("台灣加權指數 (^TWII)", "台積電 (2330.TW)"),
        index=0,
        help="⚠️ Shioaji 的期貨合約歷史數據極少（約21筆），建議使用 Yahoo Finance 獲取完整數據"
    )
    
    # ------------------------------------------------------------
    # 3.3 交易時段選擇
    # ------------------------------------------------------------
    # 全盤：顯示所有交易時段
    # 日盤：08:45 - 13:45
    # 夜盤：15:00 - 次日 05:00
    session_option = st.selectbox(
        "選擇時段",
        ("全盤", "日盤", "夜盤"),
        index=0
    )
    
    # ------------------------------------------------------------
    # 3.4 K線週期選擇
    # ------------------------------------------------------------
    # 支援從 1 分鐘到日線的多種時間週期
    # index=5 表示預設選擇日K（1d）
    interval_option = st.selectbox(
        "選擇 K 線週期",
        ("1m", "5m", "15m", "30m", "60m", "1d"),
        index=5  # 預設 日K
    )
    
    # ------------------------------------------------------------
    # 3.5 最大K棒數量滑桿
    # ------------------------------------------------------------
    # 限制圖表顯示的 K 棒數量，避免資料過多導致效能問題
    # 範圍：20-500 根，預設 100 根，每次調整 10 根
    max_kbars = st.slider(
        "顯示K棒數量",
        min_value=20,
        max_value=500,
        value=100,
        step=10,
        help="設定圖表顯示的最大K棒數量"
    )
    
    st.divider()  # 分隔線
    
    # 顯示提示訊息
    st.caption("💡 提示：實戰中建議使用 Shioaji API 接收 Tick 資料並即時合成 K 棒。")
    
    # 顯示當前設定摘要
    st.info(f"📊 **當前設定**\n- 商品: {product_option}\n- 時段: {session_option}\n- 週期: {interval_option}\n- K棒數: {max_kbars}")
    
    # 數據量統計區（會在獲取數據後自動更新）
    if 'data_stats' not in st.session_state:
        st.session_state['data_stats'] = {}

# ============================================================
# 4. 數據獲取與處理 (Data Handler)
# ============================================================

def get_contract(api, product):
    """
    根據商品選擇返回對應的 Shioaji 合約
    
    參數:
        api: Shioaji API 實例
        product (str): 使用者選擇的商品名稱
        
    返回:
        contract 或 list: Shioaji 合約物件或合約列表，若失敗則返回 None
    """
    try:
        if product == "台灣加權指數 (^TWII)":
            # 加權指數使用台指期來模擬，返回所有可用合約以便拼接
            try:
                contracts = api.Contracts.Futures.TXF
                if contracts:
                    contract_list = list(contracts.keys())
                    st.sidebar.caption(f"📋 可用台指期合約: {len(contract_list)} 個")
                    
                    # 返回所有合約以便拼接歷史數據
                    all_contracts = [contracts[key] for key in sorted(contract_list)]
                    st.sidebar.caption(f"✅ 將拼接 {len(all_contracts)} 個合約數據")
                    
                    return all_contracts
                else:
                    st.sidebar.error("❌ 無台指期合約，請確認已下載合約資料")
                    return None
            except Exception as e:
                st.sidebar.error(f"❌ 獲取台指期合約失敗: {str(e)[:100]}")
                return None
        elif product == "台積電 (2330.TW)":
            # 台積電股票
            try:
                contract = api.Contracts.Stocks["2330"]
                st.sidebar.caption(f"✅ 使用合約: 2330 台積電")
                return contract
            except Exception as e:
                st.sidebar.error(f"❌ 獲取2330合約失敗: {str(e)[:100]}")
                return None
    except Exception as e:
        st.error(f"獲取合約失敗: {e}")
        return None

def get_ticker_symbol(product):
    """
    根據使用者選擇的商品返回對應的 Yahoo Finance 股票代碼（備用）
    
    參數:
        product (str): 使用者選擇的商品名稱
        
    返回:
        str: Yahoo Finance 的股票代碼
    """
    if product == "台灣加權指數 (^TWII)":
        return "^TWII"
    elif product == "台積電 (2330.TW)":
        return "2330.TW"
    return "^TWII"

def filter_by_session(df, session, interval):
    """
    根據選擇的交易時段過濾 K 線數據
    
    參數:
        df (pd.DataFrame): K 線數據的 DataFrame
        session (str): 時段選擇 - "日盤", "夜盤" 或 "全盤"
        interval (str): K線週期（日K不應該過濾時段）
        
    返回:
        pd.DataFrame: 過濾後的 K 線數據
        
    交易時段說明:
        - 日盤：08:45 - 13:45 (一般交易時段)
        - 夜盤：15:00 - 次日 05:00 (夜間交易時段)
        - 全盤：顯示所有時段資料
        - 注意：日K線不進行時段過濾
    """
    # 檢查 DataFrame 是否為空
    if df is None or df.empty:
        return df
    
    # 日K線不應該按時段過濾（日K已經是全天彙總）
    if interval == "1d":
        return df
    
    # 全盤也不過濾
    if session == "全盤":
        return df
    
    # 確保索引具有時區資訊（台灣時間）
    if df.index.tz is None:
        df.index = df.index.tz_localize('Asia/Taipei')
    
    # 從 DataFrame 索引中提取小時和分鐘資訊
    hours = df.index.hour
    minutes = df.index.minute
    
    # 根據選擇的時段建立過濾條件
    if session == "日盤":
        # 日盤時段：08:45 - 13:45
        # 包含 8 點 45 分之後、9-12 點整點、13 點 45 分之前
        mask = ((hours == 8) & (minutes >= 45)) | \
               ((hours >= 9) & (hours < 13)) | \
               ((hours == 13) & (minutes <= 45))
        return df[mask]
    elif session == "夜盤":
        # 夜盤時段：15:00 - 次日 05:00
        # 包含 15 點之後到 5 點之前（跨日）
        mask = (hours >= 15) | (hours < 5)
        return df[mask]
    else:
        # 返回所有資料不過濾
        return df

@st.cache_data(ttl=60)  # 使用 Streamlit 快取機制，60 秒內避免重複請求相同資料
def get_data_from_shioaji(_api, interval, product, session):
    """
    從 Shioaji API 獲取 K 線數據，支援多合約拼接
    
    參數:
        _api: Shioaji API 實例（前綴 _ 避免被快取）
        interval (str): K 線週期
        product (str): 商品名稱
        session (str): 交易時段
        
    返回:
        pd.DataFrame: K 線數據
    """
    try:
        # 獲取合約
        contracts = get_contract(_api, product)
        if contracts is None:
            st.warning("⚠️ 無法獲取合約，請確認已登入並下載合約資料")
            return None
        
        # 設定時間範圍
        end_date = datetime.now()
        if interval == "1d":
            start_date = end_date - timedelta(days=730)  # 2年數據
        elif interval in ["30m", "60m"]:
            start_date = end_date - timedelta(days=60)
        elif interval == "15m":
            start_date = end_date - timedelta(days=30)
        else:
            start_date = end_date - timedelta(days=7)
        
        # 檢查是否為多合約（期貨需要拼接）
        if isinstance(contracts, list):
            st.sidebar.info(f"🔗 正在拼接 {len(contracts)} 個期貨合約數據...")
            all_dfs = []
            
            # 逐個獲取每個合約的數據
            for i, contract in enumerate(contracts):
                try:
                    st.sidebar.caption(f"📥 正在獲取 {contract.code} 數據... ({i+1}/{len(contracts)})")
                    
                    kbars = _api.kbars(
                        contract=contract,
                        start=start_date.strftime("%Y-%m-%d"),
                        end=end_date.strftime("%Y-%m-%d")
                    )
                    
                    if kbars is not None:
                        df = pd.DataFrame({**kbars})
                        if not df.empty:
                            df['ts'] = pd.to_datetime(df['ts'])
                            df = df.set_index('ts')
                            
                            # 標準化欄位名稱（檢查欄位是否存在）
                            rename_map = {}
                            if 'open' in df.columns:
                                rename_map['open'] = 'Open'
                            if 'high' in df.columns:
                                rename_map['high'] = 'High'
                            if 'low' in df.columns:
                                rename_map['low'] = 'Low'
                            if 'close' in df.columns:
                                rename_map['close'] = 'Close'
                            if 'volume' in df.columns:
                                rename_map['volume'] = 'Volume'
                            
                            if rename_map:
                                df = df.rename(columns=rename_map)
                            
                            # 如果沒有 Volume，設為0
                            if 'Volume' not in df.columns:
                                df['Volume'] = 0
                            
                            # 確保必要欄位存在
                            required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
                            if all(col in df.columns for col in required_cols):
                                df['contract'] = contract.code  # 標記合約代碼
                                all_dfs.append(df)
                                st.sidebar.caption(f"  ✅ {contract.code}: {len(df)} 筆")
                            else:
                                missing = [col for col in required_cols if col not in df.columns]
                                st.sidebar.caption(f"  ⚠️ {contract.code}: 缺少欄位 {missing}")
                except Exception as e:
                    st.sidebar.caption(f"  ⚠️ {contract.code}: {str(e)[:50]}")
                    continue
            
            if not all_dfs:
                st.sidebar.error("❌ 無法獲取任何合約數據")
                return None
            
            # 合併所有數據
            st.sidebar.caption(f"🔧 正在合併數據...")
            df = pd.concat(all_dfs)
            
            # 移除重複的時間點（保留成交量較大的，如果有 Volume 欄位的話）
            if 'Volume' in df.columns:
                df = df.sort_values(['Volume'], ascending=False)
            df = df[~df.index.duplicated(keep='first')]
            df = df.sort_index()
            
            # 確保有必要的欄位
            required_cols = ['Open', 'High', 'Low', 'Close', 'Volume']
            df = df[required_cols]
            
            st.sidebar.success(f"✅ 拼接完成！共 {len(df)} 筆原始數據")
            
            # 檢查數據間隔並進行重採樣
            if len(df) > 1:
                time_diff = (df.index[1] - df.index[0]).total_seconds() / 60
                st.sidebar.caption(f"⏱️ 數據間隔: {time_diff:.0f} 分鐘")
                
                if interval == "1d" and time_diff < 1440:
                    st.sidebar.caption(f"🔄 正在轉換為日K...")
                    df = df.resample('1D').agg({
                        'Open': 'first',
                        'High': 'max',
                        'Low': 'min',
                        'Close': 'last',
                        'Volume': 'sum'
                    }).dropna()
                    st.sidebar.success(f"✅ 轉換完成: {len(df)} 筆日K")
            
            return df
            
        else:
            # 單一合約（如股票）
            contract = contracts
            st.sidebar.caption(f"🔍 正在獲取 {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')} 的 {interval} 數據...")
            
            kbars = _api.kbars(
                contract=contract,
                start=start_date.strftime("%Y-%m-%d"),
                end=end_date.strftime("%Y-%m-%d")
            )
            
            # 轉換為 DataFrame
            if kbars is not None:
                try:
                    df = pd.DataFrame({**kbars})
                    
                    if df.empty:
                        st.warning("⚠️ Shioaji 返回空數據")
                        st.sidebar.error(f"❌ 合約: {contract.code}, 時間: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
                        return None
                    
                    raw_count = len(df)
                    st.sidebar.caption(f"📥 Shioaji API 返回 {raw_count} 筆原始數據")
                    
                    # 設定時間索引
                    df['ts'] = pd.to_datetime(df['ts'])
                    df = df.set_index('ts')
                    
                    # 標準化欄位名稱（檢查欄位是否存在）
                    rename_map = {}
                    if 'open' in df.columns:
                        rename_map['open'] = 'Open'
                    if 'high' in df.columns:
                        rename_map['high'] = 'High'
                    if 'low' in df.columns:
                        rename_map['low'] = 'Low'
                    if 'close' in df.columns:
                        rename_map['close'] = 'Close'
                    if 'volume' in df.columns:
                        rename_map['volume'] = 'Volume'
                    
                    if rename_map:
                        df = df.rename(columns=rename_map)
                    
                    # 如果沒有 Volume，設為0
                    if 'Volume' not in df.columns:
                        df['Volume'] = 0
                        st.sidebar.warning("⚠️ 數據無成交量欄位，已設為0")
                    
                    # 檢查數據間隔
                    if len(df) > 1:
                        time_diff = (df.index[1] - df.index[0]).total_seconds() / 60
                        st.sidebar.caption(f"⏱️ 數據間隔: {time_diff:.0f} 分鐘")
                        
                        if interval == "1d" and time_diff < 1440:
                            st.sidebar.warning(f"⚠️ API返回{time_diff:.0f}分K，正在轉換為日K...")
                            df = df.resample('1D').agg({
                                'Open': 'first',
                                'High': 'max',
                                'Low': 'min',
                                'Close': 'last',
                                'Volume': 'sum'
                            }).dropna()
                            st.sidebar.caption(f"✅ 重採樣後: {len(df)} 筆日K")
                        elif interval == "60m" and time_diff < 60:
                            df = df.resample('60min').agg({
                                'Open': 'first',
                                'High': 'max',
                                'Low': 'min',
                                'Close': 'last',
                                'Volume': 'sum'
                            }).dropna()
                            st.sidebar.caption(f"✅ 重採樣後: {len(df)} 筆60分K")
                        elif interval == "30m" and time_diff < 30:
                            df = df.resample('30min').agg({
                                'Open': 'first',
                                'High': 'max',
                                'Low': 'min',
                                'Close': 'last',
                                'Volume': 'sum'
                            }).dropna()
                            st.sidebar.caption(f"✅ 重採樣後: {len(df)} 筆30分K")
                        elif interval == "15m" and time_diff < 15:
                            df = df.resample('15min').agg({
                                'Open': 'first',
                                'High': 'max',
                                'Low': 'min',
                                'Close': 'last',
                                'Volume': 'sum'
                            }).dropna()
                            st.sidebar.caption(f"✅ 重採樣後: {len(df)} 筆15分K")
                    
                    return df
                except Exception as e:
                    st.error(f"❌ 資料轉換失敗: {e}")
                    return None
            else:
                st.warning("⚠️ Shioaji 未返回數據")
                return None
            
    except Exception as e:
        st.error(f"❌ Shioaji 數據獲取失敗: {e}")
        return None

@st.cache_data(ttl=60)
def get_data_from_yahoo(interval, product, session):
    """
    從 Yahoo Finance 下載 K 線數據（備用方案）
    """
    import yfinance as yf
    
    ticker = get_ticker_symbol(product)
    
    if interval == "1d":
        period = "2y"
    elif interval in ["30m", "60m"]:
        period = "60d"
    elif interval == "15m":
        period = "30d"
    else:
        period = "7d"
    
    try:
        df = yf.download(ticker, period=period, interval=interval, progress=False)
    except Exception as e:
        st.error(f"數據下載失敗: {e}")
        return None
    
    if df.empty:
        return None
    
    # 資料清理
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    df.columns = [col.capitalize() for col in df.columns]
    
    # Debug: 顯示 Yahoo 數據量
    st.sidebar.caption(f"📊 Yahoo: {len(df)} 筆{interval}K")
    
    return df

def process_kline_data(df, interval, session):
    """
    處理並計算技術指標的通用函數
    """
    if df is None or df.empty:
        return None
    
    # ------------------------------------------------------------
    # 時區轉換
    # ------------------------------------------------------------
    try:
        df.index = df.index.tz_convert('Asia/Taipei')
    except (TypeError, AttributeError):
        try:
            df.index = df.index.tz_localize('UTC').tz_convert('Asia/Taipei')
        except:
            df.index = df.index.tz_localize('Asia/Taipei')
    
    # ------------------------------------------------------------
    # 過濾非交易時間
    # ------------------------------------------------------------
    if interval == "1d":
        # 日K只過濾週末
        df = df[df.index.dayofweek < 5]
    
    # 根據時段過濾（日K不會被過濾）
    df = filter_by_session(df, session, interval)
    
    if df.empty:
        return None
    
    # ------------------------------------------------------------
    # 計算技術指標
    # ------------------------------------------------------------
    df['MA10'] = df['Close'].rolling(window=10).mean()
    df['MA20'] = df['Close'].rolling(window=20).mean()
    
    return df

# 主要數據獲取函數
def get_data(interval, product, session, use_shioaji=False, api_instance=None):
    """
    統一的數據獲取接口
    
    參數:
        interval (str): K 線週期
        product (str): 商品名稱
        session (str): 交易時段
        use_shioaji (bool): 是否使用 Shioaji API
        api_instance: Shioaji API 實例（如果使用 Shioaji）
    
    返回:
        tuple: (DataFrame, 資料來源名稱)
    """
    data_source = ""
    if use_shioaji and api_instance is not None:
        # 使用 Shioaji
        df = get_data_from_shioaji(api_instance, interval, product, session)
        data_source = "Shioaji (永豐證券)"
    else:
        # 使用 Yahoo Finance
        df = get_data_from_yahoo(interval, product, session)
        data_source = "Yahoo Finance"
    
    # 處理數據並計算技術指標
    processed_df = process_kline_data(df, interval, session)
    return processed_df, data_source

# ============================================================
# 4. 主程式執行：獲取數據並限制K棒數量
# ============================================================
# 4. 主程式執行：獲取數據並限制K棒數量
# ============================================================
# 呼叫 get_data 函數獲取 K 線數據（根據側邊欄設定決定使用哪個資料源）
try:
    use_shioaji_flag = st.session_state.get('shioaji_logged_in', False) and 'shioaji_api' in st.session_state
except:
    use_shioaji_flag = False

# 取得資料時傳遞 API 實例
if use_shioaji_flag:
    api_instance = st.session_state['shioaji_api']
    df, data_source = get_data(interval_option, product_option, session_option, use_shioaji_flag, api_instance)
else:
    df, data_source = get_data(interval_option, product_option, session_option, use_shioaji_flag)

# 顯示數據來源和數據量資訊
if df is not None:
    original_count = len(df)
    st.sidebar.success(f"✅ 已載入 {original_count} 筆 {interval_option} K線數據")
    
    # 如果數據量少於預期，顯示警告
    expected_counts = {
        "1d": 400,   # 約2年交易日
        "60m": 400,  # 約60天的小時K
        "30m": 800,  # 約60天的30分K
        "15m": 1600  # 約30天的15分K
    }
    expected = expected_counts.get(interval_option, 100)
    if original_count < expected * 0.5:  # 如果少於預期的50%
        st.sidebar.warning(f"⚠️ 數據量偏少，預期約 {expected} 筆")
else:
    st.sidebar.error("❌ 數據獲取失敗")

# 根據使用者設定的最大K棒數限制資料量
# 永遠取最後的 max_kbars 筆資料，確保滑桿連動正常
if df is not None:
    before_trim = len(df)
    if len(df) > max_kbars:
        df = df.tail(max_kbars)
        st.sidebar.info(f"📊 圖表顯示最新 {len(df)}/{before_trim} 筆")
    else:
        st.sidebar.info(f"📊 圖表顯示全部 {len(df)} 筆數據")
    
    # 顯示當前顯示的數據範圍
    if len(df) > 0:
        first_date = df.index[0].strftime('%Y-%m-%d') if hasattr(df.index[0], 'strftime') else str(df.index[0])
        last_date = df.index[-1].strftime('%Y-%m-%d') if hasattr(df.index[-1], 'strftime') else str(df.index[-1])
        st.sidebar.caption(f"📅 {first_date} ~ {last_date}")

# ============================================================
# 5. 繪製互動式 K 線圖 (Visualization)
# ============================================================
if df is not None:
    # ------------------------------------------------------------
    # 5.0 建立連續的 x 軸索引（移除所有空白間隙）
    # ------------------------------------------------------------
    # 將時間索引轉換為字串格式，用於顯示
    date_labels = df.index.strftime('%Y-%m-%d %H:%M') if len(df) > 0 and hasattr(df.index[0], 'strftime') else df.index.astype(str)
    # 建立連續的數字索引（0, 1, 2, 3...）確保沒有任何空白
    x_range = list(range(len(df)))
    
    # ------------------------------------------------------------
    # 5.1 建立雙軸圖表 (K線 + 成交量)
    # ------------------------------------------------------------
    # 使用 Plotly 的 make_subplots 建立包含 2 個子圖的圖表
    # rows=2: 兩個子圖垂直排列
    # shared_xaxes=True: 共用 x 軸（時間軸）
    # vertical_spacing: 子圖間的垂直間距
    # row_width: 各子圖的高度比例
    fig = make_subplots(
        rows=2, cols=1, 
        shared_xaxes=True, 
        vertical_spacing=0.03, 
        subplot_titles=('K 線與均線', '成交量'),
        row_width=[0.15, 0.85]  # K線圖佔 85%，成交量圖佔 15%
    )

    # ------------------------------------------------------------
    # 5.2 繪製 K 棒
    # ------------------------------------------------------------
    # 使用 Candlestick 圖表類型繪製 K 線
    # 符合台灣習慣：紅漲（increasing）、綠跌（decreasing）
    candlestick = go.Candlestick(
        x=x_range,            # 使用連續數字索引代替日期
        open=df['Open'],      # 開盤價
        high=df['High'],      # 最高價
        low=df['Low'],        # 最低價
        close=df['Close'],    # 收盤價
        name='K棒',
        increasing_line_color='red',   # 上漲顯示紅色
        decreasing_line_color='green', # 下跌顯示綠色
        increasing_line_width=2,       # 增加 K 棒線條寬度
        decreasing_line_width=2,       # 增加 K 棒線條寬度
        text=date_labels,     # 將日期作為文字資訊
        hovertext=date_labels # 懸停時顯示日期
    )
    # 將 K 棒加入第一個子圖（row=1）
    fig.add_trace(candlestick, row=1, col=1)

    # ------------------------------------------------------------
    # 5.3 繪製移動平均線 (MA)
    # ------------------------------------------------------------
    # 繪製 10 日移動平均線（橘色）
    fig.add_trace(
        go.Scatter(
            x=x_range,  # 使用連續數字索引
            y=df['MA10'], 
            line=dict(color='orange', width=1.5), 
            name='10 MA',
            text=date_labels,
            hovertext=date_labels
        ), 
        row=1, col=1
    )
    
    # 繪製 20 日移動平均線（紫色）
    fig.add_trace(
        go.Scatter(
            x=x_range,  # 使用連續數字索引
            y=df['MA20'], 
            line=dict(color='purple', width=1.5), 
            name='20 MA',
            text=date_labels,
            hovertext=date_labels
        ), 
        row=1, col=1
    )

    # ------------------------------------------------------------
    # 5.4 繪製成交量柱狀圖
    # ------------------------------------------------------------
    # 成交量的顏色根據K棒的漲跌：漲紅跌綠
    # 利用列表推導式生成顏色列表
    colors = ['red' if row['Open'] - row['Close'] >= 0 else 'green' 
              for index, row in df.iterrows()]
    
    # 建立柱狀圖並加入第二個子圖（row=2）
    fig.add_trace(
        go.Bar(
            x=x_range,  # 使用連續數字索引
            y=df['Volume'], 
            marker_color=colors, 
            name='成交量',
            text=date_labels,
            hovertext=date_labels
        ), 
        row=2, col=1
    )

    # ------------------------------------------------------------
    # 5.5 圖表美化與格式設定
    # ------------------------------------------------------------
    # 模擬專業看盤軟體的深色風格
    fig.update_layout(
        xaxis_rangeslider_visible=False,  # 隱藏下方滑動條以節省空間
        height=900,                       # 圖表高度 900 像素（加大顯示）
        plot_bgcolor='rgb(20, 20, 20)',  # 繪圖區背景色（深灰色）
        paper_bgcolor='rgb(20, 20, 20)', # 整個畫布背景色
        font=dict(color='white'),         # 字體顏色（白色）
        title_text=f"{product_option} - {session_option} - {interval_option} K線圖 [資料來源: {data_source}] (顯示 {len(df)} 筆)",
        hovermode='x unified'             # 滑鼠懸停時顯示十字線和統一提示
    )
    
    # ------------------------------------------------------------
    # 5.5.1 設定 x 軸顯示實際日期（每隔一段顯示）
    # ------------------------------------------------------------
    # 計算要顯示的刻度位置（避免過於密集）
    tick_spacing = max(1, len(df) // 10)  # 大約顯示 10 個刻度
    tickvals = list(range(0, len(df), tick_spacing))
    ticktext = [date_labels[i] for i in tickvals]
    
    # 更新 x 軸設定
    fig.update_xaxes(
        tickvals=tickvals,
        ticktext=ticktext,
        tickangle=-45  # 斜向顯示以避免重疊
    )
    
    # 更新 y 軸設定，使用自動縮放並加上邊距
    fig.update_yaxes(
        automargin=True,
        row=1, col=1
    )
    
    # ------------------------------------------------------------
    # 5.6 顯示圖表
    # ------------------------------------------------------------
    # width='stretch' 讓圖表自動伸展填滿容器寬度
    st.plotly_chart(fig, width='stretch')

    # ------------------------------------------------------------
    # 5.7 最新報價資訊顯示
    # ------------------------------------------------------------
    # 取得最後一筆資料（最新的 K 棒）
    last_row = df.iloc[-1]
    
    # 使用 Streamlit 的 columns 功能建立 4 個並排的欄位
    col1, col2, col3, col4 = st.columns(4)
    
    # 在各欄位中顯示指標（使用 metric 組件）
    col1.metric("最新收盤", f"{last_row['Close']:.0f}")  # 最新收盤價
    col2.metric("10 MA", f"{last_row['MA10']:.0f}")           # 10日均線
    col3.metric("20 MA", f"{last_row['MA20']:.0f}")           # 20日均線
    col4.metric("成交量", f"{last_row['Volume']:.0f}")        # 成交量
    
    # 顯示自動更新提示
    if use_shioaji_flag:
        st.info("📊 使用 Shioaji 即時數據，每 60 秒自動更新")
    else:
        st.info("📊 使用 Yahoo Finance 歷史數據")

else:
    # ------------------------------------------------------------
    # 當數據獲取失敗時顯示錯誤訊息
    # ------------------------------------------------------------
    st.error("目前無法獲取數據，請確認市場是否開盤或檢查網路連線。")

# ============================================================
# 程式結束
# ============================================================
# %% 記號用於 Jupyter/IPython 環境中區分代碼區塊

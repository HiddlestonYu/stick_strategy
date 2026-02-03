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
import time  # 時間處理，用於自動刷新
import pickle  # 序列化工具，用於資料快取
import os  # 檔案系統操作
from tick_database import get_kbars_from_db, save_tick, init_database  # Ticks database 模組

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
            fetch_contract = st.checkbox("登入時下載合約資料", value=True, help="取消勾選可加快登入速度，但部分功能可能受限")
            
            # 顯示登入狀態
            if 'shioaji_logged_in' in st.session_state and st.session_state.get('shioaji_logged_in'):
                st.success("✅ 已登入 Shioaji")
                
                col_logout1, col_logout2 = st.columns(2)
                
                if col_logout1.button("🔓 登出", use_container_width=True):
                    with st.spinner("正在登出..."):
                        # 關閉舊的連線
                        if 'shioaji_api' in st.session_state and st.session_state['shioaji_api']:
                            try:
                                st.session_state['shioaji_api'].logout()
                                st.success("✅ 已成功登出")
                            except Exception as e:
                                st.warning(f"⚠️ 登出時發生錯誤: {str(e)}")
                        st.session_state['shioaji_logged_in'] = False
                        st.session_state.pop('shioaji_api', None)
                        time.sleep(1)  # 等待1秒確保連線完全關閉
                        st.rerun()
                
                if col_logout2.button("🔄 強制重置", use_container_width=True):
                    # 強制清除所有連線狀態
                    st.session_state['shioaji_logged_in'] = False
                    st.session_state.pop('shioaji_api', None)
                    st.warning("⚠️ 已強制清除連線狀態，請等待1-2分鐘後重新登入")
                    st.rerun()
            
            if st.button("登入 Shioaji"):
                # 檢查必要欄位
                if use_cert:
                    if not person_id or not cert_password:
                        st.warning("請輸入身分證字號和憑證密碼")
                    else:
                        with st.spinner("🔄 使用憑證檔案登入中，請稍候..."):
                            try:
                                # 先關閉舊的連線並等待
                                if 'shioaji_api' in st.session_state and st.session_state['shioaji_api']:
                                    try:
                                        st.caption("🔄 正在關閉舊連線...")
                                        st.session_state['shioaji_api'].logout()
                                        time.sleep(2)  # 等待2秒確保舊連線完全關閉
                                    except:
                                        pass
                                    st.session_state.pop('shioaji_api', None)
                                
                                st.caption("🔄 正在建立新連線...")
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
                                    if error and ('連線數過多' in str(error) or 'Too Many Connections' in str(error)):
                                        st.warning("⚠️ 連線數過多的解決方式：")
                                        st.info("1️⃣ 點擊「🔄 強制重置」按鈕清除連線\n2️⃣ 等待 1-2 分鐘讓舊連線逾時\n3️⃣ 確認沒有其他程式或瀏覽器分頁在使用 Shioaji\n4️⃣ 聯繫永豐證券客服重置連線")
                                    else:
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
            st.info("⚠️ 請登入 Shioaji 以使用 TXF 數據")
            if 'shioaji_logged_in' in st.session_state:
                st.session_state['shioaji_logged_in'] = False
    
    st.divider()  # 分隔線
    
    # ------------------------------------------------------------
    # 3.2 商品選擇（固定為台指期貨）
    # ------------------------------------------------------------
    # 僅使用 Shioaji TXF 合約
    product_option = "台指期貨 (TXF)"
    st.markdown("**📊 商品：台指期貨 (TXF)**")
    
    # ------------------------------------------------------------
    # 3.4 K線週期選擇（提前，因為會影響時段選擇）
    # ------------------------------------------------------------
    # 支援從 1 分鐘到日線的多種時間週期
    # index=5 表示預設選擇日K（1d）
    interval_option = st.selectbox(
        "選擇 K 線週期",
        ("1m", "5m", "15m", "30m", "60m", "1d"),
        index=5  # 預設日K
    )
    
    # ------------------------------------------------------------
    # 3.3 交易時段選擇
    # ------------------------------------------------------------
    # 全盤：顯示所有交易時段
    # 日盤：08:45 - 13:45
    # 夜盤：15:00 - 次日 05:00
    # 注意：日K強制使用日盤時段
    
    if interval_option == "1d":
        # 日K固定顯示日盤
        session_option = "日盤"
        st.info("💡 日K固定顯示日盤時段（08:45-13:45）")
    else:
        # 其他週期可選擇時段
        default_session_index = 0  # 預設日盤
        session_option = st.selectbox(
            "選擇時段",
            ("日盤", "夜盤", "全盤"),
            index=default_session_index
        )
    
    # ------------------------------------------------------------
    # 3.5 最大K棒數量滑桿
    # ------------------------------------------------------------
    # 限制圖表顯示的 K 棒數量，避免資料過多導致效能問題
    # 範圍：20-1000 根，預設 100 根，每次調整 10 根
    max_kbars = st.slider(
        "顯示K棒數量",
        min_value=20,
        max_value=1000,
        value=100,
        step=10,
        help="設定圖表顯示的最大K棒數量（使用快取可顯示更多歷史數據）"
    )
    
    st.divider()  # 分隔線
    
    st.divider()  # 分隔線
    
    # ------------------------------------------------------------
    # 3.6 即時更新設定
    # ------------------------------------------------------------
    with st.expander("⚡ 即時更新設定", expanded=True):
        auto_refresh = st.checkbox(
            "啟用自動刷新", 
            value=True,  # 預設啟用
            help="啟用後，圖表會自動更新以顯示最新即時數據"
        )
        
        if auto_refresh:
            refresh_interval = st.slider(
                "刷新間隔（秒）",
                min_value=1,
                max_value=60,
                value=1,  # 預設1秒更新
                step=1,
                help="設定圖表自動更新的時間間隔"
            )
            st.success(f"✅ 自動刷新已啟用，每 {refresh_interval} 秒更新一次")
        else:
            refresh_interval = None
            st.info("ℹ️ 手動模式：點擊重新載入按鈕以更新數據")
    
    # 顯示提示訊息
    st.caption("💡 提示：啟用自動刷新可獲得動態K棒更新效果。")
    
    # 顯示當前設定摘要
    st.info(f"📊 **當前設定**\n- 商品: {product_option}\n- 時段: {session_option}\n- 週期: {interval_option}\n- K棒數: {max_kbars}\n- 自動刷新: {'✅ 啟用' if auto_refresh else '❌ 停用'}")
    
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
        contract: Shioaji 合約物件，若失敗則返回 None
    """
    try:
        if product == "台指期貨 (TXF)":
            # 獲取最近月份的台指期合約
            try:
                contracts = api.Contracts.Futures.TXF
                if contracts:
                    # 獲取所有合約代碼並排序（最近月份在前）
                    contract_list = sorted(list(contracts.keys()))
                    
                    # 找出最近的合約（通常是第一個或第二個）
                    # 優先使用第一個合約（最近月份）
                    nearest_contract_code = contract_list[0] if contract_list else None
                    
                    if nearest_contract_code:
                        contract = contracts[nearest_contract_code]
                        st.sidebar.success(f"✅ 使用台指期合約: {contract.code}")
                        st.sidebar.caption(f"📊 合約到期日: {contract.delivery_date if hasattr(contract, 'delivery_date') else '未知'}")
                        st.sidebar.caption(f"📋 可用合約: {', '.join(contract_list[:3])}...")
                        return contract
                    else:
                        st.sidebar.error("❌ 無可用台指期合約")
                        return None
                else:
                    st.sidebar.error("❌ 無台指期合約，請確認已登入並下載合約資料")
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
        product (str): 使用者選擇的商品名稱（已廢棄）
        
    返回:
        str: 已移除 Yahoo Finance 支援
    """
    return None  # Yahoo Finance 已移除

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

# ============================================================
# 資料快取管理函數
# ============================================================
def get_cache_path(product, interval, session):
    """
    生成快取檔案路徑
    """
    cache_dir = "data"
    if not os.path.exists(cache_dir):
        os.makedirs(cache_dir)
    
    # 檔名格式：產品_週期_時段.pkl
    product_code = product.split("(")[1].split(")")[0] if "(" in product else product
    filename = f"{product_code}_{interval}_{session}.pkl"
    return os.path.join(cache_dir, filename)

def load_cache(product, interval, session):
    """
    讀取快取資料
    返回: (DataFrame, 最後更新時間) 或 (None, None)
    """
    cache_path = get_cache_path(product, interval, session)
    
    if not os.path.exists(cache_path):
        return None, None
    
    try:
        with open(cache_path, 'rb') as f:
            cache_data = pickle.load(f)
            df = cache_data.get('data')
            last_update = cache_data.get('last_update')
            
            # 確保快取的 DataFrame 有正確時區
            if df is not None and not df.empty:
                taipei_tz = pytz.timezone('Asia/Taipei')
                if df.index.tz is None:
                    df.index = df.index.tz_localize('UTC').tz_convert(taipei_tz)
                else:
                    df.index = df.index.tz_convert(taipei_tz)
            
            return df, last_update
    except Exception as e:
        st.sidebar.warning(f"⚠️ 快取讀取失敗: {str(e)[:100]}")
        return None, None

def save_cache(df, product, interval, session):
    """
    儲存快取資料
    """
    cache_path = get_cache_path(product, interval, session)
    
    try:
        cache_data = {
            'data': df,
            'last_update': datetime.now(pytz.timezone('Asia/Taipei'))
        }
        with open(cache_path, 'wb') as f:
            pickle.dump(cache_data, f)
        st.sidebar.caption(f"💾 已儲存 {len(df)} 筆數據到快取")
    except Exception as e:
        st.sidebar.warning(f"⚠️ 快取儲存失敗: {str(e)[:100]}")

def merge_data(old_df, new_df):
    """
    合併舊數據和新數據，去除重複
    """
    if old_df is None or old_df.empty:
        return new_df
    if new_df is None or new_df.empty:
        return old_df
    
    # 確保兩個 DataFrame 的時區一致
    taipei_tz = pytz.timezone('Asia/Taipei')
    
    # 處理 old_df 時區
    if old_df.index.tz is None:
        old_df.index = old_df.index.tz_localize('UTC').tz_convert(taipei_tz)
    else:
        old_df.index = old_df.index.tz_convert(taipei_tz)
    
    # 處理 new_df 時區
    if new_df.index.tz is None:
        new_df.index = new_df.index.tz_localize('UTC').tz_convert(taipei_tz)
    else:
        new_df.index = new_df.index.tz_convert(taipei_tz)
    
    # 合併並去重（保留最新數據）
    combined = pd.concat([old_df, new_df])
    combined = combined[~combined.index.duplicated(keep='last')]
    combined = combined.sort_index()
    
    return combined

def get_data_from_shioaji(_api, interval, product, session):
    """
    從 Ticks Database 獲取 K 線數據（新架構）
    
    架構改動：
    1. 不再使用 api.kbars() 下載歷史 K 線
    2. 改用 tick database 讀取並組成 K 棒
    3. 確保日盤收盤時間為 13:45
    
    參數:
        _api: Shioaji API 實例（用於訂閱 ticks，暫不使用）
        interval (str): K 線週期
        product (str): 商品名稱（固定 TXF）
        session (str): 交易時段
        
    返回:
        pd.DataFrame: K 線數據
    """
    try:
        st.sidebar.info("📊 從 Ticks Database 讀取數據...")
        
        # 初始化 database
        init_database()
        
        # 根據 interval 決定回溯天數
        if interval == "1d":
            days = 60  # 日K回溯60天
        elif interval in ["30m", "60m"]:
            days = 7   # 30分/60分K回溯7天
        elif interval == "15m":
            days = 5   # 15分K回溯5天
        else:
            days = 5   # 1分/5分K回溯5天（修正：原本1天讀不到資料）
        
        # 從 database 讀取並組成 K 棒
        df = get_kbars_from_db(interval=interval, session=session, days=days)
        
        if df is None or df.empty:
            st.sidebar.warning("⚠️ Database 無數據")
            st.sidebar.caption("💡 提示：需要先訂閱 ticks 並接收數據")
            
            # 顯示詳細調試信息
            try:
                import sqlite3
                from pathlib import Path
                db_path = Path(__file__).parent / "data" / "txf_ticks.db"
                conn = sqlite3.connect(str(db_path))
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM ticks")
                total = cursor.fetchone()[0]
                cursor.execute("SELECT COUNT(DISTINCT code) FROM ticks")
                codes = cursor.fetchone()[0]
                cursor.execute("SELECT DISTINCT code FROM ticks LIMIT 5")
                code_list = [row[0] for row in cursor.fetchall()]
                st.sidebar.caption(f"📊 Database info: {total} ticks, {codes} codes: {code_list}")
                if total > 0:
                    cursor.execute("SELECT MIN(date(ts)), MAX(date(ts)) FROM ticks")
                    dates = cursor.fetchone()
                    st.sidebar.caption(f"📅 Date range: {dates[0]} ~ {dates[1]}")
                conn.close()
            except Exception as e:
                st.sidebar.caption(f"Debug error: {e}")
            
            return None
        
        st.sidebar.success(f"✅ 從 Database 讀取 {len(df)} 筆 {interval}K")
        st.sidebar.caption(f"📅 數據範圍: {df.index[0].date()} ~ {df.index[-1].date()}")
        
        # 顯示最近3日數據（調試用）
        if interval == "1d" and len(df) > 0:
            st.sidebar.caption("📊 最近3日數據：")
            for idx in df.index[-3:]:
                row = df.loc[idx]
                date_str = idx.strftime('%Y/%m/%d')
                st.sidebar.caption(f"{date_str}: 開{row['Open']:.0f} 高{row['High']:.0f} 低{row['Low']:.0f} 收{row['Close']:.0f}")
        
        return df
        
    except Exception as e:
        st.sidebar.error(f"❌ 數據讀取失敗: {str(e)}")
        import traceback
        st.sidebar.caption(f"詳細錯誤：{traceback.format_exc()[:200]}")
        return None
    """
    從 Shioaji API 獲取 K 線數據（即時更新）+ 本地快取
    
    策略：
    1. 讀取本地快取（如果有）
    2. 下載最新數據
    3. 合併並更新快取
    4. 返回完整數據
    
    注意：不使用 @st.cache_data，因為會影響本地快取累積
    
    參數:
        _api: Shioaji API 實例（前綴 _ 避免被快取）
        interval (str): K 線週期
        product (str): 商品名稱
        session (str): 交易時段
        
    返回:
        pd.DataFrame: K 線數據
    """
    try:
        # 1. 讀取本地快取
        cached_df, last_update = load_cache(product, interval, session)
        
        if cached_df is not None and not cached_df.empty:
            st.sidebar.caption(f"💾 載入快取: {len(cached_df)} 筆歷史數據")
            st.sidebar.caption(f"📅 快取範圍: {cached_df.index[0].date()} ~ {cached_df.index[-1].date()}")
            if last_update:
                st.sidebar.caption(f"🕐 快取更新: {last_update.strftime('%Y-%m-%d %H:%M')}")
            
            # 如果快取數據較少，提示可以回溯下載
            if len(cached_df) < 100:
                st.sidebar.info(f"💡 快取僅 {len(cached_df)} 筆，可多次重新整理頁面累積數據")
        else:
            st.sidebar.caption(f"ℹ️ 無本地快取，首次下載")
        
        # 2. 獲取合約
        contracts = get_contract(_api, product)
        if contracts is None:
            st.warning("⚠️ 無法獲取合約，請確認已登入並下載合約資料")
            return None
        
        # 檢查是否為多合約（期貨需要拼接）- 現在改為單一合約模式
        if isinstance(contracts, list):
            # 歷史多合約拼接模式（已停用，改用單一最近月份合約）
            st.sidebar.warning("⚠️ 檢測到多合約模式，已切換為單一合約模式以獲取即時數據")
            contracts = contracts[0] if contracts else None
            if not contracts:
                st.sidebar.error("❌ 無可用合約")
                return None
        
        # 單一合約模式（期貨或股票）
        contract = contracts
        
        # 設定時間範圍 - 即時數據使用更短時間範圍
        taipei_tz = pytz.timezone('Asia/Taipei')
        end_date = datetime.now(taipei_tz)
        
        # 特別處理：日K + 指定時段 = 下載分鐘K後彙總
        download_minute_for_daily = (interval == "1d" and session in ["日盤", "夜盤"])
        
        if download_minute_for_daily:
            # 下載15分鐘K（而非1分鐘K），用於精確過濾時段
            # 15分鐘K的歷史數據較多，可獲得更長期的數據
            st.sidebar.caption(f"⚙️ 正在下載15分K以彙總{session}日K...")
            start_date = end_date - timedelta(days=360)  # 嘗試下載360天
            actual_interval = "15m"  # 使用15分鐘K（歷史數據較1分鐘K豐富）
        elif interval == "1d":
            # 日K取近30天（全盤模式）
            start_date = end_date - timedelta(days=30)
            actual_interval = interval
        elif interval in ["30m", "60m"]:
            # 60分/30分K取近3天（確保包含夜盤）
            start_date = end_date - timedelta(days=3)
            actual_interval = interval
        elif interval == "15m":
            # 15分K取近2天
            start_date = end_date - timedelta(days=2)
            actual_interval = interval
        else:
            # 1分/5分K取近12小時（包含夜盤）
            start_date = end_date - timedelta(hours=12)
            actual_interval = interval
        
        st.sidebar.caption(f"🔍 合約: {contract.code}")
        st.sidebar.caption(f"📅 時間範圍: {start_date.strftime('%Y-%m-%d %H:%M')} ~ {end_date.strftime('%Y-%m-%d %H:%M')}")
        st.sidebar.caption(f"⏱️ 請求週期: {actual_interval}")
        st.sidebar.caption(f"🕐 台灣時間: {end_date.strftime('%H:%M:%S')}")
        
        try:
            # 構建 kbars 參數
            kbars_params = {
                'contract': contract,
                'start': start_date.strftime("%Y-%m-%d"),
                'end': end_date.strftime("%Y-%m-%d")
            }
            
            # 根據週期設定 timeout（分鐘K需要較長時間）
            if actual_interval in ["1m", "5m"]:
                timeout_seconds = 30
            else:
                timeout_seconds = 10
            
            st.sidebar.caption(f"🔄 正在下載 {actual_interval} K線數據...")
            
            kbars = _api.kbars(**kbars_params)
        except Exception as kbar_error:
            error_msg = str(kbar_error)
            st.sidebar.error(f"❌ kbars API 錯誤: {error_msg[:200]}")
            
            # 如果是 404 錯誤，嘗試更短的時間範圍
            if "404" in error_msg or "not found" in error_msg.lower():
                st.sidebar.warning("⚠️ 嘗試使用更短時間範圍...")
                start_date = end_date - timedelta(days=1)
                st.sidebar.caption(f"🔄 重試: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}")
                
                try:
                    retry_params = {
                        'contract': contract,
                        'start': start_date.strftime("%Y-%m-%d"),
                        'end': end_date.strftime("%Y-%m-%d")
                    }
                    kbars = _api.kbars(**retry_params)
                except Exception as retry_error:
                    st.sidebar.error(f"❌ 重試失敗: {str(retry_error)[:200]}")
                    return None
            else:
                return None
        
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
                # 設定時間索引（先不顯示範圍，因為時區可能不正確）
                df['ts'] = pd.to_datetime(df['ts'])
                
                # 檢查時區並轉換為台灣時間
                if df['ts'].dt.tz is None:
                    # 如果是 naive datetime，假設 Shioaji 返回的是 UTC+0
                    df['ts'] = df['ts'].dt.tz_localize('UTC').dt.tz_convert('Asia/Taipei')
                    st.sidebar.caption("🌍 時區: UTC → Asia/Taipei")
                else:
                    # 如果已有時區，轉換為台灣時間
                    df['ts'] = df['ts'].dt.tz_convert('Asia/Taipei')
                    st.sidebar.caption(f"🌍 時區: {df['ts'].dt.tz} → Asia/Taipei")
                
                df = df.set_index('ts')
                st.sidebar.caption(f"📅 API 數據範圍: {df.index[0]} ~ {df.index[-1]}")
                
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
                    
                    # 特別處理：日K + 指定時段，需要先過濾再彙總
                    if download_minute_for_daily and time_diff < 1440:
                        st.sidebar.caption(f"⚙️ 過濾{session}時段並彙總為日K...")
                        
                        # 過濾時段
                        hours = df.index.hour
                        minutes = df.index.minute
                        
                        if session == "日盤":
                            # 日盤：08:45 - 13:45（包含13:45收盤）
                            mask = ((hours == 8) & (minutes >= 45)) | \
                                   ((hours >= 9) & (hours < 13)) | \
                                   ((hours == 13) & (minutes <= 45))
                        else:  # 夜盤
                            # 夜盤：15:00 - 05:00
                            mask = (hours >= 15) | (hours < 5)
                        
                        df = df[mask]
                        
                        if df.empty:
                            st.sidebar.warning(f"⚠️ {session}時段無數據")
                            return None
                        
                        st.sidebar.caption(f"✅ 過濾後: {len(df)} 筆{session}分鐘K")
                        
                        # 顯示過濾後的日期範圍（調試用）
                        if len(df) > 0:
                            first_date = df.index[0].date()
                            last_date = df.index[-1].date()
                            first_time = df.index[0].strftime('%H:%M')
                            last_time = df.index[-1].strftime('%H:%M')
                            st.sidebar.caption(f"📅 日期範圍: {first_date} ~ {last_date}")
                            st.sidebar.caption(f"⏰ 時間範圍: {first_time} ~ {last_time}")
                        
                        # 彙總為日K
                        df['Date'] = df.index.date
                        df_grouped = df.groupby('Date').agg({
                            'Open': 'first',
                            'High': 'max',
                            'Low': 'min',
                            'Close': 'last',
                            'Volume': 'sum'
                        })
                        
                        # 顯示每日的開高低收（調試用）
                        if len(df_grouped) > 0:
                            st.sidebar.caption("📊 最近3日數據：")
                            for date_val in df_grouped.index[-3:]:
                                row = df_grouped.loc[date_val]
                                st.sidebar.caption(f"{date_val}: 開{row['Open']:.0f} 高{row['High']:.0f} 低{row['Low']:.0f} 收{row['Close']:.0f}")
                        
                        df = df_grouped
                        
                        # 將日期索引轉換回 DatetimeIndex
                        df.index = pd.to_datetime(df.index)
                        # 檢查是否已有時區
                        if df.index.tz is None:
                            df.index = df.index.tz_localize('Asia/Taipei')
                        else:
                            df.index = df.index.tz_convert('Asia/Taipei')
                        
                        st.sidebar.caption(f"✅ 彙總後: {len(df)} 筆{session}日K")
                    
                    elif interval == "1d" and time_diff < 1440:
                        # 全盤模式的日K（不過濾時段）
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
                    elif interval == "5m" and time_diff < 5:
                        df = df.resample('5min').agg({
                            'Open': 'first',
                            'High': 'max',
                            'Low': 'min',
                            'Close': 'last',
                            'Volume': 'sum'
                        }).dropna()
                        st.sidebar.caption(f"✅ 重採樣後: {len(df)} 筆5分K")
                    elif interval == "1m" and time_diff > 1:
                        # 如果API返回的不是1分K（例如5分K），但用戶要1分K
                        st.sidebar.warning(f"⚠️ API返回{time_diff:.0f}分K，無法轉換為1分K（數據不足）")
                
                # 3. 獲取即時報價並更新最後一根K棒（非日K才需要）
                if interval != "1d" and len(df) > 0:
                    try:
                        # 使用 snapshots 獲取最新報價
                        snapshot = _api.snapshots([contract])
                        if snapshot and len(snapshot) > 0:
                            latest_price = snapshot[0].close
                            if latest_price and latest_price > 0:
                                # 更新最後一根K棒（模擬進行中的K棒）
                                last_idx = df.index[-1]
                                
                                # 如果最新價格高於最高價，更新最高價
                                if latest_price > df.loc[last_idx, 'High']:
                                    df.loc[last_idx, 'High'] = latest_price
                                
                                # 如果最新價格低於最低價，更新最低價
                                if latest_price < df.loc[last_idx, 'Low']:
                                    df.loc[last_idx, 'Low'] = latest_price
                                
                                # 更新收盤價為最新價格
                                df.loc[last_idx, 'Close'] = latest_price
                                
                                st.sidebar.caption(f"⚡ 即時價格: {latest_price:.0f} (已更新至最後一根K棒)")
                    except Exception as snapshot_error:
                        st.sidebar.caption(f"⚠️ 無法獲取即時報價: {str(snapshot_error)[:50]}")
                
                # 4. 合併快取數據和新數據
                if cached_df is not None and not cached_df.empty:
                    original_len = len(df)
                    df = merge_data(cached_df, df)
                    st.sidebar.caption(f"🔄 合併快取: {original_len} 筆新 + {len(cached_df)} 筆舊 = {len(df)} 筆")
                
                # 5. 儲存到快取
                save_cache(df, product, interval, session)
                
                return df
            except Exception as e:
                st.error(f"❌ 資料轉換失敗: {e}")
                # 如果處理失敗但有快取，返回快取數據
                if cached_df is not None and not cached_df.empty:
                    st.sidebar.warning("⚠️ 使用快取數據")
                    return cached_df
                return None
        else:
            st.warning("⚠️ Shioaji 未返回數據")
            # 如果 API 失敗但有快取，返回快取數據
            if cached_df is not None and not cached_df.empty:
                st.sidebar.warning("⚠️ API 失敗，使用快取數據")
                return cached_df
            return None
            
    except Exception as e:
        st.error(f"❌ Shioaji 數據獲取失敗: {e}")
        # 如果失敗但有快取，返回快取數據
        if 'cached_df' in locals() and cached_df is not None and not cached_df.empty:
            st.sidebar.warning("⚠️ 發生錯誤，使用快取數據")
            return cached_df
        return None

@st.cache_data(ttl=60)
# ============================================================
# Yahoo Finance 相關函數已移除，改用純 Shioaji TXF 架構
# ============================================================

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
    df = df.copy()  # 避免 SettingWithCopyWarning
    df.loc[:, 'MA10'] = df['Close'].rolling(window=10).mean()
    df.loc[:, 'MA20'] = df['Close'].rolling(window=20).mean()
    
    return df

# 主要數據獲取函數
def get_data(interval, product, session, use_shioaji=False, api_instance=None):
    """
    統一的數據獲取接口，具備容錯機制
    
    參數:
        interval (str): K 線週期
        product (str): 商品名稱
        session (str): 交易時段
        use_shioaji (bool): 是否使用 Shioaji API
        api_instance: Shioaji API 實例（如果使用 Shioaji）
    
    返回:
        tuple: (DataFrame, 資料來源名稱, 是否為即時數據)
    """
    df = None
    data_source = ""
    is_realtime = False
    
    # 檢查市場狀態
    market_status_text, market_is_open, market_session = get_market_status()
    
    # 僅使用 Shioaji TXF
    if use_shioaji and api_instance is not None:
        st.sidebar.info("🔄 使用 Shioaji API 獲取 TXF 數據...")
        df = get_data_from_shioaji(api_instance, interval, product, session)
        
        if df is not None and not df.empty:
            data_source = "Shioaji (TXF)"
            is_realtime = market_is_open  # 開盤時為即時數據
            st.sidebar.success(f"✅ Shioaji TXF 數據獲取成功")
        else:
            st.sidebar.error("❌ Shioaji TXF 數據獲取失敗")
            df = None
            data_source = None
            is_realtime = False
    else:
        st.sidebar.error("❌ 請先登入 Shioaji")
        df = None
        data_source = None
        is_realtime = False
    
    # 最後的保險：確保有數據
    if df is None or df.empty:
        st.sidebar.error("❌ 無法獲取 TXF 數據")
        return None, "無可用數據", False
    
    # 處理數據並計算技術指標
    processed_df = process_kline_data(df, interval, session)
    
    if processed_df is None or processed_df.empty:
        st.sidebar.error("❌ 數據處理失敗")
        return None, data_source, is_realtime
    
    return processed_df, data_source, is_realtime

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
    df, data_source, is_realtime = get_data(interval_option, product_option, session_option, use_shioaji_flag, api_instance)
else:
    df, data_source, is_realtime = get_data(interval_option, product_option, session_option, use_shioaji_flag)

# 顯示數據來源和數據量資訊
if df is not None and not df.empty:
    original_count = len(df)
    
    # 顯示數據來源
    st.sidebar.caption(f"📊 數據來源: {data_source}")
    
    # 根據是否為即時數據顯示不同訊息
    if is_realtime:
        st.sidebar.success(f"✅ 已載入 {original_count} 筆 {interval_option} K線數據 [即時]")
    else:
        st.sidebar.info(f"📚 已載入 {original_count} 筆 {interval_option} K線數據 [歷史]")
    
    # 如果數據量少於預期，顯示提示（但不是警告）
    expected_counts = {
        "1d": 20,    # 期貨合約約1個月
        "60m": 100,  # 約1週的小時K
        "30m": 200,  # 約1週的30分K
        "15m": 400   # 約1週的15分K
    }
    expected = expected_counts.get(interval_option, 50)
    if original_count < expected * 0.3:  # 如果少於預期的30%
        st.sidebar.caption(f"ℹ️ 提示: 單一期貨合約數據有限，如需更多歷史數據請使用 Yahoo Finance")
else:
    st.sidebar.error("❌ 數據獲取失敗")
    st.sidebar.info("💡 建議: 取消勾選 Shioaji 改用 Yahoo Finance 歷史數據")

# 根據使用者設定的最大K棒數限制資料量
# 策略：先多取 20 筆用於 MA 計算，計算完後再裁切
if df is not None:
    original_count = len(df)
    
    # 計算所需的最大窗口（MA20 需要 20 筆）
    ma_window = 20
    
    # 如果數據量大於需要顯示的數量，先保留足夠計算 MA 的數據
    if original_count > max_kbars:
        # 裁切前顯示原始數據量
        st.sidebar.info(f"📊 原始數據: {original_count} 筆")
        
        # 取最後 (max_kbars + ma_window) 筆，確保 MA 計算完整
        needed_for_ma = max_kbars + ma_window
        if original_count >= needed_for_ma:
            df_for_calc = df.tail(needed_for_ma)
            st.sidebar.caption(f"⚙️ 計算用數據: {len(df_for_calc)} 筆 (含 MA 緩衝)")
        else:
            df_for_calc = df
            st.sidebar.caption(f"⚙️ 使用全部 {len(df_for_calc)} 筆數據計算")
        
        # 重新計算 MA（確保完整）
        df_for_calc = df_for_calc.copy()
        df_for_calc['MA10'] = df_for_calc['Close'].rolling(window=10).mean()
        df_for_calc['MA20'] = df_for_calc['Close'].rolling(window=20).mean()
        
        # 最後只取需要顯示的部分
        df = df_for_calc.tail(max_kbars)
        st.sidebar.info(f"📊 圖表顯示最新 {len(df)}/{original_count} 筆 (滑桿限制: {max_kbars})")
    else:
        # 數據量不足，全部顯示
        st.sidebar.info(f"📊 圖表顯示全部 {len(df)} 筆數據 (滑桿設定: {max_kbars})")
    
    # 顯示當前顯示的數據範圍
    if len(df) > 0:
        first_date = df.index[0].strftime('%Y-%m-%d') if hasattr(df.index[0], 'strftime') else str(df.index[0])
        last_date = df.index[-1].strftime('%Y-%m-%d') if hasattr(df.index[-1], 'strftime') else str(df.index[-1])
        st.sidebar.caption(f"📅 顯示範圍: {first_date} ~ {last_date}")

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
        hovertext=date_labels, # 懸停時顯示日期
        hovertemplate='<b>%{text}</b><br>' +
                      '開盤: %{open:.0f}<br>' +
                      '最高: %{high:.0f}<br>' +
                      '最低: %{low:.0f}<br>' +
                      '收盤: %{close:.0f}<br>' +
                      '<extra></extra>'  # 移除次要資訊框
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
            hovertext=date_labels,
            hovertemplate='<b>%{text}</b><br>MA10: %{y:.0f}<extra></extra>'
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
            hovertext=date_labels,
            hovertemplate='<b>%{text}</b><br>MA20: %{y:.0f}<extra></extra>'
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
    
    # 顯示自動更新提示與即時數據
    # 顯示數據類型與最後更新時間
    taipei_tz = pytz.timezone('Asia/Taipei')
    update_time = datetime.now(taipei_tz).strftime('%Y-%m-%d %H:%M:%S')
    
    col_status1, col_status2, col_status3 = st.columns(3)
    col_status1.info(f"📊 數據來源: {data_source}")
    
    if is_realtime:
        col_status2.success(f"🟢 即時數據")
    else:
        col_status2.info(f"📚 歷史數據")
    
    col_status3.caption(f"🕐 更新: {update_time}")
    
    # 自動刷新邏輯（只在即時模式啟用）
    if auto_refresh and refresh_interval and is_realtime:
        # 顯示倒數計時
        countdown_placeholder = st.empty()
        
        for remaining in range(refresh_interval, 0, -1):
            countdown_placeholder.info(f"⏱️ 下次更新倒數: {remaining} 秒")
            time.sleep(1)
        
        countdown_placeholder.success("🔄 正在更新...")
        time.sleep(0.5)
        st.rerun()
    elif auto_refresh and not is_realtime:
        st.info("ℹ️ 當前為歷史數據，自動刷新已暫停")

else:
    # ------------------------------------------------------------
    # 當數據獲取失敗時顯示錯誤訊息
    # ------------------------------------------------------------
    st.error("❌ 目前無法獲取數據")
    st.warning("💡 建議操作：")
    st.info("1️⃣ 取消勾選「使用 Shioaji 即時數據」改用 Yahoo Finance\n2️⃣ 檢查 Shioaji 登入狀態\n3️⃣ 確認網路連線正常")
    st.caption("📝 即使非交易時段，Yahoo Finance 仍可提供歷史K線數據")

# ============================================================
# 程式結束
# ============================================================
# %% 記號用於 Jupyter/IPython 環境中區分代碼區塊

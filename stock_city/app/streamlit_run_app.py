"""
台指期程式交易看盤室 - 股票城市
=====================================================
本程式提供台指期貨、台積電和台灣加權指數的 K 線圖表分析工具
支援多時段切換（日盤/夜盤/全盤）、多週期 K 線（1分-日線）
並包含移動平均線（MA20/MA60）技術指標

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
import math
import pytz  # 時區處理庫，用於處理不同時區的時間
import time  # 時間處理，用於自動刷新
import pickle  # 序列化工具，用於資料快取
import os  # 檔案系統操作

try:
    from streamlit_autorefresh import st_autorefresh
except Exception:  # pragma: no cover
    st_autorefresh = None
from stock_city.db.tick_database import (
    get_kbars_from_db,
    save_tick,
    save_ticks_batch,
    init_database,
    get_latest_tick_timestamp,
)  # Ticks database 模組

from stock_city.project_paths import get_db_path
from stock_city.strategy.ma20_ma60 import calculate_ma_trend_engulfing_signals
import sqlite3

# ============================================================
# 1. 頁面初始化設定與 Shioaji 連線
# ============================================================
# 設定頁面配置：使用寬版面並自訂標題
st.set_page_config(layout="wide", page_title="台指期程式交易看盤室")

# 顯示主標題
st.title("📈 台指期全盤 K線圖 (含 20MA/60MA)")

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
    
    # 週末判斷：
    # - 週日一定休市
    # - 週六 00:00~05:00 仍可能是「週五夜盤」延續（需要視為開盤）
    if current_weekday == 6:
        return "🚫 週末休市", False, "休市"
    
    # 轉換為總分鐘數以便比較
    current_time = current_hour * 60 + current_minute
    
    # 日盤時間: 08:45 - 13:45
    day_start = 8 * 60 + 45   # 525
    day_end = 13 * 60 + 45    # 825
    
    # 夜盤時間: 15:00 - 05:00 (次日)
    night_start = 15 * 60     # 900
    night_end = 5 * 60        # 300
    
    # 判斷當前時段（需考慮週末跨日夜盤）
    # 日盤：僅週一~週五
    if day_start <= current_time <= day_end and current_weekday < 5:
        return "🟢 日盤交易中", True, "日盤"

    # 夜盤：
    # - 15:00~23:59 僅週一~週五
    # - 00:00~05:00 僅週二~週六（屬於前一個工作日的夜盤延續）
    if current_time >= night_start and current_weekday < 5:
        return "🌙 夜盤交易中", True, "夜盤"
    if current_time <= night_end and 1 <= current_weekday <= 5:
        return "🌙 夜盤交易中", True, "夜盤"

    # 其他時間視為休息/休市
    if current_weekday >= 5:
        return "🚫 週末休市", False, "休市"
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
        st.session_state["use_shioaji_checkbox"] = use_shioaji  # 儲存到 session state
        
        # 重要提示
        if use_shioaji:
            st.info("💡 **Shioaji 多合約拼接功能**\n- 自動拼接所有可用期貨合約數據\n- 首次載入可能需要較長時間")
        
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
                st.caption("🔐 建議用環境變數或 Streamlit secrets 設定：SHIOAJI_API_KEY / SHIOAJI_SECRET_KEY（避免把金鑰寫進程式）")

                default_api_key = ""
                default_secret_key = ""
                try:
                    default_api_key = (
                        st.secrets.get("SHIOAJI_API_KEY", "")
                        or st.secrets.get("shioaji", {}).get("api_key", "")
                    )
                    default_secret_key = (
                        st.secrets.get("SHIOAJI_SECRET_KEY", "")
                        or st.secrets.get("shioaji", {}).get("secret_key", "")
                    )
                except Exception:
                    default_api_key = ""
                    default_secret_key = ""

                default_api_key = default_api_key or os.getenv("SHIOAJI_API_KEY", "")
                default_secret_key = default_secret_key or os.getenv("SHIOAJI_SECRET_KEY", "")
                api_key = st.text_input(
                    "API Key",
                    type="password",
                    value=default_api_key,
                    help="永豐證券提供的 API Key",
                )
                secret_key = st.text_input(
                    "Secret Key",
                    type="password",
                    value=default_secret_key,
                    help="永豐證券提供的 Secret Key",
                )
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
                                    st.info("✓ 已啟用多合約拼接功能，可獲取完整歷史數據")
                                    st.session_state['shioaji_logged_in'] = True
                                    st.session_state['shioaji_api'] = new_api
                                    st.rerun()
                                else:
                                    error_str = str(error) if error else '未知錯誤'
                                    st.session_state['shioaji_logged_in'] = False
                                    
                                    # 自動取消勾選，改用 DB 模式
                                    st.session_state["use_shioaji_checkbox"] = False
                                    
                                    st.error(f"❌ 登入失敗，已自動切換至本地數據庫")
                                    st.caption(f"📋 錯誤信息: {error_str}")
                                    
                                    # 針對不同錯誤提供解決方案
                                    if 'Sign data is timeout' in error_str:
                                        st.warning("🕐 **證書簽名超時 (Sign data is timeout)**")
                                        st.info(
                                            "✅ 系統已自動改用本地 SQLite 資料庫，可繼續使用所有分析功能\n\n"
                                            "**如需啟用 Shioaji 即時數據，請解決以下問題：**\n"
                                            "1️⃣ **檢查系統時間** - 確保與網路時間同步（可能差超過30秒）\n"
                                            "2️⃣ **重新下載憑證** - 到永豐證券官網重新下載最新 .pfx 文件\n"
                                            "3️⃣ **使用 API Key 登入** - 改用 API Key 和 Secret Key 方式登入\n"
                                            "4️⃣ **稍後再試** - 等 2-3 分鐘後，可能伺服器暫時繁忙\n"
                                            "5️⃣ **聯繫客服** - 若問題持續，請聯繫永豐證券客服"
                                        )
                                    elif '連線數過多' in error_str or 'Too Many Connections' in error_str:
                                        st.warning("🔗 **連線數過多**")
                                        st.info(
                                            "✅ 系統已自動改用本地 SQLite 資料庫\n\n"
                                            "**如需解除連線限制：**\n"
                                            "1️⃣ 點擊「🔄 強制重置」按鈕清除舊連線\n"
                                            "2️⃣ 等待 1-2 分鐘讓舊連線逾時\n"
                                            "3️⃣ 確認沒有其他程式或瀏覽器分頁在使用 Shioaji\n"
                                            "4️⃣ 聯繫永豐證券客服重置帳號連線數"
                                        )
                                    else:
                                        st.warning("🔍 **登入驗證失敗**")
                                        st.info(
                                            "✅ 系統已自動改用本地 SQLite 資料庫\n\n"
                                            "**檢查事項：**\n"
                                            "• 身分證字號是否輸入正確\n"
                                            "• 憑證密碼是否正確\n"
                                            "• 憑證文件 (Sinopac.pfx) 是否存在且未損壞"
                                        )
                                    
                                    st.success(
                                        "💡 **已切換至本地數據庫**\n"
                                        "✓ 可查看 300 天的歷史 K 線數據\n"
                                        "✓ 所有 MA 均線和策略分析功能可用\n"
                                        "✓ 刷新頁面後立即生效\n\n"
                                        "**如後續想使用 Shioaji 即時數據，解決上述問題後重新勾選「使用 Shioaji」即可**"
                                    )
                            except Exception as e:
                                st.session_state['shioaji_logged_in'] = False
                                st.session_state["use_shioaji_checkbox"] = False  # 自動取消勾選
                                
                                st.error(f"❌ 登入異常，已自動切換至本地數據庫")
                                st.caption(f"📋 錯誤信息: {str(e)}")
                                st.warning("🔍 **檢查您的登入憑證**")
                                st.info(
                                    "✅ 系統已自動改用本地 SQLite 資料庫\n\n"
                                    "**檢查事項：**\n"
                                    "• API Key 是否正確複製\n"
                                    "• Secret Key 是否正確複製\n"
                                    "• 是否有多餘的空格或特殊字符\n\n"
                                    "💡 如需使用 Shioaji 即時數據，請確認憑證後重新嘗試"
                                )
                                st.success(
                                    "✅ **已切換至本地數據庫**\n"
                                    "刷新頁面後即可使用 300 天歷史數據和所有分析功能"
                                )
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

    # 根據是否登入 Shioaji 設定預設 K 線條件
    logged_in = st.session_state.get("shioaji_logged_in", False) and "shioaji_api" in st.session_state

    if logged_in:
        # 已登入 Shioaji：日盤 + 5 分 K + 150 筆
        default_interval_index = 1  # "5m"
        default_session_index = 0   # "日盤"
        default_kbars = 150
    else:
        # 未登入：日盤 + 日K + 100 筆
        default_interval_index = 5  # "1d"
        default_session_index = 0   # "日盤"
        default_kbars = 100

    # ------------------------------------------------------------
    # 3.4 K線週期選擇（提前，因為會影響時段選擇）
    # ------------------------------------------------------------
    # 支援從 1 分鐘到日線的多種時間週期
    interval_option = st.selectbox(
        "選擇 K 線週期",
        ("1m", "5m", "15m", "30m", "60m", "1d"),
        index=default_interval_index
    )

    # ------------------------------------------------------------
    # 3.3 交易時段選擇
    # ------------------------------------------------------------
    # 全盤：顯示所有交易時段
    # 日盤：08:45 - 13:45
    # 夜盤：15:00 - 次日 05:00
    session_option = st.selectbox(
        "選擇時段",
        ("日盤", "夜盤", "全盤"),
        index=default_session_index
    )

    # ------------------------------------------------------------
    # 3.5 最大K棒數量滑桿
    # ------------------------------------------------------------
    # 限制圖表顯示的 K 棒數量，避免資料過多導致效能問題
    # 範圍：20-1000 根
    max_kbars = st.slider(
        "顯示K棒數量",
        min_value=20,
        max_value=1000,
        value=default_kbars,
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

        lightweight_update = st.checkbox(
            "輕量更新（只更新最新K棒）",
            value=True,
            help="開盤自動刷新時只更新最新一根 K 棒（用 snapshots 最新價），減少整張圖重繪造成的閃爍。",
        )
        st.session_state["lightweight_update"] = bool(lightweight_update)

        if st.button("🧹 重置輕量更新快取", use_container_width=True):
            st.session_state.pop("light_cache_key", None)
            st.session_state.pop("light_cache_df", None)
            st.session_state.pop("light_cache_data_source", None)
            st.session_state.pop("light_cache_is_realtime", None)
            st.success("✅ 已重置快取")
        
        if auto_refresh:
            refresh_interval = st.slider(
                "刷新間隔（秒）",
                min_value=1,
                max_value=60,
                value=3,  # 預設3秒更新（降低閃爍/負載）
                step=1,
                help="設定圖表自動更新的時間間隔"
            )
            st.success(f"✅ 自動刷新已啟用，每 {refresh_interval} 秒更新一次")
        else:
            refresh_interval = None
            st.info("ℹ️ 手動模式：點擊重新載入按鈕以更新數據")
    
    # 顯示提示訊息
    st.caption("💡 提示：啟用自動刷新可獲得動態K棒更新效果。")
    
    st.divider()  # 分隔線
    
    # ============================================================
    # 3.7 策略設定
    # ============================================================
    with st.expander("🎯 MA交叉吞噬策略", expanded=False):
        enable_strategy = st.checkbox(
            "啟用策略信號",
            value=False,
            help="啟用後將在圖表上標示進場/退場信號，並顯示交易紀錄"
        )
        st.session_state["enable_strategy"] = enable_strategy
        
        if enable_strategy:
            strategy_type = st.selectbox(
                "選擇策略類型",
                ("MA交叉吞噬策略",),  # 未來可擴展更多策略
                help="MA交叉吞噬策略：檢測MA20/MA60都向上趨勢時，在碰MA且下一根吞噬時進場"
            )
            st.session_state["strategy_type"] = strategy_type
            st.info(
                "📌 **策略規則**\n\n"
                "• **進場**：MA20+MA60都向上 → 前一根觸及MA20 → 下一根吞噬 → 進場\n"
                "• **加碼**：最新K棒吞噬前一根\n"
                "• **做空**：反向邏輯（趨勢向下 → 碰MA → 反向吞噬）\n"
                "• **退場**：相反信號出現時清倉"
            )

    # 顯示提示訊息
    st.caption("💡 提示：啟用自動刷新可獲得動態K棒更新效果。")

    # ------------------------------------------------------------
    # 3.7 DB 存量提示（各時段日K根數）
    # ------------------------------------------------------------
    @st.cache_data(ttl=60, show_spinner=False)
    def get_db_dayk_inventory(days: int = 2000):
        """讀取 SQLite DB 估算各時段日K存量。

        用途：讓使用者快速判斷為何「顯示K棒數量」滑桿對某些時段拉不動（通常是 DB 歷史不足）。
        """
        try:
            init_database()
            result = {}
            for s in ("日盤", "夜盤", "全盤"):
                df_1d = get_kbars_from_db(interval="1d", session=s, days=days)
                if df_1d is None or df_1d.empty:
                    result[s] = {"count": 0, "start": None, "end": None}
                else:
                    result[s] = {
                        "count": int(len(df_1d)),
                        "start": df_1d.index.min(),
                        "end": df_1d.index.max(),
                    }
            return result
        except Exception:
            return {
                "日盤": {"count": 0, "start": None, "end": None},
                "夜盤": {"count": 0, "start": None, "end": None},
                "全盤": {"count": 0, "start": None, "end": None},
            }

    @st.cache_data(ttl=60, show_spinner=False)
    def get_recent_dayk_gaps(session, days_back=10):
        """自動檢查最近幾個交易日是否有日K缺口（依時段門檻）。"""
        try:
            taipei_tz = pytz.timezone('Asia/Taipei')
            today = datetime.now(taipei_tz).date()

            from stock_city.market.settlement_utils import is_workday

            def get_window(d, sess):
                if sess == "日盤":
                    start_local = taipei_tz.localize(datetime(d.year, d.month, d.day, 8, 45, 0))
                    end_local = taipei_tz.localize(datetime(d.year, d.month, d.day, 13, 46, 0))
                    threshold = 250
                elif sess == "夜盤":
                    start_local = taipei_tz.localize(datetime(d.year, d.month, d.day, 15, 0, 0))
                    end_local = start_local + timedelta(hours=15)
                    threshold = 400
                else:  # 全盤
                    start_local = taipei_tz.localize(datetime(d.year, d.month, d.day, 0, 0, 0))
                    end_local = start_local + timedelta(days=1, hours=6)
                    threshold = 600
                return start_local, end_local, threshold

            db_path = get_db_path()
            gaps = []

            conn = sqlite3.connect(str(db_path))
            cur = conn.cursor()

            checked = 0
            i = 1
            while checked < days_back and i <= days_back * 3:
                d = today - timedelta(days=i)
                i += 1
                if not is_workday(d):
                    continue
                checked += 1
                start_local, end_local, threshold = get_window(d, session)
                start_utc = start_local.astimezone(pytz.UTC).isoformat()
                end_utc = end_local.astimezone(pytz.UTC).isoformat()
                cur.execute(
                    "SELECT COUNT(*) FROM ticks WHERE code=? AND ts >= ? AND ts < ?",
                    ("TXFR1", start_utc, end_utc),
                )
                cnt = int(cur.fetchone()[0] or 0)
                if cnt < threshold:
                    gaps.append({
                        "date": d,
                        "count": cnt,
                        "threshold": threshold,
                    })

            conn.close()
            return gaps
        except Exception:
            return []

    def manual_backfill_recent_dayk(api_instance, session, days_back=10):
        """手動回填最近幾個交易日的 1 分 K（用於日K彙總），優先修補日盤缺口。

        說明：
        - 僅在已登入 Shioaji 時使用。
        - 根據 session 決定時間窗與筆數門檻：
            日盤：約 08:45-13:45，門檻 250 筆以上；
            夜盤：約 15:00-05:00，門檻 400 筆以上；
            全盤：00:00-隔日 06:00，門檻 600 筆以上。
        - 若發現某個工作日筆數不足，會刪除該窗現有 ticks，
          再用 api.kbars 回填該日期範圍，並只保留對應時段。
        """
        if api_instance is None:
            st.sidebar.warning("⚠️ 尚未登入 Shioaji，無法回填日K 資料")
            return

        taipei_tz = pytz.timezone('Asia/Taipei')
        today = datetime.now(taipei_tz).date()

        from stock_city.market.settlement_utils import is_workday

        def get_window(d, sess):
            if sess == "日盤":
                start_local = taipei_tz.localize(datetime(d.year, d.month, d.day, 8, 45, 0))
                end_local = taipei_tz.localize(datetime(d.year, d.month, d.day, 13, 46, 0))
                threshold = 250
            elif sess == "夜盤":
                start_local = taipei_tz.localize(datetime(d.year, d.month, d.day, 15, 0, 0))
                end_local = start_local + timedelta(hours=15)
                threshold = 400
            else:  # 全盤
                start_local = taipei_tz.localize(datetime(d.year, d.month, d.day, 0, 0, 0))
                end_local = start_local + timedelta(days=1, hours=6)
                threshold = 600
            return start_local, end_local, threshold

        db_path = get_db_path()
        to_fill = []

        try:
            conn = sqlite3.connect(str(db_path))
            cur = conn.cursor()

            # 往回掃描最近 days_back 個工作日（不含今日），找出筆數不足的日期
            for i in range(1, days_back + 1):
                d = today - timedelta(days=i)
                if not is_workday(d):
                    continue
                start_local, end_local, threshold = get_window(d, session)
                start_utc = start_local.astimezone(pytz.UTC).isoformat()
                end_utc = end_local.astimezone(pytz.UTC).isoformat()
                cur.execute(
                    "SELECT COUNT(*) FROM ticks WHERE code=? AND ts >= ? AND ts < ?",
                    ("TXFR1", start_utc, end_utc),
                )
                cnt = int(cur.fetchone()[0] or 0)
                if cnt < threshold:
                    to_fill.append((d, start_local, end_local))

            conn.close()
        except Exception as e:
            st.sidebar.warning(f"⚠️ 檢查最近日K 狀態失敗: {str(e)[:120]}")
            return

        if not to_fill:
            st.sidebar.info("ℹ️ 最近幾個交易日的日K 已經補齊，無需回填")
            return

        # 以最小/最大日期決定一次拉取的 kbars 範圍
        dates_only = [d for d, _, _ in to_fill]
        range_start = min(dates_only)
        range_end = max(dates_only) + timedelta(days=1)
        start = range_start.strftime("%Y-%m-%d")
        end = range_end.strftime("%Y-%m-%d")

        contract = api_instance.Contracts.Futures.TXF.TXFR1
        st.sidebar.warning(
            f"🧩 手動回填：準備回填 {len(to_fill)} 個交易日的 {session} 1分K（區間 {start}~{end}）..."
        )

        try:
            kbars = api_instance.kbars(contract=contract, start=start, end=end)
        except Exception as e:
            st.sidebar.warning(f"⚠️ Shioaji kbars 請求失敗: {str(e)[:120]}")
            return

        if not kbars:
            st.sidebar.warning("⚠️ API 未返回數據，無法回填")
            return

        df_all = pd.DataFrame({**kbars})
        if df_all.empty:
            st.sidebar.warning("⚠️ API 返回空數據，無法回填")
            return

        df_all["ts"] = pd.to_datetime(df_all["ts"])
        df_all = df_all.rename(columns={"ts": "datetime"}).sort_values("datetime").reset_index(drop=True)
        df_all = df_all.set_index("datetime").sort_index()

        def filter_for_session(df_in, target_date, sess):
            idx = df_in.index
            next_date = target_date + timedelta(days=1)
            if sess == "日盤":
                hours = idx.hour
                minutes = idx.minute
                dates = idx.date
                mask = (dates == target_date) & (
                    ((hours == 8) & (minutes >= 45))
                    | ((hours >= 9) & (hours < 13))
                    | ((hours == 13) & (minutes <= 45))
                )
                return df_in.loc[mask]
            if sess == "夜盤":
                mask = ((idx.date == target_date) & (idx.hour >= 15)) | (
                    (idx.date == next_date)
                    & (
                        (idx.hour < 5)
                        | ((idx.hour == 5) & (idx.minute == 0))
                    )
                )
                return df_in.loc[mask]
            # 全盤
            mask = (idx.date == target_date) | (
                (idx.date == next_date)
                & (
                    (idx.hour < 5)
                    | ((idx.hour == 5) & (idx.minute == 0))
                )
            )
            return df_in.loc[mask]

        # 實際寫回 DB
        total_saved_days = 0
        for d, start_local, end_local in to_fill:
            try:
                # 先刪除該窗內既有 ticks，避免舊資料殘留
                start_utc = start_local.astimezone(pytz.UTC).isoformat()
                end_utc = end_local.astimezone(pytz.UTC).isoformat()
                conn = sqlite3.connect(str(db_path))
                cur = conn.cursor()
                cur.execute(
                    "DELETE FROM ticks WHERE code=? AND ts >= ? AND ts < ?",
                    ("TXFR1", start_utc, end_utc),
                )
                conn.commit()
                conn.close()

                df_new = filter_for_session(df_all, d, session)
                if df_new is None or df_new.empty:
                    continue

                batch_ticks = []
                for idx, row in df_new.iterrows():
                    if idx.tzinfo is None:
                        idx = taipei_tz.localize(idx)
                    else:
                        idx = idx.tz_convert(taipei_tz)
                    batch_ticks.append(
                        {
                            'ts': idx,
                            'code': contract.code,
                            'open': row.get('Open', row.get('Close', 0)),
                            'high': row.get('High', row.get('Close', 0)),
                            'low': row.get('Low', row.get('Close', 0)),
                            'close': row.get('Close', 0),
                            'volume': row.get('Volume', 0),
                            'bid_price': row.get('Close', 0),
                            'ask_price': row.get('Close', 0),
                            'bid_volume': 0,
                            'ask_volume': 0,
                        }
                    )

                if batch_ticks:
                    save_ticks_batch(batch_ticks)
                    total_saved_days += 1
            except Exception as e:
                st.sidebar.warning(f"⚠️ 回填 {d} 失敗: {str(e)[:120]}")

        if total_saved_days > 0:
            st.sidebar.success(f"✅ 手動回填完成：新增/更新 {total_saved_days} 個交易日的 {session} 1分K（用於日K彙總）")
            try:
                get_db_dayk_inventory.clear()
            except Exception:
                pass
        else:
            st.sidebar.info("ℹ️ 本次未回填到任何交易日，可能 API 範圍內無有效數據")
    
    # 顯示當前設定摘要
    st.info(f"📊 **當前設定**\n- 商品: {product_option}\n- 時段: {session_option}\n- 週期: {interval_option}\n- K棒數: {max_kbars}\n- 自動刷新: {'✅ 啟用' if auto_refresh else '❌ 停用'}")

    # 若為日K，立即顯示「選定時段」的 DB 日K存量，讓滑桿行為更直觀
    if interval_option == "1d":
        inv_now = get_db_dayk_inventory()
        sel = inv_now.get(session_option, {})
        sel_count = int(sel.get("count", 0) or 0)
        sel_start = sel.get("start")
        sel_end = sel.get("end")

        if sel_start is not None and sel_end is not None:
            st.caption(f"🗄️ DB {session_option} 日K存量：{sel_count} 根（{sel_start.date()} ~ {sel_end.date()}）")
        else:
            st.caption(f"🗄️ DB {session_option} 日K存量：{sel_count} 根")

        if max_kbars > sel_count and sel_count > 0:
            st.warning(
                f"⚠️ 你設定要顯示 {max_kbars} 根，但 DB 目前只有 {sel_count} 根，所以圖表不會再變多。\n"
                f"✅ 請先回填：`python backfill_kbars.py --days 500 --session {session_option} --skip-existing`"
            )

        gaps = get_recent_dayk_gaps(session_option, days_back=10)
        if gaps:
            st.warning(f"⚠️ 最近 10 個交易日偵測到 {len(gaps)} 個{session_option}日K缺口")
            gap_list = ", ".join(
                [f"{g['date']}({g['count']}/{g['threshold']})" for g in gaps]
            )
            st.caption(f"🧩 缺口清單：{gap_list}")
        else:
            st.caption(f"✅ 最近 10 個交易日 {session_option} 日K 無缺口")

    with st.expander("🗄️ DB 日K存量", expanded=False):
        inv = get_db_dayk_inventory()
        for s in ("日盤", "夜盤", "全盤"):
            count = inv.get(s, {}).get("count", 0)
            start = inv.get(s, {}).get("start")
            end = inv.get(s, {}).get("end")
            if start is not None and end is not None:
                st.caption(f"- {s}: {count} 根（{start.date()} ~ {end.date()}）")
            else:
                st.caption(f"- {s}: {count} 根")

        st.caption("💡 若滑桿拉到大於上面根數，顯示就不會再變多（因為 DB 沒有更多日K）。")
        st.caption("✅ 回填指令：")
        st.caption("- 日盤：`python backfill_kbars.py --days 500 --session 日盤 --skip-existing`")
        st.caption("- 夜盤：`python backfill_kbars.py --days 500 --session 夜盤 --skip-existing`")
        st.caption("- 全盤：`python backfill_kbars.py --days 500 --session 全盤 --skip-existing`")
    
    # 數據量統計區（會在獲取數據後自動更新）
    if 'data_stats' not in st.session_state:
        st.session_state['data_stats'] = {}

    # 手動強制回填最近日K（依當前選擇的時段）
    if logged_in:
        if st.button("🔁 強制回填最近日K (含日盤缺口)", use_container_width=True):
            api_instance = st.session_state.get('shioaji_api')
            if api_instance is None:
                st.warning("⚠️ 尚未登入 Shioaji，無法回填日K 資料")
            else:
                try:
                    manual_backfill_recent_dayk(api_instance, session_option, days_back=10)
                except Exception as e:
                    st.warning(f"⚠️ 手動回填日K 失敗: {str(e)[:120]}")

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
    根據使用者選擇的商品返回對應代碼（已廢棄）
    
    參數:
        product (str): 使用者選擇的商品名稱（已廢棄）
        
    返回:
        str: 已移除
    """
    return None

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
        # 注意：只包含 05:00 這一根，不包含 05:01~05:59
        mask = (hours >= 15) | (hours < 5) | ((hours == 5) & (minutes == 0))
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

def get_data_from_shioaji(_api, interval, product, session, max_kbars):
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

        # ------------------------------------------------------------
        # 每日自動更新機制（若今日資料不存在或過舊）
        # ------------------------------------------------------------
        def update_today_kbars_if_needed(api_instance):
            try:
                if api_instance is None:
                    return
                
                taipei_tz = pytz.timezone('Asia/Taipei')
                now = datetime.now(taipei_tz)
                today = now.date()

                # 夜盤判斷：15:00~隔日05:00（凌晨 00:00~05:00 的夜盤歸屬前一個交易日）
                is_night_time = (now.hour >= 15) or (now.hour < 6)
                night_trade_date = today if now.hour >= 15 else (today - timedelta(days=1))
                next_date = night_trade_date + timedelta(days=1)
                
                # 週末白天不抓取；但週末凌晨允許補齊夜盤（例如週五夜盤到週六 05:00）
                if today.weekday() >= 5 and now.hour >= 6:
                    return
                
                # 檢查資料是否已有/是否過舊
                market_status_text, market_is_open, _ = get_market_status()

                # 夜盤 / 全盤：以「夜盤交易日」判斷是否缺少 15:00~23:59 的資料，並以跨日區間的最新時間判斷是否過舊
                if session in ("夜盤", "全盤") and is_night_time:
                    latest_trade_date_ts = get_latest_tick_timestamp(code='TXFR1', date=night_trade_date)
                    latest_next_date_ts = get_latest_tick_timestamp(code='TXFR1', date=next_date)
                    latest_in_session = None
                    for t in (latest_trade_date_ts, latest_next_date_ts):
                        if t is not None:
                            latest_in_session = t if latest_in_session is None else max(latest_in_session, t)

                    missing_evening = (
                        latest_trade_date_ts is None or
                        (latest_trade_date_ts.hour < 15)
                    )

                    too_old = False
                    if market_is_open and latest_in_session is not None:
                        too_old = latest_in_session < now - timedelta(minutes=2)
                    if market_is_open and latest_in_session is None:
                        too_old = True

                    need_update = missing_evening or too_old
                else:
                    # 日盤：更嚴謹判斷「今天是否有開盤」以及「是否已補到收盤」
                    latest_ts = get_latest_tick_timestamp(code='TXFR1', date=today)
                    need_update = latest_ts is None

                    if latest_ts is not None:
                        # 將 DB 最新時間轉成台北時間，便於和 now / 收盤時間比較
                        latest_local = latest_ts
                        try:
                            if getattr(latest_local, 'tzinfo', None) is None:
                                latest_local = pytz.UTC.localize(latest_local).astimezone(taipei_tz)
                            else:
                                latest_local = latest_local.astimezone(taipei_tz)
                        except Exception:
                            latest_local = latest_ts

                        # 預期的日盤收盤時間（含結算日 13:30 也會在 13:45 前落在此區間內）
                        day_close_dt = taipei_tz.localize(datetime(today.year, today.month, today.day, 13, 45))

                        if market_is_open:
                            # 今天有開盤：要求 DB 最新時間與現在落差不得超過 2 分鐘
                            if latest_local < now - timedelta(minutes=2):
                                need_update = True
                        else:
                            # 今天已收盤或尚未開盤：
                            # 若為平日且理論上有日盤，且 DB 最新時間仍落在收盤前很早的位置，視為尚未補齊到收盤
                            if today.weekday() < 5 and latest_local < day_close_dt:
                                need_update = True
                
                if not need_update:
                    return
                
                st.sidebar.info("🔄 偵測到今日資料缺失或過舊，開始更新...")
                
                contract = api_instance.Contracts.Futures.TXF.TXFR1

                # 抓取範圍：依夜盤交易日抓取 ±1 天（可涵蓋 15:00~隔日05:00）
                base_date = night_trade_date if (session in ("夜盤", "全盤") and is_night_time) else today
                start = (base_date - timedelta(days=1)).strftime("%Y-%m-%d")
                end = (base_date + timedelta(days=1)).strftime("%Y-%m-%d")
                
                kbars = api_instance.kbars(contract=contract, start=start, end=end)
                if kbars is None:
                    st.sidebar.warning("⚠️ 今日數據抓取失敗")
                    return
                
                df = pd.DataFrame({**kbars})
                if df.empty:
                    st.sidebar.warning("⚠️ 今日數據為空")
                    return
                
                df["ts"] = pd.to_datetime(df["ts"])
                df = df.rename(columns={"ts": "datetime"}).sort_values("datetime").reset_index(drop=True)
                df = df.set_index("datetime").sort_index()

                # 過濾要保存的區間：
                # - 夜盤/全盤且在夜盤時間：保存 night_trade_date 15:00~23:59 + 隔日 00:00~05:00(含)
                # - 其他情況：保存今日資料
                if session in ("夜盤", "全盤") and is_night_time:
                    df = df[
                        ((df.index.date == night_trade_date) & (df.index.hour >= 15)) |
                        (
                            (df.index.date == next_date) &
                            (
                                (df.index.hour < 5) |
                                ((df.index.hour == 5) & (df.index.minute == 0))
                            )
                        )
                    ]
                else:
                    df = df[df.index.date == today]
                
                if df.empty:
                    st.sidebar.warning("⚠️ 今日數據過濾後為空")
                    return
                
                batch_ticks = []
                for idx, row in df.iterrows():
                    if idx.tzinfo is None:
                        idx = taipei_tz.localize(idx)
                    else:
                        idx = idx.tz_convert(taipei_tz)
                    
                    tick_data = {
                        'ts': idx,
                        'code': contract.code,
                        'open': row.get('Open', row.get('Close', 0)),
                        'high': row.get('High', row.get('Close', 0)),
                        'low': row.get('Low', row.get('Close', 0)),
                        'close': row.get('Close', 0),
                        'volume': row.get('Volume', 0),
                        'bid_price': row.get('Close', 0),
                        'ask_price': row.get('Close', 0),
                        'bid_volume': 0,
                        'ask_volume': 0,
                    }
                    batch_ticks.append(tick_data)
                
                save_ticks_batch(batch_ticks)
                st.sidebar.success(f"✅ 今日數據已更新：{len(batch_ticks)} 筆")
            except Exception as e:
                st.sidebar.warning(f"⚠️ 自動更新失敗: {str(e)}")
        
        update_today_kbars_if_needed(_api)

        # ------------------------------------------------------------
        # 根據「K棒數量 + 週期 + 時段」自動估算回溯天數
        # 目標：讓滑桿增加時，能自動帶出更多歷史資料
        # ------------------------------------------------------------
        def estimate_lookback_days(interval_value, session_value, kbars_needed):
            # 日K：每個交易日 1 根，估算需包含週末緩衝
            if interval_value == "1d":
                # 500 根日K 大約需要 700~800 個日曆天（含週末緩衝），因此上限拉高
                return min(max(int(kbars_needed * 7 / 5) + 30, 60), 1200)

            # 估算每個交易日可產生的 K 根數（粗估，足夠用於回溯天數）
            bars_per_day = {
                "1m": {"日盤": 300, "夜盤": 840, "全盤": 1140},
                "5m": {"日盤": 60, "夜盤": 168, "全盤": 228},
                "15m": {"日盤": 20, "夜盤": 56, "全盤": 76},
                "30m": {"日盤": 10, "夜盤": 28, "全盤": 38},
                "60m": {"日盤": 5, "夜盤": 14, "全盤": 19},
            }

            per_day = bars_per_day.get(interval_value, {}).get(session_value, 60)
            # 額外 +2 天緩衝，避免遇到週末或資料缺口
            days_needed = int((kbars_needed + per_day - 1) / per_day) + 2
            return min(max(days_needed, 3), 90)

        days = estimate_lookback_days(interval, session, max_kbars)
        st.sidebar.caption(f"📅 回溯天數: {days} 天（依 K棒數自動調整）")
        
        # 從 database 讀取並組成 K 棒
        df = get_kbars_from_db(interval=interval, session=session, days=days)

        # 若近期資料完全不足，改用更長回溯天數避免空資料
        if df is None or df.empty:
            fallback_days = 1200 if interval == "1d" else 300
            if days < fallback_days:
                st.sidebar.warning("⚠️ 近期資料不足，改用較長回溯天數查詢...")
                df = get_kbars_from_db(interval=interval, session=session, days=fallback_days)
                days = fallback_days

        # 若資料非空但明顯少於滑桿要求（例如僅有結算日 1 天約 57 根 5m K），
        # 代表最近幾天 DB 可能有缺口，嘗試用更長回溯天數補足可顯示 K 棒數
        if df is not None and not df.empty and len(df) < max_kbars:
            for extra_days in [30, 60, 120, 240]:
                if extra_days <= days:
                    continue
                bigger_df = get_kbars_from_db(interval=interval, session=session, days=extra_days)
                if bigger_df is None or bigger_df.empty:
                    continue
                df = bigger_df
                days = extra_days
                st.sidebar.warning(
                    f"⚠️ 最近 {days} 天內可用 K 棒不足，已自動擴大回溯天數至 {extra_days} 天以接近滑桿設定 {max_kbars} 根"
                )
                if len(df) >= max_kbars:
                    break

        # ------------------------------------------------------------
        # 自動回填：日K 時若 DB 歷史不足，且已登入 Shioaji，則自動往更早的交易日補齊
        # 說明：
        # - 只在 interval=1d 時啟用
        # - 為避免自動刷新每秒重跑，會做節流（throttle）與分批（batch）
        # ------------------------------------------------------------
        def _auto_backfill_dayk_history_if_needed(api_instance, current_df):
            try:
                if api_instance is None:
                    return False
                if interval != "1d":
                    return False

                current_count = 0 if current_df is None or current_df.empty else int(len(current_df))
                if current_count >= max_kbars:
                    return False

                # 節流：避免自動刷新時每秒重複回填（但仍要能持續補到滿）
                throttle_key = f"auto_backfill_dayk::{session}"
                last_run_ts = float(st.session_state.get(throttle_key, 0.0) or 0.0)
                last_saved = int(st.session_state.get(throttle_key + "::saved", 0) or 0)

                now_ts = time.time()
                # 一般情況：每次回填後至少等 20 秒再跑下一批，避免 API 壓力
                cooldown_seconds = 20
                if (now_ts - last_run_ts) < cooldown_seconds:
                    return False
                # 若上次完全沒補到任何交易日（可能遇到休市/已存在），拉長等待，避免空轉
                if last_saved == 0 and (now_ts - last_run_ts) < 300:
                    return False

                # 每次最多回填 N 個交易日，避免一次卡太久（可透過多次刷新逐步補齊）
                max_days_per_run = 30

                # 從目前資料最早日往前補為主，同時也檢查「最近幾個交易日」是否有缺（例如 2/25、2/26 日盤遺漏）
                taipei_tz = pytz.timezone('Asia/Taipei')
                if current_df is not None and not current_df.empty:
                    try:
                        earliest_dt = current_df.index.min()
                        if getattr(earliest_dt, 'tzinfo', None) is None:
                            earliest_dt = taipei_tz.localize(earliest_dt)
                        else:
                            earliest_dt = earliest_dt.tz_convert(taipei_tz)
                        cursor_date = earliest_dt.date() - timedelta(days=1)
                        latest_dt = current_df.index.max()
                        if getattr(latest_dt, 'tzinfo', None) is None:
                            latest_dt = taipei_tz.localize(latest_dt)
                        else:
                            latest_dt = latest_dt.tz_convert(taipei_tz)
                        latest_date = latest_dt.date()
                    except Exception:
                        cursor_date = datetime.now(taipei_tz).date()
                        latest_date = None
                else:
                    cursor_date = datetime.now(taipei_tz).date()
                    latest_date = None

                from stock_city.market.settlement_utils import is_workday

                # 用 DB 粗判斷該日是否已有足夠資料（避免重抓）
                def has_sufficient_data_local(d, sess):
                    try:
                        import sqlite3
                        db_path = get_db_path()
                        conn = sqlite3.connect(str(db_path))
                        cur = conn.cursor()

                        if sess == "日盤":
                            start_local = taipei_tz.localize(datetime(d.year, d.month, d.day, 8, 45, 0))
                            end_local = taipei_tz.localize(datetime(d.year, d.month, d.day, 13, 46, 0))
                        elif sess == "夜盤":
                            start_local = taipei_tz.localize(datetime(d.year, d.month, d.day, 15, 0, 0))
                            end_local = start_local + timedelta(hours=15)  # 到隔日 06:00 緩衝
                        else:  # 全盤
                            start_local = taipei_tz.localize(datetime(d.year, d.month, d.day, 0, 0, 0))
                            end_local = start_local + timedelta(days=1, hours=6)  # 含隔日 00:00~05:00

                        start_utc = start_local.astimezone(pytz.UTC).isoformat()
                        end_utc = end_local.astimezone(pytz.UTC).isoformat()

                        cur.execute(
                            "SELECT COUNT(*) FROM ticks WHERE code=? AND ts >= ? AND ts < ?",
                            ("TXFR1", start_utc, end_utc),
                        )
                        cnt = int(cur.fetchone()[0] or 0)
                        conn.close()

                        # 用筆數門檻粗判斷（避免只存到一小段就被當作完成）
                        if sess == "日盤":
                            return cnt >= 250
                        if sess == "夜盤":
                            return cnt >= 400
                        return cnt >= 600
                    except Exception:
                        return False

                # 回填前先清掉該日 window，避免殘留造成 OHLC 混雜
                def delete_window_local(d, sess):
                    try:
                        import sqlite3
                        db_path = get_db_path()
                        conn = sqlite3.connect(str(db_path))
                        cur = conn.cursor()

                        if sess == "日盤":
                            start_local = taipei_tz.localize(datetime(d.year, d.month, d.day, 8, 30, 0))
                            end_local = taipei_tz.localize(datetime(d.year, d.month, d.day, 14, 0, 0))
                        elif sess == "夜盤":
                            start_local = taipei_tz.localize(datetime(d.year, d.month, d.day, 15, 0, 0))
                            end_local = start_local + timedelta(hours=15)
                        else:  # 全盤
                            start_local = taipei_tz.localize(datetime(d.year, d.month, d.day, 0, 0, 0))
                            end_local = start_local + timedelta(days=1, hours=6)

                        start_utc = start_local.astimezone(pytz.UTC).isoformat()
                        end_utc = end_local.astimezone(pytz.UTC).isoformat()

                        cur.execute(
                            "DELETE FROM ticks WHERE code=? AND ts >= ? AND ts < ?",
                            ("TXFR1", start_utc, end_utc),
                        )
                        conn.commit()
                        conn.close()
                    except Exception:
                        return

                def filter_kbars_for_session_local(df_in, target_date, sess):
                    if df_in is None or df_in.empty:
                        return df_in
                    idx = df_in.index
                    next_date = target_date + timedelta(days=1)
                    if sess == "日盤":
                        hours = idx.hour
                        minutes = idx.minute
                        dates = idx.date
                        mask = (dates == target_date) & (
                            ((hours == 8) & (minutes >= 45))
                            | ((hours >= 9) & (hours < 13))
                            | ((hours == 13) & (minutes <= 45))
                        )
                        return df_in.loc[mask]
                    if sess == "夜盤":
                        mask = ((idx.date == target_date) & (idx.hour >= 15)) | (
                            (idx.date == next_date)
                            & (
                                (idx.hour < 5)
                                | ((idx.hour == 5) & (idx.minute == 0))
                            )
                        )
                        return df_in.loc[mask]
                    # 全盤
                    mask = (idx.date == target_date) | (
                        (idx.date == next_date)
                        & (
                            (idx.hour < 5)
                            | ((idx.hour == 5) & (idx.minute == 0))
                        )
                    )
                    return df_in.loc[mask]

                contract = api_instance.Contracts.Futures.TXF.TXFR1

                need_more = int(max_kbars) - int(current_count)
                st.sidebar.warning(
                    f"🧩 偵測到 {session} 日K 歷史不足：{current_count}/{max_kbars} 根，開始自動回填（本次最多 {max_days_per_run} 個交易日）..."
                )
                progress = st.sidebar.progress(0)

                # 先挑出要補的交易日，再用「單次 kbars 拉一段日期範圍」減少 API 呼叫次數
                target_days = min(max_days_per_run, max(1, need_more))
                to_fill = []
                checked_days = 0
                max_checks = target_days * 8  # 避免遇到一堆非工作日/無資料日卡死

                # 1) 優先檢查「最近幾個工作日」是否缺資料（例如日盤只到 2/24，優先處理 2/25、2/26）
                if latest_date is not None:
                    forward_date = latest_date + timedelta(days=1)
                    today = datetime.now(taipei_tz).date()
                    last_candidate = today - timedelta(days=1)  # 今天交給 update_today_kbars_if_needed 負責

                    while (
                        forward_date <= last_candidate
                        and len(to_fill) < target_days
                        and checked_days < max_checks * 2
                    ):
                        if not is_workday(forward_date):
                            forward_date += timedelta(days=1)
                            continue
                        checked_days += 1
                        if not has_sufficient_data_local(forward_date, session):
                            to_fill.append(forward_date)
                        forward_date += timedelta(days=1)

                # 2) 若還有名額，再從目前資料最早日往前找「更早」但尚未補齊的交易日
                scan_date = cursor_date
                while len(to_fill) < target_days and checked_days < max_checks:
                    if not is_workday(scan_date):
                        scan_date -= timedelta(days=1)
                        continue
                    checked_days += 1
                    # 避免重複加入已在 to_fill 的日期
                    if not has_sufficient_data_local(scan_date, session) and scan_date not in to_fill:
                        to_fill.append(scan_date)
                    scan_date -= timedelta(days=1)

                if not to_fill:
                    progress.empty()
                    st.session_state[throttle_key] = now_ts
                    st.session_state[throttle_key + "::saved"] = 0
                    st.sidebar.caption("ℹ️ 找不到需要回填的交易日（可能都已存在或遇到休市日）")
                    return False

                range_start = min(to_fill)
                range_end = max(to_fill) + timedelta(days=1)
                start = range_start.strftime("%Y-%m-%d")
                end = range_end.strftime("%Y-%m-%d")

                kbars = api_instance.kbars(contract=contract, start=start, end=end)
                if not kbars:
                    progress.empty()
                    st.session_state[throttle_key] = now_ts
                    st.session_state[throttle_key + "::saved"] = 0
                    st.sidebar.caption("ℹ️ API 未返回數據（可能休市/範圍過舊），稍後會再嘗試")
                    return False

                df_all = pd.DataFrame({**kbars})
                if df_all.empty:
                    progress.empty()
                    st.session_state[throttle_key] = now_ts
                    st.session_state[throttle_key + "::saved"] = 0
                    st.sidebar.caption("ℹ️ API 返回空數據，稍後會再嘗試")
                    return False

                df_all["ts"] = pd.to_datetime(df_all["ts"])
                df_all = df_all.rename(columns={"ts": "datetime"}).sort_values("datetime").reset_index(drop=True)
                df_all = df_all.set_index("datetime").sort_index()

                saved_days = 0
                for i, d in enumerate(sorted(to_fill), start=1):
                    delete_window_local(d, session)
                    df_new = filter_kbars_for_session_local(df_all, d, session)
                    if df_new is None or df_new.empty:
                        progress.progress(min(1.0, i / float(len(to_fill))))
                        continue

                    batch_ticks = []
                    for idx, row in df_new.iterrows():
                        if idx.tzinfo is None:
                            idx = taipei_tz.localize(idx)
                        else:
                            idx = idx.tz_convert(taipei_tz)
                        batch_ticks.append(
                            {
                                'ts': idx,
                                'code': contract.code,
                                'open': row.get('Open', row.get('Close', 0)),
                                'high': row.get('High', row.get('Close', 0)),
                                'low': row.get('Low', row.get('Close', 0)),
                                'close': row.get('Close', 0),
                                'volume': row.get('Volume', 0),
                                'bid_price': row.get('Close', 0),
                                'ask_price': row.get('Close', 0),
                                'bid_volume': 0,
                                'ask_volume': 0,
                            }
                        )

                    save_ticks_batch(batch_ticks)
                    saved_days += 1
                    progress.progress(min(1.0, i / float(len(to_fill))))

                progress.empty()

                st.session_state[throttle_key] = now_ts
                st.session_state[throttle_key + "::saved"] = int(saved_days)

                if saved_days > 0:
                    st.sidebar.success(f"✅ 自動回填完成：新增/更新 {saved_days} 個交易日的 {session} 1分K（用於日K彙總）")
                    # DB 變動後清除日K存量快取，讓側邊欄立即更新
                    try:
                        get_db_dayk_inventory.clear()
                    except Exception:
                        pass
                    return True

                st.sidebar.caption("ℹ️ 本次未回填到新交易日（可能該區間已存在或遇到休市日）")
                return False
            except Exception as e:
                st.sidebar.warning(f"⚠️ 自動回填失敗: {str(e)[:120]}")
                return False

        did_backfill = _auto_backfill_dayk_history_if_needed(_api, df)
        if did_backfill:
            # 回填後重新讀一次 DB，讓滑桿「真的連動」到更多日K
            df = get_kbars_from_db(interval=interval, session=session, days=days)

        # ==================== MA趨勢觸及吞噬策略計算 ====================
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
# 備援資料源相關函數已移除，改用純 Shioaji TXF 架構
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
    df.loc[:, 'MA20'] = df['Close'].rolling(window=20).mean()
    df.loc[:, 'MA60'] = df['Close'].rolling(window=60).mean()
    
    return df


def apply_realtime_snapshot_to_kbars(df: pd.DataFrame, interval: str, latest_price: float) -> pd.DataFrame:
    """用最新價格即時更新最後一根 K 棒。

    說明：
    - 這是「顯示用」的即時更新：用 snapshots 的最新成交價，更新當前這根 K 棒的 Close/High/Low。
    - 若已跨到下一個週期，會自動新增一根新 K 棒（成交量暫用 0）。
    - 日K不做即時更新（避免 00:00 的日期問題、且日K即時意義較低）。
    """
    if df is None or df.empty:
        return df
    if interval == "1d":
        return df
    if latest_price is None:
        return df
    try:
        latest_price = float(latest_price)
    except Exception:
        return df
    if not (latest_price > 0):
        return df

    minutes_map = {"1m": 1, "5m": 5, "15m": 15, "30m": 30, "60m": 60}
    interval_minutes = minutes_map.get(interval)
    if interval_minutes is None:
        return df

    taipei_tz = pytz.timezone("Asia/Taipei")
    now = datetime.now(taipei_tz).replace(second=0, microsecond=0)

    # 將 now 轉成與 df.index 一致的時區
    if getattr(df.index, "tz", None) is not None:
        try:
            now = now.astimezone(df.index.tz)
        except Exception:
            pass

    # 將目前時間 floor 到對應週期（例如 12:03 -> 12:00 for 5m）
    minute = (now.minute // interval_minutes) * interval_minutes
    bar_ts = now.replace(minute=minute)

    df = df.copy()
    last_ts = df.index[-1]

    # 若已跨到下一根，補一根新 K 棒（Open = 前一根 Close）
    if bar_ts > last_ts:
        prev_close = float(df.loc[last_ts, "Close"]) if "Close" in df.columns else latest_price
        new_row = {
            "Open": prev_close,
            "High": latest_price,
            "Low": latest_price,
            "Close": latest_price,
            "Volume": 0,
        }
        df.loc[bar_ts] = new_row
        df = df.sort_index()
    else:
        # 更新最後一根（視為當前進行中 K 棒）
        last_ts = df.index[-1]
        if "High" in df.columns:
            df.loc[last_ts, "High"] = max(float(df.loc[last_ts, "High"]), latest_price)
        if "Low" in df.columns:
            df.loc[last_ts, "Low"] = min(float(df.loc[last_ts, "Low"]), latest_price)
        df.loc[last_ts, "Close"] = latest_price

    # 重新計算均線（只要最後一根正確即可，成本也不高）
    if "Close" in df.columns:
        df.loc[:, "MA20"] = df["Close"].rolling(window=20).mean()
        df.loc[:, "MA60"] = df["Close"].rolling(window=60).mean()

    return df

# ==================== MA趨勢觸及吞噬策略計算 ====================
def calculate_ma_trend_engulfing_signals(df, min_bars=25, session="日盤", is_realtime=False):
    """
    計算 MA 趨勢觸及吞噬策略信號

     規則：
     1. 趨勢判斷：MA20 與 MA60 同方向，且 MA20 與 MA60 呈現多空排列
         - 多頭：MA20_slope > 0、MA60_slope > 0 且 MA20 > MA60
         - 空頭：MA20_slope < 0、MA60_slope < 0 且 MA20 < MA60
     2. 進場：第 N 根 K 棒觸及 MA20，且第 N+1 根收盤吞噬前一根
         - 做多：趨勢向上 + N 根觸及 MA20 + N+1 收盤 > 前一根 max(Open, Close) 且 收盤 > 兩條 MA
         - 做空：趨勢向下 + N 根觸及 MA20 + N+1 收盤 < 前一根 min(Open, Close) 且 收盤 < 兩條 MA
     3. 停損 / 退場：
         - 多頭：若當前 K 棒 Low < min(前一根 Open, 前一根 Close) 視為停損出場
         - 空頭：若當前 K 棒 High > max(前一根 Open, 前一根 Close) 視為停損出場
         - 另外，出現反向吞噬時同樣視為出場訊號
     4. 收盤前 30 分鐘風控：
         - 每個交易時段（依 session）收盤前 30 分鐘內：
             • 不再產生新的進場訊號
             • 若仍有持倉，於觸及「距收盤 30 分鐘」的第一根 K 棒強制平倉

        輸出：
                trades: 交易紀錄
                add_events: 補單信號列表

        備註：
                - 為了回測方便，函數在「非即時模式」下會將最後仍未平倉的部位，
                    於資料集最後一根 K 棒視為以收盤價強制平倉（exit_reason = "最後一根收盤"）。
                - 在即時看盤模式（is_realtime=True）下，避免這種回測式強制平倉，
                    以免造成「最新一根同時出現進場與出場」的視覺混淆。
    """
    if df is None or len(df) < min_bars:
        return [], []

    df = df.copy()
    trades = []
    add_events = []

    # 確保有 MA20/MA60
    if "MA20" not in df.columns or "MA60" not in df.columns:
        df["MA20"] = df["Close"].rolling(window=20).mean()
        df["MA60"] = df["Close"].rolling(window=60).mean()

    # 計算 MA 斜率（用簡單差分表示趨勢）
    df["MA20_slope"] = df["MA20"].diff()
    df["MA60_slope"] = df["MA60"].diff()

    # 偵測是否 K 棒「碰到」MA（touch）
    df["touch_ma20"] = (df["Low"] <= df["MA20"]) & (df["MA20"] <= df["High"])
    df["touch_ma60"] = (df["Low"] <= df["MA60"]) & (df["MA60"] <= df["High"])

    position = None
    entry_idx = None
    entry_price = None
    bars_in_position = 0
    has_added = False

    # 輔助：依 session 判斷每根 K 棒距離收盤時間（Asia/Taipei）
    def minutes_to_session_close(ts):
        """計算該時間點距離當日該交易時段收盤還有幾分鐘（負值代表已過收盤）。"""
        if not hasattr(ts, "tzinfo") or ts.tzinfo is None:
            taipei_tz = pytz.timezone("Asia/Taipei")
            ts = taipei_tz.localize(ts)
        else:
            ts = ts.astimezone(pytz.timezone("Asia/Taipei"))

        day = ts.date()
        taipei_tz = pytz.timezone("Asia/Taipei")

        if session == "日盤":
            close_dt = taipei_tz.localize(datetime(day.year, day.month, day.day, 13, 45))
        elif session == "夜盤":
            # 夜盤收盤：次日 05:00
            next_day = day + timedelta(days=1)
            close_dt = taipei_tz.localize(datetime(next_day.year, next_day.month, next_day.day, 5, 0))
        else:
            # 全盤或其他：依時間自動判斷屬於哪個時段
            if ts.hour < 12:
                close_dt = taipei_tz.localize(datetime(day.year, day.month, day.day, 13, 45))
            else:
                next_day = day + timedelta(days=1)
                close_dt = taipei_tz.localize(datetime(next_day.year, next_day.month, next_day.day, 5, 0))

        delta = close_dt - ts
        return delta.total_seconds() / 60.0

    for i in range(1, len(df)):
        row_prev = df.iloc[i - 1]
        row_curr = df.iloc[i]

        # 計算距離收盤時間（分鐘）
        minutes_left = minutes_to_session_close(df.index[i])

        # 多空排列 + 斜率同向，過濾雜訊以提高勝率
        uptrend = (
            row_curr["MA20_slope"] > 0
            and row_curr["MA60_slope"] > 0
            and row_curr["MA20"] > row_curr["MA60"]
        )
        downtrend = (
            row_curr["MA20_slope"] < 0
            and row_curr["MA60_slope"] < 0
            and row_curr["MA20"] < row_curr["MA60"]
        )

        touch_ma20 = bool(row_prev["touch_ma20"])

        # 吞噬定義：
        # 多頭：收盤 > 前一根 max(Open, Close)
        # 空頭：收盤 < 前一根 min(Open, Close)
        prev_low_ref = min(row_prev["Open"], row_prev["Close"])
        prev_high_ref = max(row_prev["Open"], row_prev["Close"])
        engulf_up = row_curr["Close"] > prev_high_ref
        engulf_down = row_curr["Close"] < prev_low_ref

        # 進場限制：只適用當日訊號，跨日不計
        prev_date = df.index[i - 1].date()
        curr_date = df.index[i].date()
        same_day_signal = prev_date == curr_date

        # 收盤前 30 分鐘內：不再開新倉
        cutoff_reached = minutes_left <= 30

        if position is None:
            # 做多進場：多頭排列 + 前一根碰 MA + 吞噬且收盤站上兩條 MA
            if (
                uptrend
                and touch_ma20
                and engulf_up
                and row_curr["Close"] > row_curr["MA20"]
                and row_curr["Close"] > row_curr["MA60"]
            ) and (not cutoff_reached) and same_day_signal:
                position = "LONG"
                entry_idx = i
                entry_price = row_curr["Close"]
                bars_in_position = 1
                has_added = False
            # 做空進場：空頭排列 + 前一根碰 MA + 吞噬且收盤跌破兩條 MA
            elif (
                downtrend
                and touch_ma20
                and engulf_down
                and row_curr["Close"] < row_curr["MA20"]
                and row_curr["Close"] < row_curr["MA60"]
            ) and (not cutoff_reached) and same_day_signal:
                position = "SHORT"
                entry_idx = i
                entry_price = row_curr["Close"]
                bars_in_position = 1
                has_added = False
            continue

        # 已持倉
        bars_in_position += 1

        # 若已進入收盤前 30 分鐘，強制在第一根觸及時平倉
        if cutoff_reached and position is not None:
            exit_idx = i
            exit_price = row_curr["Close"]
            trades.append({
                "entry_idx": entry_idx,
                "entry_ts": df.index[entry_idx],
                "entry_price": entry_price,
                "exit_idx": exit_idx,
                "exit_ts": df.index[exit_idx],
                "exit_price": exit_price,
                "direction": position,
                "bars_held": bars_in_position,
                "pnl": (exit_price - entry_price) if position == "LONG" else (entry_price - exit_price),
                "exit_reason": "收盤前30分鐘強制平倉",
            })
            position = None
            entry_idx = None
            entry_price = None
            bars_in_position = 0
            continue

        # 停損 & 退場條件
        # 1) 多頭停損：當前收盤 < 前一根 min(Open, Close)
        if position == "LONG" and row_curr["Close"] < prev_low_ref:
            exit_idx = i
            exit_price = row_curr["Close"]
            trades.append({
                "entry_idx": entry_idx,
                "entry_ts": df.index[entry_idx],
                "entry_price": entry_price,
                "exit_idx": exit_idx,
                "exit_ts": df.index[exit_idx],
                "exit_price": exit_price,
                "direction": position,
                "bars_held": bars_in_position,
                "pnl": exit_price - entry_price,
                "exit_reason": "多頭停損(收盤跌破前一根實體低點)",
            })
            position = None
            entry_idx = None
            entry_price = None
            bars_in_position = 0
            continue

        # 2) 空頭停損：當前收盤 > 前一根 max(Open, Close)
        if position == "SHORT" and row_curr["Close"] > prev_high_ref:
            exit_idx = i
            exit_price = row_curr["Close"]
            trades.append({
                "entry_idx": entry_idx,
                "entry_ts": df.index[entry_idx],
                "entry_price": entry_price,
                "exit_idx": exit_idx,
                "exit_ts": df.index[exit_idx],
                "exit_price": exit_price,
                "direction": position,
                "bars_held": bars_in_position,
                "pnl": entry_price - exit_price,
                "exit_reason": "空頭停損(收盤突破前一根實體高點)",
            })
            position = None
            entry_idx = None
            entry_price = None
            bars_in_position = 0
            continue

        # 3) 反向吞噬出場（若尚未觸發停損）
        if position == "LONG" and engulf_down:
            exit_idx = i
            exit_price = row_curr["Close"]
            trades.append({
                "entry_idx": entry_idx,
                "entry_ts": df.index[entry_idx],
                "entry_price": entry_price,
                "exit_idx": exit_idx,
                "exit_ts": df.index[exit_idx],
                "exit_price": exit_price,
                "direction": position,
                "bars_held": bars_in_position,
                "pnl": exit_price - entry_price,
                "exit_reason": "多頭反向吞噬出場",
            })
            position = None
            entry_idx = None
            entry_price = None
            bars_in_position = 0
            continue

        if position == "SHORT" and engulf_up:
            exit_idx = i
            exit_price = row_curr["Close"]
            trades.append({
                "entry_idx": entry_idx,
                "entry_ts": df.index[entry_idx],
                "entry_price": entry_price,
                "exit_idx": exit_idx,
                "exit_ts": df.index[exit_idx],
                "exit_price": exit_price,
                "direction": position,
                "bars_held": bars_in_position,
                "pnl": entry_price - exit_price,
                "exit_reason": "空頭反向吞噬出場",
            })
            position = None
            entry_idx = None
            entry_price = None
            bars_in_position = 0
            continue

    # 若最後仍持倉，且為回測模式，強制以最後一根收盤退場
    # 即時模式 (is_realtime=True) 不執行此步驟，避免最新一根同時出現進/出場標記
    if (not is_realtime) and position is not None and entry_idx is not None:
        exit_idx = len(df) - 1
        exit_price = df.iloc[exit_idx]["Close"]
        trades.append({
            "entry_idx": entry_idx,
            "entry_ts": df.index[entry_idx],
            "entry_price": entry_price,
            "exit_idx": exit_idx,
            "exit_ts": df.index[exit_idx],
            "exit_price": exit_price,
            "direction": position,
            "bars_held": bars_in_position,
            "pnl": (exit_price - entry_price) if position == "LONG" else (entry_price - exit_price),
            "exit_reason": "最後一根收盤",
        })

    return trades, add_events

# 主要數據獲取函數
def get_data(interval, product, session, max_kbars, use_shioaji=False, api_instance=None):
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

    # 一律優先從 SQLite DB 讀取顯示；若已登入 Shioaji，才啟用自動更新回填
    if use_shioaji and api_instance is not None:
        st.sidebar.info("🔄 使用 DB 顯示（並由 Shioaji 自動更新回填）...")
        df = get_data_from_shioaji(api_instance, interval, product, session, max_kbars)
        data_source = "SQLite DB（自動更新：Shioaji）"
    else:
        st.sidebar.info("📊 使用 DB 顯示（未登入 Shioaji，僅讀取不回填）")
        df = get_data_from_shioaji(None, interval, product, session, max_kbars)
        data_source = "SQLite DB（僅讀取）"

    # 即時/歷史判斷：開盤中且 DB 最新時間足夠新鮮
    try:
        taipei_tz = pytz.timezone('Asia/Taipei')
        now = datetime.now(taipei_tz)
        last_db_ts = get_latest_tick_timestamp(code='TXFR1')
        is_fresh = last_db_ts is not None and (now - last_db_ts) <= timedelta(minutes=2)
        is_realtime = bool(market_is_open and is_fresh)
    except Exception:
        is_realtime = bool(market_is_open)
    
    # 最後的保險：確保有數據
    if df is None or df.empty:
        st.sidebar.error("❌ DB 目前沒有可用數據")
        st.sidebar.warning("💡 **解決方案**（擇一）：")
        st.sidebar.info(
            "**方案1：回填歷史數據**\n"
            "執行回填腳本：\n"
            "`python backfill_kbars.py --days 500 --skip-existing`\n\n"
            "**方案2：登入 Shioaji**\n"
            "1. 在左側「⚙️ Shioaji 帳號設定」登入\n"
            "2. 系統會自動更新今日及後續數據\n"
            "3. 首次可能需要 2-3 分鐘建立連線"
        )
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
# 呼叫 get_data 函數獲取 K 線數據（本版一律使用本地 DB 顯示）
# 核心邏輯：圖表永遠從 SQLite DB 讀取顯示；若已登入 Shioaji，會在背景自動更新/回填 DB
try:
    use_shioaji_flag = st.session_state.get('shioaji_logged_in', False) and 'shioaji_api' in st.session_state
except:
    use_shioaji_flag = False

# 如果未成功登入，確保 checkbox 被取消（防止狀態不同步）
if not use_shioaji_flag:
    st.session_state["use_shioaji_checkbox"] = False

# 顯示本地 DB 模式提示
if not use_shioaji_flag:
    st.info("📊 **正在使用本地 SQLite 資料庫** | 包含歷史數據 | 所有分析功能可用")

# 輕量更新：僅更新最新 K 棒（減少閃爍）
market_status_text, market_is_open, market_session = get_market_status()
should_realtime_update = session_option == "全盤" or session_option == market_session
cache_key = f"{product_option}::{session_option}::{interval_option}::{max_kbars}"

# 取得資料時傳遞 API 實例
if use_shioaji_flag:
    api_instance = st.session_state['shioaji_api']
    # 只有在開盤、自動刷新、且非日K 時，才適用輕量更新
    use_light_update = bool(
        auto_refresh
        and refresh_interval
        and st.session_state.get("lightweight_update", True)
        and market_is_open
        and should_realtime_update
        and interval_option != "1d"
    )

    cached_df = st.session_state.get("light_cache_df")
    cached_key = st.session_state.get("light_cache_key")
    if use_light_update and cached_df is not None and cached_key == cache_key:
        # 用最新成交價更新最後一根K棒（顯示用）
        try:
            contract = api_instance.Contracts.Futures.TXF.TXFR1
            snapshot = api_instance.snapshots([contract])
            latest_price = getattr(snapshot[0], "close", None) if snapshot and len(snapshot) > 0 else None
            df = apply_realtime_snapshot_to_kbars(cached_df, interval_option, float(latest_price) if latest_price is not None else None)
            if df is not None and len(df) > max_kbars:
                df = df.tail(max_kbars)
            data_source = st.session_state.get("light_cache_data_source", "SQLite DB（輕量更新）")
            is_realtime = True
            st.session_state["light_cache_df"] = df
        except Exception:
            df, data_source, is_realtime = get_data(interval_option, product_option, session_option, max_kbars, use_shioaji_flag, api_instance)
            st.session_state["light_cache_df"] = df
            st.session_state["light_cache_key"] = cache_key
            st.session_state["light_cache_data_source"] = data_source
            st.session_state["light_cache_is_realtime"] = is_realtime
    else:
        df, data_source, is_realtime = get_data(interval_option, product_option, session_option, max_kbars, use_shioaji_flag, api_instance)
        # 存快取，供下一輪輕量更新使用
        st.session_state["light_cache_df"] = df
        st.session_state["light_cache_key"] = cache_key
        st.session_state["light_cache_data_source"] = data_source
        st.session_state["light_cache_is_realtime"] = is_realtime
else:
    df, data_source, is_realtime = get_data(interval_option, product_option, session_option, max_kbars, use_shioaji_flag)

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
        st.sidebar.caption("ℹ️ 提示: 若想顯示更多歷史 K 棒，請先回填 SQLite DB（例如 500 天）。")
else:
    st.sidebar.error("❌ 數據獲取失敗")
    st.sidebar.info("💡 建議: 先回填 DB 歷史資料，或登入 Shioaji 取得即時更新")

# 根據使用者設定的最大K棒數限制資料量
# 策略：先多取 20 筆用於 MA 計算，計算完後再裁切
if df is not None:
    original_count = len(df)
    
    # 計算所需的最大窗口（MA60 需要 60 筆）
    ma_window = 60
    
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
        df_for_calc['MA20'] = df_for_calc['Close'].rolling(window=20).mean()
        df_for_calc['MA60'] = df_for_calc['Close'].rolling(window=60).mean()
        
        # 最後只取需要顯示的部分
        df = df_for_calc.tail(max_kbars)
        st.sidebar.info(f"📊 圖表顯示最新 {len(df)}/{original_count} 筆 (滑桿限制: {max_kbars})")
    else:
        # 數據量不足，全部顯示
        st.sidebar.info(f"📊 圖表顯示全部 {len(df)} 筆數據 (滑桿設定: {max_kbars})")
    
    # 顯示當前顯示的數據範圍
    # ------------------------------------------------------------
    # 即時顯示：開盤中用最新成交價更新最後一根 K 棒
    # ------------------------------------------------------------
    try:
        market_status_text, market_is_open, market_session = get_market_status()
        should_realtime_update = session_option == "全盤" or session_option == market_session
        if use_shioaji_flag and market_is_open and should_realtime_update and interval_option != "1d":
            contract = api_instance.Contracts.Futures.TXF.TXFR1
            snapshot = api_instance.snapshots([contract])
            if snapshot and len(snapshot) > 0:
                latest_price = getattr(snapshot[0], "close", None)
                if latest_price is not None and float(latest_price) > 0:
                    df = apply_realtime_snapshot_to_kbars(df, interval_option, float(latest_price))
                    if len(df) > max_kbars:
                        df = df.tail(max_kbars)
                    st.sidebar.caption(f"⚡ 即時價格（顯示用）: {float(latest_price):.0f}")
    except Exception:
        # 即時更新失敗不影響主要顯示
        pass

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
    if len(df) > 0 and hasattr(df.index[0], 'strftime'):
        # 日K 的 index 常落在 00:00，顯示時移除時間以避免誤導
        date_fmt = '%Y-%m-%d' if interval_option == '1d' else '%Y-%m-%d %H:%M'
        date_labels = df.index.strftime(date_fmt)
    else:
        date_labels = df.index.astype(str)
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
    # 繪製 20 日移動平均線（橘色）
    fig.add_trace(
        go.Scatter(
            x=x_range,  # 使用連續數字索引
            y=df['MA20'], 
            line=dict(color='orange', width=1.5), 
            name='20 MA',
            text=date_labels,
            hovertext=date_labels,
            hovertemplate='<b>%{text}</b><br>MA20: %{y:.0f}<extra></extra>'
        ), 
        row=1, col=1
    )
    
    # 繪製 60 日移動平均線（紫色）
    fig.add_trace(
        go.Scatter(
            x=x_range,  # 使用連續數字索引
            y=df['MA60'], 
            line=dict(color='purple', width=1.5), 
            name='60 MA',
            text=date_labels,
            hovertext=date_labels,
            hovertemplate='<b>%{text}</b><br>MA60: %{y:.0f}<extra></extra>'
        ), 
        row=1, col=1
    )

    # ============================================================
    # 5.3.1 繪製策略信號標記
    # ============================================================
    if st.session_state.get("enable_strategy", False):
        trades, add_events = calculate_ma_trend_engulfing_signals(df, session=session_option, is_realtime=is_realtime)
        
        if trades:
            # 進場信號點
            entry_indices = [t["entry_idx"] for t in trades]
            entry_prices = [df.iloc[idx]["Close"] for idx in entry_indices]
            entry_symbols = ["triangle-up" if t["direction"] == "LONG" else "triangle-down" for t in trades]
            entry_colors = ["green" if t["direction"] == "LONG" else "red" for t in trades]
            entry_labels = [f"進場 {t['direction']}" for t in trades]
            
            fig.add_trace(
                go.Scatter(
                    x=entry_indices,
                    y=entry_prices,
                    mode='markers',
                    marker=dict(
                        size=12,
                        symbol=entry_symbols,
                        color=entry_colors,
                        line=dict(color='white', width=2)
                    ),
                    name='進場信號',
                    text=[f"進場: {p:.0f}<br>方向: {t['direction']}" for p, t in zip(entry_prices, trades)],
                    hovertemplate='<b>%{text}</b><extra></extra>'
                ),
                row=1, col=1
            )
            
            # 退場信號點
            exit_indices = [t["exit_idx"] for t in trades]
            exit_prices = [df.iloc[idx]["Close"] for idx in exit_indices]
            exit_colors = ["darkgreen" if t["direction"] == "LONG" else "darkred" for t in trades]
            
            fig.add_trace(
                go.Scatter(
                    x=exit_indices,
                    y=exit_prices,
                    mode='markers',
                    marker=dict(
                        size=10,
                        symbol='circle',
                        color=exit_colors,
                        line=dict(color='yellow', width=2)
                    ),
                    name='退場信號',
                    text=[f"退場: {p:.0f}<br>方向: {t['direction']}<br>損益: {t['pnl']:.0f}" for p, t in zip(exit_prices, trades)],
                    hovertemplate='<b>%{text}</b><extra></extra>'
                ),
                row=1, col=1
            )

            # 補單已視為退場，不另外標記

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
        # 開啟下方滑動條，可左右拖曳查看更早/更晚的 K 棒
        xaxis_rangeslider=dict(visible=True),
        height=900,                       # 圖表高度 900 像素（加大顯示）
        plot_bgcolor='rgb(20, 20, 20)',  # 繪圖區背景色（深灰色）
        paper_bgcolor='rgb(20, 20, 20)', # 整個畫布背景色
        font=dict(color='white'),         # 字體顏色（白色）
        title_text=f"{product_option} - {session_option} - {interval_option} K線圖 [資料來源: {data_source}] (顯示 {len(df)} 筆)",
        hovermode='x unified',            # 滑鼠懸停時顯示十字線和統一提示
        uirevision='stock-city-chart',    # 保留互動狀態，降低重繪閃爍
        transition=dict(duration=0),      # 關閉轉場動畫，避免閃亮感
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
    # ------------------------------------------------------------
    # 5.5.2 固定/黏性 Y 軸範圍（夜盤觀察時避免一直跳動）
    # ------------------------------------------------------------
    # 你的習慣：1 單位 = 0.05K = 50 點，要求上下預留 2 單位 = 100 點
    y_step_points = 50
    y_padding_points = 2 * y_step_points

    try:
        if interval_option != "1d" and ("High" in df.columns) and ("Low" in df.columns) and len(df) > 0:
            cur_low = float(df["Low"].min())
            cur_high = float(df["High"].max())

            if math.isfinite(cur_low) and math.isfinite(cur_high) and cur_low < cur_high:
                desired_low = math.floor((cur_low - y_padding_points) / y_step_points) * y_step_points
                desired_high = math.ceil((cur_high + y_padding_points) / y_step_points) * y_step_points

                # 只擴不縮：避免每次 tick 小波動就改 y 軸
                y_key = f"sticky_y_range::{product_option}::{session_option}::{interval_option}"
                prev_range = st.session_state.get(y_key)
                if isinstance(prev_range, (tuple, list)) and len(prev_range) == 2:
                    prev_low, prev_high = prev_range
                    y_low = min(float(prev_low), float(desired_low))
                    y_high = max(float(prev_high), float(desired_high))
                else:
                    y_low, y_high = float(desired_low), float(desired_high)

                # 避免範圍過小造成畫面壓縮
                if (y_high - y_low) < (4 * y_step_points):
                    mid = (y_high + y_low) / 2.0
                    half = 2 * y_step_points
                    y_low = mid - half
                    y_high = mid + half

                st.session_state[y_key] = (y_low, y_high)

                fig.update_yaxes(
                    range=[y_low, y_high],
                    autorange=False,
                    automargin=True,
                    row=1,
                    col=1,
                )
            else:
                fig.update_yaxes(automargin=True, row=1, col=1)
        else:
            fig.update_yaxes(automargin=True, row=1, col=1)
    except Exception:
        fig.update_yaxes(automargin=True, row=1, col=1)
    
    # ------------------------------------------------------------
    # 5.6 顯示圖表
    # ------------------------------------------------------------
    # 使用 placeholder 固定版面，降低每次更新的閃動感
    chart_placeholder = st.empty()
    chart_placeholder.plotly_chart(fig, use_container_width=True)
    
    # ------------------------------------------------------------
    # 5.6.0 策略選擇（K 線圖下方）
    # ------------------------------------------------------------
    st.checkbox(
        "策略選擇：20/60MA 趨勢 + 觸及 + 吞噬（進場/補單）",
        value=st.session_state.get("enable_strategy", False),
        key="enable_strategy",
        help="趨勢同向時，K棒觸及 MA 且下一根吞噬即進場；持倉期間同向吞噬補單，反向吞噬退場"
    )
    # ============================================================
    # 5.6.1 顯示策略交易紀錄
    # ============================================================
    if st.session_state.get("enable_strategy", False):
        trades, _ = calculate_ma_trend_engulfing_signals(df, session=session_option, is_realtime=is_realtime)
        
        if trades:
            with st.expander("📋 交易紀錄", expanded=True):
                # 構建交易紀錄 DataFrame
                trade_records = []
                for i, trade in enumerate(trades, 1):
                    entry_ts = trade["entry_ts"]
                    exit_ts = trade["exit_ts"]
                    
                    # 格式化時間戳
                    entry_time = entry_ts.strftime('%m-%d %H:%M') if hasattr(entry_ts, 'strftime') else str(entry_ts)
                    exit_time = exit_ts.strftime('%m-%d %H:%M') if hasattr(exit_ts, 'strftime') else str(exit_ts)
                    
                    trade_records.append({
                        "編號": i,
                        "進場時間": entry_time,
                        "進場價": f"{trade['entry_price']:.0f}",
                        "退場時間": exit_time,
                        "退場價": f"{trade['exit_price']:.0f}",
                        "方向": trade["direction"],
                        "持倉K棒數": trade["bars_held"],
                        "退場原因": trade.get("exit_reason", ""),
                        "損益": f"{trade['pnl']:+.0f}"
                    })
                
                trades_df = pd.DataFrame(trade_records)
                st.dataframe(trades_df, use_container_width=True, hide_index=True)
                
                # 統計信息
                total_trades = len(trades)
                long_trades = sum(1 for t in trades if t["direction"] == "LONG")
                short_trades = sum(1 for t in trades if t["direction"] == "SHORT")
                total_pnl = sum(t["pnl"] for t in trades)

                win_trades = sum(1 for t in trades if t["pnl"] > 0)
                loss_trades = sum(1 for t in trades if t["pnl"] < 0)
                win_rate = (win_trades / total_trades * 100) if total_trades > 0 else 0.0
                
                col_stats1, col_stats2, col_stats3, col_stats4 = st.columns(4)
                col_stats1.metric("總交易數", total_trades)
                col_stats2.metric("做多", long_trades)
                col_stats3.metric("做空", short_trades)
                col_stats4.metric("總損益", f"{total_pnl:+.0f}")

                col_stats5, col_stats6, col_stats7 = st.columns(3)
                col_stats5.metric("獲利筆數", win_trades)
                col_stats6.metric("虧損筆數", loss_trades)
                col_stats7.metric("勝率", f"{win_rate:.1f}%")
        else:
            st.info("ℹ️ 未找到符合策略的交易信號，請調整條件或檢查K棒數據")


    # ------------------------------------------------------------
    # 5.7 最新報價資訊顯示
    # ------------------------------------------------------------
    # 取得最後一筆資料（最新的 K 棒）
    last_row = df.iloc[-1]
    
    # 使用 Streamlit 的 columns 功能建立 4 個並排的欄位
    col1, col2, col3, col4 = st.columns(4)
    
    # 在各欄位中顯示指標（使用 metric 組件）
    col1.metric("最新收盤", f"{last_row['Close']:.0f}")  # 最新收盤價
    col2.metric("20 MA", f"{last_row['MA20']:.0f}")           # 20日均線
    col3.metric("60 MA", f"{last_row['MA60']:.0f}")           # 60日均線
    col4.metric("成交量", f"{last_row['Volume']:.0f}")        # 成交量
    
    # 顯示自動更新提示與即時數據
    # 顯示數據類型與最後更新時間
    taipei_tz = pytz.timezone('Asia/Taipei')
    update_time = datetime.now(taipei_tz).strftime('%Y-%m-%d %H:%M:%S')
    
    # 額外顯示：市場狀態 + DB 最新時間（用於判斷是否為即時）
    market_status_text, market_is_open, market_session = get_market_status()
    last_db_ts = get_latest_tick_timestamp(code='TXFR1')
    last_db_text = last_db_ts.strftime('%Y-%m-%d %H:%M:%S') if last_db_ts is not None else '無'

    col_status1, col_status2, col_status3, col_status4 = st.columns(4)
    col_status1.info(f"📊 數據來源: {data_source}")

    if market_is_open:
        col_status2.success(f"📈 市場: 開盤（{market_session}）")
    else:
        col_status2.warning(f"📉 市場: 休盤（{market_session}）")

    if is_realtime:
        col_status3.success("🟢 即時數據")
    else:
        col_status3.info("📚 歷史數據")

    col_status4.caption(f"🕐 更新: {update_time}\n💾 DB最新: {last_db_text}")
    
    # 自動刷新邏輯（只在即時模式啟用）
    if auto_refresh and refresh_interval and is_realtime:
        # 改成平滑刷新：避免每秒倒數造成的頻繁重繪與閃屏
        if st_autorefresh is not None:
            st_autorefresh(interval=int(refresh_interval * 1000), key="smooth_autorefresh")
            st.caption(f"⏱️ 自動刷新：每 {refresh_interval} 秒更新一次")
        else:
            st.caption(f"⏱️ 自動刷新：每 {refresh_interval} 秒更新一次")
            time.sleep(refresh_interval)
            st.rerun()
    elif auto_refresh and not is_realtime:
        st.info("ℹ️ 當前為歷史數據，自動刷新已暫停")


else:
    # 當數據獲取失敗時顯示錯誤訊息
    st.error("❌ 目前無法獲取數據")
    
    # 檢查是否是 Shioaji 未登入問題
    shioaji_checked = st.session_state.get("use_shioaji_checkbox", False)
    shioaji_logged = st.session_state.get("shioaji_logged_in", False)
    
    st.warning("🔧 **排查步驟**：")
    
    if shioaji_checked and not shioaji_logged:
        st.error(
            "**✗ 已勾選 Shioaji 但未登入**\n\n"
            "您勾選了「使用 Shioaji 即時數據」但還沒登入。\n"
            "請在左側「⚙️ Shioaji 帳號設定」中提供 API Key（或憑證）並點擊「登入 Shioaji」"
        )
    
    st.info(
        "**✓ 若無 Shioaji 帳號，請改成：**\n\n"
        "1️⃣ **移除「使用 Shioaji 即時數據」的勾選**（如果已勾選的話）\n\n"
        "2️⃣ **回填 SQLite DB 歷史數據**：\n"
        "   在終端機執行：\n"
        "   ```\n"
        "   cd stock_city/db\n"
        "   python backfill_kbars.py --days 300 --skip-existing\n"
        "   ```\n\n"
        "3️⃣ **重新載入頁面** (按 F5 或按側邊欄「⟳ 重新執行」按鈕)"
    )
    
    st.caption("📝 本專案目前以 SQLite DB 為主要資料顯示來源")

# ============================================================
# 程式結束
# ============================================================
# %% 記號用於 Jupyter/IPython 環境中區分代碼區塊

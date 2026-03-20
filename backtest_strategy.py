import argparse
import os
from datetime import datetime
import pytz
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from stock_city.db.tick_database import get_kbars_from_db
from stock_city.strategy.ma20_ma60 import run_selected_strategy


def _ensure_ma_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    if "MA20" not in df.columns:
        df["MA20"] = df["Close"].rolling(window=20).mean()
    if "MA60" not in df.columns:
        df["MA60"] = df["Close"].rolling(window=60).mean()
    return df


def _build_trade_table(df: pd.DataFrame, trades: list[dict]) -> pd.DataFrame:
    records = []
    for i, t in enumerate(trades, 1):
        entry_ts = t["entry_ts"]
        exit_ts = t["exit_ts"]
        records.append({
            "id": i,
            "direction": t["direction"],
            "entry_ts": entry_ts,
            "entry_price": float(t["entry_price"]),
            "exit_ts": exit_ts,
            "exit_price": float(t["exit_price"]),
            "bars_held": int(t["bars_held"]),
            "pnl": float(t["pnl"]),
            "exit_reason": t.get("exit_reason", ""),
        })
    return pd.DataFrame(records)


def _plot_trade_window(
    df: pd.DataFrame,
    entry_idx: int,
    exit_idx: int,
    direction: str,
    bars_before: int,
    bars_after: int,
):
    start = max(0, entry_idx - bars_before)
    end = min(len(df), entry_idx + bars_after + 1)
    df_window = df.iloc[start:end]

    date_labels = df_window.index.strftime("%Y-%m-%d %H:%M")
    x_range = list(range(len(df_window)))

    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.03,
        subplot_titles=("K 線與均線", "成交量"),
        row_width=[0.15, 0.85],
    )

    fig.add_trace(
        go.Candlestick(
            x=x_range,
            open=df_window["Open"],
            high=df_window["High"],
            low=df_window["Low"],
            close=df_window["Close"],
            name="K棒",
            increasing_line_color="red",
            decreasing_line_color="green",
            increasing_line_width=2,
            decreasing_line_width=2,
            text=date_labels,
            hovertext=date_labels,
        ),
        row=1,
        col=1,
    )

    fig.add_trace(
        go.Scatter(
            x=x_range,
            y=df_window["MA20"],
            line=dict(color="orange", width=1.5),
            name="20 MA",
            text=date_labels,
            hovertext=date_labels,
        ),
        row=1,
        col=1,
    )

    fig.add_trace(
        go.Scatter(
            x=x_range,
            y=df_window["MA60"],
            line=dict(color="purple", width=1.5),
            name="60 MA",
            text=date_labels,
            hovertext=date_labels,
        ),
        row=1,
        col=1,
    )

    # 進場/出場點標記
    entry_local_idx = entry_idx - start
    exit_local_idx = exit_idx - start
    if exit_local_idx < 0 or exit_local_idx >= len(df_window):
        exit_local_idx = None
    fig.add_trace(
        go.Scatter(
            x=[entry_local_idx],
            y=[df_window.iloc[entry_local_idx]["Close"]],
            mode="markers",
            marker=dict(size=12, symbol="triangle-up", color="yellow", line=dict(color="black", width=1)),
            name="進場",
            text=[date_labels[entry_local_idx]],
        ),
        row=1,
        col=1,
    )

    if exit_local_idx is not None:
        exit_symbol = "x"
        exit_color = "cyan" if direction == "LONG" else "magenta"
        fig.add_trace(
            go.Scatter(
                x=[exit_local_idx],
                y=[df_window.iloc[exit_local_idx]["Close"]],
                mode="markers",
                marker=dict(size=11, symbol=exit_symbol, color=exit_color, line=dict(color="black", width=1)),
                name="出場",
                text=[date_labels[exit_local_idx]],
            ),
            row=1,
            col=1,
        )

    colors = ["red" if row["Open"] - row["Close"] >= 0 else "green" for _, row in df_window.iterrows()]
    fig.add_trace(
        go.Bar(
            x=x_range,
            y=df_window["Volume"],
            marker_color=colors,
            name="成交量",
            text=date_labels,
            hovertext=date_labels,
        ),
        row=2,
        col=1,
    )

    fig.update_layout(
        xaxis_rangeslider=dict(visible=False),
        height=700,
        plot_bgcolor="rgb(20, 20, 20)",
        paper_bgcolor="rgb(20, 20, 20)",
        font=dict(color="white"),
        hovermode="x unified",
        transition=dict(duration=0),
    )

    tick_spacing = max(1, len(df_window) // 6)
    tickvals = list(range(0, len(df_window), tick_spacing))
    ticktext = [date_labels[i] for i in tickvals]
    fig.update_xaxes(tickvals=tickvals, ticktext=ticktext, tickangle=-45)

    return fig


def run_backtest(interval: str, session: str, days: int, out_dir: str, bars_before: int, bars_after: int, strategy: str):
    df = get_kbars_from_db(interval=interval, session=session, days=days)
    if df is None or df.empty:
        raise ValueError("找不到可回測的K線資料")

    df = _ensure_ma_columns(df)
    trades, _ = run_selected_strategy(df, strategy=strategy, session=session, is_realtime=False)

    trade_df = _build_trade_table(df, trades)
    os.makedirs(out_dir, exist_ok=True)

    total_trades = len(trade_df)
    total_pnl = float(trade_df["pnl"].sum()) if total_trades > 0 else 0.0
    win_trades = int((trade_df["pnl"] > 0).sum()) if total_trades > 0 else 0
    loss_trades = int((trade_df["pnl"] < 0).sum()) if total_trades > 0 else 0
    win_rate = (win_trades / total_trades * 100.0) if total_trades > 0 else 0.0

    trade_csv = os.path.join(out_dir, "trades.csv")
    trade_df_out = trade_df.copy()
    trade_df_out["total_trades"] = ""
    trade_df_out["win_trades"] = ""
    trade_df_out["loss_trades"] = ""
    trade_df_out["win_rate"] = ""
    trade_df_out["total_pnl"] = ""

    summary_row = {
        "id": "SUMMARY",
        "direction": "",
        "entry_ts": "",
        "entry_price": "",
        "exit_ts": "",
        "exit_price": "",
        "bars_held": "",
        "pnl": "",
        "exit_reason": "summary",
        "total_trades": total_trades,
        "win_trades": win_trades,
        "loss_trades": loss_trades,
        "win_rate": round(win_rate, 2),
        "total_pnl": round(total_pnl, 2),
    }
    trade_df_out = pd.concat([trade_df_out, pd.DataFrame([summary_row])], ignore_index=True)
    trade_df_out.to_csv(trade_csv, index=False, encoding="utf-8-sig")

    image_dir = os.path.join(out_dir, "trade_images")
    os.makedirs(image_dir, exist_ok=True)

    for i, t in enumerate(trades, 1):
        entry_idx = int(t["entry_idx"])
        exit_idx = int(t["exit_idx"])
        entry_ts = t["entry_ts"]
        entry_date = entry_ts.strftime("%Y%m%d") if hasattr(entry_ts, "strftime") else f"{i:03d}"
        direction = t["direction"]
        filename = f"{entry_date}_{direction}_{i:03d}.png"
        fig = _plot_trade_window(df, entry_idx, exit_idx, direction, bars_before, bars_after)
        fig.write_image(os.path.join(image_dir, filename))

    return trade_df, trade_csv, image_dir, total_trades, win_rate, total_pnl


def main():
    parser = argparse.ArgumentParser(description="MA20/MA60 策略年度回測")
    parser.add_argument("--interval", default="5m", help="K 線週期，例如 1m/5m/15m/30m/60m/1d")
    parser.add_argument("--session", default="日盤", help="時段：日盤/夜盤/全盤")
    parser.add_argument("--days", type=int, default=365, help="回測天數")
    parser.add_argument("--strategy", default="strategy1", choices=["strategy1", "strategy2", "1", "2"], help="策略選擇：strategy1 或 strategy2")
    parser.add_argument("--out", default="backtest_outputs", help="輸出資料夾")
    parser.add_argument("--bars-before", type=int, default=20, help="進場前要截取的 K 棒數")
    parser.add_argument("--bars-after", type=int, default=20, help="進場後要截取的 K 棒數")

    args = parser.parse_args()

    taipei_tz = pytz.timezone("Asia/Taipei")
    now_str = datetime.now(taipei_tz).strftime("%Y%m%d_%H%M%S")
    strategy_key = "strategy2" if str(args.strategy).strip().lower() in ("2", "strategy2") else "strategy1"
    out_dir = os.path.join(args.out, f"{strategy_key}_{args.session}_{args.interval}_{now_str}")

    trade_df, trade_csv, image_dir, total_trades, win_rate, total_pnl = run_backtest(
        interval=args.interval,
        session=args.session,
        days=args.days,
        out_dir=out_dir,
        bars_before=args.bars_before,
        bars_after=args.bars_after,
        strategy=strategy_key,
    )

    print(f"策略：{strategy_key}")
    print(f"回測完成：共 {total_trades} 筆交易")
    print(f"總勝率：{win_rate:.2f}%")
    print(f"總損益(點)：{total_pnl:.2f}")
    print(f"交易明細：{trade_csv}")
    print(f"截圖資料夾：{image_dir}")


if __name__ == "__main__":
    main()

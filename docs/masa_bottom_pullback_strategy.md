# 麻紗老師「底部拉回 / 破底翻」策略規格

## 資料來源

- 影片：
  - `masa_video/1.底部拉回策略/round1.mp4`
  - `masa_video/1.底部拉回策略/round2.mp4`
- 逐字稿：
  - `masa_video/1.底部拉回策略/文字檔.txt`
- 截圖輔助：
  - `masa_video/_frames/keyframes_sheet.jpg`

本版文件已根據逐字稿修正。策略核心不只是一般「低檔拉回」，而是老師口中的「破底翻」：先跌破一個整理區底部，把散戶甩掉，再快速站回區間，之後等待回測支撐進場。

## 策略一句話

先用周 K 找出「破底後又站回整理區」的股票；下一週若沒有開低，等待價格回測區間上緣支撐並重新站回，再進場做多。若再次跌破底部或翻回失敗，必須停損。

## 週期與使用方式

老師影片中的原始做法偏向股票選股：

- 主要觀察週期：周 K。
- 掃描時間：每週五收盤後。
- 進場觀察：下一週開盤後，用較細週期或盤中價格觀察是否回測支撐後再拉上。

若要套到本回測工具：

- 股票版：用周 K 篩選，日 K 或盤中 K 做進場。
- 台指期版：可先用較大週期建立型態，例如 60m / 1d，再用 5m / 15m 做進場；不要直接把所有 5m 小破底都當成策略訊號。

## 型態核心：破底翻

### 1. 先有整理區間

策略開始前，價格前方要有一段盤整區間。

區間包含：

- 上緣壓力：前方明顯壓力區，常由上影線或多根 K 的高點壓住形成。
- 下緣支撐：前方整理區的支撐位置。

老師特別提醒：下緣支撐不要直接框到最低影線。若把底畫在最低點，後面就看不到「破底」這件事，策略也不成立。

比較適合的底部畫法：

- 用整理區多根 K 的共同支撐位置。
- 可用 K 棒實體低點、密集成交區低點、或多次測試的支撐。
- 允許最低影線刺破區間底，因為這正是「甩轎」與「破底」的來源。

### 2. 跌破區間底

價格先跌破前方整理區下緣。

這代表：

- 原支撐被打破。
- 散戶容易認為型態轉弱而停損或放棄。
- 老師稱這段為主力洗盤、甩轎。

回測定義：

```text
breakdown = Low < box_low - break_buffer
```

其中 `box_low` 不是最低影線，而是整理區支撐線。

### 3. 快速站回區間

跌破後，如果後續 K 棒能重新站回整理區，代表「破底翻」成立。

老師提到的重點：

- 若一根 K 跌破底，下一根長出差不多大小的紅 K，前一根很可能是假跌破。
- 站回不能太模糊，最好明顯收回整理區內。
- 影片例子中，周五收 K 時已明顯站回區間。

回測定義：

```text
reclaim = Close > box_low + reclaim_buffer
```

更嚴格可用：

```text
Close >= box_mid 或 Close >= box_low + (box_high - box_low) * reclaim_ratio
```

## 進場方式

### 進場前提

每週五收盤後，策略篩出符合「破底翻」的標的。

下一週開盤先做第一層確認：

- 若開低，且沒有做出重新突破區間的樣子，放棄交易。
- 若平開、小幅開高、或開高，才進入觀察。

老師原話重點是：只要不是開低，都可以考慮；但真正好的進場不是盲目開盤買，而是等回測支撐後重新拉回。

### 最佳進場點

影片中老師描述的最佳進場位置：

1. 下一週開盤後，價格先往下測一下。
2. 這個下測是在回撤「區間頂部/支撐位置」。
3. 回撤後有守住，再拉回開盤價附近或站回支撐上方。
4. 這時是較好的進場點。

可回測進場條件：

```text
setup_found_on_weekly_close = True
next_session_open >= setup_close - open_low_tolerance
intraday_low <= entry_support + support_touch_buffer
intraday_low >= stop_reference - allowed_probe
confirm_close >= max(entry_support, next_session_open) + confirm_buffer
```

其中：

- `entry_support`：破底翻後要守的支撐。可先用 `box_low` 或站回 K 的開盤價附近。
- `next_session_open`：下一週開盤價。
- `stop_reference`：停損參考，通常是整理區底或破底低點。

### 實作成單一 K 線策略的簡化版

若目前回測程式暫時不做「周 K 篩選 + 次週盤中進場」兩層資料，可以先做簡化版：

1. 在大週期 K 中偵測破底翻。
2. 下一根 K 若不是開低，進入待進場狀態。
3. 待進場狀態中，價格回測 `entry_support` 後重新站上 `entry_support` 或開盤價。
4. 用確認 K 收盤價進場。

## 停損方式

老師在逐字稿中明確強調：這個策略一定要停損，因為破底翻失敗可能變成 M 頭或假拉回真跌破。

### 停損 1：再次破底

如果進場後又跌破原本整理區底，必須先停損。

```text
exit_stop = Low < box_low - stop_buffer
```

這是最重要的停損。

### 停損 2：翻上去後又跌回去

若價格已經完成「翻回區間」並拉上去，之後又跌回翻回支撐下方，也要先停損。

```text
exit_stop = Close < entry_support - support_fail_buffer
```

老師也提到：如果停損後又重新站回，可以再把單子撿回來。對回測來說可設成可選參數：

```text
allow_reentry_after_reclaim = true / false
```

### 不進場條件

若下一週直接開低，且沒有重新突破或站回支撐：

```text
skip_trade = next_open < setup_close - open_low_tolerance
```

如果是手滑進場，也要以再次破底作為最低停損。

## 停利方式

影片這一段主要在教進場與停損，停利老師說後續再談。因此回測 v1 不應過度腦補停利，只先放幾個可測版本。

建議 v1 停利：

1. 固定報酬停利：
   - 股票版可測 `take_profit_pct = 8% ~ 15%`。
   - 台指期版可測固定點數。
2. 壓力區停利：
   - 到整理區上緣 `box_high` 或前高先出場。
3. 移動停利：
   - 突破前高後，用最近 N 根低點或 ATR trailing stop。
4. 時間停利：
   - 進場後 N 根 K 沒有續攻，出場。

## 可回測參數

```text
box_lookback_bars = 8 ~ 30
box_min_bars = 4 ~ 12
box_max_range_pct = 8% ~ 25%
box_low_method = body_low / swing_support / quantile_low
break_buffer = 0 ~ 2 ATR 或固定 tick
reclaim_buffer = 0 ~ 1 ATR 或固定 tick
reclaim_max_bars = 1 ~ 4
open_low_tolerance = 0% ~ 1%
entry_support = box_low / setup_open / setup_close / reclaim_body_low
support_touch_buffer = 0 ~ 1 ATR
confirm_buffer = 0 ~ 0.5 ATR
stop_reference = box_low / breakdown_low / entry_support
stop_buffer = 0 ~ 1 ATR
allow_reentry_after_reclaim = true / false
take_profit_pct = 8% ~ 15%
max_hold_bars = 5 ~ 20
```

## 回測狀態機

建議用狀態機實作，不要用一個巨大 if。

```text
STATE_IDLE:
    尋找整理區 box_high / box_low
    若跌破 box_low，進入 STATE_BREAKDOWN

STATE_BREAKDOWN:
    若在 reclaim_max_bars 內收回 box_low 上方，建立 setup
    否則 setup 失敗，回到 STATE_IDLE

STATE_WAIT_NEXT_OPEN:
    等下一根或下一交易週開盤
    若開低且沒有站回，放棄
    否則進入 STATE_WAIT_PULLBACK

STATE_WAIT_PULLBACK:
    等價格回測 entry_support
    若跌破 box_low 或 stop_reference，setup 失敗
    若回測後重新站上 entry_support / open price，進場

STATE_IN_POSITION:
    若再次破底，停損
    若跌回 entry_support 下方，停損
    若達停利或時間到，出場
```

## 進出場規則 v1

### 多單進場

```text
1. 前方存在整理區。
2. 當前或近期 K 棒跌破整理區底 box_low。
3. 後續 K 棒明顯收回 box_low 之上，形成破底翻。
4. 下一根/下一週開盤不低於 setup_close 太多。
5. 價格回測 entry_support。
6. 價格重新站上 entry_support 或 next_open。
7. 以確認 K 收盤價進場做多。
```

### 多單停損

```text
1. 再次跌破 box_low。
2. 或翻上去後又跌回 entry_support 下方。
3. 若開盤直接開低且無法站回，不進場；若已進場則依 stop_reference 停損。
```

### 多單停利

```text
v1 先用可測版本：
1. 固定百分比 / 固定點數停利。
2. 到 box_high 或前高停利。
3. 突破後用移動停利。
4. 持有超過 max_hold_bars 未續攻出場。
```

## 與現有程式整合建議

已新增策略 key：

```text
masa_bottom_pullback
```

已新增策略函式：

```text
calculate_masa_bottom_pullback_signals(...)
```

目前 v1 實作方式：

- APP 選到此策略時，自動切到 `5m`，並把顯示 K 棒數提高到至少 `1500`。
- 策略函式會用 5 分 K 自動彙整日 K。
- 日 K 找前方整理區與破底翻 setup。
- setup 隔天用 5 分 K 等回測支撐後重新站回才進場。
- 進場後用 5 分 K 判斷再次破底、跌回支撐、固定停利或持有時間到。

目前預設偏保守，訊號會比原 MA 策略少很多。這符合老師原策略的精神：它是型態篩選，不是高頻進出策略。

交易記錄建議新增欄位：

```text
box_start_ts
box_end_ts
box_high
box_low
breakdown_ts
breakdown_low
reclaim_ts
entry_support
setup_close
next_open
confirm_ts
confirm_reason
stop_reference
exit_reason
```

## 台指期版本注意事項

老師原策略是用股票周 K 選股，不是台指期短線策略。若要移植到台指期，建議：

- 先用 `60m` 或 `1d` 找破底翻結構。
- 再用 `5m` 或 `15m` 做進場確認。
- 不要直接在 1m/5m 找所有小箱型破底翻，訊號會太多也太雜。
- 一定要保留停損，因為假拉回真跌破在期貨上會更快更兇。

## 待確認問題

- 老師 App 實際篩選的整理區演算法是什麼。
- 「區間底」到底偏向實體低點、轉折低點、還是人工畫線。
- 站回區間是否要求收盤站回，或盤中站回即可。
- 下一週進場是用日 K、分 K，還是只看盤中價格。
- 停利是否另有老師指定規則。

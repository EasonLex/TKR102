# TDX 公車轉乘分析 — 專案現況摘要

> 用途：接續新對話時提供給 AI 的上下文。
> 完整的決策脈絡與踩坑紀錄見 `DEVLOG.md`（44 條工程原則）。
> 更新日期：2026-08-18

---

## 一、專題定義

**問題**：計算雙北公車的「轉乘建議等待時間」——
在某站位從 A 路線轉 B 路線，應該預留多少時間。

**資料來源**：交通部 TDX 平臺（付費方案 400 點/月）
**時程**：資料自 2026-08-04 起持續收集，專題預計 9 月底完成 8 成

---

## 二、環境與架構

### 機器

| 角色 | 機器 | 用途 |
|---|---|---|
| 開發 | MacBook Pro | 寫程式、notebook 探索 |
| 生產 | Mac mini | collector + daily_batch（launchd 常駐） |
| 資料庫 | GCP e2-small (asia-east1) | MongoDB 8.0 on Docker |

三台以 **Tailscale** 互連，MongoDB 只綁 Tailscale IP，不對公網開放。

### 資料流

```
TDX API
  ↓ collector.py（launchd 常駐，白天 10s/城市、深夜 30s/城市）
raw/{city}/{date}/*.json.gz          原始層，原封不動
  ↓ convert_to_parquet.py + transform.py（明確 SCHEMA，22 欄）
output/parquet/{city}/{date}.parquet  約 110 MB/日/城市
  ↓ extract_events.py（到站判定）
output/events/{city}/{date}.parquet   約 97 萬筆/日（台北）
  ↓ transfer_stats.py（轉乘配對 + 直方圖）
output/transfers/{city}/{date}.parquet 約 255 萬列/日（台北）
  ↓ build_marts.py（跨天累加 → p50/p90）
output/marts/{city}/marts.parquet     最終產品

MongoDB (GCP)
  ├─ route_stops   2,570 筆（sub_route_uid + direction + version_id）
  └─ stations     11,436 筆（含 2dsphere 索引、SCD Type 2）
```

### 排程

| 任務 | 觸發 | 內容 |
|---|---|---|
| collector | launchd KeepAlive | 持續收集 |
| daily_batch | launchd 每日 03:00 | 轉檔 → 體檢 → 事件 → 轉乘 → marts |
| backfill.py | 手動 | 補跑指定日期區間 |

### 專案結構

```
TrafficProject/
├── raw/, output/              資料與產出（.gitignore）
└── src/trafficproject/
    ├── paths.py               唯一路徑來源 + load_dotenv
    ├── logging_util.py        make_logger(name) 工廠
    ├── collector.py
    ├── transform.py           SCHEMA 定義
    ├── convert_to_parquet.py
    ├── inspect_day.py         體檢 + metrics
    ├── extra_event.py         到站事件抽取（檔名為 extra_event）
    ├── transfer_stats.py      轉乘配對 + 直方圖
    ├── build_marts.py         跨天聚合
    ├── daily_batch.py         每日排程入口
    └── backfill.py            歷史補跑
```

執行方式：`uv run python -m trafficproject.<module>`

---

## 三、關鍵演算法與參數

### 到站判定（extra_event.py）

```python
DIST_THRESHOLD_M = 100      # 距最近站牌門檻
EVENT_GAP_S = 120           # 同站序間隔超過此值視為不同事件
TRIP_SEQ_DROP = 5           # 站序下降超過此值視為新趟次
TRIP_GAP_MIN = 30           # 事件間隔超過此值視為新趟次
```

- 僅取 `duty_status == 1`（營運中）
- **已知準確率 89%**：站間距 <200m 處會重複計算、過站不停會漏抓
- 待改進：改用「每趟每站的距離局部最小值」，可免門檻

### 轉乘配對（transfer_stats.py）

```python
MAX_WAIT_SEC = 1200         # 20 分鐘上限
BIN_WIDTH_SEC = 30          # 直方圖 bin 寬
```

- A 需可下車（boarding ≤ 0）、B 需可上車（boarding ≥ 0）
- 排除相同 `sub_route_uid`
- **輸出直方圖而非分位數**——分位數不可跨天相加，直方圖可以
- 每日輸出：`station_id, route_a, route_b, bucket, wait_bin, count, n_total, n_over, city, data_date`

### 時段分類

```python
night            h < 6 or h >= 23
weekend          週六日（未再分尖離峰）
weekday_peak     7-9, 17-19
weekday_offpeak  其餘
```

### 分位數推估（build_marts.py）

從直方圖累積計數，取首次超過 `total × q` 的 bin 中點：`bin × 30 + 15`

**已驗證**：跨天直方圖相加後推估 vs 明細直接算，
p50 中位誤差 9 秒、p90 中位誤差 10.5 秒（n≥20 的配對）。

---

## 四、資料現況（2026-08-18）

| 項目 | 數值 |
|---|---|
| 收集天數 | 14 天（08-04 起） |
| 原始觀測 | 約 1,800 萬筆/日（雙北） |
| 到站事件 | 97 萬筆/日（台北） |
| marts 配對數 | 372,198（min_samples=20） |
| n_days 最大 | 14 |

### 資料可用性

| 日期 | 狀態 |
|---|---|
| 2026-08-03 | 練習資料，跨機器搬遷，不完整 |
| 2026-08-04 | 磁碟事故日，實測中斷輕微（最大 113 秒） |
| 2026-08-05 起 | 正式資料 |

### 已建立的基準線

| 指標 | 穩定值 |
|---|---|
| 抓取間隔中位數 | 10.0 秒（白天） |
| 去重比例 | 0.585 – 0.592 |
| 端到端延遲 P95 | 24 – 27 秒 |
| 快照數/日 | 8,420 – 8,437（白天 10 秒時） |

---

## 五、目前卡在哪 ← 新對話的起點

### 核心發現：整體等待時間普遍偏長

`wait_p90` 中位數約 1,005–1,035 秒（約 17 分鐘），且**提高 min_samples 到 100 也不改變**。

已排除的假設：
- ✗ 小樣本推高 p90（門檻 20→100，中位數幾乎不動）
- ✗ 同路線誤判為轉乘（實查 route_uid 皆不同）

**結論：這是真實分布。** 多數路線配對的班距本來就長。

### 由此產生的產品定位問題

「所有轉乘的平均等待 17 分鐘」對使用者沒有價值。
真正有用的是**點查詢**：「我在 X 站要從 A 轉 B，該抓多久？」

→ marts 應定位為**可查詢的資料集**，而非統計結論。

### 最新發現：接駁式路線

`wait_p90` 最短的前十名中，有三條是小巴／觀光路線
（小19、貓空左線、市民小巴18）。

特徵：`p50 = p90 = 15 秒`，但 `success_rate` 僅 0.2–0.3。

**推論**：這類路線刻意與幹線同步發車，錯過就要等很久。
不是資料錯誤，是真實的營運設計。

### 未解決的次要問題

- [ ] `wait_bin == 40` 邊界：`1200 // 30 = 40`，恰好 1200 秒的落在第 41 個 bin
- [ ] `weekend` 未再分尖離峰，且假日僅 4 天樣本
- [ ] 國定假日未處理（僅用 `weekday()` 判斷）
- [ ] 部分配對 `n_days` 只有 8 而非 14，原因未查
- [ ] 事件表無 `route_uid`，無法在配對時排除同路線的區間車

---

## 六、後續規劃

| 階段 | 內容 | 預估 |
|---|---|---|
| 進行中 | 轉乘統計的產品定位與呈現方式 | — |
| 待做 | 結果寫入 MySQL（供 dashboard 點查詢） | 2–3 天 |
| 待做 | Dashboard（使用者有 Flutter/React 背景） | 1 週 |
| 待做 | 報告與 DEVLOG 整理 | — |

**明確不做**（並在報告中說明評估理由）：
- Kafka（無即時需求，硬加會是扣分）
- MongoDB 作為原始層（欄式儲存明顯較優）

**合規要求**：應用服務中須揭露「資料介接交通部 TDX 平臺」並加上平臺標章。

---

## 七、協作方式

使用者明確表示：**希望自己寫程式碼，AI 提供方向與 review，不要直接給完整實作。**

具體做法：
- 卡住時給方向提示，不給程式碼
- 寫完之後幫忙 review
- 回答概念問題

例外：需要驗證方法可行性時（如直方圖推估分位數的誤差評估），
可由 AI 直接運算驗證。
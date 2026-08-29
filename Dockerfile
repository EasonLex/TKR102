# =====================================================================
# trafficproject 共用映像
#
# collector / archiver / 之後 Airflow 的批次任務都用這一份，
# 避免三個環境各自漂移。
#
# 建置（在專案根目錄）：
#     docker build -t trafficproject:latest .
# =====================================================================
FROM python:3.13-slim

# tzdata：容器預設 UTC。程式裡的時間都已經是明確的 TPE
# （current_mode 與 logging_util 都用 datetime.now(TPE)），
# 裝 tzdata 只是為了讓 `date`、log 檔 mtime 之類的旁證好讀。
# 不要靠 TZ 環境變數來讓程式邏輯正確——那正是 cron 跑在 UTC 那次的教訓。
RUN apt-get update \
 && apt-get install -y --no-install-recommends tzdata ca-certificates \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 依賴層單獨一層，改 src 不會重裝依賴
COPY pyproject.toml uv.lock* README* ./

# 關鍵：用 editable 安裝。
#   paths.py 是 PROJECT_ROOT = Path(__file__).resolve().parents[2]，
#   非 editable 安裝會讓 __file__ 落在 site-packages，
#   PROJECT_ROOT 變成 /usr/local/lib/python3.13，
#   於是 .env 找不到、output/logs 生在 Python 安裝目錄裡——
#   而且不會報錯，只會安靜地寫到錯的地方。
COPY src ./src
RUN pip install --no-cache-dir -e .

# 執行期才會用到的目錄，先建好並交給非 root
RUN mkdir -p /app/output/logs /app/raw \
 && useradd -m -u 1000 app \
 && chown -R app:app /app
USER app

# 預設跑 collector，compose 裡可以覆寫成 archiver
CMD ["python", "-m", "trafficproject.collector"]

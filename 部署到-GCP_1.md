# Kafka 部署到 GCP

> v2 — 本機驗證已完成，數字皆為實測。
>
> **本機階段結論（2026-08-19，Mac mini）**
>
> | | 磁碟 | 每則位元組 |
> |---|---|---|
> | batch 1 MB + linger 50 | 92 MB / 1,308,556 則 | **73.7 B** |
> | batch 16 KB + linger 0 | 248 MB / 1,308,556 則 | 198.7 B |
> | 調校效益 | | **−63%** |
>
> 每日成長 1.3–2.0 GB · 7 天 9–14 GB · 14 天 19–28 GB

---

## 一、建立 VM

```bash
PROJECT=你的專案
ZONE=asia-east1-b          # 與現有 MongoDB 同區，省跨區流量費

gcloud compute instances create kafka-1 \
  --project=$PROJECT --zone=$ZONE \
  --machine-type=e2-medium \
  --image-family=ubuntu-2404-lts-amd64 \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=20GB --boot-disk-type=pd-balanced \
  --create-disk=name=kafka-data,size=50GB,type=pd-balanced,auto-delete=no \
  --metadata=enable-oslogin=TRUE
```

**資料另掛一顆磁碟**：可獨立擴容與快照，重建 VM 時資料不會消失。
pd-balanced 支援**線上擴容**，之後不夠再加即可，不必一開始就買大。

50 GB 的依據：14 天 retention 最多 28 GB，加上 `retention.bytes` 的
30 GB 硬上限（12 × 2.5 GB），佔磁碟 60%，離 70% 的告警線還有餘裕。

---

## 二、防火牆：什麼都不開

```bash
# 確認沒有規則對外開放 9092/9094
gcloud compute firewall-rules list --filter="allowed[].ports:(9092 OR 9094)"

# SSH 限縮到 IAP 範圍
gcloud compute firewall-rules create allow-ssh-iap \
  --network=default --allow=tcp:22 --source-ranges=35.235.240.0/20

gcloud compute firewall-rules delete default-allow-ssh   # 確認沒有其他機器依賴後再刪
```

Kafka 完全不對公網開放，存取一律走 Tailscale，與 MongoDB 一致。

---

## 三、開機設定

```bash
gcloud compute ssh kafka-1 --zone=$ZONE --tunnel-through-iap
```

```bash
# --- 掛載資料磁碟 ---
DISK=/dev/disk/by-id/google-kafka-data
sudo mkfs.ext4 -F -E lazy_itable_init=0,lazy_journal_init=0,discard $DISK
sudo mkdir -p /mnt/kafka
sudo mount -o discard,defaults $DISK /mnt/kafka
echo "$DISK /mnt/kafka ext4 discard,defaults,nofail 0 2" | sudo tee -a /etc/fstab
sudo chown -R 1000:1000 /mnt/kafka     # apache/kafka 映像以 uid 1000 執行

# --- Docker ---
curl -fsSL https://get.docker.com | sudo sh
sudo usermod -aG docker $USER && newgrp docker

# --- Tailscale ---
curl -fsSL https://tailscale.com/install.sh | sh
sudo tailscale up --ssh
tailscale ip -4                        # 記下這個 IP
```

> `chown 1000:1000` 漏掉的話 Kafka 會因無法寫入 log dir 而啟動失敗，
> 錯誤訊息不明顯。這是最常見的卡關點。

---

## 四、部署

```bash
sudo mkdir -p /opt/kafka && sudo chown $USER /opt/kafka
# 放入 docker-compose.yml 與 .env
```

`.env` 的三個值：

```
CLUSTER_ID=<在 GCE 上重新產生，不要沿用本機的>
KAFKA_EXTERNAL_HOST=<上一步的 Tailscale IP>
KAFKA_DATA_DIR=/mnt/kafka
```

```bash
docker run --rm apache/kafka:4.0.0 /opt/kafka/bin/kafka-storage.sh random-uuid
cd /opt/kafka && docker compose up -d
docker logs kafka | grep -i advertised    # 確認廣播的是 Tailscale IP
```

### 建立 topic（retention 14 天）

```bash
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --create \
  --topic bus.position.raw --partitions 12 --replication-factor 1 \
  --config retention.ms=1209600000 \
  --config retention.bytes=2500000000 \
  --config compression.type=zstd
```

> **為什麼是 14 天而不是 7 天：** 你選 Kafka 的主因是重放能力。
> 事件偵測邏輯改版後要重跑歷史，7 天的窗口太緊。
> 實測 1.3–2.0 GB/日，14 天 = 19–28 GB，仍在 30 GB 的位元組上限內。

compose 裡的 broker 級 `KAFKA_LOG_RETENTION_HOURS` 也要一併改成 `336`。

---

## 五、開機自動啟動

```bash
sudo tee /etc/systemd/system/kafka-stack.service <<'EOF'
[Unit]
Description=Kafka stack (docker compose)
Requires=docker.service
After=docker.service network-online.target
[Service]
Type=oneshot
RemainAfterExit=yes
WorkingDirectory=/opt/kafka
ExecStart=/usr/bin/docker compose up -d
ExecStop=/usr/bin/docker compose down
TimeoutStartSec=0
[Install]
WantedBy=multi-user.target
EOF

sudo systemctl daemon-reload && sudo systemctl enable --now kafka-stack
```

---

## 六、切換（cutover）

**風險很低，因為 collector 仍在寫 raw 檔。** Kafka 連不上時只會累積
發送失敗，原始資料一筆都不會少——這是保留雙寫的回報。

1. GCE Kafka 起來、topic 建好、`advertised` 確認正確
2. 從 **Mac mini** 測連線（這一步不能跳）：

   ```bash
   python3 -c "
   from confluent_kafka.admin import AdminClient
   a = AdminClient({'bootstrap.servers': '<GCE_Tailscale_IP>:9094'})
   print(a.list_topics(timeout=10).topics.keys())
   "
   ```

3. 改 collector 的 `.env`：`KAFKA_BOOTSTRAP=<GCE_Tailscale_IP>:9094`
4. 重啟 collector（launchd），確認 log 的 `kafka ok=` 重新開始成長
5. 在 GCE 上確認訊息進來：

   ```bash
   docker compose exec -T kafka /opt/kafka/bin/kafka-get-offsets.sh \
     --bootstrap-server localhost:9092 --topic bus.position.raw \
     | awk -F: '{s+=$3} END {print "messages:", s}'
   ```

6. 觀察一天，確認磁碟成長率符合 1.3–2.0 GB/日
7. 確認無誤後才停掉 Mac mini 上的 Kafka：

   ```bash
   docker compose down        # 在 Mac mini 的 compose 目錄
   ```

> 第 6 步不要跳。跨機器之後延遲與失敗率都會變，
> 觀察一天才知道 Tailscale 這條路穩不穩。

---

## 七、磁碟告警（量測清單 P0）

```bash
sudo tee /opt/kafka/disk-alert.sh <<'EOF'
#!/bin/bash
USED=$(df --output=pcent /mnt/kafka | tail -1 | tr -dc '0-9')
if [ "$USED" -gt 70 ]; then
  curl -s -X POST "$WEBHOOK_URL" \
    -H 'Content-Type: application/json' \
    -d "{\"text\":\"⚠️ Kafka 磁碟 ${USED}%，接近 retention 上限\"}"
fi
EOF
sudo chmod +x /opt/kafka/disk-alert.sh
# crontab: */10 * * * * WEBHOOK_URL=... /opt/kafka/disk-alert.sh
```

> 磁碟塞爆 → broker 掛 → producer 阻塞 → 輪詢中斷 → **資料永久遺失**。
> 唯一一條不可逆的失敗鏈，優先於其他所有監控。

---

## 八、存取 kafka-ui

只綁 localhost，透過 IAP 轉發：

```bash
gcloud compute ssh kafka-1 --zone=$ZONE --tunnel-through-iap -- -L 8080:localhost:8080
# 瀏覽器開 http://localhost:8080
```

---

## 常見卡關

| 症狀 | 原因 |
|---|---|
| 遠端連得上但一送訊息就 timeout | `advertised.listeners` 廣播了容器內部位址 |
| 容器起不來，log 說權限不足 | `/mnt/kafka` 沒有 `chown 1000:1000` |
| 重開機後資料不見 | volume 掛到容器內路徑而非 `/mnt/kafka` |
| 磁碟爆掉但 retention 看起來沒滿 | `retention.bytes` 是**每 partition**，12 個要乘 12 |
| `ClassNotFoundException: kafka.tools.*` | Kafka 4.x 搬到 `org.apache.kafka.tools.*`，或改用 `kafka-get-offsets.sh` |
| 換了 CLUSTER_ID 後讀不到舊資料 | 那等於換了一個新叢集，不可逆 |

---

## 成本（asia-east1，自行以計價器確認）

| 項目 | 約略 |
|---|---|
| e2-medium 持續執行 | 每月 USD 25–30 |
| pd-balanced 50 GB | 每月 USD 5–6 |
| 開機碟 20 GB | 每月 USD 2–3 |

同區內部流量免費——這是 zone 設成與 MongoDB 相同的主要理由。

跑一週後看實際 CPU 使用率再決定要不要降規。e2-small（2 GB）可行但
heap 要調成 `-Xmx768m`，page cache 會明顯不足。**先用 e2-medium。**

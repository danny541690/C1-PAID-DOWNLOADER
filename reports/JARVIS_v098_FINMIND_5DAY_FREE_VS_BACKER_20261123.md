# JARVIS v0.98 FinMind Free vs Backer 五交易日對照驗收
日期：2026-11-23（台灣時間）
## 實際Run
- Free: https://github.com/danny541690/C1-RAW-DOWNLOADER/actions/runs/45174158442
- Backer: https://github.com/danny541690/C1-PAID-DOWNLOADER/actions/runs/45174183730
- 五日：2024-01-02/03/04/05/08，日資料 2330 同參數、Backer 全市場按日另測。
- 每側 51 次請求，Free PASS 30 / HTTP400 21，Backer PASS 47 / EMPTY 4。
- 雙方共通 PASS 30組：欄位、筆數與排序後原始列 SHA256 皆一致。
- 付費單次全市場股價各日資料列：44272、44533、44649、44635、44803；不可當作有效上市櫃股票數量，仍須證券身分過濾。
- Backer TaiwanStockHoldingSharesPer 五日中僅 2024-01-05有17列，另外四日EMPTY（不要視為失敗，持股分級並非每日發表）。
- Period類：公司清單一次、月營收/三張財報按2024年查詢、景氣指標按季查詢，不能視為每日可用。
## 官方 vs 測試範圍
- 官方FinMind定價表 https://finmind.github.io/Pricing/：Free 45種、Backer合計81種，Backer新增36種；Free匿名300 req/h，已註冊免費600 req/h，Backer1600 req/h。
- 此次只驗證15種指定查詢類型，並非已把官方45/81種逐項全盤查核。Free 30/51、Backer47/51是「測試請求通過率」不是「資料集取得率」。
- 券商分點及主力類細緻資料屬更高級Sponsor相關範圍；FinMind 699不能直接取代XQ人工主力、買賣與分點資料。依個別dataset權限再核。
## Gold 443評估證據等級
- 既有GitHub程式 `scripts/v095_202609_443_catalog.py`、`scripts/v095_443_bidirectional_lineage.py`有443候選目錄契約，但後者明寫完整公式、PIT、確切原始欄位/血緣未驗證。
- Google Drive catalog file ID `1lbWk0qRZZFjpV-yL1gZNUb2pCZmHDHW7` 在本次連線讀取回應404，因此無法就443項逐筆驗收、不可宣稱443項已完整具備或確定缺幾項。
- 確認需要額外判斷的來源群組：券商分點及主力、歷史類股成分、自由流通股與股本異動、TDCC持股週資料、月營收/財報實際公告日、Stage相對強弱需大盤及類股時點資料。5日樣本不足計算50/200MA與多年策略。
## 建議
- 一次性Backer優先歷史Backfill／付費獨有資料，仍維持獨立原始來源及血緣。
- 日常優先六大官方來源＋Free查漏補缺＋XQ人工分點／主力，依缺口才間歇續購Backer；不要把699誤當取代分點與XQ。
- 未取得443完整目錄與資料欄位公式前，Gold覆蓋率請標記UNVERIFIED。
- 此兩個工作流只做唯讀測試並保存Actions artifacts，**未實際寫Google Drive也未啟用每日定期排程**。

# JARVIS Backer 測試歸檔

- 每輪 FinMind Backer 測試完成後，將測試結果、Run／Job／Attempt、日誌、附件及原件索引保存到私人 Google Drive `C1_0.98/00_COMMAND/R11_BACKER/TEST_RUNS/`。失敗及中止的執行同樣要留存履歷。
- 使用 Run ID 和 Attempt 管理歸檔，並回讀確認檔案完整。既有測試原件及雙 SHA 證據留在私人 `99_RECOVERY`，可從履歷索引查找。
- GitHub 公開附件與日誌限去識別狀態、程式和授權公開的內容。Backer 付費原件及私密紀錄寫入私人 Drive。
- 將測試結果、Drive 存檔結果和正式全量放行結論各自記錄。所有測試歸檔使用既有 GitHub 標準 Runner 和 Drive 授權。
- 在 `r11-backer-bootstrap.yml` 新增測試工作時，把該工作加入 `archive-test-run` 的 `needs`，讓報告與日誌在工作完成後可一併保存。

## 方案 B 與 R12 交接

- 依 `D-006` 與 `JARVIS-GOV-HANDOVER-B-001 V1.0`。先讀 `C1_0.98/00_COMMAND/AI_SHARED_CONTEXT/` 的全局 SOP、`C1_0.98/00_COMMAND/R12_BACKER/00_SHARED_CONTEXT/START_HERE.md` 及當次核准規格。以原始位元組的 SHA-256 核對版本與 `task_id`，並以最新 GitHub Run 和可讀的 Drive 帳本核對現況。
- 使用者一次交接後，Codex 實作、測試及提交，GitHub 依核准範圍執行，Google Drive 留存私人證據。D 線與 H 線獨立進行。H 線不用 FinMind，Codex 直接 ACK 與 GitHub bridge 分開記錄，ChatGPT 回讀由實際讀者另行確認。
- 用 `task_id`、規格 SHA、Run、Job、Attempt 追蹤成果。交接採單一寫入者、固定事件鍵及 append-only，重送同一事件時核對原件，使用 `VERBATIM`、`SUMMARY`、`EVIDENCE` 區分來源。
- R12 最終前置證據寫入 `R12_BACKER/00_SHARED_CONTEXT/FINAL_READINESS/`，現有 `R11_BACKER/TEST_RUNS/` 和私人 `99_RECOVERY` 依賴照常讀取。前置驗收只用隔離帳本及測試區，正式程式與現役資料僅依本次授權操作。前置報告的 `full_download_release=HOLD` 與 `production_dispatchable=0` 保持固定，正式全量下載由使用者另行核准。

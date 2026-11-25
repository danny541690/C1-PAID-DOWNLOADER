# JARVIS Backer 測試歸檔

- 每輪 FinMind Backer 測試完成後，將測試結果、Run／Job／Attempt、日誌、附件及原件索引保存到私人 Google Drive `C1_0.98/00_COMMAND/R11_BACKER/TEST_RUNS/`。失敗及中止的執行同樣要留存履歷。
- 使用 Run ID 和 Attempt 管理歸檔，並回讀確認檔案完整。既有測試原件及雙 SHA 證據留在私人 `99_RECOVERY`，可從履歷索引查找。
- GitHub 公開附件與日誌限去識別狀態、程式和授權公開的內容。Backer 付費原件及私密紀錄寫入私人 Drive。
- 將測試結果、Drive 存檔結果和正式全量放行結論各自記錄。所有測試歸檔使用既有 GitHub 標準 Runner 和 Drive 授權。
- 在 `r11-backer-bootstrap.yml` 新增測試工作時，把該工作加入 `archive-test-run` 的 `needs`，讓報告與日誌在工作完成後可一併保存。
